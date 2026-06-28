package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gaurav337/taskqueue/internal/job"
	"github.com/gaurav337/taskqueue/internal/queue"
	"github.com/redis/go-redis/v9"
)

func Run(ctx context.Context, rdb *redis.Client, poolSize int) error {
	store := job.NewStore(rdb)
	q := queue.New(rdb)

	consumerName := "worker-" + strconv.Itoa(os.Getpid())
	if err := q.EnsureGroup(ctx); err != nil {
		return err
	}

	jobs := make(chan redis.XMessage, 10)
	var wg sync.WaitGroup

	for i := 1; i <= poolSize; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for msg := range jobs {
				jobID, _ := msg.Values["job_id"].(string)
				streamKey, _ := msg.Values["_stream"].(string)

				slog.Info("processing job", "worker_id", workerID, "job_id", jobID)

				// 1. Fetch job metadata from store
				j, err := store.Get(context.Background(), jobID)
				if err != nil {
					slog.Error("failed to retrieve job metadata", "job_id", jobID, "error", err)
					q.Ack(context.Background(), streamKey, msg.ID)
					continue
				}

				// 2. Increment attempt count
				j.Attempts++
				j.UpdatedAt = time.Now()

				// 3. Execute/Process job (simulate failure if job type is "fail" or payload contains "fail": true)
				var processErr error
				if j.Type == "fail" || (j.Payload != nil && j.Payload["fail"] == true) {
					processErr = fmt.Errorf("simulated job failure")
				}

				if processErr == nil {
					// Job succeeded!
					j.Status = job.StatusDone
					j.Error = ""
					if err := store.Save(context.Background(), j); err != nil {
						slog.Error("failed to update job success status", "job_id", jobID, "error", err)
					}
					if err := q.Ack(context.Background(), streamKey, msg.ID); err != nil {
						slog.Error("failed to ack success", "msg_id", msg.ID, "error", err)
					}
				} else {
					// Job failed!
					slog.Warn("job processing failed", "job_id", jobID, "attempt", j.Attempts, "max_attempts", j.MaxAttempts, "error", processErr)
					j.Error = processErr.Error()

					if j.Attempts < j.MaxAttempts {
						// Retry: calculate exponential backoff with jitter
						j.Status = job.StatusPending

						// Backoff calculation: base = 100ms, max = 5s
						base := 100 * time.Millisecond
						maxBackoff := 5 * time.Second
						temp := float64(base) * math.Pow(2, float64(j.Attempts))
						backoff := time.Duration(temp)
						if backoff > maxBackoff {
							backoff = maxBackoff
						}
						// Add up to 100ms jitter
						jitterRange := int64(100 * time.Millisecond)
						n, randErr := rand.Int(rand.Reader, big.NewInt(jitterRange))
						if randErr == nil {
							backoff += time.Duration(n.Int64())
						}

						j.RunAfter = &time.Time{}
						*j.RunAfter = time.Now().Add(backoff)

						if err := store.Save(context.Background(), j); err != nil {
							slog.Error("failed to save retry metadata", "job_id", jobID, "error", err)
						}

						if err := q.Schedule(context.Background(), j.ID, *j.RunAfter); err != nil {
							slog.Error("failed to reschedule job for retry", "job_id", jobID, "error", err)
						}

						if err := q.Ack(context.Background(), streamKey, msg.ID); err != nil {
							slog.Error("failed to ack retried job", "job_id", jobID, "error", err)
						}
					} else {
						// Terminal Failure: Route to Dead Letter Queue (DLQ)
						j.Status = job.StatusFailed
						if err := store.Save(context.Background(), j); err != nil {
							slog.Error("failed to save terminal job metadata", "job_id", jobID, "error", err)
						}

						if err := q.PublishDLQ(context.Background(), j); err != nil {
							slog.Error("failed to publish to DLQ", "job_id", jobID, "error", err)
						}

						if err := q.Ack(context.Background(), streamKey, msg.ID); err != nil {
							slog.Error("failed to ack terminal job", "job_id", jobID, "error", err)
						}
					}
				}
			}
		}(i)
	}

	go func() {
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			default:
				msgs, err := q.Read(ctx, consumerName, 2*time.Second)
				if err != nil {
					if ctx.Err() != nil {
						break loop
					}
					continue
				}
				for _, msg := range msgs {
					select {
					case jobs <- msg:
					case <-ctx.Done():
						break loop
					}
				}
			}
		}
		close(jobs)
	}()

	<-ctx.Done()
	slog.Info("shutdown initiated, waiting for worker goroutines...")
	wg.Wait()
	slog.Info("shutdown complete")
	return nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := Run(ctx, rdb, 3); err != nil {
		slog.Error("worker error", "error", err)
		os.Exit(1)
	}
}
