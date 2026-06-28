package main

import (
	"context"
	"log/slog"
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
				time.Sleep(50 * time.Millisecond)

				err := store.UpdateStatus(context.Background(), jobID, job.StatusDone, "")
				if err != nil {
					slog.Error("failed to update status", "job_id", jobID, "error", err)
				}
				err = q.Ack(context.Background(), streamKey, msg.ID)
				if err != nil {
					slog.Error("failed to ack", "job_id", jobID, "error", err)
				}
			}
		}(i)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				break
			default:
				msgs, err := q.Read(ctx, consumerName, 2*time.Second)
				if err != nil {
					if ctx.Err() != nil {
						break
					}
					continue
				}
				for _, msg := range msgs {
					select {
					case jobs <- msg:
					case <-ctx.Done():
						break
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
