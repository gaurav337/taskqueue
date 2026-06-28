package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gaurav337/taskqueue/internal/job"
	"github.com/gaurav337/taskqueue/internal/queue"
	"github.com/gaurav337/taskqueue/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
)

var (
	jobsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "taskqueue_jobs_processed_total",
			Help: "Total number of processed jobs",
		},
		[]string{"type", "status"},
	)
	jobLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "taskqueue_job_latency_seconds",
			Help:    "Execution latency of jobs",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)
	activeWorkers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "taskqueue_active_workers",
			Help: "Current number of active worker goroutines",
		},
	)
	reclaimedJobs = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "taskqueue_reclaimed_jobs_total",
			Help: "Total number of reclaimed stranded jobs",
		},
	)
	tracer = otel.Tracer("taskqueue-worker")
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
			activeWorkers.Inc()
			defer activeWorkers.Dec()
			for msg := range jobs {
				func(msg redis.XMessage) {
					jobID, _ := msg.Values["job_id"].(string)
					streamKey, _ := msg.Values["_stream"].(string)

					// Extract trace context propagated over Redis Stream headers
					carrier := propagation.MapCarrier{}
					for k, v := range msg.Values {
						if strVal, ok := v.(string); ok {
							carrier[k] = strVal
						}
					}
					extractedCtx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)

					ctx, span := tracer.Start(extractedCtx, "process_job")
					defer span.End()

					slog.Info("processing job", "worker_id", workerID, "job_id", jobID)
					span.SetAttributes(
						attribute.String("job.id", jobID),
						attribute.String("worker.id", strconv.Itoa(workerID)),
					)

					start := time.Now()

					// 1. Fetch job metadata from store
					j, err := store.Get(ctx, jobID)
					if err != nil {
						slog.Error("failed to retrieve job metadata", "job_id", jobID, "error", err)
						q.Ack(context.Background(), streamKey, msg.ID)
						span.RecordError(err)
						return
					}

					span.SetAttributes(
						attribute.String("job.type", j.Type),
						attribute.Int("job.attempts", j.Attempts),
					)

					// Avoid double-processing already completed or failed jobs
					if j.Status == job.StatusDone || j.Status == job.StatusFailed {
						slog.Info("job already in terminal state, skipping processing and acking", "job_id", jobID, "status", j.Status)
						if err := q.Ack(context.Background(), streamKey, msg.ID); err != nil {
							slog.Error("failed to ack terminal state job", "job_id", jobID, "error", err)
						}
						return
					}

					// 2. Increment attempt count
					j.Attempts++
					j.UpdatedAt = time.Now()

					// 3. Execute/Process job (simulate failure if job type is "fail" or payload contains "fail": true)
					var processErr error
					if j.Type == "fail" || (j.Payload != nil && j.Payload["fail"] == true) {
						processErr = fmt.Errorf("simulated job failure")
					}

					duration := time.Since(start).Seconds()
					jobLatency.WithLabelValues(j.Type).Observe(duration)

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
						jobsProcessed.WithLabelValues(j.Type, "success").Inc()
					} else {
						// Job failed!
						slog.Warn("job processing failed", "job_id", jobID, "attempt", j.Attempts, "max_attempts", j.MaxAttempts, "error", processErr)
						j.Error = processErr.Error()
						span.RecordError(processErr)

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
							jobsProcessed.WithLabelValues(j.Type, "retry").Inc()
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
							jobsProcessed.WithLabelValues(j.Type, "failed").Inc()
						}
					}
				}(msg)
			}
		}(i)
	}

	// Start background reclaimer to monitor and claim orphaned PEL jobs
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				msgs, err := q.Reclaim(ctx, consumerName)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					slog.Error("failed to reclaim tasks", "error", err)
					continue
				}
				for _, msg := range msgs {
					slog.Info("reclaimer claimed orphaned task", "msg_id", msg.ID, "stream", msg.Values["_stream"])
					reclaimedJobs.Inc()
					select {
					case jobs <- msg:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()


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

	sentinels := os.Getenv("REDIS_SENTINEL_ADDRS")
	masterName := os.Getenv("REDIS_MASTER_NAME")
	var rdb *redis.Client
	if sentinels != "" && masterName != "" {
		sAddrs := strings.Split(sentinels, ",")
		rdb = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    masterName,
			SentinelAddrs: sAddrs,
		})
		slog.Info("connected to Redis Sentinel cluster", "master", masterName, "sentinels", sAddrs)
	} else {
		addr := os.Getenv("REDIS_ADDR")
		if addr == "" {
			addr = "localhost:6379"
		}
		rdb = redis.NewClient(&redis.Options{Addr: addr})
		slog.Info("connected to standalone Redis instance", "addr", addr)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otlpEndpoint == "" {
		otlpEndpoint = "http://localhost:4318"
	}
	shutdownTracer, err := telemetry.InitTracer(context.Background(), "taskqueue-worker", otlpEndpoint)
	if err != nil {
		slog.Error("failed to initialize tracer", "error", err)
	} else {
		defer shutdownTracer()
	}

	// Start background Prometheus metrics server on :9090
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		slog.Info("starting worker metrics server on :9090")
		if err := http.ListenAndServe(":9090", mux); err != nil {
			slog.Error("worker metrics server failed", "error", err)
		}
	}()

	if err := Run(ctx, rdb, 3); err != nil {
		slog.Error("worker error", "error", err)
		os.Exit(1)
	}
}
