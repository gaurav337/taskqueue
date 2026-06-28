package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/gaurav337/taskqueue/internal/job"
	"github.com/gaurav337/taskqueue/internal/queue"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	store := job.NewStore(rdb)
	q := queue.New(rdb)

	ctx := context.Background()
	consumerName := "worker-" + strconv.Itoa(os.Getpid())

	if err := q.EnsureGroup(ctx); err != nil {
		slog.Error("failed to create consumer groups", "error", err)
		os.Exit(1)
	}

	slog.Info("worker started", "name", consumerName)

	for {
		msgs, err := q.Read(ctx, consumerName, 2*time.Second)
		if err != nil {
			slog.Error("read failed", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, msg := range msgs {
			jobID, _ := msg.Values["job_id"].(string)
			streamKey, _ := msg.Values["_stream"].(string)

			slog.Info("processing job", "job_id", jobID, "stream", streamKey)
			time.Sleep(50 * time.Millisecond)

			if err := store.UpdateStatus(ctx, jobID, job.StatusDone, ""); err != nil {
				slog.Error("failed to update status", "job_id", jobID, "error", err)
			}
			if err := q.Ack(ctx, streamKey, msg.ID); err != nil {
				slog.Error("ack failed", "msg_id", msg.ID, "error", err)
			}
		}
	}
}
