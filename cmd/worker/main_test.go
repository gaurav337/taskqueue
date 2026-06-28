package main

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/gaurav337/taskqueue/internal/job"
	"github.com/gaurav337/taskqueue/internal/queue"
)

func TestWorkerGracefulShutdown(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}

	// Cleanup test keys
	t.Cleanup(func() {
		rdb.Del(context.Background(), queue.StreamCritical, queue.StreamDefault, queue.StreamLow)
	})

	q := queue.New(rdb)
	store := job.NewStore(rdb)

	// Publish a job to process
	jobID := "shutdown-test-job-1"
	j := &job.Job{
		ID:          jobID,
		Type:        "email",
		Priority:    "default",
		Payload:     map[string]any{"to": "user@example.com"},
		Status:      job.StatusPending,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if err := store.Save(ctx, j); err != nil {
		t.Fatalf("failed to save job: %v", err)
	}
	defer rdb.Del(context.Background(), "job:"+jobID)

	if err := q.Publish(ctx, j.ID, j.Type, j.Priority, j.Payload); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Start Run in background
	errChan := make(chan error, 1)
	go func() {
		errChan <- Run(ctx, rdb, 1)
	}()

	// Wait briefly for job to start processing, then cancel the context to trigger shutdown
	time.Sleep(10 * time.Millisecond)
	cancel()

	// Wait for Run to exit
	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("expected no error from Run, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not shut down within timeout")
	}

	// Verify that the job was processed and status updated
	processedJob, err := store.Get(context.Background(), jobID)
	if err != nil {
		t.Fatalf("failed to get processed job: %v", err)
	}

	if processedJob.Status != job.StatusDone {
		t.Errorf("expected job status 'done' after graceful shutdown, got: %s (Check if context cancellation aborted database update)", processedJob.Status)
	}
}
