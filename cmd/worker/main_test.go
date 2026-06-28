package main

import (
	"context"
	"encoding/json"
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

func TestWorkerRetryLifecycle(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cleanup test keys
	t.Cleanup(func() {
		rdb.Del(context.Background(), queue.StreamCritical, queue.StreamDefault, queue.StreamLow, queue.StreamScheduled)
	})

	q := queue.New(rdb)
	store := job.NewStore(rdb)

	jobID := "retry-test-job-1"
	j := &job.Job{
		ID:          jobID,
		Type:        "fail", // Triggers simulated failure
		Priority:    "default",
		Payload:     map[string]any{"fail": true},
		Status:      job.StatusPending,
		Attempts:    0,
		MaxAttempts: 3,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if err := store.Save(ctx, j); err != nil {
		t.Fatalf("failed to save: %v", err)
	}
	defer rdb.Del(context.Background(), "job:"+jobID)

	if err := q.Publish(ctx, j.ID, j.Type, j.Priority, j.Payload); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- Run(ctx, rdb, 1)
	}()

	time.Sleep(100 * time.Millisecond) // Let worker process and reschedule
	cancel()
	<-errChan

	// Verify job was rescheduled (ZAdd to StreamScheduled)
	due, err := q.DueJobs(context.Background())
	if err != nil {
		t.Fatalf("failed to read due: %v", err)
	}

	if len(due) != 1 || due[0] != jobID {
		t.Errorf("expected job %s to be scheduled, got %v", jobID, due)
	}

	// Verify metadata update in store
	retryJob, err := store.Get(context.Background(), jobID)
	if err != nil {
		t.Fatalf("failed to get job state: %v", err)
	}

	if retryJob.Attempts != 1 {
		t.Errorf("expected attempts 1, got %d", retryJob.Attempts)
	}
	if retryJob.Status != job.StatusPending {
		t.Errorf("expected status 'pending' (rescheduled), got %s", retryJob.Status)
	}
}

func TestWorkerDLQ(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cleanup test keys
	t.Cleanup(func() {
		rdb.Del(context.Background(), queue.StreamCritical, queue.StreamDefault, queue.StreamLow, queue.StreamDLQ)
	})

	q := queue.New(rdb)
	store := job.NewStore(rdb)

	jobID := "dlq-test-job-1"
	j := &job.Job{
		ID:          jobID,
		Type:        "fail",
		Priority:    "default",
		Payload:     map[string]any{"fail": true},
		Status:      job.StatusPending,
		Attempts:    2, // Max is 3, so this is attempt 3 (terminal failure)
		MaxAttempts: 3,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if err := store.Save(ctx, j); err != nil {
		t.Fatalf("failed to save: %v", err)
	}
	defer rdb.Del(context.Background(), "job:"+jobID)

	if err := q.Publish(ctx, j.ID, j.Type, j.Priority, j.Payload); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- Run(ctx, rdb, 1)
	}()

	time.Sleep(100 * time.Millisecond) // Let worker process and fail terminally
	cancel()
	<-errChan

	// Verify job status is failed
	failedJob, err := store.Get(context.Background(), jobID)
	if err != nil {
		t.Fatalf("failed to get job state: %v", err)
	}

	if failedJob.Status != job.StatusFailed {
		t.Errorf("expected status 'failed', got %s", failedJob.Status)
	}
	if failedJob.Attempts != 3 {
		t.Errorf("expected attempts 3, got %d", failedJob.Attempts)
	}

	// Verify job is present in DLQ stream
	streams, err := rdb.XRead(context.Background(), &redis.XReadArgs{
		Streams: []string{queue.StreamDLQ, "0-0"},
		Count:   1,
	}).Result()
	if err != nil {
		t.Fatalf("failed to read from DLQ stream: %v", err)
	}

	if len(streams) != 1 || len(streams[0].Messages) != 1 {
		t.Fatalf("expected 1 DLQ message, got %v", streams)
	}

	dlqDataStr, _ := streams[0].Messages[0].Values["data"].(string)
	var dlqJob job.Job
	if err := json.Unmarshal([]byte(dlqDataStr), &dlqJob); err != nil {
		t.Fatalf("failed to unmarshal DLQ job: %v", err)
	}

	if dlqJob.ID != jobID {
		t.Errorf("expected DLQ job ID %s, got %s", jobID, dlqJob.ID)
	}
	if dlqJob.Status != job.StatusFailed {
		t.Errorf("expected DLQ job status 'failed', got %s", dlqJob.Status)
	}
}
