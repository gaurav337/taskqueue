package job_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/gaurav337/taskqueue/internal/job"
)

func TestStore(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()
	// Test ping first to ensure connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}

	store := job.NewStore(rdb)

	// Clean up after tests
	t.Cleanup(func() {
		rdb.Del(ctx, "job:test-job-1")
	})

	t.Run("Save and Get Job", func(t *testing.T) {
		j := &job.Job{
			ID:          "test-job-1",
			Type:        "email",
			Priority:    "high",
			Payload:     map[string]any{"to": "user@example.com"},
			Status:      job.StatusPending,
			Attempts:    0,
			MaxAttempts: 3,
			CreatedAt:   time.Now().Round(time.Second), // Round for JSON marshalling comparison
			UpdatedAt:   time.Now().Round(time.Second),
		}

		err := store.Save(ctx, j)
		if err != nil {
			t.Fatalf("failed to save job: %v", err)
		}

		savedJob, err := store.Get(ctx, j.ID)
		if err != nil {
			t.Fatalf("failed to get job: %v", err)
		}

		if savedJob.ID != j.ID {
			t.Errorf("expected job ID %s, got %s", j.ID, savedJob.ID)
		}
		if savedJob.Status != j.Status {
			t.Errorf("expected job status %s, got %s", j.Status, savedJob.Status)
		}
	})

	t.Run("Update Status", func(t *testing.T) {
		err := store.UpdateStatus(ctx, "test-job-1", job.StatusProcessing, "some error")
		if err != nil {
			t.Fatalf("failed to update status: %v", err)
		}

		updatedJob, err := store.Get(ctx, "test-job-1")
		if err != nil {
			t.Fatalf("failed to get updated job: %v", err)
		}

		if updatedJob.Status != job.StatusProcessing {
			t.Errorf("expected status %s, got %s", job.StatusProcessing, updatedJob.Status)
		}
		if updatedJob.Error != "some error" {
			t.Errorf("expected error 'some error', got %s", updatedJob.Error)
		}
	})
}
