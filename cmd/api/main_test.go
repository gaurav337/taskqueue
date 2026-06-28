package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/gaurav337/taskqueue/internal/job"
	"github.com/gaurav337/taskqueue/internal/queue"
)

func TestAPI(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}

	// Clean up any test keys
	t.Cleanup(func() {
		rdb.Del(ctx, queue.StreamCritical, queue.StreamDefault, queue.StreamLow)
	})

	router := setupRouter(rdb)

	t.Run("POST /jobs - Success", func(t *testing.T) {
		body := map[string]any{
			"type":     "test-job",
			"priority": "critical",
			"payload":  map[string]any{"key": "value"},
		}
		jsonBody, _ := json.Marshal(body)

		req, err := http.NewRequest("POST", "/jobs", bytes.NewBuffer(jsonBody))
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		if rr.Code != http.StatusAccepted {
			t.Errorf("expected status 202, got %d", rr.Code)
		}

		var resp map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}

		jobID, ok := resp["job_id"].(string)
		if !ok || jobID == "" {
			t.Errorf("expected non-empty job_id in response, got %v", resp)
		}

		status, ok := resp["status"].(string)
		if !ok || status != string(job.StatusPending) {
			t.Errorf("expected status 'pending', got %v", resp["status"])
		}

		// Verify job is saved in Redis
		store := job.NewStore(rdb)
		j, err := store.Get(ctx, jobID)
		if err != nil {
			t.Fatalf("job was not saved in store: %v", err)
		}
		defer rdb.Del(ctx, "job:"+jobID)

		if j.Type != "test-job" {
			t.Errorf("expected job type 'test-job', got %s", j.Type)
		}
	})

	t.Run("POST /jobs - Missing Type", func(t *testing.T) {
		body := map[string]any{
			"priority": "critical",
		}
		jsonBody, _ := json.Marshal(body)

		req, err := http.NewRequest("POST", "/jobs", bytes.NewBuffer(jsonBody))
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", rr.Code)
		}
	})

	t.Run("GET /jobs/{id}/status - Success", func(t *testing.T) {
		// Save a job first
		store := job.NewStore(rdb)
		jobID := "custom-test-id-1"
		j := &job.Job{
			ID:     jobID,
			Type:   "test-status",
			Status: job.StatusDone,
		}
		err := store.Save(ctx, j)
		if err != nil {
			t.Fatalf("failed to save job: %v", err)
		}
		defer rdb.Del(ctx, "job:"+jobID)

		req, err := http.NewRequest("GET", "/jobs/custom-test-id-1/status", nil)
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rr.Code)
		}

		var retrieved job.Job
		if err := json.Unmarshal(rr.Body.Bytes(), &retrieved); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}

		if retrieved.ID != jobID {
			t.Errorf("expected ID %s, got %s", jobID, retrieved.ID)
		}
		if retrieved.Status != job.StatusDone {
			t.Errorf("expected status %s, got %s", job.StatusDone, retrieved.Status)
		}
	})

	t.Run("GET /jobs/{id}/status - Not Found", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/jobs/non-existent-id/status", nil)
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", rr.Code)
		}
	})
}
