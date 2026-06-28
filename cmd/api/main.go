package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/gaurav337/taskqueue/internal/job"
	"github.com/gaurav337/taskqueue/internal/queue"
	"github.com/redis/go-redis/v9"
)

func uuidv4() (string, error) {
	u := make([]byte, 16)
	if _, err := rand.Read(u); err != nil {
		return "", err
	}
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:]), nil
}

type JobReq struct {
	Type     string         `json:"type"`
	Priority string         `json:"priority"`
	Payload  map[string]any `json:"payload"`
}

func setupRouter(rdb *redis.Client) *http.ServeMux {
	store := job.NewStore(rdb)
	q := queue.New(rdb)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var req JobReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if req.Type == "" {
			http.Error(w, "job type is required", http.StatusBadRequest)
			return
		}
		if req.Priority == "" {
			req.Priority = "default"
		}

		id, err := uuidv4()
		if err != nil {
			http.Error(w, "failed to generate job ID", http.StatusInternalServerError)
			return
		}

		now := time.Now()
		j := &job.Job{
			ID:          id,
			Type:        req.Type,
			Priority:    req.Priority,
			Payload:     req.Payload,
			Status:      job.StatusPending,
			MaxAttempts: 3,
			SubmittedAt: now,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		if err := store.Save(r.Context(), j); err != nil {
			http.Error(w, "failed to save job", http.StatusInternalServerError)
			return
		}

		if err := q.Publish(r.Context(), j.ID, j.Type, j.Priority, j.Payload); err != nil {
			http.Error(w, "failed to publish job", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]any{
			"job_id": j.ID,
			"status": j.Status,
		})
	})

	mux.HandleFunc("GET /jobs/{id}/status", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		j, err := store.Get(r.Context(), id)
		if err != nil {
			if err == redis.Nil {
				http.Error(w, "job not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(j)
	})

	return mux
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	mux := setupRouter(rdb)

	slog.Info("starting API server on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		slog.Error("server failed to start", "error", err)
	}
}
