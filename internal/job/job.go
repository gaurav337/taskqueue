package job

import "time"

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusDone       Status = "done"
	StatusFailed     Status = "failed"
)

type Job struct {
	ID          string         `json:"job_id"`
	Type        string         `json:"type"`
	Priority    string         `json:"priority"`
	Payload     map[string]any `json:"payload"`
	Status      Status         `json:"status"`
	Attempts    int            `json:"attempts"`
	MaxAttempts int            `json:"max_attempts"`
	Error       string         `json:"error,omitempty"`
	RunAfter    *time.Time     `json:"run_after,omitempty"`
	SubmittedAt time.Time      `json:"submitted_at"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}
