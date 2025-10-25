package domain

import "time"

type JobStatus string

const (
	StatusPending   JobStatus = "PENDING"
	StatusRunning   JobStatus = "RUNNING"
	StatusPaused    JobStatus = "PAUSED"
	StatusFailed    JobStatus = "FAILED"
	StatusCanceled  JobStatus = "CANCELED"
	StatusCompleted JobStatus = "COMPLETED"
)

type Job struct {
	ID        string    `json:"id" db:"id"`
	FileName  string    `json:"file_name" db:"file_name"`
	Progress  int       `json:"progress" db:"progress"`
	Status    JobStatus `json:"status" db:"status"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	// ใช้ context สำหรับการ cancel
	// CancelFunc func() `json:"-"`
}
