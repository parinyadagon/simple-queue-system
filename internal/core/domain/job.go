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
	ID                string    `json:"id" db:"id"`
	ProcessType       string    `json:"process_type" db:"process_type"`       // NEW: Process isolation
	ProcessVersion    string    `json:"process_version" db:"process_version"` // NEW: Process versioning
	FileName          string    `json:"file_name" db:"file_name"`
	Progress          int       `json:"progress" db:"progress"`
	Status            JobStatus `json:"status" db:"status"`
	CreatedAt         time.Time `json:"created_at" db:"created_at"`
	UpdatedAt         time.Time `json:"updated_at" db:"updated_at"`
	CurrentCheckpoint string    `json:"current_checkpoint" db:"current_checkpoint"`
	CurrentStepName   string    `json:"current_step_name" db:"current_step_name"`
	CurrentMainStep   string    `json:"current_main_step" db:"current_main_step"` // NEW: Main step name
	CurrentSubStep    string    `json:"current_sub_step" db:"current_sub_step"`   // NEW: Sub step name
	// ใช้ context สำหรับการ cancel
	// CancelFunc func() `json:"-"`
}
