package ports

import (
	"context"
	"simple-queue-103/internal/core/domain"
)

// Usecase interface
type JobService interface {
	CreateJob(fileName string) (*domain.Job, error)
	CreateJobForProcess(fileName string, processType string) (*domain.Job, error) // NEW
	GetJob(id string) (*domain.Job, error)
	GetAllJobs() ([]*domain.Job, error)
	GetJobsByProcess(processType string) ([]*domain.Job, error) // NEW
	ControlJob(id string, command string) error
}

// Driven Port (Database)
type JobRepository interface {
	Save(ctx context.Context, job *domain.Job) error
	FindByID(ctx context.Context, id string) (*domain.Job, error)
	FindAll(ctx context.Context) ([]*domain.Job, error)

	// NEW: Process-specific methods for isolation
	FindByProcessType(ctx context.Context, processType string) ([]*domain.Job, error)
	FindByProcessAndStatus(ctx context.Context, processType string, status domain.JobStatus) ([]*domain.Job, error)
	CountByProcess(ctx context.Context, processType string) (int, error)
}

// Driven Port (Message Queue)
type JobQueue interface {
	EnqueueAnalysis(jobID string) error
	EnqueueForProcess(jobID string, processType string) error // NEW: Process-specific enqueue
}

// Driven Port (Real-time Notifier)
type Notifier interface {
	BroadcastUpdate(job *domain.Job)
}
