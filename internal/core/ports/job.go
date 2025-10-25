package ports

import (
	"context"
	"simple-queue-103/internal/core/domain"
)

// Usecase interface
type JobService interface {
	CreateJob(fileName string) (*domain.Job, error)
	GetJob(id string) (*domain.Job, error)
	GetAllJobs() ([]*domain.Job, error)
	ControlJob(id string, command string) error
}

// Driven Port (Database)
type JobRepository interface {
	Save(ctx context.Context, job *domain.Job) error
	FindByID(ctx context.Context, id string) (*domain.Job, error)
	FindAll(ctx context.Context) ([]*domain.Job, error)
}

// Driven Port (Message Queue)
type JobQueue interface {
	EnqueueAnalysis(jobID string) error
}

// Driven Port (Real-time Notifier)
type Notifier interface {
	BroadcastUpdate(job *domain.Job)
}
