package repository

import (
	"context"
	"fmt"
	"simple-queue-103/internal/core/domain"
	"simple-queue-103/internal/core/ports"
	"sync"
)

type inMemoryJobRepository struct {
	sync.RWMutex
	jobs map[string]*domain.Job
}

func NewInMemoryJobRepository() ports.JobRepository {
	return &inMemoryJobRepository{
		jobs: make(map[string]*domain.Job),
	}
}

func (r *inMemoryJobRepository) Save(ctx context.Context, job *domain.Job) error {
	r.Lock()
	defer r.Unlock()
	r.jobs[job.ID] = job

	return nil
}

func (r *inMemoryJobRepository) FindByID(ctx context.Context, id string) (*domain.Job, error) {
	r.RLock()
	defer r.RUnlock()
	job, ok := r.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job not found")
	}

	return job, nil
}

func (r *inMemoryJobRepository) FindAll(ctx context.Context) ([]*domain.Job, error) {
	r.RLock()
	defer r.RUnlock()
	var allJobs []*domain.Job
	for _, job := range r.jobs {
		allJobs = append(allJobs, job)
	}

	return allJobs, nil
}

// FindByProcessType returns jobs of a specific process type
func (r *inMemoryJobRepository) FindByProcessType(ctx context.Context, processType string) ([]*domain.Job, error) {
	r.RLock()
	defer r.RUnlock()
	var processJobs []*domain.Job
	for _, job := range r.jobs {
		if job.ProcessType == processType {
			processJobs = append(processJobs, job)
		}
	}

	return processJobs, nil
}

// FindByProcessAndStatus returns jobs of a specific process type and status
func (r *inMemoryJobRepository) FindByProcessAndStatus(ctx context.Context, processType string, status domain.JobStatus) ([]*domain.Job, error) {
	r.RLock()
	defer r.RUnlock()
	var filteredJobs []*domain.Job
	for _, job := range r.jobs {
		if job.ProcessType == processType && job.Status == status {
			filteredJobs = append(filteredJobs, job)
		}
	}

	return filteredJobs, nil
}

// CountByProcess returns the count of jobs for a specific process type
func (r *inMemoryJobRepository) CountByProcess(ctx context.Context, processType string) (int, error) {
	r.RLock()
	defer r.RUnlock()
	count := 0
	for _, job := range r.jobs {
		if job.ProcessType == processType {
			count++
		}
	}

	return count, nil
}
