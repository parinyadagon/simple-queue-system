// Package mocks provides shared mock implementations for testing
package mocks

import (
	"context"
	"simple-queue-103/internal/core/domain"
	"simple-queue-103/internal/core/ports"

	"github.com/stretchr/testify/mock"
)

// MockJobRepository implements ports.JobRepository for testing
type MockJobRepository struct {
	mock.Mock
}

var _ ports.JobRepository = (*MockJobRepository)(nil) // Compile-time interface check

func (m *MockJobRepository) Save(ctx context.Context, job *domain.Job) error {
	args := m.Called(ctx, job)
	return args.Error(0)
}

func (m *MockJobRepository) FindByID(ctx context.Context, id string) (*domain.Job, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.Job), args.Error(1)
}

func (m *MockJobRepository) FindAll(ctx context.Context) ([]*domain.Job, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*domain.Job), args.Error(1)
}

func (m *MockJobRepository) FindByProcessType(ctx context.Context, processType string) ([]*domain.Job, error) {
	args := m.Called(ctx, processType)
	return args.Get(0).([]*domain.Job), args.Error(1)
}

func (m *MockJobRepository) FindByProcessAndStatus(ctx context.Context, processType string, status domain.JobStatus) ([]*domain.Job, error) {
	args := m.Called(ctx, processType, status)
	return args.Get(0).([]*domain.Job), args.Error(1)
}

func (m *MockJobRepository) CountByProcess(ctx context.Context, processType string) (int, error) {
	args := m.Called(ctx, processType)
	return args.Int(0), args.Error(1)
}

// NewMockJobRepository creates a new mock repository instance
func NewMockJobRepository() *MockJobRepository {
	return new(MockJobRepository)
}
