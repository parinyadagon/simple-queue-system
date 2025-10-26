package queue_test

import (
	"context"
	"simple-queue-103/internal/core/domain"

	"github.com/stretchr/testify/mock"
)

// MockJobRepository implements ports.JobRepository for testing
type MockJobRepository struct {
	mock.Mock
}

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

// MockNotifier implements ports.Notifier for testing
type MockNotifier struct {
	mock.Mock
	Updates []domain.Job // Track broadcast calls for verification
}

func (m *MockNotifier) BroadcastUpdate(job *domain.Job) {
	m.Called(job)
	m.Updates = append(m.Updates, *job)
}

// NewMockJobRepository creates a new mock repository instance
func NewMockJobRepository() *MockJobRepository {
	return new(MockJobRepository)
}

// NewMockNotifier creates a new mock notifier instance
func NewMockNotifier() *MockNotifier {
	return &MockNotifier{
		Updates: make([]domain.Job, 0),
	}
}

// ResetMocks resets all mock expectations and call history
func ResetMocks(mocks ...interface{}) {
	for _, m := range mocks {
		if mockObj, ok := m.(*MockJobRepository); ok {
			mockObj.Mock = mock.Mock{} // Reset the mock
		}
		if mockObj, ok := m.(*MockNotifier); ok {
			mockObj.Mock = mock.Mock{}              // Reset the mock
			mockObj.Updates = make([]domain.Job, 0) // Reset updates
		}
	}
}
