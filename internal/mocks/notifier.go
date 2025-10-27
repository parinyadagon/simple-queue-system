// Package mocks provides shared mock implementations for testing
package mocks

import (
	"simple-queue-103/internal/core/domain"
	"simple-queue-103/internal/core/ports"

	"github.com/stretchr/testify/mock"
)

// MockNotifier implements ports.Notifier for testing
type MockNotifier struct {
	mock.Mock
	Updates []domain.Job // Track broadcast calls for verification
}

var _ ports.Notifier = (*MockNotifier)(nil) // Compile-time interface check

func (m *MockNotifier) BroadcastUpdate(job *domain.Job) {
	m.Called(job)
	m.Updates = append(m.Updates, *job)
}

// NewMockNotifier creates a new mock notifier instance
func NewMockNotifier() *MockNotifier {
	return &MockNotifier{
		Updates: make([]domain.Job, 0),
	}
}

// ResetNotifier resets the mock notifier state
func (m *MockNotifier) Reset() {
	m.Mock = mock.Mock{}
	m.Updates = make([]domain.Job, 0)
}
