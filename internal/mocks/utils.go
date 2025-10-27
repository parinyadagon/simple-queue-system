// Package mocks provides utility functions for managing mock objects
package mocks

import "github.com/stretchr/testify/mock"

// ResetMocks resets all mock expectations and call history
func ResetMocks(mocks ...interface{}) {
	for _, m := range mocks {
		if mockObj, ok := m.(*MockJobRepository); ok {
			mockObj.Mock = mock.Mock{} // Reset the mock
		}
		if mockObj, ok := m.(*MockNotifier); ok {
			mockObj.Reset() // Use the Reset method
		}
	}
}

// ResetAll is a convenience function to reset multiple mocks at once
func ResetAll(repo *MockJobRepository, notifier *MockNotifier) {
	if repo != nil {
		repo.Mock = mock.Mock{}
	}
	if notifier != nil {
		notifier.Reset()
	}
}
