package mocks

import (
	"context"
	"encoding/json"
	"simple-queue-103/internal/adapters/queue"
	"simple-queue-103/internal/core/domain"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestJobRepository_WithTestcontainer tests JobRepository with real MySQL database
func TestJobRepository_WithTestcontainer(t *testing.T) {
	helper := NewIntegrationTestHelper(t)
	ctx := context.Background()

	t.Run("Save and FindByID", func(t *testing.T) {
		// Create test job
		job := helper.CreateTestJob("tc-job-1", domain.StatusPending, "")

		// Save and verify using helper
		helper.SaveAndVerifyJob(ctx, t, job)
	})

	t.Run("Update job status", func(t *testing.T) {
		// Create and save initial job
		job := helper.CreateTestJob("tc-job-2", domain.StatusPending, "")
		err := helper.Repository.Save(ctx, job)
		require.NoError(t, err)

		// Update job status
		job.Status = domain.StatusRunning
		job.CurrentCheckpoint = "DOWNLOAD_SOURCE"
		job.Progress = 17

		err = helper.Repository.Save(ctx, job)
		require.NoError(t, err)

		// Verify update
		updatedJob, err := helper.Repository.FindByID(ctx, job.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusRunning, updatedJob.Status)
		assert.Equal(t, "DOWNLOAD_SOURCE", updatedJob.CurrentCheckpoint)
		assert.Equal(t, 17, updatedJob.Progress)
	})

	t.Run("FindAll jobs", func(t *testing.T) {
		// Create multiple test jobs
		job1 := helper.CreateTestJob("tc-job-3", domain.StatusPending, "")
		job2 := helper.CreateTestJob("tc-job-4", domain.StatusRunning, "ANALYSIS_MODEL_A")
		job3 := helper.CreateTestJob("tc-job-5", domain.StatusCompleted, "COMPLETED")

		// Save all jobs
		require.NoError(t, helper.Repository.Save(ctx, job1))
		require.NoError(t, helper.Repository.Save(ctx, job2))
		require.NoError(t, helper.Repository.Save(ctx, job3))

		// Find all jobs
		allJobs, err := helper.Repository.FindAll(ctx)
		require.NoError(t, err)

		// Should have at least our 3 jobs (might have more from other tests)
		assert.GreaterOrEqual(t, len(allJobs), 3)

		// Verify our jobs exist
		jobIDs := make(map[string]bool)
		for _, job := range allJobs {
			jobIDs[job.ID] = true
		}
		assert.True(t, jobIDs["tc-job-3"])
		assert.True(t, jobIDs["tc-job-4"])
		assert.True(t, jobIDs["tc-job-5"])
	})
}

// TestJobProcessing_WithTestcontainer tests end-to-end job processing with real database
func TestJobProcessing_WithTestcontainer(t *testing.T) {
	helper := NewIntegrationTestHelper(t)
	ctx := context.Background()

	t.Run("End-to-end job processing with real database", func(t *testing.T) {
		// Create initial job in database
		job := helper.CreateTestJob("tc-process-1", domain.StatusPending, "")
		err := helper.Repository.Save(ctx, job)
		require.NoError(t, err)

		// Create task handler with real repository and mock notifier
		handler := queue.NewTaskHandler(helper.Repository, helper.MockNotifier)

		// Set up mock notifier expectations
		helper.MockNotifier.On("BroadcastUpdate", mock.MatchedBy(func(job *domain.Job) bool {
			return job.ID == "tc-process-1"
		})).Return()

		// Create task payload (same as in test helper)
		payload, _ := json.Marshal(map[string]string{"job_id": "tc-process-1"})
		task := asynq.NewTask("task:analysis", payload)

		// Process with timeout to prevent infinite execution
		processCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		// Execute job processing
		err = handler.HandleAnalysisTask(processCtx, task)
		require.NoError(t, err)

		// Verify final state in database
		finalJob, err := helper.Repository.FindByID(ctx, "tc-process-1")
		require.NoError(t, err)

		assert.Equal(t, domain.StatusCompleted, finalJob.Status)
		assert.Equal(t, "COMPLETED", finalJob.CurrentCheckpoint)
		assert.Equal(t, 100, finalJob.Progress)

		// Verify notifier was called (mock verification)
		helper.MockNotifier.AssertExpectations(t)
		assert.True(t, len(helper.MockNotifier.Updates) > 0, "Expected broadcast updates")
	})

	t.Run("Job checkpoint recovery with real database", func(t *testing.T) {
		// Create fresh mock notifier for this test
		recoveryMockNotifier := NewMockNotifier()

		// Create job with existing checkpoint in database
		job := helper.CreateTestJob("tc-recovery-1", domain.StatusPending, "ANALYSIS_MODEL_A")
		job.Progress = 63 // Simulate progress from previous steps
		err := helper.Repository.Save(ctx, job)
		require.NoError(t, err)

		// Create task handler with fresh mock notifier
		handler := queue.NewTaskHandler(helper.Repository, recoveryMockNotifier)

		// Simple mock expectation - any job update is fine
		recoveryMockNotifier.On("BroadcastUpdate", mock.Anything).Return()

		// Create and process task
		payload, _ := json.Marshal(map[string]string{"job_id": "tc-recovery-1"})
		task := asynq.NewTask("task:analysis", payload)

		processCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()

		err = handler.HandleAnalysisTask(processCtx, task)
		require.NoError(t, err)

		// Verify recovery worked - should complete remaining steps
		finalJob, err := helper.Repository.FindByID(ctx, "tc-recovery-1")
		require.NoError(t, err)

		assert.Equal(t, domain.StatusCompleted, finalJob.Status)
		assert.Equal(t, "COMPLETED", finalJob.CurrentCheckpoint)
		assert.Equal(t, 100, finalJob.Progress)

		// Should have some updates (at least status + completion)
		updatesCount := len(recoveryMockNotifier.Updates)
		assert.True(t, updatesCount > 0, "Expected some updates, got %d", updatesCount)
		assert.True(t, updatesCount <= 8, "Expected reasonable number of updates, got %d", updatesCount)
	})
}

// TestConcurrentAccess_WithTestcontainer tests concurrent database access
func TestConcurrentAccess_WithTestcontainer(t *testing.T) {
	helper := NewIntegrationTestHelper(t)
	ctx := context.Background()

	t.Run("Concurrent job updates", func(t *testing.T) {
		// Create initial job
		job := helper.CreateTestJob("tc-concurrent-1", domain.StatusPending, "")
		err := helper.Repository.Save(ctx, job)
		require.NoError(t, err)

		// Create context with timeout for goroutines
		goroutineCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		// Simulate concurrent updates with better error handling
		var goroutine1Success, goroutine2Success bool
		done := make(chan struct{}, 2)

		// Goroutine 1: Update status and checkpoint
		go func() {
			defer func() { done <- struct{}{} }()

			job1, err := helper.Repository.FindByID(goroutineCtx, "tc-concurrent-1")
			if err != nil {
				t.Logf("Goroutine 1 FindByID error: %v", err)
				return
			}

			job1.Status = domain.StatusRunning
			job1.CurrentCheckpoint = "DOWNLOAD_SOURCE"

			if err := helper.Repository.Save(goroutineCtx, job1); err != nil {
				t.Logf("Goroutine 1 Save error: %v", err)
				return
			}

			goroutine1Success = true
			t.Logf("Goroutine 1: Successfully updated status to RUNNING")
		}()

		// Goroutine 2: Update progress with small delay
		go func() {
			defer func() { done <- struct{}{} }()

			// Small delay to increase chance of concurrent access
			time.Sleep(50 * time.Millisecond)

			job2, err := helper.Repository.FindByID(goroutineCtx, "tc-concurrent-1")
			if err != nil {
				t.Logf("Goroutine 2 FindByID error: %v", err)
				return
			}

			job2.Progress = 25

			if err := helper.Repository.Save(goroutineCtx, job2); err != nil {
				t.Logf("Goroutine 2 Save error: %v", err)
				return
			}

			goroutine2Success = true
			t.Logf("Goroutine 2: Successfully updated progress to 25")
		}()

		// Wait for both goroutines with timeout
		completedCount := 0
		for completedCount < 2 {
			select {
			case <-done:
				completedCount++
			case <-time.After(5 * time.Second):
				t.Fatal("Concurrent test timeout - goroutines did not complete")
			}
		}

		// Log results
		t.Logf("Goroutine results: G1=%t, G2=%t", goroutine1Success, goroutine2Success)

		// At least one operation should succeed
		assert.True(t, goroutine1Success || goroutine2Success, "At least one concurrent operation should succeed")

		// Verify final state in database
		finalJob, err := helper.Repository.FindByID(ctx, "tc-concurrent-1")
		require.NoError(t, err)

		// Log final state
		t.Logf("Final job state: Status=%s, Progress=%d, Checkpoint=%s",
			finalJob.Status, finalJob.Progress, finalJob.CurrentCheckpoint)

		// Job should not be corrupted - verify it's in a valid state
		assert.NotEmpty(t, finalJob.ID, "Job ID should not be empty")
		assert.NotEmpty(t, finalJob.FileName, "Job filename should not be empty")

		// At least the original state should be preserved if no updates succeeded
		if !goroutine1Success && !goroutine2Success {
			assert.Equal(t, domain.StatusPending, finalJob.Status)
			assert.Equal(t, 0, finalJob.Progress)
			assert.Equal(t, "", finalJob.CurrentCheckpoint)
		}
	})
}
