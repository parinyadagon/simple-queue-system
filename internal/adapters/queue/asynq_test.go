package queue_test

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
)

// --- Test Helper Functions ---
func createTestTask(jobID string) *asynq.Task {
	payload, _ := json.Marshal(map[string]string{"job_id": jobID})
	return asynq.NewTask("task:analysis", payload) // ใช้ string literal แทน
}

func createTestJob(id string, status domain.JobStatus, checkpoint string) *domain.Job {
	return &domain.Job{
		ID:                id,
		FileName:          "test_file.zip",
		Status:            status,
		CurrentCheckpoint: checkpoint,
		Progress:          0,
		CreatedAt:         time.Now(),
	}
}

// --- Test Cases ---

func TestHandleAnalysisTask_NewJob_CompletesAllSteps(t *testing.T) {
	// Arrange
	mockRepo := NewMockJobRepository()
	mockNotifier := NewMockNotifier()
	handler := queue.NewTaskHandler(mockRepo, mockNotifier)

	jobID := "test-job-1"
	task := createTestTask(jobID)

	// Initial job - no checkpoint
	initialJob := createTestJob(jobID, domain.StatusPending, "")

	// Mock FindByID calls - job stays RUNNING throughout
	mockRepo.On("FindByID", mock.Anything, jobID).Return(initialJob, nil).Times(1) // Initial load

	// Mock subsequent calls during processing (status checks)
	runningJob := *initialJob
	runningJob.Status = domain.StatusRunning
	mockRepo.On("FindByID", mock.Anything, jobID).Return(&runningJob, nil) // All other calls

	// Mock Save calls
	mockRepo.On("Save", mock.Anything, mock.MatchedBy(func(job *domain.Job) bool {
		return job.Status == domain.StatusRunning
	})).Return(nil).Once() // Initial status update

	mockRepo.On("Save", mock.Anything, mock.MatchedBy(func(job *domain.Job) bool {
		return job.CurrentCheckpoint != "" && job.CurrentCheckpoint != "COMPLETED"
	})).Return(nil).Times(6) // 6 step checkpoints

	mockRepo.On("Save", mock.Anything, mock.MatchedBy(func(job *domain.Job) bool {
		return job.Status == domain.StatusCompleted && job.CurrentCheckpoint == "COMPLETED"
	})).Return(nil).Once() // Final completion

	// Mock broadcast calls
	mockNotifier.On("BroadcastUpdate", mock.Anything).Return()

	// Act
	ctx := context.Background()
	err := handler.HandleAnalysisTask(ctx, task)

	// Assert
	assert.NoError(t, err)
	mockRepo.AssertExpectations(t)
	mockNotifier.AssertExpectations(t)

	// Verify final state
	assert.Len(t, mockNotifier.Updates, 8) // Initial + 6 steps + completion
	finalUpdate := mockNotifier.Updates[len(mockNotifier.Updates)-1]
	assert.Equal(t, domain.StatusCompleted, finalUpdate.Status)
	assert.Equal(t, "COMPLETED", finalUpdate.CurrentCheckpoint)
	assert.Equal(t, 100, finalUpdate.Progress)
}

func TestHandleAnalysisTask_ResumeFromCheckpoint(t *testing.T) {
	// Arrange
	mockRepo := NewMockJobRepository()
	mockNotifier := NewMockNotifier()
	handler := queue.NewTaskHandler(mockRepo, mockNotifier)

	jobID := "test-job-2"
	task := createTestTask(jobID)

	// Job with checkpoint at step 3 (ANALYSIS_MODEL_A)
	checkpointJob := createTestJob(jobID, domain.StatusPending, "ANALYSIS_MODEL_A")
	checkpointJob.Progress = 63 // Progress from previous steps

	mockRepo.On("FindByID", mock.Anything, jobID).Return(checkpointJob, nil).Once()

	// Job becomes RUNNING and stays RUNNING
	runningJob := *checkpointJob
	runningJob.Status = domain.StatusRunning
	mockRepo.On("FindByID", mock.Anything, jobID).Return(&runningJob, nil)

	// Should save: initial status + remaining 2 steps + completion = 4 saves
	mockRepo.On("Save", mock.Anything, mock.Anything).Return(nil).Times(4)
	mockNotifier.On("BroadcastUpdate", mock.Anything).Return()

	// Act
	ctx := context.Background()
	err := handler.HandleAnalysisTask(ctx, task)

	// Assert
	assert.NoError(t, err)
	mockRepo.AssertExpectations(t)

	// Should only process remaining steps: ANALYSIS_MODEL_B, GENERATING_REPORT
	assert.True(t, len(mockNotifier.Updates) == 4) // status + 2 steps + completion
}

func TestHandleAnalysisTask_PauseResume_Workflow(t *testing.T) {
	// Arrange
	mockRepo := NewMockJobRepository()
	mockNotifier := NewMockNotifier()
	handler := queue.NewTaskHandler(mockRepo, mockNotifier)

	jobID := "test-job-3"
	task := createTestTask(jobID)

	initialJob := createTestJob(jobID, domain.StatusPending, "")

	// แก้ไข: ใช้ sequential return values แทน function
	// Call 1: Initial load
	mockRepo.On("FindByID", mock.Anything, jobID).Return(initialJob, nil).Once()

	// Call 2: First step - running
	runningJob := *initialJob
	runningJob.Status = domain.StatusRunning
	mockRepo.On("FindByID", mock.Anything, jobID).Return(&runningJob, nil).Once()

	// Call 3: Job gets paused during first step
	pausedJob := *initialJob
	pausedJob.Status = domain.StatusPaused
	mockRepo.On("FindByID", mock.Anything, jobID).Return(&pausedJob, nil).Once()

	// Call 4: Still paused (polling)
	mockRepo.On("FindByID", mock.Anything, jobID).Return(&pausedJob, nil).Once()

	// Call 5+: Resume - back to running
	mockRepo.On("FindByID", mock.Anything, jobID).Return(&runningJob, nil)

	mockRepo.On("Save", mock.Anything, mock.Anything).Return(nil)
	mockNotifier.On("BroadcastUpdate", mock.Anything).Return()

	// Act with timeout to prevent infinite pause loop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start processing in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- handler.HandleAnalysisTask(ctx, task)
	}()

	// Wait a bit then cancel (simulates user canceling during pause)
	time.Sleep(5 * time.Second)
	cancel()

	// Assert
	select {
	case err := <-errChan:
		assert.NoError(t, err) // Should exit gracefully
	case <-time.After(2 * time.Second):
		t.Fatal("Task did not exit within timeout")
	}
}

func TestHandleAnalysisTask_AlreadyCompleted_SkipsExecution(t *testing.T) {
	// Arrange
	mockRepo := NewMockJobRepository()
	mockNotifier := NewMockNotifier()
	handler := queue.NewTaskHandler(mockRepo, mockNotifier)

	jobID := "test-job-4"
	task := createTestTask(jobID)

	// Job already completed
	completedJob := createTestJob(jobID, domain.StatusCompleted, "COMPLETED")
	completedJob.Progress = 100

	mockRepo.On("FindByID", mock.Anything, jobID).Return(completedJob, nil).Once()
	// No Save calls should happen

	// Act
	ctx := context.Background()
	err := handler.HandleAnalysisTask(ctx, task)

	// Assert
	assert.NoError(t, err)
	mockRepo.AssertExpectations(t)
	mockRepo.AssertNotCalled(t, "Save")
}

func TestHandleAnalysisTask_PreemptedDuringProcessing(t *testing.T) {
	// Arrange
	mockRepo := NewMockJobRepository()
	mockNotifier := NewMockNotifier()
	handler := queue.NewTaskHandler(mockRepo, mockNotifier)

	jobID := "test-job-5"
	task := createTestTask(jobID)

	initialJob := createTestJob(jobID, domain.StatusPending, "")

	// แก้ไข: ใช้ sequential return values
	// Call 1: Initial load
	mockRepo.On("FindByID", mock.Anything, jobID).Return(initialJob, nil).Once()

	// Call 2: Start processing - running (beginning of loop)
	runningJob := *initialJob
	runningJob.Status = domain.StatusRunning
	mockRepo.On("FindByID", mock.Anything, jobID).Return(&runningJob, nil).Once()

	// Call 3: After work - job was canceled
	canceledJob := *initialJob
	canceledJob.Status = domain.StatusCanceled
	mockRepo.On("FindByID", mock.Anything, jobID).Return(&canceledJob, nil).Once()

	// Call 4: Final status check (for completion)
	mockRepo.On("FindByID", mock.Anything, jobID).Return(&canceledJob, nil).Maybe()

	// Should save initial status but not checkpoint (due to preemption)
	mockRepo.On("Save", mock.Anything, mock.MatchedBy(func(job *domain.Job) bool {
		return job.Status == domain.StatusRunning && job.CurrentCheckpoint == ""
	})).Return(nil).Once()

	mockNotifier.On("BroadcastUpdate", mock.Anything).Return()

	// Act
	ctx := context.Background()
	err := handler.HandleAnalysisTask(ctx, task)

	// Assert
	assert.NoError(t, err) // Should exit gracefully when canceled
	mockRepo.AssertExpectations(t)
}

// TestCalculateProgress - ไม่สามารถทดสอบ private method calculateProgress ได้ใน external test
// Progress calculation จะถูกทดสอบผ่าน integration tests ใน TestHandleAnalysisTask แทน

// --- Integration Test with Timeout ---
func TestHandleAnalysisTask_Timeout_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Arrange
	mockRepo := NewMockJobRepository()
	mockNotifier := NewMockNotifier()
	handler := queue.NewTaskHandler(mockRepo, mockNotifier)

	jobID := "timeout-job"
	task := createTestTask(jobID)

	job := createTestJob(jobID, domain.StatusPending, "")
	mockRepo.On("FindByID", mock.Anything, jobID).Return(job, nil).Once()

	runningJob := *job
	runningJob.Status = domain.StatusRunning
	mockRepo.On("FindByID", mock.Anything, jobID).Return(&runningJob, nil)
	mockRepo.On("Save", mock.Anything, mock.Anything).Return(nil)
	mockNotifier.On("BroadcastUpdate", mock.Anything).Return()

	// Act with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := handler.HandleAnalysisTask(ctx, task)

	// Assert - should exit gracefully due to context timeout
	assert.NoError(t, err)
}
