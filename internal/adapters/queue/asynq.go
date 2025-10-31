package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"simple-queue-103/internal/core/domain"
	"simple-queue-103/internal/core/ports"
	"simple-queue-103/internal/lib/process"
	"time"

	"github.com/hibiken/asynq"
)

const (
	TaskTypeAnalysis = "task:analysis" // Fallback task type for unknown processes
	RedisAddr        = "127.0.0.1:6379"

	// Job processing constants
	JobTimeout          = 30 * time.Minute
	HeartbeatInterval   = 1 * time.Minute
	StatusCheckInterval = 2 * time.Second
)

// Process constants from library
var (
	processConstants          = process.DefaultProcessConstants()
	StepProcessingTime        = processConstants.StepProcessingTime
	CompletedCheckpoint       = processConstants.CompletedCheckpoint
	MaxProgressBeforeComplete = processConstants.MaxProgressBeforeComplete
)

// Type aliases for library types
type JobStepConfig = process.JobStepConfig
type JobSubStepConfig = process.JobSubStepConfig
type JobProcessConfig = process.JobProcessConfig

// Use process configurations from library
var ProcessConfigurations = process.ProcessConfigurations

// Use process manager from library
var DefaultProcessManager = process.NewProcessManager().UseProcess("data_analysis")

var JobSteps = DefaultProcessManager.GetSteps()
var SubCheckpoints = DefaultProcessManager.GetSubCheckpoints()
var StepDescriptions = DefaultProcessManager.GetStepDescriptions()

var StepIndexMap = func() map[string]int {
	m := make(map[string]int)
	for i, step := range JobSteps {
		m[step] = i
	}

	return m
}()

// StepFunction defines the signature for step processing functions
type StepFunction func(h *TaskHandler, ctx context.Context, jobID string) error

// getStepExecutor returns the appropriate step function for the given step name
func getStepExecutor(stepName string) StepFunction {
	// First, try to get from current process configuration
	if currentConfig := DefaultProcessManager.GetCurrentProcessConfig(); currentConfig != nil {
		for _, step := range currentConfig.Steps {
			if step.Name == stepName {
				if step.ExecuteFunc != nil {
					return func(h *TaskHandler, ctx context.Context, jobID string) error {
						return step.ExecuteFunc(ctx, jobID, &step)
					}
				}
				// If no custom function, use generic execution
				return func(h *TaskHandler, ctx context.Context, jobID string) error {
					return h.executeGenericStep(ctx, jobID, &step)
				}
			}
		}
	}

	// For any unrecognized steps, use generic execution if we can find it in any process config
	for _, config := range ProcessConfigurations {
		for _, step := range config.Steps {
			if step.Name == stepName {
				return func(h *TaskHandler, ctx context.Context, jobID string) error {
					return h.executeGenericStep(ctx, jobID, &step)
				}
			}
		}
	}

	// If step not found anywhere, return nil (will use default processing)
	return nil
}

// saveSubCheckpoint saves a sub-checkpoint and calculates detailed progress
func (h *TaskHandler) saveSubCheckpoint(ctx context.Context, jobID string, mainStep string, subCheckpoint string) error {
	job, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to find job for sub-checkpoint update: %w", err)
	}

	// Check if job was cancelled/paused during processing
	if job.Status != domain.StatusRunning {
		log.Printf("Job %s was preempted, skipping sub-checkpoint %s", jobID, subCheckpoint)
		return nil
	}

	// Get human-readable step description
	stepDescription, exists := StepDescriptions[subCheckpoint]
	if !exists {
		stepDescription = subCheckpoint // Fallback to checkpoint name
	}

	log.Printf("Job %s: Sub-checkpoint: %s (%s)", jobID, subCheckpoint, stepDescription)
	job.CurrentCheckpoint = subCheckpoint
	job.CurrentStepName = stepDescription
	job.Progress = h.calculateDetailedProgress(mainStep, subCheckpoint)

	if err := h.repo.Save(ctx, job); err != nil {
		log.Printf("Error saving sub-checkpoint for job %s: %v", jobID, err)
		return fmt.Errorf("failed to save sub-checkpoint: %w", err)
	}

	h.notifier.BroadcastUpdate(job)
	return nil
}

// findSubCheckpointStartIndex finds where to resume within sub-checkpoints
func (h *TaskHandler) findSubCheckpointStartIndex(currentCheckpoint string, subSteps []string) int {
	if currentCheckpoint == "" {
		return 0
	}

	// Find the index of current sub-checkpoint
	for i, subStep := range subSteps {
		if subStep == currentCheckpoint {
			// Resume from the next sub-step
			return i + 1
		}
	}

	// If not found in sub-steps, start from beginning
	return 0
}

// executeStepWithSubCheckpoints executes a step with multiple sub-checkpoints
func (h *TaskHandler) executeStepWithSubCheckpoints(ctx context.Context, jobID string, stepName string, subStepActions []func()) error {
	subSteps := SubCheckpoints[stepName]

	job, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to find job: %w", err)
	}

	startSubIndex := h.findSubCheckpointStartIndex(job.CurrentCheckpoint, subSteps)

	for i, action := range subStepActions {
		if startSubIndex <= i {
			// Save sub-checkpoint (with safety check)
			if i < len(subSteps) {
				if err := h.saveSubCheckpoint(ctx, jobID, stepName, subSteps[i]); err != nil {
					return err
				}
			} else {
				// Fallback: save step name as checkpoint for new processes
				if err := h.saveSubCheckpoint(ctx, jobID, stepName, fmt.Sprintf("%s_substep_%d", stepName, i+1)); err != nil {
					return err
				}
			}

			// Execute the action
			action()
		}
	}

	return nil
}

// calculateDetailedProgress calculates progress including sub-checkpoints
func (h *TaskHandler) calculateDetailedProgress(mainStep string, subCheckpoint string) int {
	if subCheckpoint == CompletedCheckpoint {
		return 100
	}

	// Find main step index
	mainStepIndex, exists := StepIndexMap[mainStep]
	if !exists {
		// Check if this is already a sub-checkpoint
		for step, subSteps := range SubCheckpoints {
			for subIndex, sub := range subSteps {
				if sub == subCheckpoint {
					if step == subCheckpoint[:len(step)] { // Match main step name prefix
						mainStepIndex = StepIndexMap[step]
						subStepProgress := float64(subIndex+1) / float64(len(subSteps))
						totalSteps := len(JobSteps)

						// Calculate progress: main step progress + sub-step progress within that step
						stepWeight := float64(MaxProgressBeforeComplete) / float64(totalSteps)
						mainStepProgress := float64(mainStepIndex) * stepWeight
						currentStepProgress := stepWeight * subStepProgress

						finalProgress := int(mainStepProgress + currentStepProgress)
						if finalProgress > MaxProgressBeforeComplete {
							finalProgress = MaxProgressBeforeComplete
						}
						return finalProgress
					}
				}
			}
		}
		return 0
	}

	// Find sub-checkpoint index within the main step
	subSteps, exists := SubCheckpoints[mainStep]
	if !exists {
		// Fallback to original calculation
		return h.CalculateProgress(mainStep, len(JobSteps))
	}

	subStepIndex := -1
	for i, sub := range subSteps {
		if sub == subCheckpoint {
			subStepIndex = i
			break
		}
	}

	if subStepIndex == -1 {
		// Fallback to original calculation
		return h.CalculateProgress(mainStep, len(JobSteps))
	}

	// Calculate detailed progress
	totalSteps := len(JobSteps)
	stepWeight := float64(MaxProgressBeforeComplete) / float64(totalSteps)
	mainStepProgress := float64(mainStepIndex) * stepWeight
	subStepProgress := stepWeight * (float64(subStepIndex+1) / float64(len(subSteps)))

	finalProgress := int(mainStepProgress + subStepProgress)
	if finalProgress > MaxProgressBeforeComplete {
		finalProgress = MaxProgressBeforeComplete
	}

	return finalProgress
}

// --- 1. Asynq Client (Implement JobQueue) ---
type asynqJobQueue struct {
	client *asynq.Client
}

func NewAsynqJobQueue() ports.JobQueue {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: RedisAddr})

	return &asynqJobQueue{client: client}
}

func (q *asynqJobQueue) EnqueueAnalysis(jobID string) error {
	// Backward compatibility - default to data_analysis
	return q.EnqueueForProcess(jobID, "data_analysis")
}

func (q *asynqJobQueue) EnqueueForProcess(jobID string, processType string) error {
	payload, _ := json.Marshal(map[string]string{
		"job_id":       jobID,
		"process_type": processType,
	})

	// Get process-specific task type
	taskType := getTaskTypeForProcess(processType)
	task := asynq.NewTask(taskType, payload)
	_, err := q.client.Enqueue(task)

	return err
}

// getTaskTypeForProcess returns the appropriate task type for a process
func getTaskTypeForProcess(processType string) string {
	// Check if process exists in configurations
	if _, exists := ProcessConfigurations[processType]; exists {
		return fmt.Sprintf("task:%s", processType)
	}

	// Fallback for unknown process types
	return TaskTypeAnalysis // "task:analysis"
}

// GetTaskTypeForProcess is the exported version for external use
func GetTaskTypeForProcess(processType string) string {
	return getTaskTypeForProcess(processType)
}

// --- 2. Asynq Task Handlers (Worker Logic)
type TaskHandler struct {
	repo     ports.JobRepository
	notifier ports.Notifier
}

func NewTaskHandler(repo ports.JobRepository, notifier ports.Notifier) *TaskHandler {
	return &TaskHandler{repo: repo, notifier: notifier}
}

// executeGenericStep executes a generic step using process configuration
func (h *TaskHandler) executeGenericStep(ctx context.Context, jobID string, stepConfig *JobStepConfig) error {
	if len(stepConfig.SubSteps) == 0 {
		// No sub-steps, execute simple processing
		log.Printf("Job %s: Processing %s...", jobID, stepConfig.Description)
		time.Sleep(StepProcessingTime)
		return nil
	}

	// Execute with sub-steps
	actions := make([]func(), len(stepConfig.SubSteps))
	for i, subStep := range stepConfig.SubSteps {
		subStepDesc := subStep.Description
		actions[i] = func() {
			log.Printf("Job %s: %s", jobID, subStepDesc)
			time.Sleep(StepProcessingTime / time.Duration(len(stepConfig.SubSteps)))
		}
	}

	return h.executeStepWithSubCheckpoints(ctx, jobID, stepConfig.Name, actions)
}

// HandleAnalysisTask คือ Worker ที่ทำงานจริง
func (h *TaskHandler) HandleAnalysisTask(ctx context.Context, t *asynq.Task) error {
	jobID, err := h.extractJobID(t)
	if err != nil {
		return err
	}

	jobCtx, cancel := h.setupJobContext(ctx, jobID)

	initialJob, err := h.initializeJob(ctx, jobID)
	if err != nil {
		// If job doesn't exist, it's likely from previous session - just skip
		log.Printf("Skipping job %s as it doesn't exist in current session", jobID)
		return nil
	}

	// Skip if job is already canceled or completed
	if initialJob.Status == domain.StatusCanceled {
		return nil
	}

	// Handle paused jobs - they should be skipped until explicitly resumed
	if initialJob.Status == domain.StatusPaused {
		log.Printf("Job %s is PAUSED, skipping execution until resumed", jobID)
		return nil
	}

	startIndex := h.determineStartIndex(initialJob, jobID)

	// If job is already completed, skip all processing
	if startIndex == -1 {
		return nil
	}

	if startIndex >= len(JobSteps) {
		log.Printf("Job %s: All steps completed, proceeding to finalization.", jobID)
	}

	// Set job to running status
	if err := h.setJobRunning(jobCtx, initialJob); err != nil {
		return err
	}

	h.logJobStart(jobID, startIndex)

	// Process remaining steps
	if err := h.processJobSteps(jobCtx, jobID, startIndex, cancel); err != nil {
		return err
	}

	// Complete the job
	return h.completedJob(jobCtx, jobID, cancel)
}

// extractJobID extracts job ID from task payload
func (h *TaskHandler) extractJobID(t *asynq.Task) (string, error) {
	var payload map[string]string
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return "", fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	return payload["job_id"], nil
}

// setupJobContext creates job context with timeout and heartbeat
func (h *TaskHandler) setupJobContext(ctx context.Context, jobID string) (context.Context, context.CancelFunc) {
	jobCtx, cancel := context.WithTimeout(ctx, JobTimeout)

	// Start heartbeat goroutine
	heartbeatTicker := time.NewTicker(HeartbeatInterval)
	go func() {
		defer heartbeatTicker.Stop()
		for {
			select {
			case <-heartbeatTicker.C:
				log.Printf("Job %s heartbeat - still processing", jobID)
			case <-jobCtx.Done():
				return
			}
		}
	}()

	return jobCtx, cancel
}

// initializeJob retrieves and validates initial job state
func (h *TaskHandler) initializeJob(ctx context.Context, jobID string) (*domain.Job, error) {
	job, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		log.Printf("Job %s not found in repository (probably from previous session): %v", jobID, err)
		return nil, fmt.Errorf("failed to find job %s: %w", jobID, err)
	}

	return job, nil
}

// determineStartIndex calculates where to resume job processing
func (h *TaskHandler) determineStartIndex(job *domain.Job, jobID string) int {
	if job.CurrentCheckpoint == "" {
		return 0
	}

	if job.CurrentCheckpoint == CompletedCheckpoint {
		log.Printf("Job %s already completed. Skipping execution.", jobID)
		return -1
	}

	// Check for main step checkpoint
	if index, ok := StepIndexMap[job.CurrentCheckpoint]; ok {
		return index + 1
	}

	// Check for sub-checkpoint - find which main step it belongs to
	for mainStep, subSteps := range SubCheckpoints {
		for subIndex, subCheckpoint := range subSteps {
			if subCheckpoint == job.CurrentCheckpoint {
				mainStepIndex := StepIndexMap[mainStep]

				// If it's the last sub-checkpoint of a step, move to next main step
				if subIndex == len(subSteps)-1 {
					log.Printf("Job %s: Resuming from next step after completing %s", jobID, mainStep)
					return mainStepIndex + 1
				}

				// Otherwise, resume the current main step (it will handle sub-checkpoint internally)
				log.Printf("Job %s: Resuming %s from sub-checkpoint %s", jobID, mainStep, job.CurrentCheckpoint)
				return mainStepIndex
			}
		}
	}

	log.Printf("Warning: job %s has unknown checkpoint: %s. Starting from beginning.", jobID, job.CurrentCheckpoint)
	return 0
}

// setJobRunning updates job status to running
func (h *TaskHandler) setJobRunning(ctx context.Context, job *domain.Job) error {
	job.Status = domain.StatusRunning
	if err := h.repo.Save(ctx, job); err != nil {
		return fmt.Errorf("failed to save job status: %w", err)
	}
	h.notifier.BroadcastUpdate(job)

	return nil
}

// logJobStart logs the start of job processing
func (h *TaskHandler) logJobStart(jobID string, startIndex int) {
	if startIndex < len(JobSteps) {
		log.Printf("Starting job: %s. Resuming from step: %s", jobID, JobSteps[startIndex])
	} else {
		log.Printf("Starting job: %s. All steps completed, finalizing...", jobID)
	}
}

// processJobSteps handles the main job processing loop
func (h *TaskHandler) processJobSteps(ctx context.Context, jobID string, startIndex int, cancel context.CancelFunc) error {
	totalSteps := len(JobSteps)

	for i := startIndex; i < totalSteps; i++ {
		currentStepName := JobSteps[i]

		// Check current job status
		currentJob, err := h.repo.FindByID(ctx, jobID)
		if err != nil {
			return fmt.Errorf("failed to find job in processing loop: %w", err)
		}

		// Handle pause/cancel states
		if err := h.handleJobStateChanges(ctx, jobID, currentJob, cancel); err != nil {
			return err
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			log.Printf("Job %s CANCELED (via context)", jobID)
			return nil
		default:
			// Continue processing
		}

		// Execute specific step function
		stepExecutor := getStepExecutor(currentStepName)
		if stepExecutor == nil {
			log.Printf("Job %s: Unknown step %s, using default processing", jobID, currentStepName)
			log.Printf("Job %s: Running task: %s", jobID, currentStepName)
			time.Sleep(StepProcessingTime)
		} else {
			if err := stepExecutor(h, ctx, jobID); err != nil {
				return fmt.Errorf("failed to execute step %s: %w", currentStepName, err)
			}
		}

		// Check if job was preempted during processing
		if shouldSkipProgress, err := h.checkJobPreemption(ctx, jobID, i, cancel); err != nil {
			return nil
		} else if shouldSkipProgress {
			continue
		}

		// Save progress
		if err := h.saveStepProgress(ctx, jobID, currentStepName, totalSteps); err != nil {
			return err
		}
	}

	return nil
}

func (h *TaskHandler) handleJobStateChanges(ctx context.Context, jobID string, job *domain.Job, cancel context.CancelFunc) error {
	if job.Status == domain.StatusCanceled {
		log.Printf("Job %s CANCELED", jobID)
		cancel()
		return nil
	}

	if job.Status == domain.StatusPaused {
		log.Printf("Job %s PAUSED - task will exit and wait for resume", jobID)
		// Save current state and exit task - let resume create new task
		return fmt.Errorf("job paused - task exiting to allow resume")
	}

	return nil
}

// checkJobPreemption checks if job was paused/canceled during step processing
func (h *TaskHandler) checkJobPreemption(ctx context.Context, jobID string, stepIndex int, cancel context.CancelFunc) (bool, error) {
	jobAfterWork, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return false, fmt.Errorf("failed to check job after work: %w", err)
	}

	if jobAfterWork.Status != domain.StatusRunning {
		log.Printf("Job %s was preempted (Status %s), discarding progress for step %d", jobID, jobAfterWork.Status, stepIndex+1)

		if jobAfterWork.Status == domain.StatusCanceled {
			cancel()
		}

		return true, nil //Skip saving progress
	}

	return false, nil // Continue with saving progress
}

// saveStepProgress saves the current step progress
func (h *TaskHandler) saveStepProgress(ctx context.Context, jobID string, stepName string, totalSteps int) error {
	job, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to find job for progress update: %w", err)
	}

	// Get human-readable step description
	stepDescription, exists := StepDescriptions[stepName]
	if !exists {
		stepDescription = stepName // Fallback to step name
	}

	log.Printf("Job %s: Saving Checkpoint: %s (%s)", jobID, stepName, stepDescription)
	job.CurrentCheckpoint = stepName
	job.CurrentStepName = stepDescription
	job.Progress = h.CalculateProgress(stepName, totalSteps)

	if err := h.repo.Save(ctx, job); err != nil {
		log.Printf("Error saving job %s:%v", jobID, err)
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	h.notifier.BroadcastUpdate(job)
	return nil
}

// completeJob finalizes job completion
func (h *TaskHandler) completedJob(ctx context.Context, jobID string, cancel context.CancelFunc) error {
	log.Printf("Job %s COMPLETED:", jobID)

	finalJob, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to get final job state: %w", err)
	}

	// Check if job was preempted before completion
	if finalJob.Status != domain.StatusRunning {
		log.Printf("Job %s was preempted before completion (Status: %s)", finalJob.FileName, finalJob.Status)
		if finalJob.Status == domain.StatusCanceled {
			cancel()
		}

		return nil
	}

	// Mark job as completed
	finalJob.CurrentCheckpoint = CompletedCheckpoint
	finalJob.CurrentStepName = StepDescriptions["COMPLETED"]
	finalJob.Status = domain.StatusCompleted
	finalJob.Progress = 100

	if err := h.repo.Save(ctx, finalJob); err != nil {
		return fmt.Errorf("failed to save completed job: %w", err)
	}

	h.notifier.BroadcastUpdate(finalJob)
	return nil
}

func (h *TaskHandler) CalculateProgress(checkpoint string, totalStep int) int {
	if checkpoint == "" {
		return 0
	}

	// ถ้า checkpoint เป็น "COMPLETED" ให้ return 100%
	if checkpoint == "COMPLETED" {
		return 100
	}

	if index, ok := StepIndexMap[checkpoint]; ok {
		// คำนวณ progress โดยให้ step สุดท้ายได้แค่ 95%
		// เฉพาะ "COMPLETED" เท่านั้นที่จะได้ 100%
		stepProgress := ((index + 1) * 95) / totalStep

		// ป้องกันไม่ให้เกิน 95% จนกว่าจะ COMPLETED จริงๆ
		if stepProgress > 95 {
			stepProgress = 95
		}

		return stepProgress
	}

	return 0
}
