package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"simple-queue-103/internal/core/domain"
	"simple-queue-103/internal/core/ports"
	"simple-queue-103/internal/lib/process"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq"
)

// ProcessTaskHandler provides process-isolated job handling
type ProcessTaskHandler struct {
	repo          ports.JobRepository
	notifier      ports.Notifier
	processType   string
	processConfig *process.JobProcessConfig
}

// NewProcessTaskHandler creates a process-specific task handler
func NewProcessTaskHandler(repo ports.JobRepository, notifier ports.Notifier, processType string) *ProcessTaskHandler {
	config, exists := process.ProcessConfigurations[processType]
	if !exists {
		log.Printf("Warning: Process type '%s' not found, using data_analysis as fallback", processType)
		config = process.ProcessConfigurations["data_analysis"]
	}

	return &ProcessTaskHandler{
		repo:          repo,
		notifier:      notifier,
		processType:   processType,
		processConfig: config,
	}
}

// HandleAnalysisTask processes jobs for the specific process type
func (h *ProcessTaskHandler) HandleAnalysisTask(ctx context.Context, t *asynq.Task) error {
	jobID, err := h.extractJobID(t)
	if err != nil {
		return err
	}

	log.Printf("🔄 ProcessTaskHandler[%s]: Starting job %s", h.processType, jobID)

	// Verify job belongs to this process
	job, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		log.Printf("⚠️ ProcessTaskHandler[%s]: Job %s not found, skipping", h.processType, jobID)
		return nil
	}

	// Process isolation check
	if job.ProcessType != h.processType {
		log.Printf("🚫 ProcessTaskHandler[%s]: Job %s belongs to process '%s', rejecting",
			h.processType, jobID, job.ProcessType)
		return fmt.Errorf("job process type mismatch: expected %s, got %s", h.processType, job.ProcessType)
	}

	jobCtx, cancel := h.setupJobContext(ctx, jobID)
	defer cancel()

	// Skip if job is already canceled or completed
	if job.Status == domain.StatusCanceled || job.Status == domain.StatusCompleted {
		log.Printf("✅ ProcessTaskHandler[%s]: Job %s already in final state: %s",
			h.processType, jobID, job.Status)
		return nil
	}

	// Handle paused jobs
	if job.Status == domain.StatusPaused {
		log.Printf("⏸️ ProcessTaskHandler[%s]: Job %s is PAUSED, skipping execution", h.processType, jobID)
		return nil
	}

	startIndex := h.determineStartIndex(job, jobID)
	if startIndex == -1 {
		return nil // Job already completed
	}

	// Set job to running status
	if err := h.setJobRunning(jobCtx, job); err != nil {
		return err
	}

	log.Printf("🚀 ProcessTaskHandler[%s]: Processing job %s from step %d",
		h.processType, jobID, startIndex)

	// Process job steps using process-specific configuration
	if err := h.processJobSteps(jobCtx, jobID, startIndex, cancel); err != nil {
		return err
	}

	// Complete the job
	return h.completeJob(jobCtx, jobID, cancel)
}

// determineStartIndex calculates where to resume job processing using process-specific steps
func (h *ProcessTaskHandler) determineStartIndex(job *domain.Job, jobID string) int {
	if job.CurrentCheckpoint == "" {
		return 0
	}

	if job.CurrentCheckpoint == CompletedCheckpoint {
		log.Printf("✅ ProcessTaskHandler[%s]: Job %s already completed", h.processType, jobID)
		return -1
	}

	// Use process-specific steps for calculation
	stepIndexMap := h.getProcessStepIndexMap()

	// Check for main step checkpoint
	if index, ok := stepIndexMap[job.CurrentCheckpoint]; ok {
		return index + 1
	}

	// Check for sub-checkpoint
	processSubCheckpoints := h.getProcessSubCheckpoints()
	for mainStep, subSteps := range processSubCheckpoints {
		for subIndex, subCheckpoint := range subSteps {
			if subCheckpoint == job.CurrentCheckpoint {
				mainStepIndex := stepIndexMap[mainStep]

				// If it's the last sub-checkpoint of a step, move to next main step
				if subIndex == len(subSteps)-1 {
					log.Printf("⏭️ ProcessTaskHandler[%s]: Job %s resuming from next step after completing %s",
						h.processType, jobID, mainStep)
					return mainStepIndex + 1
				}

				// Otherwise, resume the current main step
				log.Printf("🔄 ProcessTaskHandler[%s]: Job %s resuming %s from sub-checkpoint %s",
					h.processType, jobID, mainStep, job.CurrentCheckpoint)
				return mainStepIndex
			}
		}
	}

	log.Printf("⚠️ ProcessTaskHandler[%s]: Job %s has unknown checkpoint: %s. Starting from beginning.",
		h.processType, jobID, job.CurrentCheckpoint)
	return 0
}

// processJobSteps handles the main job processing loop using process-specific configuration
func (h *ProcessTaskHandler) processJobSteps(ctx context.Context, jobID string, startIndex int, cancel context.CancelFunc) error {
	processSteps := h.getProcessSteps()
	totalSteps := len(processSteps)

	for i := startIndex; i < totalSteps; i++ {
		currentStepName := processSteps[i]

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
			log.Printf("❌ ProcessTaskHandler[%s]: Job %s CANCELED (via context)", h.processType, jobID)
			return nil
		default:
			// Continue processing
		}

		// Execute step using process-specific executor
		stepExecutor := h.getProcessStepExecutor(currentStepName)
		if stepExecutor == nil {
			log.Printf("⚠️ ProcessTaskHandler[%s]: Job %s - Unknown step %s, using default processing",
				h.processType, jobID, currentStepName)
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

		// Save progress using process-specific descriptions
		if err := h.saveProcessStepProgress(ctx, jobID, currentStepName, totalSteps); err != nil {
			return err
		}
	}

	return nil
}

// Process-specific helper methods
func (h *ProcessTaskHandler) getProcessSteps() []string {
	steps := make([]string, len(h.processConfig.Steps))
	for i, step := range h.processConfig.Steps {
		steps[i] = step.Name
	}
	return steps
}

func (h *ProcessTaskHandler) getProcessStepIndexMap() map[string]int {
	stepMap := make(map[string]int)
	for i, step := range h.processConfig.Steps {
		stepMap[step.Name] = i
	}
	return stepMap
}

func (h *ProcessTaskHandler) getProcessSubCheckpoints() map[string][]string {
	checkpoints := make(map[string][]string)
	for _, step := range h.processConfig.Steps {
		subSteps := make([]string, len(step.SubSteps))
		for i, subStep := range step.SubSteps {
			subSteps[i] = subStep.Name
		}
		checkpoints[step.Name] = subSteps
	}
	return checkpoints
}

func (h *ProcessTaskHandler) getProcessStepDescriptions() map[string]string {
	descriptions := make(map[string]string)

	// Add main step descriptions
	for _, step := range h.processConfig.Steps {
		descriptions[step.Name] = step.Description

		// Add sub-step descriptions
		for _, subStep := range step.SubSteps {
			descriptions[subStep.Name] = subStep.Description
		}
	}

	// Add special states
	descriptions["COMPLETED"] = "งานเสร็จสิ้นแล้ว"

	return descriptions
}

// GetProcessStepExecutor is exported version for testing
func (h *ProcessTaskHandler) GetProcessStepExecutor(stepName string) ProcessStepFunction {
	return h.getProcessStepExecutor(stepName)
}

func (h *ProcessTaskHandler) getProcessStepExecutor(stepName string) ProcessStepFunction {
	// First, try to get from process configuration
	for _, step := range h.processConfig.Steps {
		if step.Name == stepName {
			if step.ExecuteFunc != nil {
				return func(handler *ProcessTaskHandler, ctx context.Context, jobID string) error {
					return step.ExecuteFunc(ctx, jobID, &step)
				}
			}
			// If no custom function, use process-specific generic execution
			return func(handler *ProcessTaskHandler, ctx context.Context, jobID string) error {
				return handler.executeProcessGenericStep(ctx, jobID, &step)
			}
		}
	}

	// Fallback to original TaskHandler methods if needed
	return func(handler *ProcessTaskHandler, ctx context.Context, jobID string) error {
		taskHandler := &TaskHandler{repo: handler.repo, notifier: handler.notifier}
		switch stepName {
		case "DOWNLOAD_SOURCE":
			return taskHandler.executeDownloadSource(ctx, jobID)
		case "DECOMPRESS_FILE":
			return taskHandler.executeDecompressFile(ctx, jobID)
		case "CLEANING_DATA":
			return taskHandler.executeCleaningData(ctx, jobID)
		case "ANALYSIS_MODEL_A":
			return taskHandler.executeAnalysisModelA(ctx, jobID)
		case "ANALYSIS_MODEL_B":
			return taskHandler.executeAnalysisModelB(ctx, jobID)
		case "GENERATING_REPORT":
			return taskHandler.executeGeneratingReport(ctx, jobID)
		default:
			log.Printf("⚠️ ProcessTaskHandler[%s]: Unknown step %s", handler.processType, stepName)
			time.Sleep(StepProcessingTime)
			return nil
		}
	}
}

// ProcessStepFunction defines the signature for process step processing functions
type ProcessStepFunction func(h *ProcessTaskHandler, ctx context.Context, jobID string) error

// executeProcessGenericStep executes a generic step using process-specific sub-checkpoints
func (h *ProcessTaskHandler) executeProcessGenericStep(ctx context.Context, jobID string, stepConfig *process.JobStepConfig) error {
	if len(stepConfig.SubSteps) == 0 {
		// No sub-steps, execute simple processing
		log.Printf("🔄 ProcessTaskHandler[%s]: Job %s - Processing %s...", h.processType, jobID, stepConfig.Description)
		time.Sleep(StepProcessingTime)
		return nil
	}

	// Execute with process-specific sub-steps
	for _, subStep := range stepConfig.SubSteps {
		// Save sub-checkpoint using actual sub-step name from process config
		if err := h.saveProcessSubCheckpoint(ctx, jobID, stepConfig.Name, subStep.Name); err != nil {
			return err
		}

		// Execute the sub-step
		log.Printf("🔄 ProcessTaskHandler[%s]: Job %s - %s", h.processType, jobID, subStep.Description)
		if subStep.Duration > 0 {
			time.Sleep(subStep.Duration)
		}

		// Call custom action if provided
		if subStep.Action != nil {
			subStep.Action()
		}
	}

	return nil
}

// saveProcessSubCheckpoint saves a sub-checkpoint with process-specific details
func (h *ProcessTaskHandler) saveProcessSubCheckpoint(ctx context.Context, jobID string, mainStep string, subCheckpoint string) error {
	job, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to find job for sub-checkpoint update: %w", err)
	}

	// Check if job was cancelled/paused during processing
	if job.Status != domain.StatusRunning {
		log.Printf("⚠️ ProcessTaskHandler[%s]: Job %s was preempted, skipping sub-checkpoint %s", h.processType, jobID, subCheckpoint)
		return nil
	}

	// Get human-readable step description from process config
	stepDescription := h.getProcessStepDescription(subCheckpoint)
	if stepDescription == "" {
		stepDescription = subCheckpoint // Fallback to checkpoint name
	}

	log.Printf("📋 ProcessTaskHandler[%s]: Job %s - Sub-checkpoint: %s (%s)", h.processType, jobID, subCheckpoint, stepDescription)
	job.CurrentCheckpoint = subCheckpoint
	job.CurrentStepName = stepDescription
	job.Progress = h.calculateProcessProgress(subCheckpoint, len(h.processConfig.Steps))

	if err := h.repo.Save(ctx, job); err != nil {
		log.Printf("⚠️ ProcessTaskHandler[%s]: Error saving sub-checkpoint for job %s: %v", h.processType, jobID, err)
		return fmt.Errorf("failed to save sub-checkpoint: %w", err)
	}

	h.notifier.BroadcastUpdate(job)
	return nil
}

// getProcessStepDescription gets description from process configuration
func (h *ProcessTaskHandler) getProcessStepDescription(checkpoint string) string {
	// Check main steps
	for _, step := range h.processConfig.Steps {
		if step.Name == checkpoint {
			return step.Description
		}
		// Check sub-steps
		for _, subStep := range step.SubSteps {
			if subStep.Name == checkpoint {
				return subStep.Description
			}
		}
	}

	// Special states
	if checkpoint == CompletedCheckpoint {
		return "งานเสร็จสิ้นแล้ว"
	}

	return ""
}

// Helper methods (reuse from original TaskHandler with process context)
func (h *ProcessTaskHandler) extractJobID(t *asynq.Task) (string, error) {
	var payload map[string]string
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return "", fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	return payload["job_id"], nil
}

func (h *ProcessTaskHandler) setupJobContext(ctx context.Context, jobID string) (context.Context, context.CancelFunc) {
	jobCtx, cancel := context.WithTimeout(ctx, JobTimeout)

	// Start heartbeat goroutine with process info
	heartbeatTicker := time.NewTicker(HeartbeatInterval)
	go func() {
		defer heartbeatTicker.Stop()
		for {
			select {
			case <-heartbeatTicker.C:
				log.Printf("💓 ProcessTaskHandler[%s]: Job %s heartbeat - still processing", h.processType, jobID)
			case <-jobCtx.Done():
				return
			}
		}
	}()

	return jobCtx, cancel
}

func (h *ProcessTaskHandler) setJobRunning(ctx context.Context, job *domain.Job) error {
	job.Status = domain.StatusRunning
	if err := h.repo.Save(ctx, job); err != nil {
		return fmt.Errorf("failed to save job status: %w", err)
	}
	h.notifier.BroadcastUpdate(job)
	return nil
}

func (h *ProcessTaskHandler) handleJobStateChanges(ctx context.Context, jobID string, job *domain.Job, cancel context.CancelFunc) error {
	if job.Status == domain.StatusCanceled {
		log.Printf("❌ ProcessTaskHandler[%s]: Job %s CANCELED", h.processType, jobID)
		cancel()
		return nil
	}

	if job.Status == domain.StatusPaused {
		log.Printf("⏸️ ProcessTaskHandler[%s]: Job %s PAUSED - task will exit and wait for resume", h.processType, jobID)
		return fmt.Errorf("job paused - task exiting to allow resume")
	}

	return nil
}

func (h *ProcessTaskHandler) checkJobPreemption(ctx context.Context, jobID string, stepIndex int, cancel context.CancelFunc) (bool, error) {
	jobAfterWork, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return false, fmt.Errorf("failed to check job after work: %w", err)
	}

	if jobAfterWork.Status != domain.StatusRunning {
		log.Printf("⚠️ ProcessTaskHandler[%s]: Job %s was preempted (Status %s), discarding progress for step %d",
			h.processType, jobID, jobAfterWork.Status, stepIndex+1)

		if jobAfterWork.Status == domain.StatusCanceled {
			cancel()
		}

		return true, nil // Skip saving progress
	}

	return false, nil // Continue with saving progress
}

func (h *ProcessTaskHandler) saveProcessStepProgress(ctx context.Context, jobID string, stepName string, totalSteps int) error {
	job, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to find job for progress update: %w", err)
	}

	// Get process-specific step description
	processDescriptions := h.getProcessStepDescriptions()
	stepDescription, exists := processDescriptions[stepName]
	if !exists {
		stepDescription = stepName // Fallback to step name
	}

	log.Printf("💾 ProcessTaskHandler[%s]: Job %s - Saving Checkpoint: %s (%s)",
		h.processType, jobID, stepName, stepDescription)

	job.CurrentCheckpoint = stepName
	job.CurrentStepName = stepDescription
	job.Progress = h.calculateProcessProgress(stepName, totalSteps)

	if err := h.repo.Save(ctx, job); err != nil {
		log.Printf("❌ ProcessTaskHandler[%s]: Error saving job %s: %v", h.processType, jobID, err)
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	h.notifier.BroadcastUpdate(job)
	return nil
}

func (h *ProcessTaskHandler) calculateProcessProgress(checkpoint string, totalSteps int) int {
	if checkpoint == "" {
		return 0
	}

	if checkpoint == CompletedCheckpoint {
		return 100
	}

	// Use actual number of steps from process configuration
	actualTotalSteps := len(h.processConfig.Steps)
	stepIndexMap := h.getProcessStepIndexMap()

	// Check for main step checkpoint
	if index, ok := stepIndexMap[checkpoint]; ok {
		stepProgress := ((index + 1) * MaxProgressBeforeComplete) / actualTotalSteps
		if stepProgress > MaxProgressBeforeComplete {
			stepProgress = MaxProgressBeforeComplete
		}
		return stepProgress
	}

	// Check for sub-checkpoint and calculate more detailed progress
	processSubCheckpoints := h.getProcessSubCheckpoints()
	for mainStep, subSteps := range processSubCheckpoints {
		for subIndex, subCheckpoint := range subSteps {
			if subCheckpoint == checkpoint {
				if mainStepIndex, exists := stepIndexMap[mainStep]; exists {
					// Calculate progress within the main step
					subProgress := float64(subIndex+1) / float64(len(subSteps))
					stepWeight := float64(MaxProgressBeforeComplete) / float64(actualTotalSteps)
					mainStepProgress := float64(mainStepIndex) * stepWeight
					currentStepProgress := stepWeight * subProgress

					finalProgress := int(mainStepProgress + currentStepProgress)
					if finalProgress > MaxProgressBeforeComplete {
						finalProgress = MaxProgressBeforeComplete
					}
					return finalProgress
				}
			}
		}
	}

	// Fallback: try to parse substep pattern (STEPNAME_substep_N)
	processSteps := h.getProcessSteps()
	for stepIndex, stepName := range processSteps {
		if strings.HasPrefix(checkpoint, stepName+"_substep_") {
			subStepStr := strings.TrimPrefix(checkpoint, stepName+"_substep_")
			if subStepIndex, err := strconv.Atoi(subStepStr); err == nil && subStepIndex > 0 {
				// Assume 3 sub-steps per main step for fallback
				subStepsCount := 3
				subProgress := float64(subStepIndex) / float64(subStepsCount)
				stepWeight := float64(MaxProgressBeforeComplete) / float64(actualTotalSteps)
				mainStepProgress := float64(stepIndex) * stepWeight
				currentStepProgress := stepWeight * subProgress

				finalProgress := int(mainStepProgress + currentStepProgress)
				if finalProgress > MaxProgressBeforeComplete {
					finalProgress = MaxProgressBeforeComplete
				}
				return finalProgress
			}
		}
	}

	return 0
}

func (h *ProcessTaskHandler) completeJob(ctx context.Context, jobID string, cancel context.CancelFunc) error {
	log.Printf("🎉 ProcessTaskHandler[%s]: Job %s COMPLETED", h.processType, jobID)

	finalJob, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to get final job state: %w", err)
	}

	// Check if job was preempted before completion
	if finalJob.Status != domain.StatusRunning {
		log.Printf("⚠️ ProcessTaskHandler[%s]: Job %s was preempted before completion (Status: %s)",
			h.processType, finalJob.FileName, finalJob.Status)
		if finalJob.Status == domain.StatusCanceled {
			cancel()
		}
		return nil
	}

	// Mark job as completed
	finalJob.CurrentCheckpoint = CompletedCheckpoint
	finalJob.CurrentStepName = "งานเสร็จสิ้นแล้ว"
	finalJob.Status = domain.StatusCompleted
	finalJob.Progress = 100

	if err := h.repo.Save(ctx, finalJob); err != nil {
		return fmt.Errorf("failed to save completed job: %w", err)
	}

	h.notifier.BroadcastUpdate(finalJob)
	log.Printf("✅ ProcessTaskHandler[%s]: Job %s completed successfully", h.processType, jobID)
	return nil
}
