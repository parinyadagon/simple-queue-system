package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"simple-queue-103/internal/core/domain"
	"simple-queue-103/internal/core/ports"
	"time"

	"github.com/hibiken/asynq"
)

const (
	TaskTypeAnalysis = "task:analysis"
	RedisAddr        = "127.0.0.1:6379"

	// Job processing constants
	JobTimeout                = 30 * time.Minute
	HeartbeatInterval         = 1 * time.Minute
	StatusCheckInterval       = 2 * time.Second
	StepProcessingTime        = 2 * time.Second
	CompletedCheckpoint       = "COMPLETED"
	MaxProgressBeforeComplete = 95
)

var JobSteps = []string{
	"DOWNLOAD_SOURCE",
	"DECOMPRESS_FILE",
	"CLEANING_DATA",
	"ANALYSIS_MODEL_A",
	"ANALYSIS_MODEL_B",
	"GENERATING_REPORT",
}

var StepIndexMap = func() map[string]int {
	m := make(map[string]int)
	for i, step := range JobSteps {
		m[step] = i
	}

	return m
}()

// --- 1. Asynq Client (Implement JobQueue) ---
type asynqJobQueue struct {
	client *asynq.Client
}

func NewAsynqJobQueue() ports.JobQueue {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: RedisAddr})

	return &asynqJobQueue{client: client}
}

func (q *asynqJobQueue) EnqueueAnalysis(jobID string) error {
	payload, _ := json.Marshal(map[string]string{"job_id": jobID})
	task := asynq.NewTask(TaskTypeAnalysis, payload)
	_, err := q.client.Enqueue(task)

	return err
}

// --- 2. Asynq Task Handlers (Worker Logic)
type TaskHandler struct {
	repo     ports.JobRepository
	notifier ports.Notifier
}

func NewTaskHandler(repo ports.JobRepository, notifier ports.Notifier) *TaskHandler {
	return &TaskHandler{repo: repo, notifier: notifier}
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
		return err
	}

	// Skip if job is already canceled or completed
	if initialJob.Status == domain.StatusCanceled {
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

	if index, ok := StepIndexMap[job.CurrentCheckpoint]; ok {
		return index + 1
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

		// Simulate work processing
		log.Printf("Job %s: Running task: %s", jobID, currentStepName)
		time.Sleep(StepProcessingTime)

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
	for job.Status == domain.StatusPaused || job.Status == domain.StatusCanceled {
		if job.Status == domain.StatusCanceled {
			log.Printf("Job%s CANCELED (at start of loop)", jobID)
			cancel()
			return nil
		}

		// Job is paused, wait and recheck
		log.Printf("Job %s PAUSED, waiting...", jobID)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(StatusCheckInterval):
			var err error
			job, err = h.repo.FindByID(ctx, jobID)
			if err != nil {
				return fmt.Errorf("failed to check job status during pause: %w", err)
			}
		}
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

	log.Printf("Job %s: Saving Checkpoint: %s", jobID, stepName)
	job.CurrentCheckpoint = stepName
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
