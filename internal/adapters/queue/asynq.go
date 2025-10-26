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
	var payload map[string]string
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	jobID := payload["job_id"]

	// ใช้ context ที่ส่งมาจาก Asynq แทนการสร้างใหม่
	// เพื่อให้สามารถ handle timeout และ cancellation ได้ถูกต้อง
	// เพิ่ม timeout 30 นาที สำหรับงานที่ทำนานเกินไป
	jobCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	// เพิ่ม heartbeat เพื่อบอกว่างานยังทำงานอยู่
	heartbeatTicker := time.NewTicker(1 * time.Minute)
	defer heartbeatTicker.Stop()

	go func() {
		for {
			select {
			case <-heartbeatTicker.C:
				log.Printf("Job %s heartbeat - still processing", jobID)
			case <-jobCtx.Done():
				return
			}
		}
	}()

	// --- 1. ตั้งค่าสถานะเริ่มต้นเป็น RUNNING ---
	// (ใช้ 'initialJob' แค่ครั้งนี้ครั้งเดียว)
	initialJob, err := h.repo.FindByID(jobCtx, jobID)
	if err != nil {
		return fmt.Errorf("failed to find job %s: %w", jobID, err)
	}

	// ถ้า Job ถูกสั่ง Cancel ก่อนที่จะเริ่มได้
	if initialJob.Status == domain.StatusCanceled {
		return nil
	}

	startIndex := 0
	if initialJob.CurrentCheckpoint != "" {
		// ถ้า checkpoint เป็น "COMPLETED" แสดงว่างานเสร็จสิ้นแล้ว
		if initialJob.CurrentCheckpoint == "COMPLETED" {
			log.Printf("Job %s already completed. Skipping execution.", jobID)
			return nil
		}

		// ถ้ามี Checkpoint เก่า ให้หา Index ของ Checkpoint นั้น
		// และเริ่มทำต่อจาก *ขั้นตอนถัดไป* (+1)
		if index, ok := StepIndexMap[initialJob.CurrentCheckpoint]; ok {
			startIndex = index + 1
		} else {
			// Checkpoint ไม่ถูกต้อง (งานถูกแก้ไข), ให้เริ่มต้นใหม่ หรือ Log Error
			log.Printf("Warning: job %s has unknown checkpoint: %s. Starting from beginning.", jobID, initialJob.CurrentCheckpoint)
			startIndex = 0
		}
	}

	if startIndex >= len(JobSteps) {
		// ถ้า startIndex >= len(JobSteps) แสดงว่าทำ steps ครบแล้ว
		// ต้องไปทำ completion step (set COMPLETED)
		log.Printf("Job %s: All steps completed, proceeding to finalization.", jobID)
		// ไม่ return nil ให้ไปทำ completion ต่อ
	}

	totalStep := len(JobSteps)

	initialJob.Status = domain.StatusRunning
	if err := h.repo.Save(jobCtx, initialJob); err != nil {
		return fmt.Errorf("failed to save job status: %w", err)
	}
	h.notifier.BroadcastUpdate(initialJob)

	if startIndex < len(JobSteps) {
		log.Printf("Starting job: %s. Resuming from step: %s", jobID, JobSteps[startIndex])
	} else {
		log.Printf("Starting job: %s. All steps completed, finalizing...", jobID)
	}

	// --- 2. เริ่ม Loop การทำงานจากจุดที่ค้างไว้ (The Fix) ---
	// วน Loop ด้วย index ตั้งแต่ startIndex จนถึงจำนวน Steps ทั้งหมด
	for i := startIndex; i < totalStep; i++ {
		currentStepName := JobSteps[i]

		// --- 2A. ดึงสถานะล่าสุด (Source of Truth) ---
		// *ต้อง* ดึงใหม่ทุกครั้งที่เริ่ม Loop
		currentJob, err := h.repo.FindByID(jobCtx, jobID)

		if err != nil {
			return fmt.Errorf("failed to find job in processing loop: %w", err)
		}

		// สร้าง Loop ตรวจสอบสถานะ (สำหรับ Pause/Cancel)
		for currentJob.Status == domain.StatusPaused || currentJob.Status == domain.StatusCanceled {
			if currentJob.Status == domain.StatusCanceled {
				log.Printf("Job %s CANCELED (at start of loop)", jobID)
				cancel()
				return nil
			}

			// ถ้่า Paused
			log.Printf("Job %s PAUSED, waiting...", jobID)

			select {
			case <-jobCtx.Done(): // ถูก Cancel ระหว่าง Pause
				return nil
			case <-time.After(2 * time.Second):
				// ครบ 2 วิ, ไปเช็ค DB ใหม่
				currentJob, err = h.repo.FindByID(jobCtx, jobID)
				if err != nil {
					return fmt.Errorf("failed to check job status during pause: %w", err)
				}
				// วนกลับไปเช็ค while loop (status == PAUSED or CANCELED)
			}
		}
		// ถ้าหลุดจาก Loop นี้มาได้ แสดงว่า Status คือ

		// --- 2B. ตรวจสอบ Context ---
		select {
		case <-jobCtx.Done():
			log.Printf("Job %s CANCELED (via context)", jobID)
			return nil
		default:
			// OK, สถานะคือ RUNNING
		}

		// --- 2C. ทำงานหนัก (จำลอง) ---
		log.Printf("Job %s: Running task: %s", jobID, currentStepName)
		time.Sleep(2 * time.Second) // <-- ‼️ นี่คือ "หน้าต่างเวลา" ของ Race Condition ‼️

		// --- 2D. ตรวจสอบสถานะอีกครั้งหลังจากทำงานเสร็จ
		jobAfterWork, err := h.repo.FindByID(jobCtx, jobID)
		if err != nil {
			return fmt.Errorf("failed to check job after work: %w", err)
		}

		// ถ้า User กด Pause/Cancel ระหว่างที่เรากำลัง Sleep 2 วิ
		if jobAfterWork.Status != domain.StatusRunning {
			log.Printf("Job %s was preempted (Status %s), discarding progress for step %d", jobID, jobAfterWork.Status, i+1)

			if jobAfterWork.Status == domain.StatusCanceled {
				cancel()
			}

			// วนกลับไปที่ Loop 'for' (i++) เพื่อประเมิณสถานะใหม่ (Pause/Cancel)
			// เรา *ไม่* บันทึก Progress ของ step นี้
			continue
		}

		// --- 2E. บันทึกความคืบหน้า (ถ้าปลอดภัย)
		// เรา *ไม่* บันทึก Progress ของ step นี้
		log.Printf("Job %s: Saving Checkpoint: %s", jobID, currentStepName)
		jobAfterWork.CurrentCheckpoint = currentStepName
		jobAfterWork.Progress = h.CalculateProgress(currentStepName, totalStep)
		if err := h.repo.Save(jobCtx, jobAfterWork); err != nil {
			log.Printf("Error saving job %s: %v", jobID, err)
			return fmt.Errorf("failed to save checkpoint: %w", err)
		}
		h.notifier.BroadcastUpdate(jobAfterWork)
	}

	// --- 3. งานเสร็จสิ้น ---
	// (ตรรกะนี้กีต้องเช็ค Race Condition ด้วย)
	log.Printf("Job %s COMPLETED:", jobID)

	finalJob, err := h.repo.FindByID(jobCtx, jobID)
	if err != nil {
		return fmt.Errorf("failed to get final job state: %w", err)
	}

	// ถ้าถูสั่งให้ Pause/Cancel วินาทีก่อนที่จะ Complete?
	if finalJob.Status != domain.StatusRunning {
		log.Printf("Job %s was preempted before completion (Status: %s)", finalJob.FileName, finalJob.Status)
		if finalJob.Status == domain.StatusCanceled {
			cancel()
		}

		return nil // ไม่ต้องตั้งค่าเป็น COMPLETED
	}

	finalJob.CurrentCheckpoint = "COMPLETED"
	finalJob.Status = domain.StatusCompleted
	finalJob.Progress = 100
	if err := h.repo.Save(jobCtx, finalJob); err != nil {
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
