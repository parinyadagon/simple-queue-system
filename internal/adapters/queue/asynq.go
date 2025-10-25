package queue

import (
	"context"
	"encoding/json"
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
	json.Unmarshal(t.Payload(), &payload)
	jobID := payload["job_id"]

	// สร้าง Ctx สำหรับ Cancel
	jobCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- 1. ตั้งค่าสถานะเริ่มต้นเป็น RUNNING ---
	// (ใช้ 'initialJob' แค่ครั้งนี้ครั้งเดียว)
	initialJob, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return err
	}

	// ถ้า Job ถูกสั่ง Cancel ก่อนที่จะเริ่มได้
	if initialJob.Status == domain.StatusCanceled {
		return nil
	}

	startIndex := 0
	if initialJob.CurrentCheckpoint != "" {
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
		log.Printf("Job %s already completed. Skipping retry.", jobID)

		return nil
	}

	totalStep := len(JobSteps)

	initialJob.Status = domain.StatusRunning
	h.repo.Save(ctx, initialJob) // บันทึกว่าเริ่ม RUNNING
	h.notifier.BroadcastUpdate(initialJob)

	log.Printf("Starting job: %s. Resuming from step: %s", jobID, JobSteps[startIndex])

	// --- 2. เริ่ม Loop การทำงานจากจุดที่ค้างไว้ (The Fix) ---
	// วน Loop ด้วย index ตั้งแต่ startIndex จนถึงจำนวน Steps ทั้งหมด
	for i := startIndex; i < totalStep; i++ {
		currentStepName := JobSteps[i]

		// --- 2A. ดึงสถานะล่าสุด (Source of Truth) ---
		// *ต้อง* ดึงใหม่ทุกครั้งที่เริ่ม Loop
		currentJob, err := h.repo.FindByID(ctx, jobID)

		if err != nil {
			return err // Job หาย ?
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
				currentJob, err = h.repo.FindByID(ctx, jobID)
				if err != nil {
					return err
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
		jobAfterWork, err := h.repo.FindByID(ctx, jobID)
		if err != nil {
			return err
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
		jobAfterWork.Progress = h.calculateProgress(currentStepName, totalStep)
		if err := h.repo.Save(ctx, jobAfterWork); err != nil {
			log.Printf("Error saving job %s: %v", jobID, err)
		}
		h.notifier.BroadcastUpdate(jobAfterWork)
	}

	// --- 3. งานเสร็จสิ้น ---
	// (ตรรกะนี้กีต้องเช็ค Race Condition ด้วย)
	log.Printf("Job %s COMPLETED:", jobID)

	finalJob, _ := h.repo.FindByID(ctx, jobID)
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
	h.repo.Save(ctx, finalJob)
	h.notifier.BroadcastUpdate(finalJob)

	return nil
}

func (h *TaskHandler) calculateProgress(checkpoint string, totalStep int) int {
	if checkpoint == "" {
		return 0
	}
	if index, ok := StepIndexMap[checkpoint]; ok {
		// (index + 1) ตือจำนวน Step ที่เสร็จสิ้น
		return ((index + 1) * 100) / totalStep
	}

	return 0
}
