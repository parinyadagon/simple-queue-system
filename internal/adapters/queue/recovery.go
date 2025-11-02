package queue

import (
	"context"
	"fmt"
	"log"
	"simple-queue-103/internal/core/domain"
	"simple-queue-103/internal/core/ports"
	"time"
)

// RecoveryConfig defines configuration for job recovery system
type RecoveryConfig struct {
	StuckJobTimeout     time.Duration // Time after which a RUNNING job is considered stuck
	RecoveryInterval    time.Duration // Interval between recovery scans
	MaxRecoveryAttempts int           // Maximum attempts to recover a job
}

// DefaultRecoveryConfig returns default recovery configuration
func DefaultRecoveryConfig() *RecoveryConfig {
	return &RecoveryConfig{
		StuckJobTimeout:     10 * time.Minute,
		RecoveryInterval:    2 * time.Minute,
		MaxRecoveryAttempts: 3,
	}
}

// RecoveryManager จัดการการ recover jobs ที่ค้างเมื่อ Redis ล้ม
type RecoveryManager struct {
	jobRepo  ports.JobRepository
	jobQueue ports.JobQueue
	notifier ports.Notifier
}

// NewRecoveryManager สร้าง RecoveryManager ใหม่
func NewRecoveryManager(jobRepo ports.JobRepository, jobQueue ports.JobQueue, notifier ports.Notifier) *RecoveryManager {
	return &RecoveryManager{
		jobRepo:  jobRepo,
		jobQueue: jobQueue,
		notifier: notifier,
	}
}

// RecoverySystem manages job recovery operations
type RecoverySystem struct {
	config   *RecoveryConfig
	jobRepo  ports.JobRepository
	queue    ports.JobQueue
	notifier ports.Notifier
}

// NewRecoverySystem creates a new recovery system instance
func NewRecoverySystem(config *RecoveryConfig, jobRepo ports.JobRepository, queue ports.JobQueue, notifier ports.Notifier) *RecoverySystem {
	if config == nil {
		config = DefaultRecoveryConfig()
	}

	return &RecoverySystem{
		config:   config,
		jobRepo:  jobRepo,
		queue:    queue,
		notifier: notifier,
	}
}

// StartRecoveryRoutine starts the automatic recovery system
func (rs *RecoverySystem) StartRecoveryRoutine() {
	go func() {
		// รอให้ระบบ start ก่อน
		time.Sleep(10 * time.Second)

		ticker := time.NewTicker(rs.config.RecoveryInterval)
		defer ticker.Stop()

		log.Printf("🔥 Recovery System started: scan every %v, stuck threshold: %v",
			rs.config.RecoveryInterval, rs.config.StuckJobTimeout)

		for range ticker.C {
			if err := rs.RecoverStuckJobs(); err != nil {
				log.Printf("🚨 Recovery Error: %v", err)
			}
		}
	}()
}

// RecoverStuckJobs จัดการ job ที่ค้างเมื่อ Redis ล้ม หรือ worker หยุดทำงาน
func (rs *RecoverySystem) RecoverStuckJobs() error {
	log.Println("🔍 Starting job recovery scan...")

	// ค้นหา jobs ที่ค้างในสถานะ RUNNING เกินเวลาที่กำหนด
	stuckJobs, err := rs.findStuckRunningJobs()
	if err != nil {
		return fmt.Errorf("failed to find stuck jobs: %w", err)
	}

	if len(stuckJobs) == 0 {
		log.Println("✅ No stuck jobs found")
		return nil
	}

	log.Printf("🚨 Found %d stuck jobs, attempting recovery...", len(stuckJobs))

	recoveredCount := 0
	for _, job := range stuckJobs {
		if recovered := rs.recoverSingleJob(job); recovered {
			recoveredCount++
		}
	}

	log.Printf("✅ Recovery completed: %d/%d jobs recovered", recoveredCount, len(stuckJobs))
	return nil
}

// findStuckRunningJobs ค้นหา jobs ที่ค้างในสถานะ RUNNING นานเกินที่กำหนด
func (rs *RecoverySystem) findStuckRunningJobs() ([]*domain.Job, error) {
	allJobs, err := rs.jobRepo.FindAll(context.Background())
	if err != nil {
		return nil, err
	}

	var stuckJobs []*domain.Job
	cutoffTime := time.Now().Add(-rs.config.StuckJobTimeout)

	for _, job := range allJobs {
		if job.Status == domain.StatusRunning && job.UpdatedAt.Before(cutoffTime) {
			stuckJobs = append(stuckJobs, job)
			log.Printf("🔍 Found stuck job: %s (%s) - stuck for %v",
				job.ID, job.FileName, time.Since(job.UpdatedAt).Round(time.Second))
		}
	}

	return stuckJobs, nil
}

// recoverSingleJob attempts to recover a single stuck job
func (rs *RecoverySystem) recoverSingleJob(job *domain.Job) bool {
	// ตรวจสอบว่า Redis กลับมาแล้วหรือยัง
	if err := rs.testRedisConnection(); err != nil {
		log.Printf("⚠️ Redis still down, cannot recover job %s: %v", job.ID, err)
		return false
	}

	// เปลี่ยนสถานะเป็น PENDING และ re-enqueue
	originalStatus := job.Status
	job.Status = domain.StatusPending

	if err := rs.jobRepo.Save(context.Background(), job); err != nil {
		log.Printf("❌ Failed to reset job %s: %v", job.ID, err)
		job.Status = originalStatus // Restore original status
		return false
	}

	// Re-enqueue job กลับเข้าไปใน Redis
	if err := rs.queue.EnqueueForProcess(job.ID, job.ProcessType); err != nil {
		log.Printf("❌ Failed to re-enqueue job %s: %v", job.ID, err)

		// Restore original status if re-enqueue fails
		job.Status = originalStatus
		if saveErr := rs.jobRepo.Save(context.Background(), job); saveErr != nil {
			log.Printf("❌ Failed to restore job status %s: %v", job.ID, saveErr)
		}
		return false
	}

	log.Printf("🔄 Recovered stuck job: %s (%s) - process: %s", job.ID, job.FileName, job.ProcessType)
	rs.notifier.BroadcastUpdate(job)
	return true
}

// RecoverStuckJobs จัดการ job ที่ค้างเมื่อ Redis ล้ม หรือ worker หยุดทำงาน
func (rm *RecoveryManager) RecoverStuckJobs() error {
	log.Println("🔍 Starting job recovery scan...")

	// ค้นหา jobs ที่ค้างในสถานะ RUNNING เกิน 10 นาที
	stuckJobs, err := rm.findStuckRunningJobs()
	if err != nil {
		return fmt.Errorf("failed to find stuck jobs: %w", err)
	}

	if len(stuckJobs) == 0 {
		log.Println("✅ No stuck jobs found")
		return nil
	}

	log.Printf("🚨 Found %d stuck jobs, attempting recovery...", len(stuckJobs))

	recoveredCount := 0
	for _, job := range stuckJobs {
		// ตรวจสอบว่า Redis กลับมาแล้วหรือยัง
		if err := rm.testRedisConnection(); err != nil {
			log.Printf("⚠️ Redis still down, cannot recover jobs: %v", err)
			return err
		}

		// เปลี่ยนสถานะเป็น PENDING และ re-enqueue
		job.Status = domain.StatusPending
		if err := rm.jobRepo.Save(context.Background(), job); err != nil {
			log.Printf("❌ Failed to reset job %s: %v", job.ID, err)
			continue
		}

		// Re-enqueue job กลับเข้าไปใน Redis
		if err := rm.jobQueue.EnqueueForProcess(job.ID, job.ProcessType); err != nil {
			log.Printf("❌ Failed to re-enqueue job %s: %v", job.ID, err)
			continue
		}

		log.Printf("🔄 Recovered stuck job: %s (%s)", job.ID, job.FileName)
		rm.notifier.BroadcastUpdate(job)
		recoveredCount++
	}

	log.Printf("✅ Recovery completed: %d/%d jobs recovered", recoveredCount, len(stuckJobs))
	return nil
}

// findStuckRunningJobs ค้นหา jobs ที่ค้างในสถานะ RUNNING นานเกิน 10 นาที
func (rm *RecoveryManager) findStuckRunningJobs() ([]*domain.Job, error) {
	allJobs, err := rm.jobRepo.FindAll(context.Background())
	if err != nil {
		return nil, err
	}

	var stuckJobs []*domain.Job
	cutoffTime := time.Now().Add(-10 * time.Minute)

	for _, job := range allJobs {
		if job.Status == domain.StatusRunning && job.UpdatedAt.Before(cutoffTime) {
			stuckJobs = append(stuckJobs, job)
		}
	}

	return stuckJobs, nil
}

// testRedisConnection ทดสอบการเชื่อมต่อ Redis
func (rm *RecoveryManager) testRedisConnection() error {
	// สร้าง dummy job เพื่อทดสอบ
	testJobID := fmt.Sprintf("test-%d", time.Now().Unix())
	return rm.jobQueue.EnqueueForProcess(testJobID, "test_connection")
}

// testRedisConnection ทดสอบการเชื่อมต่อ Redis
func (rs *RecoverySystem) testRedisConnection() error {
	// สร้าง dummy job เพื่อทดสอบ
	testJobID := fmt.Sprintf("health-check-%d", time.Now().Unix())

	// ลองส่ง job ไปที่ Redis
	if err := rs.queue.EnqueueForProcess(testJobID, "health_check"); err != nil {
		return fmt.Errorf("redis connection test failed: %w", err)
	}

	log.Println("✅ Redis connection test successful")
	return nil
}

// GetStuckJobsCount returns the number of currently stuck jobs (for monitoring)
func (rs *RecoverySystem) GetStuckJobsCount() (int, error) {
	stuckJobs, err := rs.findStuckRunningJobs()
	if err != nil {
		return 0, err
	}
	return len(stuckJobs), nil
}

// ForceRecoverJob forces recovery of a specific job by ID
func (rs *RecoverySystem) ForceRecoverJob(jobID string) error {
	job, err := rs.jobRepo.FindByID(context.Background(), jobID)
	if err != nil {
		return fmt.Errorf("job not found: %w", err)
	}

	if job.Status != domain.StatusRunning {
		return fmt.Errorf("job %s is not in RUNNING state (current: %s)", jobID, job.Status)
	}

	log.Printf("🔧 Force recovering job: %s", jobID)

	if rs.recoverSingleJob(job) {
		log.Printf("✅ Successfully force-recovered job: %s", jobID)
		return nil
	}

	return fmt.Errorf("failed to force-recover job: %s", jobID)
}
