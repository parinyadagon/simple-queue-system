package service

import (
	"context"
	"simple-queue-103/internal/core/domain"
	"simple-queue-103/internal/core/ports"
	"time"

	"github.com/google/uuid"
)

type jobService struct {
	repo     ports.JobRepository
	queue    ports.JobQueue
	notifier ports.Notifier
}

func NewJobService(repo ports.JobRepository, queue ports.JobQueue, notifier ports.Notifier) ports.JobService {
	return &jobService{
		repo:     repo,
		queue:    queue,
		notifier: notifier,
	}
}

func (s *jobService) CreateJob(fileName string) (*domain.Job, error) {
	// Default to data_analysis process for backward compatibility
	return s.CreateJobForProcess(fileName, "data_analysis")
}

func (s *jobService) CreateJobForProcess(fileName string, processType string) (*domain.Job, error) {
	ctx := context.Background()
	job := &domain.Job{
		ID:             uuid.NewString(),
		ProcessType:    processType,
		ProcessVersion: "1.0",
		FileName:       fileName,
		Status:         domain.StatusPending,
		Progress:       0,
		CreatedAt:      time.Now(),
	}

	if err := s.repo.Save(ctx, job); err != nil {
		return nil, err
	}

	// ส่งเข้า Queue เพื่อให้ Worker เริ่มทำงาน (with process-specific task type)
	if err := s.queue.EnqueueForProcess(job.ID, processType); err != nil {
		return nil, err
	}

	s.notifier.BroadcastUpdate(job) // แจ้ง Client ว่ามี Job ใหม่

	return job, nil
}

func (s *jobService) GetAllJobs() ([]*domain.Job, error) {
	return s.repo.FindAll(context.Background())
}

func (s *jobService) GetJobsByProcess(processType string) ([]*domain.Job, error) {
	return s.repo.FindByProcessType(context.Background(), processType)
}

func (s *jobService) GetJob(id string) (*domain.Job, error) {
	return s.repo.FindByID(context.Background(), id)
}

func (s *jobService) ControlJob(id string, command string) error {
	ctx := context.Background()
	job, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return err
	}

	switch command {
	case "PAUSE":
		job.Status = domain.StatusPaused
	case "RESTART": // (RESTART = PAUSE -> RUNNING with forced re-enqueue)
		job.Status = domain.StatusRunning
		// บังคับ enqueue job กลับเข้า queue อีกครั้งเพื่อให้ worker ทำงานต่อ
		if err := s.queue.EnqueueForProcess(job.ID, job.ProcessType); err != nil {
			return err
		}
	case "CANCEL":
		job.Status = domain.StatusCanceled
		/* if job.CancelFunc != nil {
			job.CancelFunc() // เรียก context.CancelFunc()
		} */
	}

	if err := s.repo.Save(ctx, job); err != nil {
		return err
	}

	s.notifier.BroadcastUpdate(job) // แจ้ง Client ว่าสถานะเปลี่ยน

	return nil
}
