package repository

import (
	"context"
	"log"
	"simple-queue-103/internal/core/domain"
	"simple-queue-103/internal/core/ports"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type mysqlJobRepository struct {
	db *sqlx.DB
}

func NewSQLJobRepository(dataSourceName string) (ports.JobRepository, error) {
	// DSN example: "user:pass%tcp(localhosT:3306)/dbname?parseTime=true"
	db, err := sqlx.Connect("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}

	// Config connection pool
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	log.Println("Successfully connected to MySQL")

	return &mysqlJobRepository{db: db}, nil
}

// Save จะใช้ INSERT ... ON DUPLICATE KEY UPDATE (เรียกว่า "Upsert")
func (r *mysqlJobRepository) Save(ctx context.Context, job *domain.Job) error {
	query := `
			INSERT INTO jobs (id, process_type, process_version, file_name, status, progress, current_checkpoint, current_step_name, created_at)
			VALUES (:id, :process_type, :process_version, :file_name, :status, :progress, :current_checkpoint, :current_step_name, :created_at)
			ON DUPLICATE KEY UPDATE
					status = VALUES(status),
					current_checkpoint = VALUES(current_checkpoint),
					current_step_name = VALUES(current_step_name),
					progress = VALUES(progress)
	`
	// db.NameExeContext ใชั struct filed (ที่ tag `db:"..."`)
	_, err := r.db.NamedExecContext(ctx, query, job)

	return err
}

func (r *mysqlJobRepository) FindByID(ctx context.Context, id string) (*domain.Job, error) {
	var job domain.Job
	query := `SELECT * FROM jobs WHERE id = ?`

	// db.GetContext จะ map ผลลัพท์เข้า struct `job`
	err := r.db.GetContext(ctx, &job, query, id)
	if err != nil {
		return nil, err
	}

	return &job, nil
}

func (r *mysqlJobRepository) FindAll(ctx context.Context) ([]*domain.Job, error) {
	var jobs []*domain.Job
	query := `SELECT * FROM jobs ORDER BY created_at DESC`

	// db.SelectContext จะ map ผลลัพท์เข้า slice
	err := r.db.SelectContext(ctx, &jobs, query)
	if err != nil {
		return nil, err
	}

	return jobs, nil
}

// FindByProcessType returns jobs of a specific process type
func (r *mysqlJobRepository) FindByProcessType(ctx context.Context, processType string) ([]*domain.Job, error) {
	var jobs []*domain.Job
	query := `SELECT * FROM jobs WHERE process_type = ? ORDER BY created_at DESC`

	err := r.db.SelectContext(ctx, &jobs, query, processType)
	if err != nil {
		return nil, err
	}

	return jobs, nil
}

// FindByProcessAndStatus returns jobs of a specific process type and status
func (r *mysqlJobRepository) FindByProcessAndStatus(ctx context.Context, processType string, status domain.JobStatus) ([]*domain.Job, error) {
	var jobs []*domain.Job
	query := `SELECT * FROM jobs WHERE process_type = ? AND status = ? ORDER BY created_at DESC`

	err := r.db.SelectContext(ctx, &jobs, query, processType, status)
	if err != nil {
		return nil, err
	}

	return jobs, nil
}

// CountByProcess returns the count of jobs for a specific process type
func (r *mysqlJobRepository) CountByProcess(ctx context.Context, processType string) (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM jobs WHERE process_type = ?`

	err := r.db.GetContext(ctx, &count, query, processType)
	if err != nil {
		return 0, err
	}

	return count, nil
}
