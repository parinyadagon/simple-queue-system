package mocks

import (
	"context"
	"database/sql"
	"log"
	"simple-queue-103/internal/adapters/repository"
	"simple-queue-103/internal/core/domain"
	"simple-queue-103/internal/core/ports"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestContainerSetup provides real database setup for integration tests
type TestContainerSetup struct {
	Container    *mysql.MySQLContainer
	Repository   ports.JobRepository // Use interface instead of concrete type
	MockNotifier *MockNotifier
	DSN          string
	teardownFunc func(context.Context, ...testcontainers.TerminateOption) error
}

// NewTestContainerSetup creates a MySQL testcontainer with real repository
func NewTestContainerSetup(ctx context.Context) (*TestContainerSetup, error) {
	var (
		dbName = "testdb"
		dbPwd  = "testpass"
		dbUser = "testuser"
	)

	// Start MySQL container
	dbContainer, err := mysql.Run(ctx,
		"mysql:8.0.36",
		mysql.WithDatabase(dbName),
		mysql.WithUsername(dbUser),
		mysql.WithPassword(dbPwd),
		testcontainers.WithWaitStrategy(
			wait.ForLog("port: 3306  MySQL Community Server - GPL").
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		return nil, err
	}

	// Get connection details
	dbHost, err := dbContainer.Host(ctx)
	if err != nil {
		return nil, err
	}

	dbPort, err := dbContainer.MappedPort(ctx, "3306/tcp")
	if err != nil {
		return nil, err
	}

	// Build DSN
	dsn := dbUser + ":" + dbPwd + "@tcp(" + dbHost + ":" + dbPort.Port() + ")/" + dbName + "?parseTime=true"

	// Create real repository with testcontainer database
	repo, err := repository.NewSQLJobRepository(dsn)
	if err != nil {
		return nil, err
	}

	// Initialize database schema for testing
	if err := initializeTestSchema(dsn); err != nil {
		return nil, err
	}

	// Create mock notifier for testing
	mockNotifier := NewMockNotifier()

	return &TestContainerSetup{
		Container:    dbContainer,
		Repository:   repo,
		MockNotifier: mockNotifier,
		DSN:          dsn,
		teardownFunc: dbContainer.Terminate,
	}, nil
}

// initializeTestSchema creates the jobs table for testing
func initializeTestSchema(dsn string) error {
	// Open direct connection for schema initialization
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	return createJobsTable(db)
}

// createJobsTable creates the jobs table schema
func createJobsTable(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id VARCHAR(255) PRIMARY KEY,
		file_name VARCHAR(255) NOT NULL,
		status ENUM('PENDING', 'RUNNING', 'PAUSED', 'CANCELED', 'COMPLETED') NOT NULL DEFAULT 'PENDING',
		progress INT DEFAULT 0,
		current_checkpoint VARCHAR(255) DEFAULT '',
		current_step_name VARCHAR(500) DEFAULT '',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	);`

	_, err := db.Exec(schema)
	if err != nil {
		log.Printf("Failed to create jobs table: %v", err)
		return err
	}

	log.Println("Successfully created jobs table for testing")
	return nil
}

// Teardown cleans up the testcontainer
func (ts *TestContainerSetup) Teardown(ctx context.Context) error {
	if ts.teardownFunc != nil {
		return ts.teardownFunc(ctx)
	}
	return nil
}

// CreateTestJob creates a test job for integration testing
func (ts *TestContainerSetup) CreateTestJob(id string, status domain.JobStatus, checkpoint string) *domain.Job {
	return &domain.Job{
		ID:                id,
		FileName:          "testcontainer_file.zip",
		Status:            status,
		CurrentCheckpoint: checkpoint,
		Progress:          0,
		CreatedAt:         time.Now(),
	}
}

// IntegrationTestHelper provides helper methods for testcontainer integration tests
type IntegrationTestHelper struct {
	*TestContainerSetup
}

// NewIntegrationTestHelper creates a new integration test helper
func NewIntegrationTestHelper(t *testing.T) *IntegrationTestHelper {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	setup, err := NewTestContainerSetup(ctx)
	if err != nil {
		t.Skipf("Skipping testcontainer integration test (Docker not available): %v", err)
	}

	// Cleanup when test finishes
	t.Cleanup(func() {
		if err := setup.Teardown(ctx); err != nil {
			log.Printf("Failed to cleanup testcontainer: %v", err)
		}
	})

	return &IntegrationTestHelper{
		TestContainerSetup: setup,
	}
}

// SaveAndVerifyJob saves a job to real database and verifies it
func (h *IntegrationTestHelper) SaveAndVerifyJob(ctx context.Context, t *testing.T, job *domain.Job) {
	// Save to real database
	err := h.Repository.Save(ctx, job)
	if err != nil {
		t.Fatalf("Failed to save job: %v", err)
	}

	// Verify by reading back
	savedJob, err := h.Repository.FindByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("Failed to find saved job: %v", err)
	}

	// Verify key fields
	if savedJob.ID != job.ID {
		t.Errorf("Expected ID %s, got %s", job.ID, savedJob.ID)
	}
	if savedJob.Status != job.Status {
		t.Errorf("Expected status %s, got %s", job.Status, savedJob.Status)
	}
	if savedJob.CurrentCheckpoint != job.CurrentCheckpoint {
		t.Errorf("Expected checkpoint %s, got %s", job.CurrentCheckpoint, savedJob.CurrentCheckpoint)
	}
}
