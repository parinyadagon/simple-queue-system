package main

import (
	"context"
	"log"
	"simple-queue-103/internal/adapters/queue"
	"simple-queue-103/internal/adapters/repository"
	"simple-queue-103/internal/core/domain"
	"simple-queue-103/internal/lib/process"
	"simple-queue-103/internal/mocks"
)

func main() {
	log.Println("🧪 Testing Simple Job Execution...")

	// Initialize components
	repo := repository.NewInMemoryJobRepository()
	notifier := &mocks.MockNotifier{}

	// Create a simple data_analysis job
	job := &domain.Job{
		ID:          "test-job-001",
		FileName:    "test_file.csv",
		Status:      domain.StatusPending,
		Progress:    0,
		ProcessType: "data_analysis",
	}

	// Save job
	if err := repo.Save(context.Background(), job); err != nil {
		log.Fatal("Failed to save job:", err)
	}

	log.Printf("📋 Created job: %s (%s)", job.FileName, job.ID)

	// Test process configuration access
	config, exists := process.ProcessConfigurations["data_analysis"]
	if !exists {
		log.Fatal("❌ data_analysis process configuration not found")
	}

	log.Printf("✅ Found process config for data_analysis:")
	log.Printf("   - Steps: %d", len(config.Steps))

	for i, step := range config.Steps {
		log.Printf("     %d. %s (%s)", i+1, step.Name, step.Description)
		log.Printf("        Sub-steps: %d", len(step.SubSteps))
	}

	// Test creating a task handler
	handler := queue.NewProcessTaskHandler(repo, notifier, "data_analysis")
	if handler == nil {
		log.Fatal("❌ Failed to create process task handler")
	}

	log.Println("✅ Created ProcessTaskHandler successfully")

	// Test step executor
	log.Println("🔍 Testing step executor functions...")

	stepExecutor := handler.GetProcessStepExecutor("LOAD_RAW_DATA")
	if stepExecutor == nil {
		log.Println("⚠️ No custom executor found for LOAD_RAW_DATA, this will use generic processing")
	} else {
		log.Println("✅ Found custom executor for LOAD_RAW_DATA")
	}

	// Test the execute generic step safely
	log.Println("🚀 Testing generic step execution...")

	stepConfig := &queue.JobStepConfig{
		Name:        "TEST_STEP",
		Description: "ทดสอบการทำงาน",
		SubSteps: []queue.JobSubStepConfig{
			{Name: "TEST_SUBSTEP_1", Description: "ขั้นตอนที่ 1"},
			{Name: "TEST_SUBSTEP_2", Description: "ขั้นตอนที่ 2"},
		},
	}

	taskHandler := queue.NewTaskHandler(repo, notifier)
	if err := taskHandler.ExecuteGenericStep(context.Background(), job.ID, stepConfig); err != nil {
		log.Printf("❌ Generic step execution failed: %v", err)
	} else {
		log.Println("✅ Generic step execution completed successfully")
	}

	// Check job status
	updatedJob, err := repo.FindByID(context.Background(), job.ID)
	if err != nil {
		log.Printf("❌ Failed to get updated job: %v", err)
	} else {
		log.Printf("📊 Job status after execution:")
		log.Printf("   - Status: %s", updatedJob.Status)
		log.Printf("   - Progress: %d%%", updatedJob.Progress)
		log.Printf("   - Current Step: %s", updatedJob.CurrentStepName)
		log.Printf("   - Checkpoint: %s", updatedJob.CurrentCheckpoint)
	}

	log.Println("🎉 Simple execution test completed!")
}
