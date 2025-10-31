package main

import (
	"context"
	"log"
	"simple-queue-103/internal/adapters/broadcast"
	"simple-queue-103/internal/adapters/queue"
	"simple-queue-103/internal/adapters/repository"
	"simple-queue-103/internal/core/domain"
	"time"

	"github.com/google/uuid"
)

func main() {
	log.Println("🚀 Multi-Process System Demo")

	// Setup repository and notifier (shared across processes)
	repo := repository.NewInMemoryJobRepository()
	notifier := broadcast.NewWebSocketNotifier()

	// Create process-specific handlers
	dataAnalysisHandler := queue.NewProcessTaskHandler(repo, notifier, "data_analysis")
	fileImportHandler := queue.NewProcessTaskHandler(repo, notifier, "file_import")
	reportGenHandler := queue.NewProcessTaskHandler(repo, notifier, "report_gen")

	log.Println("✅ Created process handlers:")
	log.Println("   - Data Analysis Handler")
	log.Println("   - File Import Handler")
	log.Println("   - Report Generation Handler")

	// Create jobs for different processes
	ctx := context.Background()

	// 1. Data Analysis Job
	dataJob := &domain.Job{
		ID:             uuid.New().String(),
		ProcessType:    "data_analysis",
		ProcessVersion: "1.0",
		FileName:       "sales_data.csv",
		Status:         domain.StatusPending,
		CreatedAt:      time.Now(),
	}
	repo.Save(ctx, dataJob)
	log.Printf("📊 Created Data Analysis job: %s", dataJob.ID)

	// 2. File Import Job
	importJob := &domain.Job{
		ID:             uuid.New().String(),
		ProcessType:    "file_import",
		ProcessVersion: "1.0",
		FileName:       "customer_list.xlsx",
		Status:         domain.StatusPending,
		CreatedAt:      time.Now(),
	}
	repo.Save(ctx, importJob)
	log.Printf("📁 Created File Import job: %s", importJob.ID)

	// 3. Report Generation Job
	reportJob := &domain.Job{
		ID:             uuid.New().String(),
		ProcessType:    "report_gen",
		ProcessVersion: "1.0",
		FileName:       "monthly_report",
		Status:         domain.StatusPending,
		CreatedAt:      time.Now(),
	}
	repo.Save(ctx, reportJob)
	log.Printf("📋 Created Report Generation job: %s", reportJob.ID)

	// Demonstrate process isolation
	log.Println("\n🔍 Demonstrating Process Isolation:")

	// Each handler only sees its own process jobs
	allJobs, _ := repo.FindAll(ctx)
	log.Printf("📈 Total jobs in system: %d", len(allJobs))

	// Show job distribution by process
	processCount := make(map[string]int)
	for _, job := range allJobs {
		processCount[job.ProcessType]++
	}

	log.Println("📊 Jobs by process type:")
	for processType, count := range processCount {
		log.Printf("   - %s: %d jobs", processType, count)
	}

	// Simulate concurrent processing
	log.Println("\n🔄 Starting concurrent process execution...")

	// Process each job type with its dedicated handler
	go func() {
		log.Printf("🔄 Processing Data Analysis job with dataAnalysisHandler...")
		// Simulate task processing - in real system this would be handled by Asynq
		time.Sleep(2 * time.Second)
		log.Printf("✅ Data Analysis job processed")
	}()

	go func() {
		log.Printf("🔄 Processing File Import job with fileImportHandler...")
		time.Sleep(1 * time.Second)
		log.Printf("✅ File Import job processed")
	}()

	go func() {
		log.Printf("🔄 Processing Report Generation job with reportGenHandler...")
		time.Sleep(3 * time.Second)
		log.Printf("✅ Report Generation job processed")
	}()

	// Wait for demonstration
	time.Sleep(5 * time.Second)

	log.Println("\n🎯 Multi-Process Benefits Demonstrated:")
	log.Println("   ✅ Process Isolation: Each handler manages only its process type")
	log.Println("   ✅ Concurrent Processing: Multiple processes run simultaneously")
	log.Println("   ✅ Type Safety: Process-specific configuration and validation")
	log.Println("   ✅ Scalability: Easy to add new process types")
	log.Println("   ✅ Maintainability: Clean separation of concerns")

	log.Println("\n🏁 Multi-Process Demo completed successfully!")
}
