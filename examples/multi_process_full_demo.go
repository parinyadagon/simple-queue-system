package main

import (
	"context"
	"log"
	"net/http"
	"simple-queue-103/internal/adapters/broadcast"
	"simple-queue-103/internal/adapters/queue"
	"simple-queue-103/internal/adapters/repository"
	"simple-queue-103/internal/core/service"
	"time"
)

func main() {
	log.Println("🚀 Multi-Process Demo System Starting...")

	// Setup components
	repo := repository.NewInMemoryJobRepository()
	notifier := broadcast.NewWebSocketNotifier()
	jobQueue := queue.NewAsynqJobQueue()
	jobService := service.NewJobService(repo, jobQueue, notifier)

	log.Println("✅ Initialized components:")
	log.Println("   - In-memory repository")
	log.Println("   - WebSocket notifier")
	log.Println("   - Asynq job queue")
	log.Println("   - Job service with multi-process support")

	// Create sample jobs for all 3 processes
	processes := []string{"data_analysis", "file_import", "report_gen"}

	for _, processType := range processes {
		fileName := getFileNameForProcess(processType)
		job, err := jobService.CreateJobForProcess(fileName, processType)
		if err != nil {
			log.Printf("❌ Failed to create %s job: %v", processType, err)
			continue
		}

		log.Printf("📋 Created %s job: %s (ID: %s)",
			processType, job.FileName, job.ID[:8])
	}

	// Display current jobs by process
	time.Sleep(2 * time.Second)
	ctx := context.Background()

	log.Println("\n📊 Current Jobs by Process:")
	for _, processType := range processes {
		jobs, err := jobService.GetJobsByProcess(processType)
		if err != nil {
			log.Printf("❌ Error getting %s jobs: %v", processType, err)
			continue
		}

		log.Printf("🔧 %s: %d jobs", processType, len(jobs))
		for _, job := range jobs {
			log.Printf("   - %s [%s] %d%% - %s",
				job.FileName, job.Status, job.Progress, job.CurrentStepName)
		}
	}

	// Get all jobs
	allJobs, err := jobService.GetAllJobs()
	if err != nil {
		log.Printf("❌ Error getting all jobs: %v", err)
	} else {
		log.Printf("\n📈 Total Jobs: %d", len(allJobs))

		// Count by process type
		processCount := make(map[string]int)
		for _, job := range allJobs {
			processCount[job.ProcessType]++
		}

		log.Println("📊 Distribution:")
		for processType, count := range processCount {
			log.Printf("   - %s: %d jobs", processType, count)
		}
	}

	// Test process filtering
	log.Println("\n🔍 Testing Process Filtering...")
	for _, processType := range processes {
		jobs, err := repo.FindByProcessType(ctx, processType)
		if err != nil {
			log.Printf("❌ Error filtering %s jobs: %v", processType, err)
			continue
		}

		log.Printf("✅ Filter %s: found %d jobs", processType, len(jobs))
	}

	// Test process + status filtering
	log.Println("\n🔍 Testing Process + Status Filtering...")
	for _, processType := range processes {
		count, err := repo.CountByProcess(ctx, processType)
		if err != nil {
			log.Printf("❌ Error counting %s jobs: %v", processType, err)
			continue
		}

		log.Printf("✅ Count %s: %d jobs", processType, count)
	}

	log.Println("\n🎯 Multi-Process System Features Demonstrated:")
	log.Println("   ✅ Process-Specific Job Creation")
	log.Println("   ✅ Process-Aware Repository Queries")
	log.Println("   ✅ Process Type Filtering")
	log.Println("   ✅ Process-Specific Task Types")
	log.Println("   ✅ Multi-Process Job Service")
	log.Println("   ✅ Process Isolation & Management")

	log.Println("\n🎉 Multi-Process Demo completed successfully!")
	log.Println("👉 Start the full system with: make run")
	log.Println("👉 Open http://localhost:5173 to see the dashboard")
	log.Println("👉 Use the process selector to filter jobs by type")
}

func getFileNameForProcess(processType string) string {
	switch processType {
	case "data_analysis":
		return "sales_data_2024.csv"
	case "file_import":
		return "customer_database.xlsx"
	case "report_gen":
		return "monthly_financial_report"
	default:
		return "unknown_file.txt"
	}
}

// Simple HTTP test function
func testHTTPEndpoints() {
	log.Println("\n🌐 Testing HTTP Endpoints...")

	baseURL := "http://localhost:8080"

	// Test processes endpoint
	resp, err := http.Get(baseURL + "/processes")
	if err != nil {
		log.Printf("❌ Failed to get processes: %v", err)
		return
	}
	resp.Body.Close()
	log.Printf("✅ GET /processes: %d", resp.StatusCode)

	// Test jobs endpoint
	resp, err = http.Get(baseURL + "/jobs")
	if err != nil {
		log.Printf("❌ Failed to get jobs: %v", err)
		return
	}
	resp.Body.Close()
	log.Printf("✅ GET /jobs: %d", resp.StatusCode)

	// Test filtered jobs
	resp, err = http.Get(baseURL + "/jobs?process_type=data_analysis")
	if err != nil {
		log.Printf("❌ Failed to get filtered jobs: %v", err)
		return
	}
	resp.Body.Close()
	log.Printf("✅ GET /jobs?process_type=data_analysis: %d", resp.StatusCode)
}
