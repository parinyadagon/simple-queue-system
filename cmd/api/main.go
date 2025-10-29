package main

import (
	"context"
	"log"
	"os"
	"simple-queue-103/internal/adapters/broadcast"
	"simple-queue-103/internal/adapters/http"
	"simple-queue-103/internal/adapters/queue"
	"simple-queue-103/internal/adapters/repository"
	"simple-queue-103/internal/core/ports"
	"simple-queue-103/internal/core/service"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/hibiken/asynq"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Failed to load .env file")
	}

	// --- 1. Initialize Adapters (Singletons) ---

	// Use in-memory repository for testing
	// jobRepo := repository.NewInMemoryJobRepository()

	notifier := broadcast.NewWebSocketNotifier()
	JobQueue := queue.NewAsynqJobQueue()

	//MySQL repository for production
	mysqlDSN := os.Getenv("MYSQL_DSN")
	if mysqlDSN == "" {
		log.Fatal("MYSQL_DSN environment variable is not set")
	}

	var jobRepo ports.JobRepository
	var err error

	// (สำคัญ) เพิ่ม Retry loop เพื่อรอ DB
	for i := 0; i < 10; i++ {
		jobRepo, err = repository.NewSQLJobRepository(mysqlDSN)
		if err == nil {
			break
		}
		log.Printf("Failed to connect to MySQL (attempt %d): %v", i+1, err)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		log.Fatalf("Could not connect to MySQL after retries: %v", err)
	}

	// --- 2. Initialize Core Service (Inject Adapters) ---
	jobService := service.NewJobService(jobRepo, JobQueue, notifier)

	// --- 3. Initialize Asynq Worker (Server) ---
	// Worker จะต้องใช้ Repo และ Notifier ตัวเดียวกับ API
	asynqServer := asynq.NewServer(
		asynq.RedisClientOpt{Addr: queue.RedisAddr},
		asynq.Config{
			Concurrency: 10,
			// เพิ่ม retry middleware และ error handling
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
				log.Printf("Task failed: %s, Error: %v", task.Type(), err)
			}),
		},
	)

	taskHandler := queue.NewTaskHandler(jobRepo, notifier)
	mux := asynq.NewServeMux()
	mux.HandleFunc(queue.TaskTypeAnalysis, taskHandler.HandleAnalysisTask)

	// TODO: เพิ่ม Recovery goroutine สำหรับ Production environment
	// สำหรับ Development - Asynq มี built-in retry เพียงพอแล้ว

	// Uncomment สำหรับ Production:
	/*
		go func() {
			time.Sleep(10 * time.Second) // รอให้ระบบ start ก่อน

			// ทุกๆ 5 นาที ตรวจสอบงานที่ค้างอยู่ (ลด frequency)
			ticker := time.NewTicker(5 * time.Minute)
			defer ticker.Stop()

			for range ticker.C {
				if err := recoverStuckJobs(jobRepo, JobQueue, notifier); err != nil {
					log.Printf("Error recovering stuck jobs: %v", err)
				}
			}
		}()
	*/

	// --- 4. Initialize Fiber API (Server) ---
	app := fiber.New()
	app.Use(cors.New())

	// (ส่ง service และ notifier เข้าไป)
	http.RegisterRoutes(app, jobService, notifier)

	// --- 5. Run Everything! ---
	// รัน Asynq Worker ใน goroutine
	go func() {
		log.Println("Starting Asynq Worker...")
		if err := asynqServer.Run(mux); err != nil {
			log.Fatalf("Could not run Asynq worker: %v", err)
		}

	}()

	// รัน Fiber API
	log.Println("Starting Fiber API Server on :8080...")
	log.Fatal(app.Listen(":8080"))
}
