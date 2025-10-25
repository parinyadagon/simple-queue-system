package main

import (
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

	// jobRepo := repository.NewInMemoryJobRepository()
	notifier := broadcast.NewWebSocketNotifier()
	JobQueue := queue.NewAsynqJobQueue()

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
		asynq.Config{Concurrency: 10},
	)

	taskHandler := queue.NewTaskHandler(jobRepo, notifier)
	mux := asynq.NewServeMux()
	mux.HandleFunc(queue.TaskTypeAnalysis, taskHandler.HandleAnalysisTask)

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
