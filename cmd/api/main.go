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
	"simple-queue-103/internal/lib/process"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/hibiken/asynq"
	"github.com/joho/godotenv"
)

// registerBuilderProcesses creates production-ready processes using optimized Builder Pattern
func registerBuilderProcesses() {
	log.Println("Registering Builder Pattern processes...")

	manager := process.NewProcessManager()

	// 1. Email Campaign Pro - High performance email processing
	emailCampaign := manager.CreateCustomProcessWithCapacity("Email Campaign Pro", 4)
	emailCampaign.SetDescription("High-performance email campaign processing with optimized Builder Pattern").
		AddStep("validate_recipients", "ตรวจสอบผู้รับอีเมล").
		AddSubStep("verify_email_format", "ตรวจสอบรูปแบบอีเมล", 5*time.Second).
		AddSubStep("check_spam_filters", "ตรวจสอบตัวกรองสแปม", 3*time.Second).
		AddStep("prepare_content", "เตรียมเนื้อหาอีเมล").
		AddSubStep("load_templates", "โหลดเทมเพลต", 2*time.Second).
		AddSubStep("personalize_content", "ปรับแต่งเนื้อหา", 4*time.Second).
		AddStep("batch_sending", "ส่งอีเมลเป็นชุด").
		AddSubStep("create_batches", "สร้างกลุ่มการส่ง", 3*time.Second).
		AddSubStep("send_emails", "ส่งอีเมล", 8*time.Second).
		AddStep("track_delivery", "ติดตามการส่ง").
		AddSubStep("monitor_delivery", "ตรวจสอบสถานะการส่ง", 5*time.Second).
		AddSubStep("handle_bounces", "จัดการอีเมลตีกลับ", 3*time.Second)

	// Register with unique key
	emailCampaign.BuildAndRegister("email_campaign_pro")

	// 2. Batch Processing - Optimized for large datasets
	batchProcess := manager.CreateCustomProcessWithCapacity("Batch Processing", 3)
	batchProcess.SetDescription("Optimized batch processing for large datasets with memory management").
		AddStep("data_ingestion", "รับข้อมูลเข้าระบบ").
		AddSubStep("validate_input", "ตรวจสอบข้อมูลนำเข้า", 4*time.Second).
		AddSubStep("parse_format", "แยกวิเคราะห์รูปแบบข้อมูล", 6*time.Second).
		AddStep("processing", "ประมวลผลข้อมูล").
		AddSubStep("transform_data", "แปลงข้อมูล", 10*time.Second).
		AddSubStep("apply_business_rules", "ใช้กฎทางธุรกิจ", 8*time.Second).
		AddStep("quality_assurance", "ประกันคุณภาพ").
		AddSubStep("validate_output", "ตรวจสอบข้อมูลผลลัพธ์", 5*time.Second).
		AddSubStep("generate_report", "สร้างรายงานคุณภาพ", 4*time.Second)

	batchProcess.BuildAndRegister("batch_processing")

	// 3. Image Processing - Memory optimized for large files
	imageProcess := manager.CreateCustomProcessWithCapacity("Image Processing", 3)
	imageProcess.SetDescription("Advanced image processing with memory optimization for large files").
		AddStep("image_analysis", "วิเคราะห์รูปภาพ").
		AddSubStep("detect_format", "ตรวจจับรูปแบบภาพ", 2*time.Second).
		AddSubStep("analyze_metadata", "วิเคราะห์ข้อมูลเมตา", 3*time.Second).
		AddSubStep("check_dimensions", "ตรวจสอบขนาดภาพ", 1*time.Second).
		AddStep("processing", "ประมวลผลรูปภาพ").
		AddSubStep("resize_image", "ปรับขนาดรูปภาพ", 8*time.Second).
		AddSubStep("apply_filters", "ใช้ตัวกรอง", 6*time.Second).
		AddSubStep("optimize_quality", "ปรับปรุงคุณภาพ", 5*time.Second).
		AddStep("output_generation", "สร้างไฟล์ผลลัพธ์").
		AddSubStep("convert_format", "แปลงรูปแบบ", 4*time.Second).
		AddSubStep("compress_file", "บีบอัดไฟล์", 3*time.Second).
		AddSubStep("generate_thumbnails", "สร้างภาพขนาดย่อ", 2*time.Second)

	imageProcess.BuildAndRegister("image_processing")

	log.Printf("✅ Successfully registered %d Builder Pattern processes", 3)

	// 4. Database Process - Reliable DB operations with error handling
	dbProcess := manager.CreateCustomProcessWithCapacity("Database Processing", 3)
	dbProcess.SetDescription("Reliable database operations with error handling").
		AddStep("connect_db", "เชื่อมต่อฐานข้อมูล").
		AddSubStep("initialize_connection", "เริ่มต้นการเชื่อมต่อ", 2*time.Second).
		AddSubStepWithAction("check_health", "ตรวจสอบสถานะ", func() {
			// สมมติว่าเช็คสถานะ DB ที่นี่
			log.Println("Checking database health...")

			time.Sleep(10 * time.Second)

			log.Println("Database is healthy.")

		}).
		AddStep("execute_query", "ดำเนินการคำสั่ง SQL").
		AddSubStep("prepare_statement", "เตรียมคำสั่ง", 3*time.Second).
		AddSubStep("handle_results", "จัดการผลลัพธ์", 4*time.Second).
		AddStep("close_connection", "ปิดการเชื่อมต่อ").
		AddSubStep("commit_transaction", "ยืนยันธุรกรรม", 2*time.Second).
		AddSubStep("rollback_transaction", "ย้อนกลับธุรกรรม", 3*time.Second)

	dbProcess.BuildAndRegister("database_processing")
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Failed to load .env file")
	}

	// Register production-ready Builder Pattern processes
	registerBuilderProcesses()

	// --- 1. Initialize Adapters (Singletons) ---

	// Use in-memory repository for testing
	notifier := broadcast.NewWebSocketNotifier()
	JobQueue := queue.NewAsynqJobQueue()

	// MySQL repository for production
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

	// --- 3. Initialize Asynq Worker (Multi-Process) ---
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

	// Create process-specific task handlers
	dataAnalysisHandler := queue.NewProcessTaskHandler(jobRepo, notifier, "data_analysis")
	fileImportHandler := queue.NewProcessTaskHandler(jobRepo, notifier, "file_import")
	reportGenHandler := queue.NewProcessTaskHandler(jobRepo, notifier, "report_gen")

	// Register all process handlers
	mux := asynq.NewServeMux()
	mux.HandleFunc(queue.GetTaskTypeForProcess("data_analysis"), dataAnalysisHandler.HandleAnalysisTask)
	mux.HandleFunc(queue.GetTaskTypeForProcess("file_import"), fileImportHandler.HandleAnalysisTask)
	mux.HandleFunc(queue.GetTaskTypeForProcess("report_gen"), reportGenHandler.HandleAnalysisTask)

	// Register ALL processes from ProcessConfigurations (including Builder-created ones)
	registeredProcesses := map[string]bool{} // Track registered processes

	// First register the main processes
	registeredProcesses["data_analysis"] = true
	registeredProcesses["file_import"] = true
	registeredProcesses["report_gen"] = true

	// Then register any additional processes (Builder Pattern processes)
	dynamicCount := 0
	for processType := range queue.ProcessConfigurations {
		if !registeredProcesses[processType] {
			// Create handler for dynamic process
			dynamicHandler := queue.NewProcessTaskHandler(jobRepo, notifier, processType)
			mux.HandleFunc(queue.GetTaskTypeForProcess(processType), dynamicHandler.HandleAnalysisTask)
			log.Printf("   - %s: %s", processType, queue.GetTaskTypeForProcess(processType))
			dynamicCount++
		}
	}

	if dynamicCount > 0 {
		log.Printf("✅ Registered %d additional dynamic processes", dynamicCount)
	}

	log.Println("✅ Registered Process Task Handlers:")
	log.Println("   - Data Analysis: task:data_analysis")
	log.Println("   - File Import: task:file_import")
	log.Println("   - Report Generation: task:report_gen")

	// 🔥 Redis Recovery System - จัดการ job ที่ค้างเมื่อ Redis ล้ม
	recoveryManager := queue.NewRecoveryManager(jobRepo, JobQueue, notifier)

	go func() {
		time.Sleep(10 * time.Second) // รอให้ระบบ start ก่อน

		// ทุกๆ 2 นาทีตรวจสอบงานที่ค้างอยู่และ Redis connectivity
		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			if err := recoveryManager.RecoverStuckJobs(); err != nil {
				log.Printf("🚨 Recovery Error: %v", err)
			}
		}
	}()

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
