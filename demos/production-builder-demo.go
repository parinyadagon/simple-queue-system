package main

import (
	"context"
	"log"
	"simple-queue-103/internal/lib/process"
	"time"
)

// Demo: Production-Ready Builder Pattern Performance
func main() {
	log.Println("🚀 Starting Production-Ready Builder Pattern Demo...")

	// Create all optimized processes
	createOptimizedProcesses()

	// Show registered processes
	showRegisteredProcesses()

	log.Println("✅ Demo completed successfully!")
}

func createOptimizedProcesses() {
	// Example 1: Email Campaign (Pre-allocated)
	log.Println("📧 Creating Email Campaign Process...")
	process.NewProcessManager().
		CreateCustomProcessWithCapacity("Email Campaign Pro", 3).
		AddStep("LOAD_CONTACTS", "โหลดรายชื่อผู้รับ").
		AddSubStep("LOAD_CONTACTS_VALIDATING", "ตรวจสอบรายชื่อ", 1*time.Second).
		AddSubStep("LOAD_CONTACTS_IMPORTING", "นำเข้ารายชื่อ", 2*time.Second).
		AddSubStep("LOAD_CONTACTS_COMPLETED", "โหลดเสร็จสิ้น", 0).
		AddStep("CREATE_CAMPAIGN", "สร้างแคมเปญ").
		AddSubStep("CREATE_CAMPAIGN_DESIGN", "ออกแบบอีเมล", 3*time.Second).
		AddSubStep("CREATE_CAMPAIGN_SCHEDULE", "กำหนดเวลาส่ง", 1*time.Second).
		AddSubStep("CREATE_CAMPAIGN_COMPLETED", "สร้างแคมเปญเสร็จสิ้น", 0).
		AddStep("SEND_EMAILS", "ส่งอีเมล").
		AddSubStep("SEND_EMAILS_PREPARING", "เตรียมส่ง", 1*time.Second).
		AddSubStep("SEND_EMAILS_SENDING", "กำลังส่ง", 10*time.Second).
		AddSubStep("SEND_EMAILS_COMPLETED", "ส่งเสร็จสิ้น", 0).
		BuildAndRegister("email_campaign_pro")

	// Example 2: Batch Processing with Custom Actions
	log.Println("⚡ Creating Batch Processing Process...")
	process.NewProcessManager().
		CreateCustomProcessWithCapacity("Batch Processing", 2).
		AddStep("PREPARE_BATCH", "เตรียมแบทช์").
		AddSubStepWithAction("PREPARE_BATCH_INIT", "เริ่มต้นแบทช์", 1*time.Second, func() {
			log.Printf("🔥 Custom action: Batch pool initialized")
		}).
		AddSubStep("PREPARE_BATCH_COMPLETED", "เตรียมเสร็จสิ้น", 0).
		AddStepWithFunc("PROCESS_BATCH", "ประมวลผลแบทช์", func(ctx context.Context, jobID string, step *process.JobStepConfig) error {
			log.Printf("⚡ Custom execution for job %s", jobID)
			return nil
		}).
		AddSubStep("PROCESS_BATCH_WORKING", "กำลังประมวลผล", 5*time.Second).
		AddSubStep("PROCESS_BATCH_CLEANUP", "ทำความสะอาด", 1*time.Second).
		AddSubStep("PROCESS_BATCH_COMPLETED", "ประมวลผลเสร็จสิ้น", 0).
		BuildAndRegister("batch_processing")

	// Example 3: Image Processing Pipeline
	log.Println("🎨 Creating Image Processing Pipeline...")
	process.NewProcessManager().
		CreateCustomProcessWithCapacity("Image Processing", 3).
		AddStep("UPLOAD_IMAGES", "อัพโหลดรูปภาพ").
		AddSubStep("UPLOAD_IMAGES_VALIDATING", "ตรวจสอบรูปภาพ", 1*time.Second).
		AddSubStep("UPLOAD_IMAGES_UPLOADING", "กำลังอัพโหลด", 3*time.Second).
		AddSubStep("UPLOAD_IMAGES_COMPLETED", "อัพโหลดเสร็จสิ้น", 0).
		AddStep("PROCESS_IMAGES", "ประมวลผลรูปภาพ").
		AddSubStep("PROCESS_IMAGES_RESIZING", "ปรับขนาด", 2*time.Second).
		AddSubStep("PROCESS_IMAGES_FILTERING", "ใส่ฟิลเตอร์", 3*time.Second).
		AddSubStep("PROCESS_IMAGES_COMPLETED", "ประมวลผลเสร็จสิ้น", 0).
		AddStep("SAVE_RESULTS", "บันทึกผลลัพธ์").
		AddSubStep("SAVE_RESULTS_COMPRESSING", "บีบอัดไฟล์", 1*time.Second).
		AddSubStep("SAVE_RESULTS_STORING", "บันทึกไฟล์", 2*time.Second).
		AddSubStep("SAVE_RESULTS_COMPLETED", "บันทึกเสร็จสิ้น", 0).
		BuildAndRegister("image_processing")
}

func showRegisteredProcesses() {
	log.Println("\n📋 Registered Processes:")
	for key := range process.ProcessConfigurations {
		config := process.ProcessConfigurations[key]
		log.Printf("  ✅ %s: %s (%d steps)", key, config.ProcessName, len(config.Steps))
	}

	log.Println("\n🎯 Usage Examples:")
	log.Println("  curl -X POST http://localhost:8080/jobs -d '{\"fileName\": \"newsletter.html\", \"processType\": \"email_campaign_pro\"}'")
	log.Println("  curl -X POST http://localhost:8080/jobs -d '{\"fileName\": \"data_batch.csv\", \"processType\": \"batch_processing\"}'")
	log.Println("  curl -X POST http://localhost:8080/jobs -d '{\"fileName\": \"photos.zip\", \"processType\": \"image_processing\"}'")
}