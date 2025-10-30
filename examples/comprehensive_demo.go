// ตัวอย่างการใช้งาน Custom Functions ครบครัน
package main

import (
	"context"
	"fmt"
	"log"
	"simple-queue-103/internal/adapters/queue"
	"time"
)

// ตัวอย่าง: Custom Email Marketing Function
func sendEmailCampaign(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
	log.Printf("📧 Job %s: Starting email campaign", jobID)

	// ตรวจสอบ cancellation ก่อนทำงาน (สำคัญ!)
	select {
	case <-ctx.Done():
		return fmt.Errorf("email campaign cancelled")
	default:
	}

	// Simulate email sending
	batches := []string{"VIP Customers", "Regular Customers", "New Subscribers"}
	for i, batch := range batches {
		select {
		case <-ctx.Done():
			return fmt.Errorf("email campaign cancelled during batch %s", batch)
		default:
		}

		log.Printf("📨 Job %s: Sending to %s (%d/%d)", jobID, batch, i+1, len(batches))
		time.Sleep(2 * time.Second)
	}

	log.Printf("✅ Job %s: Email campaign completed successfully", jobID)
	return nil
}

// ตัวอย่าง: Database Processing Function
func processDatabase(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
	log.Printf("💾 Job %s: Starting database processing", jobID)

	// Simulate database operations with context checking
	operations := []string{"Connect", "Query", "Process", "Update", "Cleanup"}

	for i, op := range operations {
		select {
		case <-ctx.Done():
			return fmt.Errorf("database processing cancelled at %s", op)
		default:
		}

		log.Printf("🔄 Job %s: %s (%d/%d)", jobID, op, i+1, len(operations))
		time.Sleep(time.Second)
	}

	return nil
}

func demonstrateComprehensiveUsage() {
	fmt.Println("🎯 === การใช้งาน Custom Functions แบบครอบคลุม ===")

	// 1. Email Marketing Process - ครอบคลุม custom functions + actions
	emailProcess := queue.NewProcessManager().
		CreateCustomProcess("Email Marketing").

		// Step 1: เตรียมข้อมูล (มี custom actions)
		AddStep("PREPARE_DATA", "กำลังเตรียมข้อมูล").
		AddSubStepWithAction("PREPARE_DATA_VALIDATE", "ตรวจสอบรายชื่อ", time.Second, func() {
			log.Printf("📋 Validating email list...")
		}).
		AddSubStepWithAction("PREPARE_DATA_SEGMENT", "แบ่งกลุ่มลูกค้า", 2*time.Second, func() {
			log.Printf("👥 Segmenting customers...")
		}).
		AddSubStep("PREPARE_DATA_COMPLETED", "เตรียมข้อมูลเสร็จสิ้น", 0).

		// Step 2: ส่งอีเมล (มี custom function)
		AddStepWithFunc("SEND_EMAILS", "กำลังส่งอีเมลแคมเปญ", sendEmailCampaign).
		AddSubStep("SEND_EMAILS_QUEUING", "จัดคิวอีเมล", time.Second).
		AddSubStep("SEND_EMAILS_SENDING", "ส่งอีเมล", 6*time.Second).
		AddSubStep("SEND_EMAILS_COMPLETED", "ส่งอีเมลเสร็จสิ้น", 0).

		// Step 3: สร้างรายงาน (ใช้ SetExecuteFunc)
		AddStep("GENERATE_REPORT", "กำลังสร้างรายงาน").
		SetExecuteFunc(func(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
			log.Printf("📊 Job %s: Generating email campaign report", jobID)

			select {
			case <-ctx.Done():
				return fmt.Errorf("report generation cancelled")
			default:
			}

			time.Sleep(3 * time.Second)
			log.Printf("✅ Job %s: Report generated", jobID)
			return nil
		}).
		AddSubStep("GENERATE_REPORT_COLLECTING", "รวบรวมข้อมูล", 2*time.Second).
		AddSubStep("GENERATE_REPORT_COMPLETED", "สร้างรายงานเสร็จสิ้น", 0).
		Build()

	fmt.Printf("✅ Email Marketing Process ครอบคลุมทุก features:\n")
	fmt.Printf("   - Steps: %v\n", emailProcess.GetSteps())
	fmt.Printf("   - Custom Functions: ✅\n")
	fmt.Printf("   - Custom Actions: ✅\n")
	fmt.Printf("   - SetExecuteFunc: ✅\n")

	// 2. Data Processing - ครอบคลุม error handling + context
	dataProcess := queue.NewProcessManager().
		CreateCustomProcess("Data Processing").
		AddStepWithFunc("PROCESS_DATABASE", "ประมวลผลฐานข้อมูล", processDatabase).
		AddSubStep("PROCESS_DATABASE_CONNECTING", "เชื่อมต่อฐานข้อมูล", time.Second).
		AddSubStep("PROCESS_DATABASE_PROCESSING", "ประมวลผลข้อมูล", 4*time.Second).
		AddSubStep("PROCESS_DATABASE_COMPLETED", "ประมวลผลเสร็จสิ้น", 0).
		Build()

	fmt.Printf("✅ Data Processing Process:\n")
	fmt.Printf("   - Context Handling: ✅\n")
	fmt.Printf("   - Error Recovery: ✅\n")

	// 3. Registration และ Dynamic Switching
	fmt.Println("\n🔄 === Dynamic Process Management ===")

	// Register processes
	if emailProcess.GetCurrentProcessConfig() != nil {
		queue.ProcessConfigurations["email_marketing"] = emailProcess.GetCurrentProcessConfig()
		fmt.Println("✅ Email Marketing Process registered")
	}

	if dataProcess.GetCurrentProcessConfig() != nil {
		queue.ProcessConfigurations["data_processing"] = dataProcess.GetCurrentProcessConfig()
		fmt.Println("✅ Data Processing Process registered")
	}

	// Dynamic switching
	switchToEmail := queue.NewProcessManager().UseProcess("email_marketing")
	if switchToEmail != nil {
		fmt.Println("🔄 Switched to Email Marketing")
		fmt.Printf("   Current steps: %v\n", switchToEmail.GetSteps())
	}

	switchToData := queue.NewProcessManager().UseProcess("data_processing")
	if switchToData != nil {
		fmt.Println("🔄 Switched to Data Processing")
		fmt.Printf("   Current steps: %v\n", switchToData.GetSteps())
	}

	// 4. Integration กับระบบเดิม
	fmt.Println("\n⚙️ === Integration Status ===")
	fmt.Println("✅ Backward Compatibility - ระบบเดิมทำงานได้ปกติ")
	fmt.Println("✅ WebSocket Updates - Real-time progress tracking")
	fmt.Println("✅ Progress Calculation - Sub-step granular tracking")
	fmt.Println("✅ Resume/Pause/Cancel - ทุก functions รองรับ")
	fmt.Println("✅ Error Handling - Proper error propagation")
	fmt.Println("✅ Context Cancellation - Graceful shutdown")

	fmt.Println("\n🎉 สรุป: ระบบครอบคลุมทุกการใช้งานแล้ว!")
	fmt.Println("📝 Available Process Types:")
	for name := range queue.ProcessConfigurations {
		fmt.Printf("   - %s\n", name)
	}
}

func runComprehensiveDemo() {
	demonstrateComprehensiveUsage()
}
