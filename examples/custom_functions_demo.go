package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"simple-queue-103/internal/adapters/queue"
	"time"
)

// Custom business logic functions
func processUserData(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
	log.Printf("🔍 Job %s: Starting custom user data processing...", jobID)

	// Simulate database operations
	users := []string{"John", "Jane", "Bob", "Alice", "Charlie"}

	for i, user := range users {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("user data processing cancelled")
		default:
		}

		// Simulate processing each user
		log.Printf("👤 Job %s: Processing user: %s (%d/%d)", jobID, user, i+1, len(users))
		time.Sleep(time.Second)

		// Simulate occasional errors for demonstration
		if rand.Intn(10) == 0 {
			log.Printf("⚠️  Job %s: Warning - skipping invalid data for user: %s", jobID, user)
		}
	}

	log.Printf("✅ Job %s: User data processing completed successfully", jobID)
	return nil
}

func generateAdvancedReport(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
	log.Printf("📊 Job %s: Starting advanced report generation...", jobID)

	// Simulate complex report generation
	reportSections := []string{"Executive Summary", "Data Analysis", "Charts & Graphs", "Recommendations", "Appendix"}

	for i, section := range reportSections {
		select {
		case <-ctx.Done():
			return fmt.Errorf("report generation cancelled")
		default:
		}

		log.Printf("📝 Job %s: Generating section: %s (%d/%d)", jobID, section, i+1, len(reportSections))

		// Simulate different processing times for different sections
		switch section {
		case "Data Analysis":
			time.Sleep(3 * time.Second) // Heavy processing
		case "Charts & Graphs":
			time.Sleep(2 * time.Second) // Medium processing
		default:
			time.Sleep(time.Second) // Light processing
		}
	}

	log.Printf("✅ Job %s: Advanced report generated successfully", jobID)
	return nil
}

func performSecurityAudit(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
	log.Printf("🔒 Job %s: Starting security audit...", jobID)

	// Simulate security checks
	securityChecks := []string{
		"Authentication validation",
		"Authorization verification",
		"Data encryption check",
		"Access log analysis",
		"Vulnerability scanning",
	}

	vulnerabilities := 0
	for i, check := range securityChecks {
		select {
		case <-ctx.Done():
			return fmt.Errorf("security audit cancelled")
		default:
		}

		log.Printf("🔍 Job %s: Performing: %s (%d/%d)", jobID, check, i+1, len(securityChecks))
		time.Sleep(time.Second)

		// Simulate finding occasional vulnerabilities
		if rand.Intn(4) == 0 {
			vulnerabilities++
			log.Printf("⚠️  Job %s: Found vulnerability in: %s", jobID, check)
		}
	}

	if vulnerabilities > 0 {
		log.Printf("⚠️  Job %s: Security audit completed with %d vulnerabilities found", jobID, vulnerabilities)
	} else {
		log.Printf("✅ Job %s: Security audit completed - no vulnerabilities found", jobID)
	}

	return nil
}

func main() {
	// Seed random for demonstration
	rand.Seed(time.Now().UnixNano())

	fmt.Println("🚀 === ตัวอย่างการสร้าง Custom Functions ===")

	// Example 1: Complete Custom Process with Custom Functions
	fmt.Println("\n1️⃣ === Custom Data Processing Workflow ===")

	customDataProcess := queue.NewProcessManager().
		CreateCustomProcess("Custom Data Processing").
		AddStepWithFunc("PROCESS_USERS", "กำลังประมวลผลข้อมูลผู้ใช้", processUserData).
		AddSubStep("PROCESS_USERS_LOADING", "กำลังโหลดข้อมูลผู้ใช้", time.Second).
		AddSubStep("PROCESS_USERS_VALIDATING", "กำลังตรวจสอบข้อมูล", 2*time.Second).
		AddSubStep("PROCESS_USERS_PROCESSING", "กำลังประมวลผลข้อมูล", time.Second).
		AddSubStep("PROCESS_USERS_COMPLETED", "ประมวลผลข้อมูลผู้ใช้เสร็จสิ้น", 0).
		AddStepWithFunc("GENERATE_REPORT", "กำลังสร้างรายงานขั้นสูง", generateAdvancedReport).
		AddSubStep("GENERATE_REPORT_COLLECTING", "กำลังรวบรวมข้อมูล", time.Second).
		AddSubStep("GENERATE_REPORT_ANALYZING", "กำลังวิเคราะห์ข้อมูล", 2*time.Second).
		AddSubStep("GENERATE_REPORT_FORMATTING", "กำลังจัดรูปแบบรายงาน", time.Second).
		AddSubStep("GENERATE_REPORT_COMPLETED", "สร้างรายงานเสร็จสิ้น", 0).
		Build()

	fmt.Printf("✅ Custom Data Processing Process ถูกสร้างแล้ว!\n")
	fmt.Printf("   - Steps: %v\n", customDataProcess.GetSteps())

	// Example 2: Security Audit Process with Mixed Functions
	fmt.Println("\n2️⃣ === Security Audit Workflow ===")

	securityProcess := queue.NewProcessManager().
		CreateCustomProcess("Security Audit").
		AddStep("PREPARE_AUDIT", "กำลังเตรียมการตรวจสอบ").
		AddSubStepWithAction("PREPARE_AUDIT_SETUP", "กำลังตั้งค่าระบบ", time.Second, func() {
			log.Printf("🔧 Setting up audit environment...")
		}).
		AddSubStepWithAction("PREPARE_AUDIT_LOAD_RULES", "กำลังโหลดกฎการตรวจสอบ", time.Second, func() {
			log.Printf("📋 Loading security rules...")
		}).
		AddSubStep("PREPARE_AUDIT_COMPLETED", "เตรียมการตรวจสอบเสร็จสิ้น", 0).
		AddStepWithFunc("SECURITY_SCAN", "กำลังตรวจสอบความปลอดภัย", performSecurityAudit).
		AddSubStep("SECURITY_SCAN_RUNNING", "กำลังสแกนความปลอดภัย", 3*time.Second).
		AddSubStep("SECURITY_SCAN_ANALYZING", "กำลังวิเคราะห์ผลลัพธ์", 2*time.Second).
		AddSubStep("SECURITY_SCAN_COMPLETED", "ตรวจสอบความปลอดภัยเสร็จสิ้น", 0).
		Build()

	fmt.Printf("✅ Security Audit Process ถูกสร้างแล้ว!\n")
	fmt.Printf("   - Steps: %v\n", securityProcess.GetSteps())

	// Example 3: Custom Function with SetExecuteFunc
	fmt.Println("\n3️⃣ === Custom Function Example ===")

	customFunc := func(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
		log.Printf("🎯 Job %s: Executing completely custom logic", jobID)

		operations := []string{"Initialize", "Process", "Validate", "Finalize"}

		for i, op := range operations {
			select {
			case <-ctx.Done():
				return fmt.Errorf("custom operation cancelled")
			default:
			}

			log.Printf("⚙️  Job %s: %s (%d/%d)", jobID, op, i+1, len(operations))
			time.Sleep(500 * time.Millisecond)
		}

		log.Printf("✅ Job %s: Custom function completed successfully", jobID)
		return nil
	}

	customFuncProcess := queue.NewProcessManager().
		CreateCustomProcess("Custom Function Demo").
		AddStep("CUSTOM_LOGIC", "กำลังประมวลผลด้วยฟังก์ชันพิเศษ").
		SetExecuteFunc(customFunc).
		AddSubStep("CUSTOM_LOGIC_INIT", "กำลังเริ่มต้นระบบ", 500*time.Millisecond).
		AddSubStep("CUSTOM_LOGIC_PROCESSING", "กำลังประมวลผลข้อมูล", 2*time.Second).
		AddSubStep("CUSTOM_LOGIC_COMPLETED", "ประมวลผลด้วยฟังก์ชันพิเศษเสร็จสิ้น", 0).
		Build()

	fmt.Printf("✅ Custom Function Process ถูกสร้างแล้ว!\n")
	fmt.Printf("   - Steps: %v\n", customFuncProcess.GetSteps())

	fmt.Println("\n🎉 === การสาธิตเสร็จสิ้น ===")
	fmt.Println("ตอนนี้คุณสามารถ:")
	fmt.Println("1. ใช้ AddStepWithFunc() เพื่อเพิ่ม step ที่มี custom function")
	fmt.Println("2. ใช้ AddSubStepWithAction() เพื่อเพิ่ม sub-step ที่มี custom action")
	fmt.Println("3. ใช้ SetExecuteFunc() เพื่อกำหนด custom function ให้ step ที่มีอยู่")
	fmt.Println("4. รวม custom functions กับ standard sub-steps ได้")
	fmt.Println("5. สร้าง business logic ที่ซับซ้อนได้ตามต้องการ")
}
