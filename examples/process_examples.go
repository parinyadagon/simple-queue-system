package main

import (
	"fmt"
	"log"
	"simple-queue-103/internal/adapters/queue"
	"time"
)

// ตัวอย่างการสร้าง Custom Process สำหรับ Email Campaign
func createEmailCampaignProcess() {
	processManager := queue.NewProcessManager()

	// สร้าง Email Campaign Process
	processManager.CreateCustomProcess("Email Campaign").
		AddStep("LOAD_CONTACTS", "กำลังโหลดรายชื่อผู้รับ").
		AddSubStep("LOAD_CONTACTS_READING", "กำลังอ่านไฟล์รายชื่อ", 2*time.Second).
		AddSubStep("LOAD_CONTACTS_VALIDATING", "กำลังตรวจสอบอีเมล", 3*time.Second).
		AddSubStep("LOAD_CONTACTS_FILTERING", "กำลังกรองรายชื่อ", 1*time.Second).
		AddSubStep("LOAD_CONTACTS_COMPLETED", "โหลดรายชื่อเสร็จสิ้น", 0).
		AddStep("CREATE_CONTENT", "กำลังสร้างเนื้อหาอีเมล").
		AddSubStep("CREATE_CONTENT_TEMPLATE", "กำลังโหลดเท็มเพลต", 1*time.Second).
		AddSubStep("CREATE_CONTENT_PERSONALIZE", "กำลังปรับแต่งเนื้อหา", 2*time.Second).
		AddSubStep("CREATE_CONTENT_PREVIEW", "กำลังสร้างตัวอย่าง", 1*time.Second).
		AddSubStep("CREATE_CONTENT_COMPLETED", "สร้างเนื้อหาเสร็จสิ้น", 0).
		AddStep("SEND_EMAILS", "กำลังส่งอีเมล").
		AddSubStep("SEND_EMAILS_PREPARING", "กำลังเตรียมส่ง", 1*time.Second).
		AddSubStep("SEND_EMAILS_SENDING", "กำลังส่งอีเมลทีละกลุ่ม", 8*time.Second).
		AddSubStep("SEND_EMAILS_TRACKING", "กำลังติดตามผล", 1*time.Second).
		AddSubStep("SEND_EMAILS_COMPLETED", "ส่งอีเมลเสร็จสิ้น", 0).
		Build()

	// เปลี่ยนเป็น default process
	queue.DefaultProcessManager = processManager

	fmt.Println("✅ Email Campaign Process ถูกติดตั้งแล้ว!")
	fmt.Println("📧 Process นี้มี 3 ขั้นตอนหลัก:")
	fmt.Println("   1. โหลดรายชื่อผู้รับ (4 sub-steps)")
	fmt.Println("   2. สร้างเนื้อหาอีเมล (4 sub-steps)")
	fmt.Println("   3. ส่งอีเมล (4 sub-steps)")
}

// ตัวอย่างการสร้าง Advanced Process พร้อม Custom Logic
func createAdvancedImageProcess() {
	processManager := queue.NewProcessManager()

	// สำหรับการสาธิต เราจะแสดงว่าสามารถสร้าง custom logic ได้
	fmt.Println("🤖 Process นี้สามารถมี custom AI logic ในการประมวลผลรูปภาพ")

	// สำหรับตัวอย่างนี้ เราจะใช้ builder pattern แทน
	processManager.CreateCustomProcess("Advanced Image Processing").
		AddStep("UPLOAD_IMAGES", "กำลังอัพโหลดรูปภาพ").
		AddSubStep("UPLOAD_IMAGES_VALIDATING", "กำลังตรวจสอบไฟล์รูปภาพ", 1*time.Second).
		AddSubStep("UPLOAD_IMAGES_UPLOADING", "กำลังอัพโหลด", 3*time.Second).
		AddSubStep("UPLOAD_IMAGES_COMPLETED", "อัพโหลดเสร็จสิ้น", 0).
		AddStep("PROCESS_IMAGES", "กำลังประมวลผลรูปภาพ").
		AddSubStep("PROCESS_IMAGES_ANALYZING", "กำลังวิเคราะห์รูปภาพ", 2*time.Second).
		AddSubStep("PROCESS_IMAGES_ENHANCING", "กำลังปรับปรุงคุณภาพ", 4*time.Second).
		AddSubStep("PROCESS_IMAGES_COMPLETED", "ประมวลผลเสร็จสิ้น", 0).
		AddStep("SAVE_RESULTS", "กำลังบันทึกผลลัพธ์").
		AddSubStep("SAVE_RESULTS_ORGANIZING", "กำลังจัดระเบียบไฟล์", 1*time.Second).
		AddSubStep("SAVE_RESULTS_SAVING", "กำลังบันทึกไฟล์", 2*time.Second).
		AddSubStep("SAVE_RESULTS_COMPLETED", "บันทึกผลลัพธ์เสร็จสิ้น", 0).
		Build()

	queue.DefaultProcessManager = processManager

	fmt.Println("✅ Advanced Image Processing ถูกติดตั้งแล้ว!")
	fmt.Println("🖼️ Process นี้มี custom AI processing logic")
}

// ตัวอย่างการสร้าง Simple Process สำหรับ File Import
func createSimpleFileImportProcess() {
	processManager := queue.NewProcessManager()

	// ใช้ pre-defined process ที่มีอยู่แล้ว
	processManager.UseProcess("file_import")
	queue.DefaultProcessManager = processManager

	fmt.Println("✅ Simple File Import Process ถูกเลือกใช้แล้ว!")
	fmt.Println("📁 Process นี้มี 2 ขั้นตอน: Upload → Process Data")
}

// ฟังก์ชันแสดงตัวอย่างการใช้งาน
func demonstrateProcesses() {
	fmt.Println("\n🚀 === ตัวอย่างการใช้งาน Process ต่างๆ ===")

	fmt.Println("1️⃣ Email Campaign Process:")
	createEmailCampaignProcess()

	fmt.Println("\n2️⃣ Advanced Image Processing:")
	createAdvancedImageProcess()

	fmt.Println("\n3️⃣ Simple File Import:")
	createSimpleFileImportProcess()

	fmt.Println("\n📊 Available Pre-defined Processes:")
	for name := range queue.ProcessConfigurations {
		fmt.Printf("   - %s\n", name)
	}

	fmt.Println("\n💡 วิธีใช้: เรียก createXXXProcess() ใน main() ก่อนเริ่มเซิร์ฟเวอร์")
}

// ฟังก์ชันสำหรับเปลี่ยน process ระหว่างทำงาน
func switchToEmailCampaign() {
	createEmailCampaignProcess()
	log.Println("🔄 เปลี่ยนเป็น Email Campaign Process แล้ว")
}

func switchToImageProcessing() {
	createAdvancedImageProcess()
	log.Println("🔄 เปลี่ยนเป็น Image Processing แล้ว")
}

func switchToDataAnalysis() {
	processManager := queue.NewProcessManager().UseProcess("data_analysis")
	queue.DefaultProcessManager = processManager
	log.Println("🔄 เปลี่ยนกลับเป็น Data Analysis แล้ว")
}

// สำหรับทดสอบใน main function
func main() {
	// แสดงตัวอย่างการใช้งาน
	demonstrateProcesses()

	// หมายเหตุ: ดูตัวอย่าง Custom Functions ได้ที่ examples/custom_functions_demo.go
	fmt.Println("\n==================================================")
	fmt.Println("🔧 === หมายเหตุ: Custom Functions Examples ===")
	fmt.Println("ดูตัวอย่างการใช้ Custom Functions ได้ที่:")
	fmt.Println("👉 examples/custom_functions_demo.go")
	fmt.Println("รันด้วย: go run examples/custom_functions_demo.go")

	// สำหรับทดสอบ - เปลี่ยนเป็น Email Campaign
	// createEmailCampaignProcess()

	// หรือจะใช้ default data_analysis
	// queue.DefaultProcessManager.UseProcess("data_analysis")

	fmt.Println("\n🎉 ระบบพร้อมใช้งานแล้ว! คุณสามารถเปลี่ยน process ได้ตามต้องการ")
}
