# 🚀 Easy Usage Examples - Simple Queue System

## 📋 Overview

ระบบ Job Queue ใหม่ได้รับการปรับปรุงให้ใช้งานง่ายขึ้นมาก สามารถสร้าง process ใหม่ๆ ได้อย่างง่ายดาย

## 🎯 วิธีการใช้งาน

### 1. ใช้ Process ที่มีอยู่แล้ว

```go
package main

import (
    "simple-queue-103/internal/adapters/queue"
)

func main() {
    // เลือกใช้ process ที่มีอยู่แล้ว
    processManager := queue.NewProcessManager()
    
    // ตัวเลือกที่มี: "data_analysis", "file_import", "report_gen"
    processManager.UseProcess("file_import")
    
    // ทำให้เป็น default สำหรับระบบ
    queue.DefaultProcessManager = processManager
}
```

### 2. สร้าง Custom Process แบบง่าย

```go
func createSimpleEmailProcess() {
    processManager := queue.NewProcessManager()
    
    // สร้าง process ใหม่
    processManager.CreateCustomProcess("Email Campaign").
        AddStep("LOAD_CONTACTS", "กำลังโหลดรายชื่อผู้รับ").
            AddSubStep("LOAD_CONTACTS_READING", "กำลังอ่านไฟล์", 2*time.Second).
            AddSubStep("LOAD_CONTACTS_VALIDATING", "กำลังตรวจสอบข้อมูล", 3*time.Second).
            AddSubStep("LOAD_CONTACTS_COMPLETED", "โหลดรายชื่อเสร็จสิ้น", 0).
        AddStep("SEND_EMAILS", "กำลังส่งอีเมล").
            AddSubStep("SEND_EMAILS_PREPARING", "กำลังเตรียมเนื้อหา", 1*time.Second).
            AddSubStep("SEND_EMAILS_SENDING", "กำลังส่งอีเมล", 5*time.Second).
            AddSubStep("SEND_EMAILS_COMPLETED", "ส่งอีเมลเสร็จสิ้น", 0).
        Build()
    
    // ใช้เป็น default
    queue.DefaultProcessManager = processManager
}
```

### 3. สร้าง Advanced Custom Process

```go
func createAdvancedImageProcess() {
    processManager := queue.NewProcessManager()
    
    processManager.CreateCustomProcess("Image Processing").
        AddStep("UPLOAD_IMAGES", "กำลังอัพโหลดรูปภาพ").
            AddSubStep("UPLOAD_IMAGES_VALIDATING", "กำลังตรวจสอบรูปภาพ", time.Second).
            AddSubStep("UPLOAD_IMAGES_UPLOADING", "กำลังอัพโหลด", 3*time.Second).
            AddSubStep("UPLOAD_IMAGES_COMPLETED", "อัพโหลดเสร็จสิ้น", 0).
        AddStep("RESIZE_IMAGES", "กำลังปรับขนาดรูปภาพ").
            AddSubStep("RESIZE_IMAGES_LOADING", "กำลังโหลดรูปภาพ", time.Second).
            AddSubStep("RESIZE_IMAGES_RESIZING", "กำลังปรับขนาด", 4*time.Second).
            AddSubStep("RESIZE_IMAGES_OPTIMIZING", "กำลังปรับแต่ง", 2*time.Second).
            AddSubStep("RESIZE_IMAGES_COMPLETED", "ปรับขนาดเสร็จสิ้น", 0).
        AddStep("APPLY_FILTERS", "กำลังใส่เอฟเฟกต์").
            AddSubStep("APPLY_FILTERS_LOADING", "กำลังโหลดฟิลเตอร์", time.Second).
            AddSubStep("APPLY_FILTERS_APPLYING", "กำลังใส่เอฟเฟกต์", 3*time.Second).
            AddSubStep("APPLY_FILTERS_COMPLETED", "ใส่เอฟเฟกต์เสร็จสิ้น", 0).
        Build()
    
    queue.DefaultProcessManager = processManager
}
```

### 4. สร้าง Process พร้อม Custom Actions

```go
func createProcessWithCustomActions() {
    processManager := queue.NewProcessManager()
    
    // สร้างขั้นตอนพร้อม custom logic
    step := queue.JobStepConfig{
        Name:        "CUSTOM_PROCESSING",
        Description: "กำลังประมวลผลแบบกำหนดเอง",
        SubSteps: []queue.JobSubStepConfig{
            {
                Name:        "CUSTOM_PROCESSING_INIT",
                Description: "กำลังเริ่มต้นระบบ",
                Duration:    2 * time.Second,
                Action: func() {
                    // Custom logic ที่ต้องการ
                    fmt.Println("🚀 เริ่มต้นระบบพิเศษ...")
                    // เรียก API, เชื่อมต่อ database, etc.
                },
            },
            {
                Name:        "CUSTOM_PROCESSING_WORK",
                Description: "กำลังทำงานหลัก",
                Duration:    5 * time.Second,
                Action: func() {
                    fmt.Println("⚡ กำลังทำงานหลัก...")
                    // ทำงานจริงตรงนี้
                },
            },
            {
                Name:        "CUSTOM_PROCESSING_COMPLETED",
                Description: "ประมวลผลเสร็จสิ้น",
                Duration:    0,
            },
        },
        ExecuteFunc: func(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
            // Custom execution logic สำหรับทั้ง step
            fmt.Printf("🎯 กำลังประมวลผล Job: %s\n", jobID)
            
            // เรียกใช้ default execution
            return nil // หรือเรียก executeGenericStep
        },
    }
    
    // เพิ่มใน process configuration
    config := &queue.JobProcessConfig{
        ProcessName: "Custom Process",
        Steps:       []queue.JobStepConfig{step},
    }
    
    processManager.currentProcess = config
    queue.DefaultProcessManager = processManager
}
```

## 📖 วิธีการใช้ในโปรเจคอื่น

### 1. Copy ไฟล์ที่จำเป็น
```bash
# Copy หลักๆ แค่ไฟล์ asynq.go
cp internal/adapters/queue/asynq.go YOUR_PROJECT/internal/adapters/queue/
```

### 2. ปรับ Import ใน main.go
```go
package main

import (
    "your-project/internal/adapters/queue"
)

func main() {
    // สร้าง process ที่ต้องการ
    processManager := queue.NewProcessManager()
    
    // สร้าง custom process สำหรับโปรเจคของคุณ
    processManager.CreateCustomProcess("Your Process Name").
        AddStep("STEP_1", "ขั้นตอนที่ 1").
            AddSubStep("STEP_1_INIT", "เริ่มต้น", 1*time.Second).
            AddSubStep("STEP_1_WORK", "ทำงาน", 3*time.Second).
            AddSubStep("STEP_1_COMPLETED", "เสร็จสิ้น", 0).
        AddStep("STEP_2", "ขั้นตอนที่ 2").
            AddSubStep("STEP_2_PROCESS", "ประมวลผล", 2*time.Second).
            AddSubStep("STEP_2_COMPLETED", "เสร็จสิ้น", 0).
        Build()
    
    // ตั้งเป็น default
    queue.DefaultProcessManager = processManager
    
    // ใช้งานต่อตามปกติ...
}
```

## 🎨 ตัวอย่าง Process ต่างๆ

### 📧 Email Marketing Process
```go
queue.NewProcessManager().CreateCustomProcess("Email Marketing").
    AddStep("IMPORT_CONTACTS", "นำเข้ารายชื่อ").
        AddSubStep("IMPORT_CONTACTS_READING", "อ่านไฟล์", 2*time.Second).
        AddSubStep("IMPORT_CONTACTS_VALIDATING", "ตรวจสอบข้อมูล", 3*time.Second).
        AddSubStep("IMPORT_CONTACTS_COMPLETED", "นำเข้าเสร็จสิ้น", 0).
    AddStep("CREATE_CAMPAIGN", "สร้างแคมเปญ").
        AddSubStep("CREATE_CAMPAIGN_DESIGN", "ออกแบบ", 2*time.Second).
        AddSubStep("CREATE_CAMPAIGN_SCHEDULE", "จัดตารางส่ง", 1*time.Second).
        AddSubStep("CREATE_CAMPAIGN_COMPLETED", "สร้างแคมเปญเสร็จสิ้น", 0).
    AddStep("SEND_EMAILS", "ส่งอีเมล").
        AddSubStep("SEND_EMAILS_PREPARING", "เตรียมส่ง", 1*time.Second).
        AddSubStep("SEND_EMAILS_SENDING", "กำลังส่ง", 10*time.Second).
        AddSubStep("SEND_EMAILS_COMPLETED", "ส่งเสร็จสิ้น", 0).
    Build()
```

### 🛒 E-commerce Order Process
```go
queue.NewProcessManager().CreateCustomProcess("Order Processing").
    AddStep("VALIDATE_ORDER", "ตรวจสอบคำสั่งซื้อ").
        AddSubStep("VALIDATE_ORDER_CHECKING", "ตรวจสอบสินค้า", 1*time.Second).
        AddSubStep("VALIDATE_ORDER_PAYMENT", "ตรวจสอบการชำระเงิน", 2*time.Second).
        AddSubStep("VALIDATE_ORDER_COMPLETED", "ตรวจสอบเสร็จสิ้น", 0).
    AddStep("PREPARE_SHIPMENT", "เตรียมจัดส่ง").
        AddSubStep("PREPARE_SHIPMENT_PICKING", "เก็บสินค้า", 3*time.Second).
        AddSubStep("PREPARE_SHIPMENT_PACKING", "บรรจุสินค้า", 2*time.Second).
        AddSubStep("PREPARE_SHIPMENT_COMPLETED", "เตรียมจัดส่งเสร็จสิ้น", 0).
    AddStep("SHIP_ORDER", "จัดส่งสินค้า").
        AddSubStep("SHIP_ORDER_BOOKING", "จองขนส่ง", 1*time.Second).
        AddSubStep("SHIP_ORDER_TRACKING", "สร้างเลขติดตาม", 1*time.Second).
        AddSubStep("SHIP_ORDER_COMPLETED", "จัดส่งเสร็จสิ้น", 0).
    Build()
```

### 🎬 Video Processing
```go
queue.NewProcessManager().CreateCustomProcess("Video Processing").
    AddStep("UPLOAD_VIDEO", "อัพโหลดวิดีโอ").
        AddSubStep("UPLOAD_VIDEO_VALIDATING", "ตรวจสอบไฟล์", 1*time.Second).
        AddSubStep("UPLOAD_VIDEO_UPLOADING", "อัพโหลด", 5*time.Second).
        AddSubStep("UPLOAD_VIDEO_COMPLETED", "อัพโหลดเสร็จสิ้น", 0).
    AddStep("ENCODE_VIDEO", "แปลงไฟล์วิดีโอ").
        AddSubStep("ENCODE_VIDEO_ANALYZING", "วิเคราะห์วิดีโอ", 2*time.Second).
        AddSubStep("ENCODE_VIDEO_ENCODING", "แปลงไฟล์", 15*time.Second).
        AddSubStep("ENCODE_VIDEO_COMPLETED", "แปลงไฟล์เสร็จสิ้น", 0).
    AddStep("GENERATE_THUMBNAILS", "สร้างภาพตัวอย่าง").
        AddSubStep("GENERATE_THUMBNAILS_EXTRACTING", "สกัดภาพ", 3*time.Second).
        AddSubStep("GENERATE_THUMBNAILS_OPTIMIZING", "ปรับแต่งภาพ", 2*time.Second).
        AddSubStep("GENERATE_THUMBNAILS_COMPLETED", "สร้างภาพตัวอย่างเสร็จสิ้น", 0).
    Build()
```

## 🔧 Advanced Configuration

### การเพิ่ม Custom Validation
```go
func createProcessWithValidation() {
    step := queue.JobStepConfig{
        Name:        "VALIDATE_DATA",
        Description: "ตรวจสอบข้อมูล",
        ExecuteFunc: func(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
            // Custom validation logic
            if !isValidData() {
                return fmt.Errorf("ข้อมูลไม่ถูกต้อง")
            }
            return nil
        },
        SubSteps: []queue.JobSubStepConfig{
            // ... sub steps
        },
    }
    // ... rest of configuration
}
```

### การเพิ่ม Error Handling
```go
func createRobustProcess() {
    processManager := queue.NewProcessManager()
    
    step := queue.JobStepConfig{
        Name:        "SAFE_PROCESSING",
        Description: "ประมวลผลแบบปลอดภัย",
        ExecuteFunc: func(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
            defer func() {
                if r := recover(); r != nil {
                    log.Printf("Job %s recovered from panic: %v", jobID, r)
                }
            }()
            
            // ทำงานจริง...
            return nil
        },
    }
    // ... rest of configuration
}
```

## ✨ สรุป

ด้วยระบบใหม่นี้ คุณสามารถ:

1. **ใช้ Process ที่มีอยู่**: เลือกจาก "data_analysis", "file_import", "report_gen"
2. **สร้าง Custom Process**: ใช้ Builder Pattern แบบง่ายๆ
3. **เพิ่ม Custom Logic**: ใส่ฟังก์ชันพิเศษได้ตามต้องการ
4. **Copy ไปโปรเจคอื่น**: แค่ copy ไฟล์เดียวก็ใช้ได้เลย
5. **Flexible**: ปรับแต่งได้ตามความต้องการ

🎉 **ระบบนี้ทำให้การสร้าง Job Process ใหม่ๆ ง่ายขึ้นมาก!**