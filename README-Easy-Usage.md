# 🚀 Simple Queue System - Easy Usage Guide

## 📖 Overview

ระบบ Simple Queue System ได้รับการปรับปรุงให้ **ใช้งานง่ายมาก** สามารถสร้าง job process ใหม่ๆ ได้ในไม่กี่บรรทัด และนำไปใช้กับโปรเจคอื่นๆ ได้อย่างง่ายดาย

## ✨ Features

- 🎯 **Builder Pattern**: สร้าง process ง่ายด้วย fluent interface
- 🔧 **Configurable**: ปรับแต่งได้ทุกอย่าง - steps, durations, descriptions
- 🌐 **Multi-Language**: รองรับคำอธิบายภาษาไทยและภาษาอื่นๆ
- 📊 **Real-time Progress**: ติดตามความคืบหน้าแบบ real-time ด้วย WebSocket
- 🔄 **Resume Support**: pause/resume ได้จากจุดใดก็ได้
- 📱 **Modern UI**: Dashboard สวยงามและใช้งานง่าย
- 🚀 **Production Ready**: พร้อมใช้งานจริง

## 🚀 Quick Start

### 1. ใช้ Pre-defined Process

```go
package main

import "simple-queue-103/internal/adapters/queue"

func main() {
    // เลือกใช้ process ที่มีอยู่แล้ว
    processManager := queue.NewProcessManager().UseProcess("file_import")
    queue.DefaultProcessManager = processManager
    
    // เริ่มเซิร์ฟเวอร์ต่อตามปกติ...
}
```

**Available processes:**
- `data_analysis` - วิเคราะห์ข้อมูล (6 steps)
- `file_import` - นำเข้าไฟล์ (2 steps) 
- `report_gen` - สร้างรายงาน (3 steps)

### 2. สร้าง Custom Process แบบง่าย

```go
func setupEmailCampaign() {
    processManager := queue.NewProcessManager()
    
    processManager.CreateCustomProcess("Email Campaign").
        AddStep("LOAD_CONTACTS", "กำลังโหลดรายชื่อผู้รับ").
            AddSubStep("LOAD_CONTACTS_READING", "กำลังอ่านไฟล์", 2*time.Second).
            AddSubStep("LOAD_CONTACTS_VALIDATING", "กำลังตรวจสอบอีเมล", 3*time.Second).
            AddSubStep("LOAD_CONTACTS_COMPLETED", "โหลดเสร็จสิ้น", 0).
        AddStep("SEND_EMAILS", "กำลังส่งอีเมล").
            AddSubStep("SEND_EMAILS_PREPARING", "กำลังเตรียมส่ง", 1*time.Second).
            AddSubStep("SEND_EMAILS_SENDING", "กำลังส่ง", 10*time.Second).
            AddSubStep("SEND_EMAILS_COMPLETED", "ส่งเสร็จสิ้น", 0).
        Build()
    
    queue.DefaultProcessManager = processManager
}
```

### 3. เปลี่ยน Process ระหว่างทำงาน

```go
// เปลี่ยนเป็น File Import Process
func switchToFileImport() {
    queue.DefaultProcessManager.UseProcess("file_import")
    log.Println("เปลี่ยนเป็น File Import Process แล้ว")
}

// เปลี่ยนเป็น Custom Process
func switchToCustom() {
    setupEmailCampaign() // เรียกฟังก์ชันสร้าง custom process
    log.Println("เปลี่ยนเป็น Email Campaign Process แล้ว")
}
```

## 📚 Process Examples

### 📧 Email Marketing
```go
queue.NewProcessManager().CreateCustomProcess("Email Marketing").
    AddStep("IMPORT_CONTACTS", "นำเข้ารายชื่อ").
        AddSubStep("IMPORT_CONTACTS_READING", "อ่านไฟล์", 2*time.Second).
        AddSubStep("IMPORT_CONTACTS_VALIDATING", "ตรวจสอบข้อมูล", 3*time.Second).
        AddSubStep("IMPORT_CONTACTS_COMPLETED", "นำเข้าเสร็จสิ้น", 0).
    AddStep("SEND_EMAILS", "ส่งอีเมล").
        AddSubStep("SEND_EMAILS_PREPARING", "เตรียมส่ง", 1*time.Second).
        AddSubStep("SEND_EMAILS_SENDING", "กำลังส่ง", 10*time.Second).
        AddSubStep("SEND_EMAILS_COMPLETED", "ส่งเสร็จสิ้น", 0).
    Build()
```

### 🛒 Order Processing
```go
queue.NewProcessManager().CreateCustomProcess("Order Processing").
    AddStep("VALIDATE_ORDER", "ตรวจสอบคำสั่งซื้อ").
        AddSubStep("VALIDATE_ORDER_CHECKING", "ตรวจสอบสินค้า", 1*time.Second).
        AddSubStep("VALIDATE_ORDER_PAYMENT", "ตรวจสอบการชำระเงิน", 2*time.Second).
        AddSubStep("VALIDATE_ORDER_COMPLETED", "ตรวจสอบเสร็จสิ้น", 0).
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
    Build()
```

## 🎯 วิธีนำไปใช้ในโปรเจคอื่น

### 1. Copy ไฟล์
```bash
# Copy หลักๆ แค่ไฟล์เดียว
cp internal/adapters/queue/asynq.go YOUR_PROJECT/internal/adapters/queue/

# Copy domain และ ports ถ้าต้องการ
cp -r internal/core YOUR_PROJECT/internal/
```

### 2. ปรับ Import
```go
import "your-project/internal/adapters/queue"
```

### 3. สร้าง Process ของคุณ
```go
func setupYourProcess() {
    queue.NewProcessManager().CreateCustomProcess("Your Process").
        AddStep("YOUR_STEP_1", "ขั้นตอนของคุณ").
            AddSubStep("YOUR_STEP_1_INIT", "เริ่มต้น", 1*time.Second).
            AddSubStep("YOUR_STEP_1_COMPLETED", "เสร็จสิ้น", 0).
        Build()
}
```

## 🔧 Advanced Usage

### Custom Actions
```go
step := queue.JobStepConfig{
    Name: "CUSTOM_STEP",
    Description: "ขั้นตอนพิเศษ",
    SubSteps: []queue.JobSubStepConfig{
        {
            Name: "CUSTOM_STEP_WORK",
            Description: "ทำงานพิเศษ",
            Duration: 5*time.Second,
            Action: func() {
                // Custom logic ของคุณ
                fmt.Println("กำลังทำงานพิเศษ...")
                // เรียก API, เชื่อมต่อ DB, etc.
            },
        },
    },
}
```

### Error Handling
```go
step.ExecuteFunc = func(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
    defer func() {
        if r := recover(); r != nil {
            log.Printf("Job %s recovered from panic: %v", jobID, r)
        }
    }()
    
    // ทำงานของคุณ
    return nil
}
```

## 🚀 Running the System

### Start Server
```bash
# ใช้ Makefile (recommended)
make run

# หรือ manual
go run cmd/api/main.go

# พร้อม Redis
docker-compose -f compose.redis.yml up -d
```

### Access Dashboard
- **Frontend**: http://localhost:5173
- **API**: http://localhost:8080
- **WebSocket**: ws://localhost:8080/ws/status

## 📖 API Endpoints

```bash
# สร้าง job ใหม่
POST http://localhost:8080/jobs

# ดู job ทั้งหมด  
GET http://localhost:8080/jobs

# ควบคุม job (PAUSE/RESTART/CANCEL)
POST http://localhost:8080/jobs/{id}/control
Content-Type: application/json
{"command": "PAUSE"}
```

## 🎨 UI Features

- 📊 **Real-time Statistics**: แสดงสถิติแบบ real-time
- 🎯 **Job Control**: pause, resume, cancel jobs ได้
- 🔄 **Live Updates**: อัปเดตผ่าน WebSocket
- 📱 **Responsive Design**: ใช้งานได้ทุกอุปกรณ์
- 🌟 **Modern Design**: UI สวยงามและใช้งานง่าย

## 💡 Tips

### 1. การเปลี่ยน Process แบบ Dynamic
```go
// สร้างฟังก์ชันสำหรับเปลี่ยน process
func SwitchProcess(processType string) {
    switch processType {
    case "email":
        setupEmailCampaign()
    case "order":
        setupOrderProcessing()
    case "video":
        setupVideoProcessing()
    default:
        queue.DefaultProcessManager.UseProcess("data_analysis")
    }
}
```

### 2. การ Debug
```go
// ดูข้อมูล process ปัจจุบัน
fmt.Printf("Current Steps: %v\n", queue.DefaultProcessManager.GetSteps())
fmt.Printf("Sub-checkpoints: %v\n", queue.DefaultProcessManager.GetSubCheckpoints())
fmt.Printf("Descriptions: %v\n", queue.DefaultProcessManager.GetStepDescriptions())
```

### 3. การทดสอบ
```bash
# ทดสอบตัวอย่าง
go run examples/process_examples.go

# ทดสอบ integration
make itest
```

## 🎯 สรุป

**ระบบใหม่นี้ทำให้:**

✅ **สร้าง Process ใหม่ง่าย** - แค่เรียก Builder methods  
✅ **ใช้งานง่าย** - เปลี่ยน process ได้ในบรรทัดเดียว  
✅ **Flexible** - ปรับแต่งได้ทุกอย่าง  
✅ **Reusable** - copy ไปโปรเจคอื่นได้ง่าย  
✅ **Production Ready** - มี error handling และ monitoring  

🚀 **เริ่มใช้งานได้เลย!**

---

📝 **See Also:**
- [Technical Documentation](../docs/easy-usage-examples.md)
- [Process Examples](../examples/process_examples.go)
- [Architecture Guide](../.github/copilot-instructions.md)