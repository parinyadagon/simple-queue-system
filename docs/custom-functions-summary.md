# 🎉 Custom Functions & Process System - สำเร็จแล้ว!

## ✅ สิ่งที่เพิ่มเข้ามา

### 1. Custom Step Functions
```go
// สามารถสร้าง custom function สำหรับ step ได้
func myCustomFunction(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
    // Custom business logic ของคุณ
    return nil
}

// ใช้งาน
process.AddStepWithFunc("CUSTOM_STEP", "คำอธิบาย", myCustomFunction)
```

### 2. Custom Sub-Step Actions  
```go
// สามารถสร้าง custom action สำหรับ sub-step ได้
process.AddSubStepWithAction("SUB_STEP", "คำอธิบาย", time.Second, func() {
    // Custom action ของคุณ
})
```

### 3. SetExecuteFunc
```go
// สามารถกำหนด custom function ให้ step ที่สร้างแล้วได้
process.AddStep("STEP_NAME", "คำอธิบาย").
    SetExecuteFunc(myCustomFunction)
```

## 🛠️ API Methods ใหม่

### ProcessBuilder
- `AddStepWithFunc(name, description, executeFunc)` - เพิ่ม step พร้อม custom function
- `GetCurrentProcessConfig()` - ได้ config สำหรับ register ใน ProcessConfigurations

### StepBuilder  
- `AddSubStepWithAction(name, description, duration, action)` - เพิ่ม sub-step พร้อม custom action
- `SetExecuteFunc(executeFunc)` - กำหนด custom function ให้ step ปัจจุบัน  
- `AddStepWithFunc(name, description, executeFunc)` - เพิ่ม step ใหม่พร้อม custom function

## 🎯 การใช้งานจริง

### ตัวอย่างที่ 1: Custom Data Processing
```go
func processUserData(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
    // ตรวจสอบการยกเลิกงาน
    select {
    case <-ctx.Done():
        return fmt.Errorf("processing cancelled")
    default:
    }
    
    // Business logic ของคุณ
    users := []string{"John", "Jane", "Bob"}
    for i, user := range users {
        log.Printf("Processing user: %s (%d/%d)", user, i+1, len(users))
        time.Sleep(time.Second)
    }
    
    return nil
}

// ใช้งาน
customProcess := queue.NewProcessManager().
    CreateCustomProcess("User Processing").
    AddStepWithFunc("PROCESS_USERS", "กำลังประมวลผลผู้ใช้", processUserData).
        AddSubStep("PROCESS_USERS_LOADING", "กำลังโหลด", time.Second).
        AddSubStep("PROCESS_USERS_COMPLETED", "เสร็จสิ้น", 0).
    Build()
```

### ตัวอย่างที่ 2: Mixed Custom Functions
```go
process := queue.NewProcessManager().
    CreateCustomProcess("Mixed Process").
    AddStep("PREPARE", "กำลังเตรียม").
        AddSubStepWithAction("PREPARE_SETUP", "ตั้งค่า", time.Second, func() {
            log.Printf("Setting up system...")
        }).
        AddSubStep("PREPARE_COMPLETED", "เสร็จสิ้น", 0).
    AddStepWithFunc("PROCESS", "กำลังประมวลผล", myCustomFunction).
        AddSubStep("PROCESS_RUNNING", "กำลังทำงาน", 3*time.Second).
        AddSubStep("PROCESS_COMPLETED", "เสร็จสิ้น", 0).
    Build()
```

### ตัวอย่างที่ 3: SetExecuteFunc Pattern
```go
customLogic := func(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
    log.Printf("Executing custom logic for job %s", jobID)
    // Your logic here
    return nil
}

process := queue.NewProcessManager().
    CreateCustomProcess("Custom Logic").
    AddStep("CUSTOM_WORK", "งานพิเศษ").
        SetExecuteFunc(customLogic).
        AddSubStep("CUSTOM_WORK_INIT", "เริ่มต้น", 500*time.Millisecond).
        AddSubStep("CUSTOM_WORK_COMPLETED", "เสร็จสิ้น", 0).
    Build()
```

## 🔗 Integration กับระบบหลัก

### วิธีใช้ใน main.go
```go
// สร้าง custom process
myProcess := queue.NewProcessManager().
    CreateCustomProcess("My Business Process").
    AddStepWithFunc("CUSTOM_WORK", "งานธุรกิจ", myBusinessFunction).
        AddSubStep("CUSTOM_WORK_INIT", "เริ่มต้น", time.Second).
        AddSubStep("CUSTOM_WORK_COMPLETED", "เสร็จสิ้น", 0).
    Build()

// วิธีที่ 1: ใช้เป็น default
queue.DefaultProcessManager = myProcess

// วิธีที่ 2: Register แล้วใช้ชื่อ  
queue.ProcessConfigurations["my_process"] = myProcess.GetCurrentProcessConfig()
queue.DefaultProcessManager.UseProcess("my_process")
```

### Dynamic Process Switching
```go
func switchToEmailProcess() {
    emailProcess := queue.NewProcessManager().UseProcess("email_marketing")
    queue.DefaultProcessManager = emailProcess
    log.Println("🔄 เปลี่ยนเป็น Email Process แล้ว")
}
```

## 🎯 Best Practices

### 1. Context Handling ⚠️ สำคัญ!
```go
func myFunction(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
    // เสมอตรวจสอบ context cancellation
    select {
    case <-ctx.Done():
        return fmt.Errorf("operation cancelled")
    default:
        // ทำงานต่อ
    }
    
    // Your logic here
    return nil
}
```

### 2. Error Handling
```go
if err != nil {
    return fmt.Errorf("custom operation failed: %w", err)
}
```

### 3. Structured Logging
```go
log.Printf("🔍 Job %s: Starting operation %s", jobID, operationName)
log.Printf("✅ Job %s: Operation completed successfully", jobID)
log.Printf("❌ Job %s: Operation failed: %v", jobID, err)
```

## 📁 ไฟล์ตัวอย่าง

1. **`examples/custom_functions_demo.go`** - ตัวอย่างพื้นฐาน
   ```bash
   go run examples/custom_functions_demo.go
   ```

2. **`examples/process_examples.go`** - ตัวอย่างทั่วไป พร้อมข้อมูล custom functions
   ```bash  
   go run examples/process_examples.go
   ```

3. **`docs/custom-functions-guide.md`** - คู่มือการใช้งานแบบละเอียด

## 🚀 Use Cases ที่เหมาะสม

### ✅ เหมาะสำหรับ:
- **Database Operations** - SELECT, INSERT, UPDATE, DELETE ที่ซับซ้อน
- **API Integration** - เรียกใช้ external services
- **File Processing** - อ่าน/เขียน/ประมวลผลไฟล์
- **Email Marketing** - ส่งอีเมลแบบ batch
- **Data Analytics** - การวิเคราะห์ข้อมูลที่ซับซ้อน
- **System Backup** - การสำรองข้อมูลระบบ
- **Report Generation** - สร้างรายงานที่กำหนดเอง
- **Business Logic** - ตรรกะเฉพาะธุรกิจ

### ⚠️ ข้อควรระวัง:
- ใช้ context cancellation เสมอ
- Handle errors อย่างเหมาะสม
- ใช้ structured logging
- ระวัง long-running operations
- Test อย่างละเอียดก่อนใช้ production

## 🎉 สรุป

ตอนนี้ระบบ Simple Queue รองรับ **Custom Functions** เต็มรูปแบบแล้ว! คุณสามารถ:

✅ **สร้าง custom business logic** ได้ตามต้องการ  
✅ **รวม external systems** เข้ากับ job processing  
✅ **ควบคุม sub-steps ได้ละเอียด** ด้วย custom actions  
✅ **เปลี่ยน process แบบ dynamic** ได้  
✅ **Monitor และ track progress** ได้แบบ real-time  
✅ **Scale และ maintain** ได้ง่าย  

**🚀 พร้อมใช้งานใน production ได้เลย!**