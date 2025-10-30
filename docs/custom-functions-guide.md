# Custom Functions Guide - คู่มือการใช้งาน Custom Functions

## 🎯 ภาพรวม

ระบบ Simple Queue ตอนนี้รองรับการสร้าง **Custom Functions** และ **Custom Actions** แล้ว! คุณสามารถสร้าง business logic ที่เฉพาะเจาะจงสำหรับ process ของคุณได้

## 🚀 วิธีการใช้งาน

### 1. Custom Step Functions

```go
// สร้าง custom function สำหรับ step
func processUserData(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
    log.Printf("🔍 Job %s: Starting custom user data processing...", jobID)
    
    // ตรวจสอบการยกเลิก
    select {
    case <-ctx.Done():
        return fmt.Errorf("user data processing cancelled")
    default:
    }
    
    // Business logic ของคุณ
    users := []string{"John", "Jane", "Bob", "Alice"}
    for i, user := range users {
        log.Printf("👤 Processing user: %s (%d/%d)", user, i+1, len(users))
        time.Sleep(time.Second)
    }
    
    log.Printf("✅ Job %s: User data processing completed", jobID)
    return nil
}

// ใช้ custom function ใน process
process := queue.NewProcessManager().
    CreateCustomProcess("My Custom Process").
    AddStepWithFunc("PROCESS_USERS", "กำลังประมวลผลข้อมูลผู้ใช้", processUserData).
        AddSubStep("PROCESS_USERS_LOADING", "กำลังโหลดข้อมูล", time.Second).
        AddSubStep("PROCESS_USERS_COMPLETED", "ประมวลผลเสร็จสิ้น", 0).
    Build()
```

### 2. Custom Sub-Step Actions

```go
// สร้าง custom action สำหรับ sub-step
process := queue.NewProcessManager().
    CreateCustomProcess("API Integration").
    AddStep("FETCH_DATA", "กำลังดึงข้อมูลจาก API").
        AddSubStepWithAction("FETCH_DATA_CONNECTING", "กำลังเชื่อมต่อ", 500*time.Millisecond, func() {
            log.Printf("🌐 Connecting to external API...")
            // API connection logic ของคุณ
        }).
        AddSubStepWithAction("FETCH_DATA_DOWNLOADING", "กำลังดาวน์โหลด", time.Second, func() {
            log.Printf("⬇️ Downloading data from API...")
            // Download logic ของคุณ
        }).
    Build()
```

### 3. SetExecuteFunc - กำหนด Function ให้ Step ที่มีอยู่

```go
// สร้าง custom function
customLogic := func(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
    log.Printf("🎯 Job %s: Executing custom logic", jobID)
    
    operations := []string{"Initialize", "Process", "Validate", "Finalize"}
    
    for i, op := range operations {
        select {
        case <-ctx.Done():
            return fmt.Errorf("operation cancelled")
        default:
        }
        
        log.Printf("⚙️ %s (%d/%d)", op, i+1, len(operations))
        time.Sleep(500 * time.Millisecond)
    }
    
    return nil
}

// กำหนดให้ step ที่สร้างแล้ว
process := queue.NewProcessManager().
    CreateCustomProcess("Custom Logic Demo").
    AddStep("CUSTOM_LOGIC", "กำลังประมวลผลด้วยฟังก์ชันพิเศษ").
        SetExecuteFunc(customLogic).
        AddSubStep("CUSTOM_LOGIC_INIT", "กำลังเริ่มต้น", 500*time.Millisecond).
        AddSubStep("CUSTOM_LOGIC_COMPLETED", "เสร็จสิ้น", 0).
    Build()
```

## 🛠️ API Reference

### ProcessBuilder Methods

| Method | Description |
|--------|-------------|
| `AddStep(name, description)` | เพิ่ม step ปกติ |
| `AddStepWithFunc(name, description, func)` | เพิ่ม step พร้อม custom function |

### StepBuilder Methods

| Method | Description |
|--------|-------------|
| `AddSubStep(name, description, duration)` | เพิ่ม sub-step ปกติ |
| `AddSubStepWithAction(name, description, duration, action)` | เพิ่ม sub-step พร้อม custom action |
| `SetExecuteFunc(func)` | กำหนด custom function ให้ step ปัจจุบัน |
| `AddStep(name, description)` | เพิ่ม step ใหม่ต่อจากนี้ |
| `AddStepWithFunc(name, description, func)` | เพิ่ม step ใหม่พร้อม custom function |
| `Build()` | สร้าง ProcessManager |

## 📝 Function Signatures

### Custom Step Function
```go
func(ctx context.Context, jobID string, step *JobStepConfig) error
```

### Custom Sub-Step Action
```go
func()
```

## ⚠️ Best Practices

### 1. Context Handling
เสมอตรวจสอบ `ctx.Done()` เพื่อรองรับการยกเลิก:

```go
select {
case <-ctx.Done():
    return fmt.Errorf("operation cancelled")
default:
    // ทำงานต่อ
}
```

### 2. Error Handling
Return error เมื่อเจอปัญหา:

```go
if err != nil {
    return fmt.Errorf("custom operation failed: %w", err)
}
```

### 3. Logging
ใช้ structured logging สำหรับ debugging:

```go
log.Printf("🔍 Job %s: Starting operation %s", jobID, operationName)
```

### 4. Progress Tracking
sub-steps จะถูกติดตามโดยอัตโนมัติ ไม่ต้องจัดการ progress เอง

## 🎯 Use Cases

### 1. Database Operations
```go
func processDatabase(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
    // Connect to database
    db, err := sql.Open("mysql", dsn)
    if err != nil {
        return fmt.Errorf("database connection failed: %w", err)
    }
    defer db.Close()
    
    // Your database operations
    rows, err := db.QueryContext(ctx, "SELECT * FROM users")
    if err != nil {
        return fmt.Errorf("query failed: %w", err)
    }
    defer rows.Close()
    
    // Process results
    for rows.Next() {
        select {
        case <-ctx.Done():
            return fmt.Errorf("database processing cancelled")
        default:
        }
        
        // Process each row
    }
    
    return nil
}
```

### 2. API Integration
```go
func callExternalAPI(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
    client := &http.Client{Timeout: 30 * time.Second}
    
    req, err := http.NewRequestWithContext(ctx, "GET", "https://api.example.com/data", nil)
    if err != nil {
        return fmt.Errorf("request creation failed: %w", err)
    }
    
    resp, err := client.Do(req)
    if err != nil {
        return fmt.Errorf("API call failed: %w", err)
    }
    defer resp.Body.Close()
    
    // Process response
    if resp.StatusCode != 200 {
        return fmt.Errorf("API returned status: %d", resp.StatusCode)
    }
    
    // Parse and process data
    var data map[string]interface{}
    if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
        return fmt.Errorf("response parsing failed: %w", err)
    }
    
    log.Printf("✅ Job %s: API data processed successfully", jobID)
    return nil
}
```

### 3. File Processing
```go
func processFiles(ctx context.Context, jobID string, step *queue.JobStepConfig) error {
    files := []string{"file1.txt", "file2.txt", "file3.txt"}
    
    for i, filename := range files {
        select {
        case <-ctx.Done():
            return fmt.Errorf("file processing cancelled")
        default:
        }
        
        log.Printf("📁 Job %s: Processing file %s (%d/%d)", jobID, filename, i+1, len(files))
        
        // Process each file
        data, err := os.ReadFile(filename)
        if err != nil {
            log.Printf("⚠️ Job %s: Failed to read %s: %v", jobID, filename, err)
            continue
        }
        
        // Process file data
        processedData := strings.ToUpper(string(data))
        
        // Save processed data
        err = os.WriteFile("processed_"+filename, []byte(processedData), 0644)
        if err != nil {
            return fmt.Errorf("failed to write processed file: %w", err)
        }
        
        time.Sleep(500 * time.Millisecond) // Simulate processing time
    }
    
    return nil
}
```

## 🔗 Integration กับระบบหลัก

### การติดตั้งใน main.go
```go
// สร้าง custom process
customProcess := queue.NewProcessManager().
    CreateCustomProcess("My Business Process").
    AddStepWithFunc("CUSTOM_WORK", "กำลังประมวลผลธุรกิจ", myCustomFunction).
        AddSubStep("CUSTOM_WORK_INIT", "กำลังเริ่มต้น", time.Second).
        AddSubStep("CUSTOM_WORK_PROCESSING", "กำลังประมวลผล", 3*time.Second).
        AddSubStep("CUSTOM_WORK_COMPLETED", "เสร็จสิ้น", 0).
    Build()

// ใช้เป็น default process
queue.DefaultProcessManager = customProcess

// หรือ register ไว้ใน ProcessConfigurations
queue.ProcessConfigurations["my_custom"] = customProcess.currentProcess
```

### การเปลี่ยน Process แบบ Dynamic
```go
// สลับไปใช้ custom process
func switchToCustomProcess() {
    customProcess := queue.NewProcessManager().UseProcess("my_custom")
    queue.DefaultProcessManager = customProcess
    log.Println("🔄 เปลี่ยนเป็น Custom Process แล้ว")
}
```

## 🎉 สรุป

ด้วย Custom Functions คุณสามารถ:

✅ **สร้าง business logic เฉพาะ** - เขียน functions ที่ตรงกับความต้องการของธุรกิจ  
✅ **รวม external APIs** - เชื่อมต่อกับ services ภายนอกได้  
✅ **ประมวลผลฐานข้อมูล** - ดำเนินการ database operations ที่ซับซ้อน  
✅ **จัดการไฟล์** - อ่าน/เขียน/ประมวลผลไฟล์ต่างๆ  
✅ **Flexible Architecture** - รองรับการขยายและปรับแต่งได้อย่างง่ายดาย  

🚀 **เริ่มต้นใช้งานได้เลย!** ดูตัวอย่างเพิ่มเติมใน `examples/custom_functions_demo.go`