# Multi-Process Architecture Design

## 🎯 Goal
Isolate multiple job processes completely - separate database records, broadcasting channels, and control systems.

## 🏗️ Architecture Changes

### 1. Enhanced Job Domain
```go
type Job struct {
    ID                string    `json:"id" db:"id"`
    ProcessType       string    `json:"process_type" db:"process_type"`  // NEW
    ProcessVersion    string    `json:"process_version" db:"process_version"` // NEW
    FileName          string    `json:"file_name" db:"file_name"`
    Progress          int       `json:"progress" db:"progress"`
    Status            JobStatus `json:"status" db:"status"`
    CreatedAt         time.Time `json:"created_at" db:"created_at"`
    UpdatedAt         time.Time `json:"updated_at" db:"updated_at"`
    CurrentCheckpoint string    `json:"current_checkpoint" db:"current_checkpoint"`
    CurrentStepName   string    `json:"current_step_name" db:"current_step_name"`
}
```

### 2. Process-Aware Repository
```go
type JobRepository interface {
    Save(ctx context.Context, job *Job) error
    FindByID(ctx context.Context, id string) (*Job, error)
    FindAll(ctx context.Context) ([]*Job, error)
    
    // NEW: Process-specific methods
    FindByProcessType(ctx context.Context, processType string) ([]*Job, error)
    FindByProcessAndStatus(ctx context.Context, processType string, status JobStatus) ([]*Job, error)
    CountByProcess(ctx context.Context, processType string) (int, error)
}
```

### 3. Process-Isolated Task Handlers
```go
type ProcessTaskHandler struct {
    repo         ports.JobRepository
    notifier     ports.Notifier
    processType  string
    processConfig *JobProcessConfig
}

func NewProcessTaskHandler(repo ports.JobRepository, notifier ports.Notifier, processType string) *ProcessTaskHandler {
    config := ProcessConfigurations[processType]
    return &ProcessTaskHandler{
        repo: repo,
        notifier: notifier,
        processType: processType,
        processConfig: config,
    }
}
```

### 4. Broadcasting Channels
```go
type ProcessNotifier interface {
    BroadcastUpdate(job *Job)
    BroadcastToProcess(processType string, job *Job)  // NEW
    SubscribeToProcess(processType string) <-chan *Job // NEW
}
```

## 🚀 Implementation Strategy

### Phase 1: Database Schema Migration
1. Add `process_type` and `process_version` columns
2. Update existing records with default values
3. Create indices for performance

### Phase 2: Repository Enhancement
1. Implement process-aware queries
2. Add process filtering methods
3. Update save operations

### Phase 3: Process Isolation
1. Create ProcessTaskHandler per process type
2. Isolate global variables per process
3. Implement process-specific WebSocket channels

### Phase 4: Frontend Updates
1. Process selector in UI
2. Process-specific job lists
3. Process-specific controls

## 📊 Database Schema Updates

```sql
-- Migration: Add process isolation columns
ALTER TABLE jobs ADD COLUMN process_type VARCHAR(100) DEFAULT 'data_analysis';
ALTER TABLE jobs ADD COLUMN process_version VARCHAR(50) DEFAULT '1.0';

-- Create indices for performance
CREATE INDEX idx_jobs_process_type ON jobs(process_type);
CREATE INDEX idx_jobs_process_status ON jobs(process_type, status);
CREATE INDEX idx_jobs_process_created ON jobs(process_type, created_at);
```

## 🎮 Usage Examples

### Multiple Process Registration
```go
// Register multiple processes
dataAnalysisHandler := NewProcessTaskHandler(repo, notifier, "data_analysis")
fileImportHandler := NewProcessTaskHandler(repo, notifier, "file_import") 
reportGenHandler := NewProcessTaskHandler(repo, notifier, "report_gen")

// Each handler manages its own jobs independently
```

### Process-Specific Job Creation
```go
func (s *jobService) CreateJobForProcess(fileName, processType string) (*Job, error) {
    job := &Job{
        ID:          uuid.New().String(),
        ProcessType: processType,
        ProcessVersion: ProcessConfigurations[processType].Version,
        FileName:    fileName,
        Status:      StatusPending,
        CreatedAt:   time.Now(),
    }
    
    return job, s.repo.Save(context.Background(), job)
}
```

### Process-Isolated Queries
```go
// Get jobs only from specific process
dataAnalysisJobs := repo.FindByProcessType(ctx, "data_analysis")
fileImportJobs := repo.FindByProcessType(ctx, "file_import")

// No cross-contamination between processes
```

## 🔧 Benefits

1. **Complete Isolation**: Each process manages its own jobs
2. **Scalability**: Add new processes without affecting existing ones
3. **Maintainability**: Process-specific logic is encapsulated
4. **Debugging**: Easy to identify issues per process type
5. **Analytics**: Process-specific metrics and monitoring
6. **Version Control**: Track process configurations over time

## ⚠️ Migration Considerations

1. **Backward Compatibility**: Existing jobs get default process_type
2. **Zero Downtime**: Migration can be done gradually
3. **Data Integrity**: All existing functionality preserved
4. **Performance**: New indices ensure query performance