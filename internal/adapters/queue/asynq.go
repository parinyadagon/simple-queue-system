package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"simple-queue-103/internal/core/domain"
	"simple-queue-103/internal/core/ports"
	"time"

	"github.com/hibiken/asynq"
)

const (
	TaskTypeAnalysis = "task:analysis"
	RedisAddr        = "127.0.0.1:6379"

	// Job processing constants
	JobTimeout                = 30 * time.Minute
	HeartbeatInterval         = 1 * time.Minute
	StatusCheckInterval       = 2 * time.Second
	StepProcessingTime        = 2 * time.Second
	CompletedCheckpoint       = "COMPLETED"
	MaxProgressBeforeComplete = 95
)

// JobStepConfig defines a configurable job step with sub-checkpoints
type JobStepConfig struct {
	Name        string                                                             `json:"name"`
	Description string                                                             `json:"description"`
	SubSteps    []JobSubStepConfig                                                 `json:"sub_steps"`
	ExecuteFunc func(ctx context.Context, jobID string, step *JobStepConfig) error `json:"-"`
}

// JobSubStepConfig defines a sub-checkpoint within a step
type JobSubStepConfig struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Duration    time.Duration `json:"duration"`
	Action      func()        `json:"-"`
}

// JobProcessConfig defines the entire job process configuration
type JobProcessConfig struct {
	ProcessName string          `json:"process_name"`
	Steps       []JobStepConfig `json:"steps"`
}

// Pre-defined process configurations
var ProcessConfigurations = map[string]*JobProcessConfig{
	"data_analysis": NewDataAnalysisProcess(),
	"file_import":   NewFileImportProcess(),
	"report_gen":    NewReportGenerationProcess(),
}

// NewDataAnalysisProcess creates the default data analysis job configuration
func NewDataAnalysisProcess() *JobProcessConfig {
	return &JobProcessConfig{
		ProcessName: "Data Analysis",
		Steps: []JobStepConfig{
			{
				Name:        "DOWNLOAD_SOURCE",
				Description: "กำลังดาวน์โหลดไฟล์ต้นฉบับ",
				SubSteps: []JobSubStepConfig{
					{Name: "DOWNLOAD_SOURCE_CONNECTING", Description: "กำลังเชื่อมต่อกับเซิร์ฟเวอร์", Duration: StepProcessingTime / 2},
					{Name: "DOWNLOAD_SOURCE_DOWNLOADING", Description: "กำลังดาวน์โหลดไฟล์", Duration: StepProcessingTime},
					{Name: "DOWNLOAD_SOURCE_VALIDATING", Description: "กำลังตรวจสอบไฟล์ที่ดาวน์โหลด", Duration: StepProcessingTime / 2},
					{Name: "DOWNLOAD_SOURCE_COMPLETED", Description: "ดาวน์โหลดไฟล์เสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "DECOMPRESS_FILE",
				Description: "กำลังแตกไฟล์ข้อมูล",
				SubSteps: []JobSubStepConfig{
					{Name: "DECOMPRESS_FILE_READING", Description: "กำลังอ่านไฟล์บีบอัด", Duration: StepProcessingTime / 2},
					{Name: "DECOMPRESS_FILE_EXTRACTING", Description: "กำลังแตกไฟล์", Duration: StepProcessingTime},
					{Name: "DECOMPRESS_FILE_VERIFYING", Description: "กำลังตรวจสอบไฟล์ที่แตก", Duration: StepProcessingTime / 2},
					{Name: "DECOMPRESS_FILE_COMPLETED", Description: "แตกไฟล์เสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "CLEANING_DATA",
				Description: "กำลังทำความสะอาดข้อมูล",
				SubSteps: []JobSubStepConfig{
					{Name: "CLEANING_DATA_SCANNING", Description: "กำลังสแกนหาข้อมูลที่ผิดปกติ", Duration: StepProcessingTime / 2},
					{Name: "CLEANING_DATA_FILTERING", Description: "กำลังกรองข้อมูลที่ไม่ถูกต้อง", Duration: StepProcessingTime},
					{Name: "CLEANING_DATA_NORMALIZING", Description: "กำลังปรับรูปแบบข้อมูล", Duration: StepProcessingTime / 2},
					{Name: "CLEANING_DATA_COMPLETED", Description: "ทำความสะอาดข้อมูลเสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "ANALYSIS_MODEL_A",
				Description: "กำลังวิเคราะห์ด้วยโมเดล A",
				SubSteps: []JobSubStepConfig{
					{Name: "ANALYSIS_MODEL_A_LOADING", Description: "กำลังโหลดโมเดลวิเคราะห์ A", Duration: StepProcessingTime / 2},
					{Name: "ANALYSIS_MODEL_A_PROCESSING", Description: "กำลังประมวลผลด้วยโมเดล A", Duration: StepProcessingTime},
					{Name: "ANALYSIS_MODEL_A_CALCULATING", Description: "กำลังคำนวณผลลัพธ์โมเดล A", Duration: StepProcessingTime / 2},
					{Name: "ANALYSIS_MODEL_A_COMPLETED", Description: "วิเคราะห์ด้วยโมเดล A เสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "ANALYSIS_MODEL_B",
				Description: "กำลังวิเคราะห์ด้วยโมเดล B",
				SubSteps: []JobSubStepConfig{
					{Name: "ANALYSIS_MODEL_B_LOADING", Description: "กำลังโหลดโมเดลวิเคราะห์ B", Duration: StepProcessingTime / 2},
					{Name: "ANALYSIS_MODEL_B_PROCESSING", Description: "กำลังประมวลผลด้วยโมเดล B", Duration: StepProcessingTime},
					{Name: "ANALYSIS_MODEL_B_CALCULATING", Description: "กำลังคำนวณผลลัพธ์โมเดล B", Duration: StepProcessingTime / 2},
					{Name: "ANALYSIS_MODEL_B_COMPLETED", Description: "วิเคราะห์ด้วยโมเดล B เสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "GENERATING_REPORT",
				Description: "กำลังสร้างรายงาน",
				SubSteps: []JobSubStepConfig{
					{Name: "GENERATING_REPORT_COLLECTING", Description: "กำลังรวบรวมผลการวิเคราะห์", Duration: StepProcessingTime / 2},
					{Name: "GENERATING_REPORT_FORMATTING", Description: "กำลังจัดรูปแบบรายงาน", Duration: StepProcessingTime},
					{Name: "GENERATING_REPORT_FINALIZING", Description: "กำลังจัดเรียงรายงานขั้นสุดท้าย", Duration: StepProcessingTime / 2},
					{Name: "GENERATING_REPORT_COMPLETED", Description: "สร้างรายงานเสร็จสิ้น", Duration: 0},
				},
			},
		},
	}
}

// NewFileImportProcess creates a simple file import job configuration
func NewFileImportProcess() *JobProcessConfig {
	return &JobProcessConfig{
		ProcessName: "File Import",
		Steps: []JobStepConfig{
			{
				Name:        "UPLOAD_FILE",
				Description: "กำลังอัพโหลดไฟล์",
				SubSteps: []JobSubStepConfig{
					{Name: "UPLOAD_FILE_VALIDATING", Description: "กำลังตรวจสอบไฟล์", Duration: time.Second},
					{Name: "UPLOAD_FILE_UPLOADING", Description: "กำลังอัพโหลด", Duration: 3 * time.Second},
					{Name: "UPLOAD_FILE_COMPLETED", Description: "อัพโหลดเสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "PROCESS_DATA",
				Description: "กำลังประมวลผลข้อมูล",
				SubSteps: []JobSubStepConfig{
					{Name: "PROCESS_DATA_PARSING", Description: "กำลังแปลงข้อมูล", Duration: 2 * time.Second},
					{Name: "PROCESS_DATA_IMPORTING", Description: "กำลังนำเข้าข้อมูล", Duration: 3 * time.Second},
					{Name: "PROCESS_DATA_COMPLETED", Description: "ประมวลผลเสร็จสิ้น", Duration: 0},
				},
			},
		},
	}
}

// NewReportGenerationProcess creates a report generation job configuration
func NewReportGenerationProcess() *JobProcessConfig {
	return &JobProcessConfig{
		ProcessName: "Report Generation",
		Steps: []JobStepConfig{
			{
				Name:        "COLLECT_DATA",
				Description: "กำลังรวบรวมข้อมูล",
				SubSteps: []JobSubStepConfig{
					{Name: "COLLECT_DATA_QUERYING", Description: "กำลังค้นหาข้อมูล", Duration: 2 * time.Second},
					{Name: "COLLECT_DATA_AGGREGATING", Description: "กำลังรวมข้อมูล", Duration: 3 * time.Second},
					{Name: "COLLECT_DATA_COMPLETED", Description: "รวบรวมข้อมูลเสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "GENERATE_CHARTS",
				Description: "กำลังสร้างกราฟ",
				SubSteps: []JobSubStepConfig{
					{Name: "GENERATE_CHARTS_CREATING", Description: "กำลังสร้างกราฟ", Duration: 2 * time.Second},
					{Name: "GENERATE_CHARTS_STYLING", Description: "กำลังตกแต่งกราฟ", Duration: time.Second},
					{Name: "GENERATE_CHARTS_COMPLETED", Description: "สร้างกราฟเสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "EXPORT_REPORT",
				Description: "กำลังส่งออกรายงาน",
				SubSteps: []JobSubStepConfig{
					{Name: "EXPORT_REPORT_FORMATTING", Description: "กำลังจัดรูปแบบ", Duration: time.Second},
					{Name: "EXPORT_REPORT_SAVING", Description: "กำลังบันทึกไฟล์", Duration: 2 * time.Second},
					{Name: "EXPORT_REPORT_COMPLETED", Description: "ส่งออกรายงานเสร็จสิ้น", Duration: 0},
				},
			},
		},
	}
}

// ProcessManager provides easy-to-use methods for creating and managing job processes
type ProcessManager struct {
	currentProcess *JobProcessConfig
}

// NewProcessManager creates a new process manager
func NewProcessManager() *ProcessManager {
	return &ProcessManager{}
}

// UseProcess sets the process configuration to use
func (pm *ProcessManager) UseProcess(processName string) *ProcessManager {
	if config, exists := ProcessConfigurations[processName]; exists {
		pm.currentProcess = config
	} else {
		// Default to data_analysis if not found
		pm.currentProcess = ProcessConfigurations["data_analysis"]
	}
	return pm
}

// CreateCustomProcess allows creating a completely custom process
func (pm *ProcessManager) CreateCustomProcess(name string) *ProcessBuilder {
	return &ProcessBuilder{
		config: &JobProcessConfig{
			ProcessName: name,
			Steps:       []JobStepConfig{},
		},
		manager: pm,
	}
}

// GetSteps returns the current process steps (for backward compatibility)
func (pm *ProcessManager) GetSteps() []string {
	if pm.currentProcess == nil {
		return []string{}
	}

	steps := make([]string, len(pm.currentProcess.Steps))
	for i, step := range pm.currentProcess.Steps {
		steps[i] = step.Name
	}
	return steps
}

// GetSubCheckpoints returns sub-checkpoints for a step (for backward compatibility)
func (pm *ProcessManager) GetSubCheckpoints() map[string][]string {
	if pm.currentProcess == nil {
		return map[string][]string{}
	}

	checkpoints := make(map[string][]string)
	for _, step := range pm.currentProcess.Steps {
		subSteps := make([]string, len(step.SubSteps))
		for i, subStep := range step.SubSteps {
			subSteps[i] = subStep.Name
		}
		checkpoints[step.Name] = subSteps
	}
	return checkpoints
}

// GetStepDescriptions returns step descriptions (for backward compatibility)
func (pm *ProcessManager) GetStepDescriptions() map[string]string {
	if pm.currentProcess == nil {
		return map[string]string{"COMPLETED": "งานเสร็จสิ้นแล้ว"}
	}

	descriptions := make(map[string]string)

	// Add main step descriptions
	for _, step := range pm.currentProcess.Steps {
		descriptions[step.Name] = step.Description

		// Add sub-step descriptions
		for _, subStep := range step.SubSteps {
			descriptions[subStep.Name] = subStep.Description
		}
	}

	// Add special states
	descriptions["COMPLETED"] = "งานเสร็จสิ้นแล้ว"

	return descriptions
}

// GetCurrentProcessConfig returns the current process configuration for registration
func (pm *ProcessManager) GetCurrentProcessConfig() *JobProcessConfig {
	return pm.currentProcess
}

// ProcessBuilder provides a fluent interface for building custom processes
type ProcessBuilder struct {
	config  *JobProcessConfig
	manager *ProcessManager
}

// AddStep adds a step to the process
func (pb *ProcessBuilder) AddStep(name, description string) *StepBuilder {
	step := JobStepConfig{
		Name:        name,
		Description: description,
		SubSteps:    []JobSubStepConfig{},
	}

	pb.config.Steps = append(pb.config.Steps, step)

	return &StepBuilder{
		step:    &pb.config.Steps[len(pb.config.Steps)-1],
		builder: pb,
	}
}

// AddStepWithFunc adds a step with a custom execution function
func (pb *ProcessBuilder) AddStepWithFunc(name, description string, executeFunc func(ctx context.Context, jobID string, step *JobStepConfig) error) *StepBuilder {
	step := JobStepConfig{
		Name:        name,
		Description: description,
		SubSteps:    []JobSubStepConfig{},
		ExecuteFunc: executeFunc,
	}

	pb.config.Steps = append(pb.config.Steps, step)

	return &StepBuilder{
		step:    &pb.config.Steps[len(pb.config.Steps)-1],
		builder: pb,
	}
}

// Build completes the process building and returns the manager
func (pb *ProcessBuilder) Build() *ProcessManager {
	pb.manager.currentProcess = pb.config
	return pb.manager
}

// StepBuilder provides a fluent interface for building steps
type StepBuilder struct {
	step    *JobStepConfig
	builder *ProcessBuilder
}

// AddSubStep adds a sub-step to the current step
func (sb *StepBuilder) AddSubStep(name, description string, duration time.Duration) *StepBuilder {
	subStep := JobSubStepConfig{
		Name:        name,
		Description: description,
		Duration:    duration,
	}

	sb.step.SubSteps = append(sb.step.SubSteps, subStep)
	return sb
}

// AddSubStepWithAction adds a sub-step with a custom action function
func (sb *StepBuilder) AddSubStepWithAction(name, description string, duration time.Duration, action func()) *StepBuilder {
	subStep := JobSubStepConfig{
		Name:        name,
		Description: description,
		Duration:    duration,
		Action:      action,
	}

	sb.step.SubSteps = append(sb.step.SubSteps, subStep)
	return sb
}

// SetExecuteFunc sets a custom execution function for the current step
func (sb *StepBuilder) SetExecuteFunc(executeFunc func(ctx context.Context, jobID string, step *JobStepConfig) error) *StepBuilder {
	sb.step.ExecuteFunc = executeFunc
	return sb
}

// AddStep continues adding another step to the process
func (sb *StepBuilder) AddStep(name, description string) *StepBuilder {
	return sb.builder.AddStep(name, description)
}

// AddStepWithFunc continues adding another step with custom function to the process
func (sb *StepBuilder) AddStepWithFunc(name, description string, executeFunc func(ctx context.Context, jobID string, step *JobStepConfig) error) *StepBuilder {
	return sb.builder.AddStepWithFunc(name, description, executeFunc)
}

// Build completes the building process
func (sb *StepBuilder) Build() *ProcessManager {
	return sb.builder.Build()
}

// Global process manager instance (for backward compatibility)
var DefaultProcessManager = NewProcessManager().UseProcess("data_analysis")

var JobSteps = DefaultProcessManager.GetSteps()
var SubCheckpoints = DefaultProcessManager.GetSubCheckpoints()
var StepDescriptions = DefaultProcessManager.GetStepDescriptions()

var StepIndexMap = func() map[string]int {
	m := make(map[string]int)
	for i, step := range JobSteps {
		m[step] = i
	}

	return m
}()

// StepFunction defines the signature for step processing functions
type StepFunction func(h *TaskHandler, ctx context.Context, jobID string) error

// getStepExecutor returns the appropriate step function for the given step name
func getStepExecutor(stepName string) StepFunction {
	// First, try to get from current process configuration
	if DefaultProcessManager.currentProcess != nil {
		for _, step := range DefaultProcessManager.currentProcess.Steps {
			if step.Name == stepName {
				if step.ExecuteFunc != nil {
					return func(h *TaskHandler, ctx context.Context, jobID string) error {
						return step.ExecuteFunc(ctx, jobID, &step)
					}
				}
				// If no custom function, use generic execution
				return func(h *TaskHandler, ctx context.Context, jobID string) error {
					return h.executeGenericStep(ctx, jobID, &step)
				}
			}
		}
	}

	// Fallback to hardcoded functions for backward compatibility
	switch stepName {
	case "DOWNLOAD_SOURCE":
		return (*TaskHandler).executeDownloadSource
	case "DECOMPRESS_FILE":
		return (*TaskHandler).executeDecompressFile
	case "CLEANING_DATA":
		return (*TaskHandler).executeCleaningData
	case "ANALYSIS_MODEL_A":
		return (*TaskHandler).executeAnalysisModelA
	case "ANALYSIS_MODEL_B":
		return (*TaskHandler).executeAnalysisModelB
	case "GENERATING_REPORT":
		return (*TaskHandler).executeGeneratingReport
	default:
		return nil
	}
}

// saveSubCheckpoint saves a sub-checkpoint and calculates detailed progress
func (h *TaskHandler) saveSubCheckpoint(ctx context.Context, jobID string, mainStep string, subCheckpoint string) error {
	job, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to find job for sub-checkpoint update: %w", err)
	}

	// Check if job was cancelled/paused during processing
	if job.Status != domain.StatusRunning {
		log.Printf("Job %s was preempted, skipping sub-checkpoint %s", jobID, subCheckpoint)
		return nil
	}

	// Get human-readable step description
	stepDescription, exists := StepDescriptions[subCheckpoint]
	if !exists {
		stepDescription = subCheckpoint // Fallback to checkpoint name
	}

	log.Printf("Job %s: Sub-checkpoint: %s (%s)", jobID, subCheckpoint, stepDescription)
	job.CurrentCheckpoint = subCheckpoint
	job.CurrentStepName = stepDescription
	job.Progress = h.calculateDetailedProgress(mainStep, subCheckpoint)

	if err := h.repo.Save(ctx, job); err != nil {
		log.Printf("Error saving sub-checkpoint for job %s: %v", jobID, err)
		return fmt.Errorf("failed to save sub-checkpoint: %w", err)
	}

	h.notifier.BroadcastUpdate(job)
	return nil
}

// findSubCheckpointStartIndex finds where to resume within sub-checkpoints
func (h *TaskHandler) findSubCheckpointStartIndex(currentCheckpoint string, subSteps []string) int {
	if currentCheckpoint == "" {
		return 0
	}

	// Find the index of current sub-checkpoint
	for i, subStep := range subSteps {
		if subStep == currentCheckpoint {
			// Resume from the next sub-step
			return i + 1
		}
	}

	// If not found in sub-steps, start from beginning
	return 0
}

// executeStepWithSubCheckpoints executes a step with multiple sub-checkpoints
func (h *TaskHandler) executeStepWithSubCheckpoints(ctx context.Context, jobID string, stepName string, subStepActions []func()) error {
	subSteps := SubCheckpoints[stepName]

	job, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to find job: %w", err)
	}

	startSubIndex := h.findSubCheckpointStartIndex(job.CurrentCheckpoint, subSteps)

	for i, action := range subStepActions {
		if startSubIndex <= i {
			// Save sub-checkpoint
			if err := h.saveSubCheckpoint(ctx, jobID, stepName, subSteps[i]); err != nil {
				return err
			}

			// Execute the action
			action()
		}
	}

	return nil
}

// calculateDetailedProgress calculates progress including sub-checkpoints
func (h *TaskHandler) calculateDetailedProgress(mainStep string, subCheckpoint string) int {
	if subCheckpoint == CompletedCheckpoint {
		return 100
	}

	// Find main step index
	mainStepIndex, exists := StepIndexMap[mainStep]
	if !exists {
		// Check if this is already a sub-checkpoint
		for step, subSteps := range SubCheckpoints {
			for subIndex, sub := range subSteps {
				if sub == subCheckpoint {
					if step == subCheckpoint[:len(step)] { // Match main step name prefix
						mainStepIndex = StepIndexMap[step]
						subStepProgress := float64(subIndex+1) / float64(len(subSteps))
						totalSteps := len(JobSteps)

						// Calculate progress: main step progress + sub-step progress within that step
						stepWeight := float64(MaxProgressBeforeComplete) / float64(totalSteps)
						mainStepProgress := float64(mainStepIndex) * stepWeight
						currentStepProgress := stepWeight * subStepProgress

						finalProgress := int(mainStepProgress + currentStepProgress)
						if finalProgress > MaxProgressBeforeComplete {
							finalProgress = MaxProgressBeforeComplete
						}
						return finalProgress
					}
				}
			}
		}
		return 0
	}

	// Find sub-checkpoint index within the main step
	subSteps, exists := SubCheckpoints[mainStep]
	if !exists {
		// Fallback to original calculation
		return h.CalculateProgress(mainStep, len(JobSteps))
	}

	subStepIndex := -1
	for i, sub := range subSteps {
		if sub == subCheckpoint {
			subStepIndex = i
			break
		}
	}

	if subStepIndex == -1 {
		// Fallback to original calculation
		return h.CalculateProgress(mainStep, len(JobSteps))
	}

	// Calculate detailed progress
	totalSteps := len(JobSteps)
	stepWeight := float64(MaxProgressBeforeComplete) / float64(totalSteps)
	mainStepProgress := float64(mainStepIndex) * stepWeight
	subStepProgress := stepWeight * (float64(subStepIndex+1) / float64(len(subSteps)))

	finalProgress := int(mainStepProgress + subStepProgress)
	if finalProgress > MaxProgressBeforeComplete {
		finalProgress = MaxProgressBeforeComplete
	}

	return finalProgress
}

// --- 1. Asynq Client (Implement JobQueue) ---
type asynqJobQueue struct {
	client *asynq.Client
}

func NewAsynqJobQueue() ports.JobQueue {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: RedisAddr})

	return &asynqJobQueue{client: client}
}

func (q *asynqJobQueue) EnqueueAnalysis(jobID string) error {
	payload, _ := json.Marshal(map[string]string{"job_id": jobID})
	task := asynq.NewTask(TaskTypeAnalysis, payload)
	_, err := q.client.Enqueue(task)

	return err
}

// --- 2. Asynq Task Handlers (Worker Logic)
type TaskHandler struct {
	repo     ports.JobRepository
	notifier ports.Notifier
}

func NewTaskHandler(repo ports.JobRepository, notifier ports.Notifier) *TaskHandler {
	return &TaskHandler{repo: repo, notifier: notifier}
}

// Step execution functions - each step simulates specific work with sub-checkpoints
func (h *TaskHandler) executeDownloadSource(ctx context.Context, jobID string) error {
	stepName := "DOWNLOAD_SOURCE"

	actions := []func(){
		func() {
			log.Printf("Job %s: Connecting to remote source...", jobID)
			time.Sleep(StepProcessingTime / 2)
		},
		func() {
			log.Printf("Job %s: Downloading files...", jobID)
			time.Sleep(StepProcessingTime)
		},
		func() {
			log.Printf("Job %s: Validating downloaded files...", jobID)
			time.Sleep(StepProcessingTime / 2)
		},
		func() {
			log.Printf("Job %s: Source files downloaded successfully", jobID)
		},
	}

	return h.executeStepWithSubCheckpoints(ctx, jobID, stepName, actions)
}

func (h *TaskHandler) executeDecompressFile(ctx context.Context, jobID string) error {
	stepName := "DECOMPRESS_FILE"

	actions := []func(){
		func() {
			log.Printf("Job %s: Reading compressed files...", jobID)
			time.Sleep(StepProcessingTime / 2)
		},
		func() {
			log.Printf("Job %s: Extracting files...", jobID)
			time.Sleep(StepProcessingTime)
		},
		func() {
			log.Printf("Job %s: Verifying extracted files...", jobID)
			time.Sleep(StepProcessingTime / 2)
		},
		func() {
			log.Printf("Job %s: Files decompressed successfully", jobID)
		},
	}

	return h.executeStepWithSubCheckpoints(ctx, jobID, stepName, actions)
}

func (h *TaskHandler) executeCleaningData(ctx context.Context, jobID string) error {
	stepName := "CLEANING_DATA"

	actions := []func(){
		func() {
			log.Printf("Job %s: Scanning data for inconsistencies...", jobID)
			time.Sleep(StepProcessingTime / 2)
		},
		func() {
			log.Printf("Job %s: Filtering invalid data...", jobID)
			time.Sleep(StepProcessingTime)
		},
		func() {
			log.Printf("Job %s: Normalizing data format...", jobID)
			time.Sleep(StepProcessingTime / 2)
		},
		func() {
			log.Printf("Job %s: Data cleaning completed", jobID)
		},
	}

	return h.executeStepWithSubCheckpoints(ctx, jobID, stepName, actions)
}

func (h *TaskHandler) executeAnalysisModelA(ctx context.Context, jobID string) error {
	stepName := "ANALYSIS_MODEL_A"

	actions := []func(){
		func() {
			log.Printf("Job %s: Loading Analysis Model A...", jobID)
			time.Sleep(StepProcessingTime / 2)
		},
		func() {
			log.Printf("Job %s: Processing data with Model A...", jobID)
			time.Sleep(StepProcessingTime)
		},
		func() {
			log.Printf("Job %s: Calculating results for Model A...", jobID)
			time.Sleep(StepProcessingTime / 2)
		},
		func() {
			log.Printf("Job %s: Analysis Model A completed", jobID)
		},
	}

	return h.executeStepWithSubCheckpoints(ctx, jobID, stepName, actions)
}

func (h *TaskHandler) executeAnalysisModelB(ctx context.Context, jobID string) error {
	stepName := "ANALYSIS_MODEL_B"

	actions := []func(){
		func() {
			log.Printf("Job %s: Loading Analysis Model B...", jobID)
			time.Sleep(StepProcessingTime / 2)
		},
		func() {
			log.Printf("Job %s: Processing data with Model B...", jobID)
			time.Sleep(StepProcessingTime)
		},
		func() {
			log.Printf("Job %s: Calculating results for Model B...", jobID)
			time.Sleep(StepProcessingTime / 2)
		},
		func() {
			log.Printf("Job %s: Analysis Model B completed", jobID)
		},
	}

	return h.executeStepWithSubCheckpoints(ctx, jobID, stepName, actions)
}

func (h *TaskHandler) executeGeneratingReport(ctx context.Context, jobID string) error {
	stepName := "GENERATING_REPORT"

	actions := []func(){
		func() {
			log.Printf("Job %s: Collecting analysis results...", jobID)
			time.Sleep(StepProcessingTime / 2)
		},
		func() {
			log.Printf("Job %s: Formatting report data...", jobID)
			time.Sleep(StepProcessingTime)
		},
		func() {
			log.Printf("Job %s: Finalizing report layout...", jobID)
			time.Sleep(StepProcessingTime / 2)
		},
		func() {
			log.Printf("Job %s: Report generated successfully", jobID)
		},
	}

	return h.executeStepWithSubCheckpoints(ctx, jobID, stepName, actions)
}

// executeGenericStep executes a step using its configuration
func (h *TaskHandler) executeGenericStep(ctx context.Context, jobID string, step *JobStepConfig) error {
	// Create actions from sub-step configurations
	actions := make([]func(), len(step.SubSteps))

	for i, subStep := range step.SubSteps {
		subStep := subStep // capture loop variable
		actions[i] = func() {
			if subStep.Action != nil {
				subStep.Action()
			} else {
				// Default action: log and sleep
				log.Printf("Job %s: %s", jobID, subStep.Description)
				if subStep.Duration > 0 {
					time.Sleep(subStep.Duration)
				}
			}
		}
	}

	return h.executeStepWithSubCheckpoints(ctx, jobID, step.Name, actions)
}

// HandleAnalysisTask คือ Worker ที่ทำงานจริง
func (h *TaskHandler) HandleAnalysisTask(ctx context.Context, t *asynq.Task) error {
	jobID, err := h.extractJobID(t)
	if err != nil {
		return err
	}

	jobCtx, cancel := h.setupJobContext(ctx, jobID)

	initialJob, err := h.initializeJob(ctx, jobID)
	if err != nil {
		// If job doesn't exist, it's likely from previous session - just skip
		log.Printf("Skipping job %s as it doesn't exist in current session", jobID)
		return nil
	}

	// Skip if job is already canceled or completed
	if initialJob.Status == domain.StatusCanceled {
		return nil
	}

	// Handle paused jobs - they should be skipped until explicitly resumed
	if initialJob.Status == domain.StatusPaused {
		log.Printf("Job %s is PAUSED, skipping execution until resumed", jobID)
		return nil
	}

	startIndex := h.determineStartIndex(initialJob, jobID)

	// If job is already completed, skip all processing
	if startIndex == -1 {
		return nil
	}

	if startIndex >= len(JobSteps) {
		log.Printf("Job %s: All steps completed, proceeding to finalization.", jobID)
	}

	// Set job to running status
	if err := h.setJobRunning(jobCtx, initialJob); err != nil {
		return err
	}

	h.logJobStart(jobID, startIndex)

	// Process remaining steps
	if err := h.processJobSteps(jobCtx, jobID, startIndex, cancel); err != nil {
		return err
	}

	// Complete the job
	return h.completedJob(jobCtx, jobID, cancel)
}

// extractJobID extracts job ID from task payload
func (h *TaskHandler) extractJobID(t *asynq.Task) (string, error) {
	var payload map[string]string
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return "", fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	return payload["job_id"], nil
}

// setupJobContext creates job context with timeout and heartbeat
func (h *TaskHandler) setupJobContext(ctx context.Context, jobID string) (context.Context, context.CancelFunc) {
	jobCtx, cancel := context.WithTimeout(ctx, JobTimeout)

	// Start heartbeat goroutine
	heartbeatTicker := time.NewTicker(HeartbeatInterval)
	go func() {
		defer heartbeatTicker.Stop()
		for {
			select {
			case <-heartbeatTicker.C:
				log.Printf("Job %s heartbeat - still processing", jobID)
			case <-jobCtx.Done():
				return
			}
		}
	}()

	return jobCtx, cancel
}

// initializeJob retrieves and validates initial job state
func (h *TaskHandler) initializeJob(ctx context.Context, jobID string) (*domain.Job, error) {
	job, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		log.Printf("Job %s not found in repository (probably from previous session): %v", jobID, err)
		return nil, fmt.Errorf("failed to find job %s: %w", jobID, err)
	}

	return job, nil
}

// determineStartIndex calculates where to resume job processing
func (h *TaskHandler) determineStartIndex(job *domain.Job, jobID string) int {
	if job.CurrentCheckpoint == "" {
		return 0
	}

	if job.CurrentCheckpoint == CompletedCheckpoint {
		log.Printf("Job %s already completed. Skipping execution.", jobID)
		return -1
	}

	// Check for main step checkpoint
	if index, ok := StepIndexMap[job.CurrentCheckpoint]; ok {
		return index + 1
	}

	// Check for sub-checkpoint - find which main step it belongs to
	for mainStep, subSteps := range SubCheckpoints {
		for subIndex, subCheckpoint := range subSteps {
			if subCheckpoint == job.CurrentCheckpoint {
				mainStepIndex := StepIndexMap[mainStep]

				// If it's the last sub-checkpoint of a step, move to next main step
				if subIndex == len(subSteps)-1 {
					log.Printf("Job %s: Resuming from next step after completing %s", jobID, mainStep)
					return mainStepIndex + 1
				}

				// Otherwise, resume the current main step (it will handle sub-checkpoint internally)
				log.Printf("Job %s: Resuming %s from sub-checkpoint %s", jobID, mainStep, job.CurrentCheckpoint)
				return mainStepIndex
			}
		}
	}

	log.Printf("Warning: job %s has unknown checkpoint: %s. Starting from beginning.", jobID, job.CurrentCheckpoint)
	return 0
}

// setJobRunning updates job status to running
func (h *TaskHandler) setJobRunning(ctx context.Context, job *domain.Job) error {
	job.Status = domain.StatusRunning
	if err := h.repo.Save(ctx, job); err != nil {
		return fmt.Errorf("failed to save job status: %w", err)
	}
	h.notifier.BroadcastUpdate(job)

	return nil
}

// logJobStart logs the start of job processing
func (h *TaskHandler) logJobStart(jobID string, startIndex int) {
	if startIndex < len(JobSteps) {
		log.Printf("Starting job: %s. Resuming from step: %s", jobID, JobSteps[startIndex])
	} else {
		log.Printf("Starting job: %s. All steps completed, finalizing...", jobID)
	}
}

// processJobSteps handles the main job processing loop
func (h *TaskHandler) processJobSteps(ctx context.Context, jobID string, startIndex int, cancel context.CancelFunc) error {
	totalSteps := len(JobSteps)

	for i := startIndex; i < totalSteps; i++ {
		currentStepName := JobSteps[i]

		// Check current job status
		currentJob, err := h.repo.FindByID(ctx, jobID)
		if err != nil {
			return fmt.Errorf("failed to find job in processing loop: %w", err)
		}

		// Handle pause/cancel states
		if err := h.handleJobStateChanges(ctx, jobID, currentJob, cancel); err != nil {
			return err
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			log.Printf("Job %s CANCELED (via context)", jobID)
			return nil
		default:
			// Continue processing
		}

		// Execute specific step function
		stepExecutor := getStepExecutor(currentStepName)
		if stepExecutor == nil {
			log.Printf("Job %s: Unknown step %s, using default processing", jobID, currentStepName)
			log.Printf("Job %s: Running task: %s", jobID, currentStepName)
			time.Sleep(StepProcessingTime)
		} else {
			if err := stepExecutor(h, ctx, jobID); err != nil {
				return fmt.Errorf("failed to execute step %s: %w", currentStepName, err)
			}
		}

		// Check if job was preempted during processing
		if shouldSkipProgress, err := h.checkJobPreemption(ctx, jobID, i, cancel); err != nil {
			return nil
		} else if shouldSkipProgress {
			continue
		}

		// Save progress
		if err := h.saveStepProgress(ctx, jobID, currentStepName, totalSteps); err != nil {
			return err
		}
	}

	return nil
}

func (h *TaskHandler) handleJobStateChanges(ctx context.Context, jobID string, job *domain.Job, cancel context.CancelFunc) error {
	if job.Status == domain.StatusCanceled {
		log.Printf("Job %s CANCELED", jobID)
		cancel()
		return nil
	}

	if job.Status == domain.StatusPaused {
		log.Printf("Job %s PAUSED - task will exit and wait for resume", jobID)
		// Save current state and exit task - let resume create new task
		return fmt.Errorf("job paused - task exiting to allow resume")
	}

	return nil
}

// checkJobPreemption checks if job was paused/canceled during step processing
func (h *TaskHandler) checkJobPreemption(ctx context.Context, jobID string, stepIndex int, cancel context.CancelFunc) (bool, error) {
	jobAfterWork, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return false, fmt.Errorf("failed to check job after work: %w", err)
	}

	if jobAfterWork.Status != domain.StatusRunning {
		log.Printf("Job %s was preempted (Status %s), discarding progress for step %d", jobID, jobAfterWork.Status, stepIndex+1)

		if jobAfterWork.Status == domain.StatusCanceled {
			cancel()
		}

		return true, nil //Skip saving progress
	}

	return false, nil // Continue with saving progress
}

// saveStepProgress saves the current step progress
func (h *TaskHandler) saveStepProgress(ctx context.Context, jobID string, stepName string, totalSteps int) error {
	job, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to find job for progress update: %w", err)
	}

	// Get human-readable step description
	stepDescription, exists := StepDescriptions[stepName]
	if !exists {
		stepDescription = stepName // Fallback to step name
	}

	log.Printf("Job %s: Saving Checkpoint: %s (%s)", jobID, stepName, stepDescription)
	job.CurrentCheckpoint = stepName
	job.CurrentStepName = stepDescription
	job.Progress = h.CalculateProgress(stepName, totalSteps)

	if err := h.repo.Save(ctx, job); err != nil {
		log.Printf("Error saving job %s:%v", jobID, err)
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	h.notifier.BroadcastUpdate(job)
	return nil
}

// completeJob finalizes job completion
func (h *TaskHandler) completedJob(ctx context.Context, jobID string, cancel context.CancelFunc) error {
	log.Printf("Job %s COMPLETED:", jobID)

	finalJob, err := h.repo.FindByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to get final job state: %w", err)
	}

	// Check if job was preempted before completion
	if finalJob.Status != domain.StatusRunning {
		log.Printf("Job %s was preempted before completion (Status: %s)", finalJob.FileName, finalJob.Status)
		if finalJob.Status == domain.StatusCanceled {
			cancel()
		}

		return nil
	}

	// Mark job as completed
	finalJob.CurrentCheckpoint = CompletedCheckpoint
	finalJob.CurrentStepName = StepDescriptions["COMPLETED"]
	finalJob.Status = domain.StatusCompleted
	finalJob.Progress = 100

	if err := h.repo.Save(ctx, finalJob); err != nil {
		return fmt.Errorf("failed to save completed job: %w", err)
	}

	h.notifier.BroadcastUpdate(finalJob)
	return nil
}

func (h *TaskHandler) CalculateProgress(checkpoint string, totalStep int) int {
	if checkpoint == "" {
		return 0
	}

	// ถ้า checkpoint เป็น "COMPLETED" ให้ return 100%
	if checkpoint == "COMPLETED" {
		return 100
	}

	if index, ok := StepIndexMap[checkpoint]; ok {
		// คำนวณ progress โดยให้ step สุดท้ายได้แค่ 95%
		// เฉพาะ "COMPLETED" เท่านั้นที่จะได้ 100%
		stepProgress := ((index + 1) * 95) / totalStep

		// ป้องกันไม่ให้เกิน 95% จนกว่าจะ COMPLETED จริงๆ
		if stepProgress > 95 {
			stepProgress = 95
		}

		return stepProgress
	}

	return 0
}
