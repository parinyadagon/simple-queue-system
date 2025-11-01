package process

import (
	"context"
	"sync"
	"time"
)

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

// CreateCustomProcess allows creating a completely custom process with optimizations
func (pm *ProcessManager) CreateCustomProcess(name string) *ProcessBuilder {
	return getProcessBuilder(name, pm)
}

// CreateCustomProcessWithCapacity creates a process with pre-allocated capacity
func (pm *ProcessManager) CreateCustomProcessWithCapacity(name string, stepsCapacity int) *ProcessBuilder {
	return getProcessBuilderWithCapacity(name, pm, stepsCapacity)
}

// Object pool for ProcessBuilder to reduce allocations
var processBuilderPool = sync.Pool{
	New: func() interface{} {
		return &ProcessBuilder{}
	},
}

// getProcessBuilder returns an optimized ProcessBuilder from pool
func getProcessBuilder(name string, pm *ProcessManager) *ProcessBuilder {
	pb := processBuilderPool.Get().(*ProcessBuilder)
	pb.reset(name, pm, 4) // Default capacity of 4 steps
	return pb
}

// getProcessBuilderWithCapacity returns a ProcessBuilder with specific capacity
func getProcessBuilderWithCapacity(name string, pm *ProcessManager, capacity int) *ProcessBuilder {
	pb := processBuilderPool.Get().(*ProcessBuilder)
	pb.reset(name, pm, capacity)
	return pb
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

// ProcessBuilder provides a high-performance fluent interface for building custom processes
type ProcessBuilder struct {
	config      *JobProcessConfig
	manager     *ProcessManager
	stepBuilder *StepBuilder // Reused step builder to reduce allocations
}

// reset resets the ProcessBuilder for reuse (object pooling)
func (pb *ProcessBuilder) reset(name string, pm *ProcessManager, stepsCapacity int) {
	if pb.config == nil {
		pb.config = &JobProcessConfig{}
	}
	pb.config.ProcessName = name
	// Pre-allocate with capacity to avoid multiple reallocations
	if cap(pb.config.Steps) < stepsCapacity {
		pb.config.Steps = make([]JobStepConfig, 0, stepsCapacity)
	} else {
		pb.config.Steps = pb.config.Steps[:0] // Reset slice but keep capacity
	}
	pb.manager = pm

	// Reuse step builder
	if pb.stepBuilder == nil {
		pb.stepBuilder = &StepBuilder{}
	}
}

// release returns the ProcessBuilder to the pool
func (pb *ProcessBuilder) release() {
	// Clear references to prevent memory leaks
	pb.config = nil
	pb.manager = nil
	if pb.stepBuilder != nil {
		pb.stepBuilder.step = nil
		pb.stepBuilder.builder = nil
	}
	processBuilderPool.Put(pb)
}

// SetDescription sets the description for the process
func (pb *ProcessBuilder) SetDescription(description string) *ProcessBuilder {
	pb.config.Description = description
	return pb
}

// AddStep adds a step to the process with optimizations
func (pb *ProcessBuilder) AddStep(name, description string) *StepBuilder {
	step := JobStepConfig{
		Name:        name,
		Description: description,
		SubSteps:    make([]JobSubStepConfig, 0, 4), // Pre-allocate for typical 4 sub-steps
	}

	pb.config.Steps = append(pb.config.Steps, step)

	// Reuse step builder
	pb.stepBuilder.step = &pb.config.Steps[len(pb.config.Steps)-1]
	pb.stepBuilder.builder = pb
	return pb.stepBuilder
}

// AddStepWithFunc adds a step with a custom execution function (optimized)
func (pb *ProcessBuilder) AddStepWithFunc(name, description string, executeFunc func(ctx context.Context, jobID string, step *JobStepConfig) error) *StepBuilder {
	step := JobStepConfig{
		Name:        name,
		Description: description,
		SubSteps:    make([]JobSubStepConfig, 0, 4), // Pre-allocate
		ExecuteFunc: executeFunc,
	}

	pb.config.Steps = append(pb.config.Steps, step)

	// Reuse step builder
	pb.stepBuilder.step = &pb.config.Steps[len(pb.config.Steps)-1]
	pb.stepBuilder.builder = pb
	return pb.stepBuilder
}

// Build completes the process building and returns the manager (with cleanup)
func (pb *ProcessBuilder) Build() *ProcessManager {
	pb.manager.currentProcess = pb.config

	// Make a copy of the config to prevent issues after release
	config := &JobProcessConfig{
		ProcessName: pb.config.ProcessName,
		Steps:       make([]JobStepConfig, len(pb.config.Steps)),
	}
	copy(config.Steps, pb.config.Steps)
	pb.manager.currentProcess = config

	// Return builder to pool for reuse
	pb.release()

	return pb.manager
}

// BuildAndRegister builds the process and registers it in ProcessConfigurations
func (pb *ProcessBuilder) BuildAndRegister(processKey string) *ProcessManager {
	pb.manager.currentProcess = pb.config

	// Make a copy for registration
	config := &JobProcessConfig{
		ProcessName: pb.config.ProcessName,
		Description: pb.config.Description,
		Steps:       make([]JobStepConfig, len(pb.config.Steps)),
	}
	copy(config.Steps, pb.config.Steps)

	// Register in global configurations
	ProcessConfigurations[processKey] = config
	pb.manager.currentProcess = config

	// Return builder to pool
	pb.release()

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

// BuildAndRegister completes the building process and registers the process
func (sb *StepBuilder) BuildAndRegister(processKey string) *ProcessManager {
	return sb.builder.BuildAndRegister(processKey)
}
