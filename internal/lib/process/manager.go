package process

import (
	"context"
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
