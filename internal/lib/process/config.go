package process

import (
	"context"
	"time"
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
	Action      func()        `json:"-"` // Optional custom action for this sub-step
}

// JobProcessConfig defines the entire job process configuration
type JobProcessConfig struct {
	ProcessName string          `json:"process_name"`
	Steps       []JobStepConfig `json:"steps"`
}

// ProcessConstants defines common constants used across all processes
type ProcessConstants struct {
	StepProcessingTime        time.Duration
	CompletedCheckpoint       string
	MaxProgressBeforeComplete int
}

// DefaultProcessConstants returns the default constants
func DefaultProcessConstants() ProcessConstants {
	return ProcessConstants{
		StepProcessingTime:        2 * time.Second,
		CompletedCheckpoint:       "COMPLETED",
		MaxProgressBeforeComplete: 95,
	}
}
