package http

import (
	"simple-queue-103/internal/adapters/broadcast"
	"simple-queue-103/internal/core/domain"
	"simple-queue-103/internal/core/ports"

	"github.com/gofiber/fiber/v2"
)

type fiberHandler struct {
	service ports.JobService
}

func NewFiberHandler(service ports.JobService) *fiberHandler {
	return &fiberHandler{service: service}
}

func (h *fiberHandler) CreateJob(c *fiber.Ctx) error {
	var body struct {
		FileName       string `json:"fileName"` // Support both camelCase and snake_case
		FileNameAlt    string `json:"file_name"`
		ProcessType    string `json:"processType,omitempty"` // Support both camelCase and snake_case
		ProcessTypeAlt string `json:"process_type,omitempty"`
	}

	if err := c.BodyParser(&body); err != nil {
		// Fallback for backward compatibility
		body.FileName = "uploaded_file.csv"
		body.ProcessType = "data_analysis"
	}

	// Use alternative fields if main fields are empty
	if body.FileName == "" && body.FileNameAlt != "" {
		body.FileName = body.FileNameAlt
	}
	if body.ProcessType == "" && body.ProcessTypeAlt != "" {
		body.ProcessType = body.ProcessTypeAlt
	}

	if body.FileName == "" {
		body.FileName = "uploaded_file.csv"
	}
	if body.ProcessType == "" {
		body.ProcessType = "data_analysis"
	}

	job, err := h.service.CreateJobForProcess(body.FileName, body.ProcessType)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(job)
}

func (h *fiberHandler) GetAllJobs(c *fiber.Ctx) error {
	// Check if process_type query parameter is provided
	processType := c.Query("process_type")

	var jobs []*domain.Job
	var err error

	if processType != "" {
		jobs, err = h.service.GetJobsByProcess(processType)
	} else {
		jobs, err = h.service.GetAllJobs()
	}

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(jobs)
}

func (h *fiberHandler) GetJob(c *fiber.Ctx) error {
	id := c.Params("id")
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Job ID is required",
		})
	}

	job, err := h.service.GetJob(id)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"job": job,
	})
}

func (h *fiberHandler) GetProcesses(c *fiber.Ctx) error {
	processes := []map[string]interface{}{
		{
			"id":          "data_analysis",
			"name":        "Data Analysis",
			"description": "Comprehensive data analysis with ML models",
			"steps":       6,
		},
		{
			"id":          "file_import",
			"name":        "File Import",
			"description": "Import and process uploaded files",
			"steps":       2,
		},
		{
			"id":          "report_gen",
			"name":        "Report Generation",
			"description": "Generate charts and export reports",
			"steps":       3,
		},
	}

	return c.JSON(processes)
}

func (h *fiberHandler) ControlJob(c *fiber.Ctx) error {
	id := c.Params("id")
	var body struct {
		Command string `json:"command"` // "PAUSE", "RESTART", "CANCEL"
	}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(err)
	}

	if err := h.service.ControlJob(id, body.Command); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(err)
	}

	return c.SendStatus(fiber.StatusOK)
}

// RegisterRoutes คือตัวช่วยตั้วค่า Routes
func RegisterRoutes(app *fiber.App, service ports.JobService, notifier *broadcast.WebSocketNotifier) {
	h := NewFiberHandler(service)

	// Job routes
	app.Get("/jobs", h.GetAllJobs) // GET /jobs?process_type=data_analysis
	app.Get("/jobs/:id", h.GetJob) // GET /jobs/:id - get single job
	app.Post("/jobs", h.CreateJob) // POST /jobs with process_type in body
	app.Post("/jobs/:id/control", h.ControlJob)

	// Process routes
	app.Get("/processes", h.GetProcesses) // GET /processes - list available processes

	// WebSocket Route
	app.Get("/ws/status", notifier.HandleWS)
}
