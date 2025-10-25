package http

import (
	"simple-queue-103/internal/adapters/broadcast"
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
	// ในตัวอย่างจริง ควรอ้่าน fileName จาก body
	fileName := "uploaded_file.csv"
	job, err := h.service.CreateJob(fileName)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(err)
	}

	return c.Status(fiber.StatusOK).JSON(job)
}

func (h *fiberHandler) GetAllJobs(c *fiber.Ctx) error {
	jobs, err := h.service.GetAllJobs()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(err)
	}

	return c.JSON(jobs)
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

	app.Get("/jobs", h.GetAllJobs)
	app.Post("/jobs", h.CreateJob)
	app.Post("/jobs/:id/control", h.ControlJob)

	// WebSocket Rote
	app.Get("/ws/status", notifier.HandleWS)
}
