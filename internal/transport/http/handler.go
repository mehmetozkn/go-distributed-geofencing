package http

import (
	"github.com/gofiber/fiber/v2"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/domain"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/model"
)

// LocationHandler handles HTTP requests for location ingestion.
type LocationHandler struct {
	service domain.ILocationService
}

// NewLocationHandler creates a handler with the injected service.
func NewLocationHandler(service domain.ILocationService) *LocationHandler {
	return &LocationHandler{service: service}
}

// RegisterRoutes wires the handler to the Fiber app.
func (h *LocationHandler) RegisterRoutes(app *fiber.App) {
	api := app.Group("/api/v1")
	api.Post("/locations", h.IngestLocation)
}

// IngestLocation godoc
// POST /api/v1/locations
// Body: { "device_id": "...", "latitude": 41.0, "longitude": 29.0, "timestamp": 1715500000000 }
func (h *LocationHandler) IngestLocation(c *fiber.Ctx) error {
	var event model.LocationEvent
	if err := c.BodyParser(&event); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	if err := h.service.Ingest(c.UserContext(), event); err != nil {
		return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{"error": err.Error()})
	}

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"status": "accepted"})
}
