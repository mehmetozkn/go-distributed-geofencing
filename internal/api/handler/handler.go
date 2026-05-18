package handler

import (
	"github.com/gofiber/fiber/v2"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/model"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/service"
)

type ILocationHandler interface {
	IngestLocation(c *fiber.Ctx) error
}

type locationHandler struct {
	service service.ILocationService
}

func NewLocationHandler(s service.ILocationService) ILocationHandler {
	return &locationHandler{service: s}
}

func (h *locationHandler) IngestLocation(c *fiber.Ctx) error {
	var loc model.Location
	if err := c.BodyParser(&loc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	if err := h.service.Ingest(c.UserContext(), loc); err != nil {
		return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{"error": err.Error()})
	}

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"status": "accepted"})
}
