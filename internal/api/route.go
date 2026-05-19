package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/handler"
)

type RouteContext struct {
	App *fiber.App
}

type IRoute interface {
	SetupRoutes(r *RouteContext)
}

type route struct {
	locationHandler handler.ILocationHandler
}

func NewRoute(
	lHandler handler.ILocationHandler,
) IRoute {
	return &route{
		locationHandler: lHandler,
	}
}

func (r *route) SetupRoutes(rc *RouteContext) {
	v1Group := rc.App.Group("/api/v1")

	r.locationRoutes(v1Group)
}

func (r *route) locationRoutes(fr fiber.Router) {
	locGroup := fr.Group("/locations")
	locGroup.Post("/ingest", r.locationHandler.IngestLocation)
}
