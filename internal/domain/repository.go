package domain

import (
	"context"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/model"
)

// ILocationRepository defines persistence operations for location data.
type ILocationRepository interface {
	// SaveBatch inserts a batch of locations into PostGIS using ST_MakePoint.
	SaveBatch(ctx context.Context, locations []model.Location) error

	// FindByDeviceID returns the latest locations for a given device.
	FindByDeviceID(ctx context.Context, deviceID string, limit int) ([]model.Location, error)
}

// IGeofenceRepository defines persistence operations for geofences.
type IGeofenceRepository interface {
	// Create persists a new geofence polygon.
	Create(ctx context.Context, geofence *model.Geofence) error

	// FindAll returns all active geofences.
	FindAll(ctx context.Context) ([]model.Geofence, error)

	// FindContaining returns geofences that contain the given point.
	FindContaining(ctx context.Context, lat, lng float64) ([]model.Geofence, error)
}
