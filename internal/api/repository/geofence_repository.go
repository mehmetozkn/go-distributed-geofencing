package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/model"
)

type IGeofenceRepository interface {
	// FindContaining returns all geofence zones whose polygon contains the given point.
	// Note: PostGIS uses (longitude, latitude) order for ST_MakePoint.
	FindContaining(ctx context.Context, lat, lng float64) ([]model.Geofence, error)
}

type geofenceRepository struct {
	db *gorm.DB
}

func NewGeofenceRepository(db *gorm.DB) IGeofenceRepository {
	return &geofenceRepository{db: db}
}

func (r *geofenceRepository) FindContaining(ctx context.Context, lat, lng float64) ([]model.Geofence, error) {
	var fences []model.Geofence
	err := r.db.WithContext(ctx).Raw(
		`SELECT id, name
		   FROM geofences
		  WHERE ST_Contains(geometry, ST_SetSRID(ST_MakePoint(?, ?), 4326))`,
		lng, lat, // ST_MakePoint(X, Y) → (longitude, latitude)
	).Scan(&fences).Error
	if err != nil {
		return nil, fmt.Errorf("geofenceRepository.FindContaining: %w", err)
	}
	return fences, nil
}
