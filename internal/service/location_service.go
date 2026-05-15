package service

import (
	"context"
	"errors"
	"time"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/domain"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/model"
)

var (
	ErrInvalidDeviceID  = errors.New("device_id is required")
	ErrInvalidLatitude  = errors.New("latitude must be between -90 and 90")
	ErrInvalidLongitude = errors.New("longitude must be between -180 and 180")
	ErrInvalidTimestamp = errors.New("timestamp must be a positive unix epoch")
)

// LocationService implements domain.ILocationService.
type LocationService struct {
	producer     domain.ILocationProducer
	locationRepo domain.ILocationRepository
	geofenceRepo domain.IGeofenceRepository
}

// NewLocationService creates a new LocationService with injected dependencies.
func NewLocationService(
	producer domain.ILocationProducer,
	locationRepo domain.ILocationRepository,
	geofenceRepo domain.IGeofenceRepository,
) *LocationService {
	return &LocationService{
		producer:     producer,
		locationRepo: locationRepo,
		geofenceRepo: geofenceRepo,
	}
}

// Ingest validates the incoming event and publishes it to Kafka.
func (s *LocationService) Ingest(ctx context.Context, event model.LocationEvent) error {
	if err := validateEvent(event); err != nil {
		return err
	}
	return s.producer.Publish(ctx, event)
}

// ProcessBatch converts events to Location entities, persists them via PostGIS,
// and checks each point against all known geofences.
func (s *LocationService) ProcessBatch(ctx context.Context, events []model.LocationEvent) ([]model.GeofenceEvent, error) {
	if len(events) == 0 {
		return nil, nil
	}

	now := time.Now().UTC()
	locations := make([]model.Location, 0, len(events))
	for _, e := range events {
		locations = append(locations, model.Location{
			DeviceID:  e.DeviceID,
			Latitude:  e.Latitude,
			Longitude: e.Longitude,
			Timestamp: time.UnixMilli(e.Timestamp).UTC(),
			CreatedAt: now,
		})
	}

	if err := s.locationRepo.SaveBatch(ctx, locations); err != nil {
		return nil, err
	}

	var geofenceEvents []model.GeofenceEvent
	for _, e := range events {
		fences, err := s.geofenceRepo.FindContaining(ctx, e.Latitude, e.Longitude)
		if err != nil {
			return nil, err
		}
		for _, f := range fences {
			geofenceEvents = append(geofenceEvents, model.GeofenceEvent{
				DeviceID:     e.DeviceID,
				GeofenceID:   f.ID,
				GeofenceName: f.Name,
				EventType:    "enter",
				Timestamp:    e.Timestamp,
			})
		}
	}

	return geofenceEvents, nil
}

func validateEvent(e model.LocationEvent) error {
	if e.DeviceID == "" {
		return ErrInvalidDeviceID
	}
	if e.Latitude < -90 || e.Latitude > 90 {
		return ErrInvalidLatitude
	}
	if e.Longitude < -180 || e.Longitude > 180 {
		return ErrInvalidLongitude
	}
	if e.Timestamp <= 0 {
		return ErrInvalidTimestamp
	}
	return nil
}
