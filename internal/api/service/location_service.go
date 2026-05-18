package service

import (
	"context"
	"fmt"
	"log"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/model"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/repository"
)

type ILocationService interface {
	Ingest(ctx context.Context, loc model.Location) error
}

type locationService struct {
	repo repository.ILocationRepository
}

func NewLocationService(repo repository.ILocationRepository) ILocationService {
	return &locationService{repo: repo}
}

func (s *locationService) Ingest(ctx context.Context, loc model.Location) error {
	if err := s.repo.Create(ctx, &loc); err != nil {
		return fmt.Errorf("locationService.Ingest: %w", err)
	}

	log.Printf("[LocationService] Saved -> DeviceID: %q, Lat: %f, Lng: %f, Timestamp: %d",
		loc.DeviceID, loc.Latitude, loc.Longitude, loc.Timestamp)

	return nil
}
