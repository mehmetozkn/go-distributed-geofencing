package service

import (
	"context"
	"fmt"
	"log"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/model"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/transport/kafka"
)

type ILocationService interface {
	Ingest(ctx context.Context, loc model.Location) error
}

type locationService struct {
	producer kafka.Producer
}

func NewLocationService(producer kafka.Producer) ILocationService {
	return &locationService{producer: producer}
}

func (s *locationService) Ingest(ctx context.Context, loc model.Location) error {
	if err := s.producer.PublishLocation(ctx, loc); err != nil {
		return fmt.Errorf("locationService.Ingest: %w", err)
	}

	log.Printf("[LocationService] Pushed to Kafka -> DeviceID: %q, Lat: %f, Lng: %f, Timestamp: %d",
		loc.DeviceID, loc.Latitude, loc.Longitude, loc.Timestamp)

	return nil
}
