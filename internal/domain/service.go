package domain

import (
	"context"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/model"
)

// ILocationService defines the business logic for location processing.
type ILocationService interface {
	// Ingest validates and publishes a location event to the message broker.
	Ingest(ctx context.Context, event model.LocationEvent) error

	// ProcessBatch persists a batch of locations and checks geofence membership.
	ProcessBatch(ctx context.Context, events []model.LocationEvent) ([]model.GeofenceEvent, error)
}

// ILocationProducer defines the message broker producer interface.
type ILocationProducer interface {
	// Publish sends a location event to the message broker.
	Publish(ctx context.Context, event model.LocationEvent) error

	// Close gracefully shuts down the producer.
	Close() error
}

// ILocationConsumer defines the message broker consumer interface.
type ILocationConsumer interface {
	// Start begins consuming messages. Blocks until ctx is cancelled.
	Start(ctx context.Context) error

	// Close gracefully shuts down the consumer.
	Close() error
}
