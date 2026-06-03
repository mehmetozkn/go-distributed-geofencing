package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/model"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/repository"
	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	Start(ctx context.Context)
	Close() error
}

type consumer struct {
	reader *kafka.Reader
	repo   repository.ILocationRepository
}

func NewConsumer(brokers []string, topic string, groupID string, repo repository.ILocationRepository) (Consumer, error) {
	if err := ensureTopic(brokers[0], topic, 3, 1); err != nil {
		return nil, fmt.Errorf("ensure topic: %w", err)
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
	})

	return &consumer{
		reader: r,
		repo:   repo,
	}, nil
}

// ensureTopic creates the topic if it does not already exist.
func ensureTopic(broker, topic string, numPartitions, replicationFactor int) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("find controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	})
	if err != nil && err != kafka.TopicAlreadyExists {
		return fmt.Errorf("create topic: %w", err)
	}
	return nil
}

func (c *consumer) Start(ctx context.Context) {
	log.Printf("[Kafka Consumer] Starting consumer for topic %q", c.reader.Config().Topic)
	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Context cancelled, stop gracefully
				log.Println("[Kafka Consumer] Context cancelled, stopping reader")
				return
			}
			log.Printf("[Kafka Consumer] Error reading message: %v", err)
			continue
		}

		var loc model.Location
		if err := json.Unmarshal(m.Value, &loc); err != nil {
			log.Printf("[Kafka Consumer] Error unmarshaling message: %v (Message: %s)", err, string(m.Value))
			continue
		}

		// Save to repository (PostgreSQL)
		if err := c.repo.Create(ctx, &loc); err != nil {
			log.Printf("[Kafka Consumer] Error saving to DB: %v", err)
			// Depending on requirements, we might want to implement a retry or dead-letter queue.
			continue
		}

		log.Printf("[Kafka Consumer] Processed and saved -> DeviceID: %q, Lat: %f, Lng: %f", loc.DeviceID, loc.Latitude, loc.Longitude)
	}
}

func (c *consumer) Close() error {
	return c.reader.Close()
}
