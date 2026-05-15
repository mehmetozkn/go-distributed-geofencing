package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/model"
)

const TopicLocationEvents = "location-events"

// Producer implements domain.ILocationProducer using Sarama.
type Producer struct {
	producer sarama.SyncProducer
}

// NewProducer creates a new Kafka sync producer.
func NewProducer(brokers []string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3

	sp, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	log.Printf("[kafka-producer] connected to brokers: %v", brokers)
	return &Producer{producer: sp}, nil
}

// Publish marshals the event to JSON and sends it to Kafka, keyed by device_id.
func (p *Producer) Publish(ctx context.Context, event model.LocationEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: TopicLocationEvents,
		Key:   sarama.StringEncoder(event.DeviceID),
		Value: sarama.ByteEncoder(payload),
	}

	_, _, err = p.producer.SendMessage(msg)
	return err
}

// Close shuts down the producer.
func (p *Producer) Close() error {
	return p.producer.Close()
}
