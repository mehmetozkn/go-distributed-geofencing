package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/model"
	"github.com/segmentio/kafka-go"
)

type Producer interface {
	PublishLocation(ctx context.Context, loc model.Location) error
	Close() error
}

type producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) Producer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	return &producer{writer: w}
}

func (p *producer) PublishLocation(ctx context.Context, loc model.Location) error {
	data, err := json.Marshal(loc)
	if err != nil {
		return fmt.Errorf("failed to marshal location: %w", err)
	}

	err = p.writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte(loc.DeviceID),
			Value: data,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	return nil
}

func (p *producer) Close() error {
	return p.writer.Close()
}
