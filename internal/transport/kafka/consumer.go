package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/IBM/sarama"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/domain"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/model"
)

const batchSize = 100

// Consumer implements domain.ILocationConsumer.
// It consumes messages in batches of 100 and delegates processing to the service.
type Consumer struct {
	group   sarama.ConsumerGroup
	service domain.ILocationService
	topics  []string
	ready   chan bool
}

// NewConsumer creates a Kafka consumer group.
func NewConsumer(brokers []string, groupID string, service domain.ILocationService) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRange(),
	}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	log.Printf("[kafka-consumer] group=%s connected to brokers: %v", groupID, brokers)
	return &Consumer{
		group:   group,
		service: service,
		topics:  []string{TopicLocationEvents},
		ready:   make(chan bool),
	}, nil
}

// Start begins the consume loop. Blocks until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) error {
	handler := &consumerGroupHandler{
		service: c.service,
		ready:   c.ready,
	}

	for {
		if err := c.group.Consume(ctx, c.topics, handler); err != nil {
			log.Printf("[kafka-consumer] error: %v", err)
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.ready = make(chan bool)
		handler.ready = c.ready
	}
}

// Close shuts down the consumer group.
func (c *Consumer) Close() error {
	return c.group.Close()
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler.
type consumerGroupHandler struct {
	service domain.ILocationService
	ready   chan bool
}

func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim reads messages and processes them in batches of 100.
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var (
		mu    sync.Mutex
		batch = make([]model.LocationEvent, 0, batchSize)
	)

	flush := func() {
		mu.Lock()
		defer mu.Unlock()

		if len(batch) == 0 {
			return
		}

		toProcess := make([]model.LocationEvent, len(batch))
		copy(toProcess, batch)
		batch = batch[:0]

		geoEvents, err := h.service.ProcessBatch(session.Context(), toProcess)
		if err != nil {
			log.Printf("[kafka-consumer] batch processing error: %v", err)
			return
		}
		if len(geoEvents) > 0 {
			log.Printf("[kafka-consumer] detected %d geofence events", len(geoEvents))
		}
	}

	for msg := range claim.Messages() {
		var event model.LocationEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("[kafka-consumer] unmarshal error: %v", err)
			session.MarkMessage(msg, "")
			continue
		}

		mu.Lock()
		batch = append(batch, event)
		shouldFlush := len(batch) >= batchSize
		mu.Unlock()

		session.MarkMessage(msg, "")

		if shouldFlush {
			flush()
		}
	}

	// flush remaining
	flush()
	return nil
}
