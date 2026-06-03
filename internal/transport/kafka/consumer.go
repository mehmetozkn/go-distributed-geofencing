package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/model"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/repository"
	goredis "github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	Start(ctx context.Context)
	Close() error
}

type consumer struct {
	reader       *kafka.Reader
	repo         repository.ILocationRepository
	geofenceRepo repository.IGeofenceRepository
	rdb          *goredis.Client
}

func NewConsumer(brokers []string, topic string, groupID string, repo repository.ILocationRepository, geofenceRepo repository.IGeofenceRepository, rdb *goredis.Client) (Consumer, error) {
	if err := ensureTopic(brokers[0], topic, 3, 1); err != nil {
		return nil, fmt.Errorf("ensure topic: %w", err)
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
	})

	return &consumer{
		reader:       r,
		repo:         repo,
		geofenceRepo: geofenceRepo,
		rdb:          rdb,
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

		log.Printf("[Kafka Consumer] Device: %s | Coordinates: %f, %f", loc.DeviceID, loc.Latitude, loc.Longitude)

		// ── Geofence State Machine ──────────────────────────────────
		fences, err := c.geofenceRepo.FindContaining(ctx, loc.Latitude, loc.Longitude)
		if err != nil {
			log.Printf("[Geofence] Error checking geofences: %v", err)
			continue
		}
		c.updateGeofenceState(ctx, &loc, fences)
	}
}

// updateGeofenceState runs an ENTER/EXIT state machine per device using a
// Redis Hash as the source of truth for which zones a device is currently in.
//
//	Key schema:  geofence_active:{deviceID}
//	Hash fields: {zoneID (string)} → {zoneName}
func (c *consumer) updateGeofenceState(ctx context.Context, loc *model.Location, activeFences []model.Geofence) {
	deviceKey := fmt.Sprintf("geofence_active:%s", loc.DeviceID)

	// Build map of currently active zones: id → name
	activeMap := make(map[string]string, len(activeFences))
	for _, f := range activeFences {
		activeMap[strconv.FormatUint(uint64(f.ID), 10)] = f.Name
	}

	// ── PostGIS result ───────────────────────────────────────
	if len(activeFences) == 0 {
		log.Printf("[PostGIS] Spatial check in progress... NOT inside any boundary.")
	} else {
		for _, f := range activeFences {
			log.Printf("[PostGIS] Spatial check in progress... INSIDE boundary! Zone: %s", f.Name)
		}
	}

	// Retrieve previously known zones from Redis
	previousZones, err := c.rdb.HGetAll(ctx, deviceKey).Result()
	if err != nil {
		log.Printf("[Geofence] Redis HGetAll error: %v", err)
		return
	}

	// ── Case 1: Outside AND no Redis record ───────────────────
	if len(activeFences) == 0 && len(previousZones) == 0 {
		log.Printf("[Redis] Does the device have a previous state in memory? Checking... NONE.")
		log.Printf("[Status] Device was already outside and remains outside. Skipped silently.")
		return
	}

	// ── ENTER (Case 2) / Spam filter (Case 3) ─────────────────
	for _, f := range activeFences {
		id := strconv.FormatUint(uint64(f.ID), 10)
		if _, known := previousZones[id]; !known {
			// Case 2: First time entering this zone
			log.Printf("[Redis] Is the device registered in memory? Checking... NO (Entering for the first time).")
			log.Printf("[Redis DB] -> KEY WRITTEN: \"geofence:%s\" -> \"%s\"", loc.DeviceID, f.Name)
			log.Printf("[Geofence ENTER] Device %s entered Zone %s!", loc.DeviceID, f.Name)
			c.rdb.HSet(ctx, deviceKey, id, f.Name)
		} else {
			// Case 3: Already inside — suppress spam
			log.Printf("[Redis] Is the device registered in memory? Checking... YES!")
			log.Printf("[Redis DB] -> Info: Device is already marked as inside zone \"%s\".", f.Name)
			log.Printf("[Status] Duplicate (spam) log suppressed. Skipped silently.")
		}
	}

	// ── EXIT (Case 4) ───────────────────────────────────
	for id, name := range previousZones {
		if _, active := activeMap[id]; !active {
			log.Printf("[Redis] Is the device registered in memory? Checking... YES! Was last inside \"%s\".", name)
			log.Printf("[Redis DB] -> KEY DELETED: \"geofence:%s\"", loc.DeviceID)
			log.Printf("[Geofence EXIT] Device %s left Zone %s!", loc.DeviceID, name)
			c.rdb.HDel(ctx, deviceKey, id)
		}
	}
}

func (c *consumer) Close() error {
	return c.reader.Close()
}
