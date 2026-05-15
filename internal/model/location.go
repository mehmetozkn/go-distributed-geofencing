package model

import "time"

// Location represents a single GPS data point received from a mobile device.
type Location struct {
	ID        int64     `json:"id" db:"id"`
	DeviceID  string    `json:"device_id" db:"device_id"`
	Latitude  float64   `json:"latitude" db:"latitude"`
	Longitude float64   `json:"longitude" db:"longitude"`
	Timestamp time.Time `json:"timestamp" db:"timestamp"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// LocationEvent is the Kafka message payload for a location ingestion event.
type LocationEvent struct {
	DeviceID  string  `json:"device_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Timestamp int64   `json:"timestamp"` // Unix epoch millis
}
