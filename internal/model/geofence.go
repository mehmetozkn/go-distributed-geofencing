package model

import "time"

// Point represents a single coordinate in a polygon ring.
type Point struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// Geofence represents a named geographic polygon boundary.
type Geofence struct {
	ID        int64     `json:"id" db:"id"`
	Name      string    `json:"name" db:"name"`
	Polygon   []Point   `json:"polygon"` // ordered ring of vertices
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// GeofenceEvent is emitted when a device enters or exits a geofence.
type GeofenceEvent struct {
	DeviceID     string `json:"device_id"`
	GeofenceID   int64  `json:"geofence_id"`
	GeofenceName string `json:"geofence_name"`
	EventType    string `json:"event_type"` // "enter" or "exit"
	Timestamp    int64  `json:"timestamp"`
}
