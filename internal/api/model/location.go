package model

import "time"

// Location represents a single GPS data point received from a mobile device.
type Location struct {
	ID        uint      `gorm:"primaryKey;autoIncrement"          json:"id,omitempty"`
	DeviceID  string    `gorm:"column:device_id;not null;index"   json:"device_id"`
	Latitude  float64   `gorm:"column:latitude;not null"          json:"latitude"`
	Longitude float64   `gorm:"column:longitude;not null"         json:"longitude"`
	Timestamp int64     `gorm:"column:ts;not null;index:,sort:desc" json:"timestamp"`
	CreatedAt time.Time `gorm:"column:created_at;autoCreateTime"  json:"created_at,omitempty"`
}
