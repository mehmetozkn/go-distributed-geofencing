package model

// Geofence represents a named geographic zone stored as a PostGIS polygon.
// The geometry column is managed exclusively via raw SQL (ST_Contains / ST_GeomFromText)
// and is therefore excluded from GORM's automatic column mapping.
type Geofence struct {
	ID   uint   `gorm:"primaryKey;autoIncrement" json:"id"`
	Name string `gorm:"column:name;not null"     json:"name"`
}
