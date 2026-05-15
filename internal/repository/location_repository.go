package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/model"
)

// LocationRepository implements domain.ILocationRepository using PostGIS.
type LocationRepository struct {
	db *sql.DB
}

// NewLocationRepository returns a new PostGIS-backed location repository.
func NewLocationRepository(db *sql.DB) *LocationRepository {
	return &LocationRepository{db: db}
}

// SaveBatch inserts a batch of locations with PostGIS geometry in a single statement.
func (r *LocationRepository) SaveBatch(ctx context.Context, locations []model.Location) error {
	if len(locations) == 0 {
		return nil
	}

	var (
		builder strings.Builder
		args    []interface{}
	)

	builder.WriteString(`
		INSERT INTO locations (device_id, geom, timestamp, created_at)
		VALUES `)

	for i, loc := range locations {
		if i > 0 {
			builder.WriteString(", ")
		}
		offset := i * 4
		builder.WriteString(fmt.Sprintf(
			"($%d, ST_SetSRID(ST_MakePoint($%d, $%d), 4326), $%d, NOW())",
			offset+1, offset+2, offset+3, offset+4,
		))
		args = append(args, loc.DeviceID, loc.Longitude, loc.Latitude, loc.Timestamp)
	}

	_, err := r.db.ExecContext(ctx, builder.String(), args...)
	return err
}

// FindByDeviceID returns the most recent locations for a device.
func (r *LocationRepository) FindByDeviceID(ctx context.Context, deviceID string, limit int) ([]model.Location, error) {
	query := `
		SELECT id, device_id, ST_Y(geom) AS latitude, ST_X(geom) AS longitude, timestamp, created_at
		FROM locations
		WHERE device_id = $1
		ORDER BY timestamp DESC
		LIMIT $2`

	rows, err := r.db.QueryContext(ctx, query, deviceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locations []model.Location
	for rows.Next() {
		var loc model.Location
		if err := rows.Scan(&loc.ID, &loc.DeviceID, &loc.Latitude, &loc.Longitude, &loc.Timestamp, &loc.CreatedAt); err != nil {
			return nil, err
		}
		locations = append(locations, loc)
	}
	return locations, rows.Err()
}
