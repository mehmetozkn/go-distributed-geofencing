package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/model"
)

// GeofenceRepository implements domain.IGeofenceRepository using PostGIS.
type GeofenceRepository struct {
	db *sql.DB
}

// NewGeofenceRepository returns a new PostGIS-backed geofence repository.
func NewGeofenceRepository(db *sql.DB) *GeofenceRepository {
	return &GeofenceRepository{db: db}
}

// Create inserts a new geofence with its polygon geometry.
func (r *GeofenceRepository) Create(ctx context.Context, geofence *model.Geofence) error {
	wkt := polygonToWKT(geofence.Polygon)

	query := `
		INSERT INTO geofences (name, geom, created_at, updated_at)
		VALUES ($1, ST_SetSRID(ST_GeomFromText($2), 4326), NOW(), NOW())
		RETURNING id`

	return r.db.QueryRowContext(ctx, query, geofence.Name, wkt).Scan(&geofence.ID)
}

// FindAll returns every geofence in the database.
func (r *GeofenceRepository) FindAll(ctx context.Context) ([]model.Geofence, error) {
	query := `
		SELECT id, name, created_at, updated_at
		FROM geofences
		ORDER BY id`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fences []model.Geofence
	for rows.Next() {
		var f model.Geofence
		if err := rows.Scan(&f.ID, &f.Name, &f.CreatedAt, &f.UpdatedAt); err != nil {
			return nil, err
		}
		fences = append(fences, f)
	}
	return fences, rows.Err()
}

// FindContaining returns all geofences whose polygon contains the given point.
func (r *GeofenceRepository) FindContaining(ctx context.Context, lat, lng float64) ([]model.Geofence, error) {
	query := `
		SELECT id, name, created_at, updated_at
		FROM geofences
		WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint($1, $2), 4326))`

	rows, err := r.db.QueryContext(ctx, query, lng, lat)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fences []model.Geofence
	for rows.Next() {
		var f model.Geofence
		if err := rows.Scan(&f.ID, &f.Name, &f.CreatedAt, &f.UpdatedAt); err != nil {
			return nil, err
		}
		fences = append(fences, f)
	}
	return fences, rows.Err()
}

// polygonToWKT converts a slice of Points to a WKT POLYGON string.
// NOTE: the first and last point must be identical to close the ring;
// this function auto-closes the ring if needed.
func polygonToWKT(points []model.Point) string {
	if len(points) == 0 {
		return "POLYGON EMPTY"
	}

	// auto-close ring
	first := points[0]
	last := points[len(points)-1]
	if first.Longitude != last.Longitude || first.Latitude != last.Latitude {
		points = append(points, first)
	}

	wkt := "POLYGON(("
	for i, p := range points {
		if i > 0 {
			wkt += ", "
		}
		wkt += fmt.Sprintf("%f %f", p.Longitude, p.Latitude)
	}
	wkt += "))"
	return wkt
}
