-- Enable PostGIS extension (idempotent)
CREATE EXTENSION IF NOT EXISTS postgis;

-- Geofence zones table
CREATE TABLE IF NOT EXISTS geofences (
    id       BIGSERIAL                  PRIMARY KEY,
    name     TEXT                       NOT NULL,
    geometry geometry(Polygon, 4326)    NOT NULL
);

-- GIST index for fast spatial lookups under high load
CREATE INDEX IF NOT EXISTS idx_geofences_geometry ON geofences USING GIST (geometry);

-- Sample data: Sultanahmet Meydanı, İstanbul
INSERT INTO geofences (name, geometry)
VALUES (
    'Sultanahmet Meydanı',
    ST_GeomFromText(
        'POLYGON((28.9700 41.0060, 28.9850 41.0060, 28.9850 40.9960, 28.9700 40.9960, 28.9700 41.0060))',
        4326
    )
);
