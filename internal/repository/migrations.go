package repository

const MigrateSQL = `
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS locations (
    id         BIGSERIAL PRIMARY KEY,
    device_id  VARCHAR(128) NOT NULL,
    geom       GEOMETRY(Point, 4326) NOT NULL,
    timestamp  TIMESTAMPTZ  NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_locations_device_id ON locations (device_id);
CREATE INDEX IF NOT EXISTS idx_locations_geom      ON locations USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_locations_timestamp  ON locations (timestamp DESC);

CREATE TABLE IF NOT EXISTS geofences (
    id         BIGSERIAL PRIMARY KEY,
    name       VARCHAR(256) NOT NULL,
    geom       GEOMETRY(Polygon, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_geofences_geom ON geofences USING GIST (geom);
`
