CREATE TABLE IF NOT EXISTS locations (
    id          BIGSERIAL        PRIMARY KEY,
    device_id   TEXT             NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    ts          BIGINT           NOT NULL,
    created_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_locations_device_id ON locations (device_id);
CREATE INDEX IF NOT EXISTS idx_locations_ts        ON locations (ts DESC);
