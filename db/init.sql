-- db/init.sql
-- EagleVision TimescaleDB schema
-- Matches the full assessment Kafka payload format.

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Drop old table if it exists with a mismatched schema
DROP TABLE IF EXISTS equipment_logs;

CREATE TABLE equipment_logs (
    time                  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    frame_id              INTEGER          NOT NULL,
    equipment_id          TEXT             NOT NULL,
    equipment_class       TEXT             NOT NULL,
    current_state         TEXT             NOT NULL,
    current_activity      TEXT             NOT NULL,
    motion_source         TEXT             NOT NULL,
    total_tracked_seconds DOUBLE PRECISION,
    total_active_seconds  DOUBLE PRECISION,
    total_idle_seconds    DOUBLE PRECISION,
    util_percent          DOUBLE PRECISION
);

SELECT create_hypertable('equipment_logs', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_logs_equip_time
    ON equipment_logs (equipment_id, time DESC);