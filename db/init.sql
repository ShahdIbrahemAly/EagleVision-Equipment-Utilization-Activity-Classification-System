CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS equipment_events (
    time                TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    frame_id            INTEGER           NOT NULL,
    equipment_id        TEXT              NOT NULL,
    equipment_class     TEXT              NOT NULL,
    current_state       TEXT              NOT NULL,
    current_activity    TEXT              NOT NULL,
    motion_source       TEXT              NOT NULL,
    util_percent        DOUBLE PRECISION,
    active_seconds      DOUBLE PRECISION,
    idle_seconds        DOUBLE PRECISION
);

SELECT create_hypertable('equipment_events', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_equip_time
    ON equipment_events (equipment_id, time DESC);
