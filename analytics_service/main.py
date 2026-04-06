"""
analytics_service/main.py

EagleVision Analytics Service.
Consumes the full assessment-format Kafka payload and persists it to
TimescaleDB.  Self-heals the schema on startup if columns are mismatched.
"""

import json
import logging
import os
import signal
import sys
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER:   str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC:    str = os.getenv("KAFKA_TOPIC", "equipment-events")
KAFKA_GROUP_ID: str = os.getenv("KAFKA_GROUP_ID", "analytics-group")

DATABASE_URL: str = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'ev_user')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'ev_pass')}"
    f"@{os.getenv('POSTGRES_HOST', 'timescaledb')}:5432/"
    f"{os.getenv('POSTGRES_DB', 'eaglevision')}"
)

DB_CONNECT_ATTEMPTS:      int = 15
DB_CONNECT_DELAY:         int = 2
KAFKA_TOPIC_WAIT_ATTEMPTS: int = 60
KAFKA_TOPIC_WAIT_DELAY:   int = 5

# ---------------------------------------------------------------------------
# State constants
# ---------------------------------------------------------------------------
STATE_ACTIVE:   str = "ACTIVE"
STATE_INACTIVE: str = "INACTIVE"

# ---------------------------------------------------------------------------
# SQL — schema matches the full assessment payload
# ---------------------------------------------------------------------------
INIT_SQL: str = """
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS equipment_logs (
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
"""

INSERT_SQL: str = """
INSERT INTO equipment_logs (
    frame_id, equipment_id, equipment_class,
    current_state, current_activity, motion_source,
    total_tracked_seconds, total_active_seconds, total_idle_seconds,
    util_percent
) VALUES (
    %(frame_id)s, %(equipment_id)s, %(equipment_class)s,
    %(current_state)s, %(current_activity)s, %(motion_source)s,
    %(total_tracked_seconds)s, %(total_active_seconds)s, %(total_idle_seconds)s,
    %(util_percent)s
);
"""

# Expected columns in the DB — used for schema self-heal
EXPECTED_COLUMNS: frozenset = frozenset({
    "time", "frame_id", "equipment_id", "equipment_class",
    "current_state", "current_activity", "motion_source",
    "total_tracked_seconds", "total_active_seconds", "total_idle_seconds",
    "util_percent",
})

logging.basicConfig(
    format="%(asctime)s [analytics_service] %(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def connect_db() -> psycopg2.extensions.connection:
    """Connect to TimescaleDB with retries. Exits on failure."""
    for i in range(DB_CONNECT_ATTEMPTS):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            logger.info("Connected to TimescaleDB.")
            return conn
        except psycopg2.OperationalError as exc:
            logger.warning("DB connect attempt %d/%d: %s", i + 1, DB_CONNECT_ATTEMPTS, exc)
            time.sleep(DB_CONNECT_DELAY)
    logger.error("Could not connect to TimescaleDB.")
    sys.exit(1)


def get_actual_columns(conn: psycopg2.extensions.connection) -> frozenset:
    """Return current column names of equipment_logs table."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'equipment_logs'"
            )
            return frozenset(row[0] for row in cur.fetchall())
    except psycopg2.DatabaseError:
        return frozenset()


def init_db(conn: psycopg2.extensions.connection) -> None:
    """
    Initialise schema.  Drops and recreates the table if columns mismatch
    (handles upgrades from previous schema versions automatically).
    """
    actual = get_actual_columns(conn)
    if actual and not EXPECTED_COLUMNS.issubset(actual):
        logger.warning(
            "Schema mismatch detected. Recreating equipment_logs table. "
            "Missing: %s", EXPECTED_COLUMNS - actual
        )
        try:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS equipment_logs CASCADE;")
            logger.info("Old table dropped.")
        except psycopg2.DatabaseError as exc:
            logger.error("Failed to drop old table: %s", exc)
            sys.exit(1)

    try:
        with conn.cursor() as cur:
            cur.execute(INIT_SQL)
        logger.info("Database schema ready.")
    except psycopg2.DatabaseError as exc:
        logger.error("Schema init failed: %s", exc)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Payload helpers
# ---------------------------------------------------------------------------

def flatten_payload(data: dict) -> dict | None:
    """
    Convert the nested assessment-format Kafka payload into a flat dict
    suitable for the INSERT_SQL parameters.

    Returns None if the payload is malformed.
    """
    try:
        util_block = data["utilization"]
        time_block = data["time_analytics"]
        return {
            "frame_id":              data["frame_id"],
            "equipment_id":          data["equipment_id"],
            "equipment_class":       data["equipment_class"],
            "current_state":         util_block["current_state"],
            "current_activity":      util_block["current_activity"],
            "motion_source":         util_block["motion_source"],
            "total_tracked_seconds": time_block["total_tracked_seconds"],
            "total_active_seconds":  time_block["total_active_seconds"],
            "total_idle_seconds":    time_block["total_idle_seconds"],
            "util_percent":          time_block["utilization_percent"],
        }
    except (KeyError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def build_consumer() -> Consumer:
    """Create consumer and wait until the topic exists before subscribing."""
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id":          KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)

    logger.info("Waiting for Kafka topic '%s'...", KAFKA_TOPIC)
    for attempt in range(KAFKA_TOPIC_WAIT_ATTEMPTS):
        try:
            meta = consumer.list_topics(timeout=5)
            if KAFKA_TOPIC in meta.topics:
                logger.info("Topic '%s' found — subscribing.", KAFKA_TOPIC)
                consumer.subscribe([KAFKA_TOPIC])
                return consumer
            logger.warning(
                "Topic not ready (attempt %d/%d), retrying in %ds...",
                attempt + 1, KAFKA_TOPIC_WAIT_ATTEMPTS, KAFKA_TOPIC_WAIT_DELAY,
            )
        except KafkaException as exc:
            logger.error("Error listing topics: %s", exc)
        time.sleep(KAFKA_TOPIC_WAIT_DELAY)

    logger.error("Topic '%s' never appeared. Exiting.", KAFKA_TOPIC)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run() -> None:
    """Initialise connections and start the Kafka consumer loop."""
    logger.info("Starting Analytics Service...")

    db_conn  = connect_db()
    init_db(db_conn)
    consumer = build_consumer()

    running = True

    def _stop(sig: int, frame: object) -> None:
        nonlocal running
        running = False
        logger.info("Shutdown signal received.")

    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka error: %s", msg.error())
                continue

            try:
                raw  = json.loads(msg.value().decode("utf-8"))
                flat = flatten_payload(raw)
                if flat:
                    with db_conn.cursor() as cur:
                        cur.execute(INSERT_SQL, flat)
                    logger.info(
                        "Consumed message: %s  activity=%s  util=%.1f%%",
                        flat["equipment_id"],
                        flat["current_activity"],
                        flat["util_percent"],
                    )
                else:
                    logger.warning("Malformed payload skipped: %s", raw)
            except (json.JSONDecodeError, psycopg2.DatabaseError, KeyError) as exc:
                logger.error("Processing error: %s", exc)
    finally:
        consumer.close()
        db_conn.close()
        logger.info("Analytics service stopped.")


if __name__ == "__main__":
    run()