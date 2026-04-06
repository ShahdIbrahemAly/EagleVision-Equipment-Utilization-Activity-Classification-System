"""
dashboard/app.py

EagleVision Streamlit Dashboard.
Left panel: live video feed from Redis.
Right panel: per-equipment cards showing state, current activity,
             working time, idle time, and utilization — as required by
             the assessment spec.
"""

import io
import logging
import os
import time

import psycopg2
import redis
import streamlit as st
from dotenv import load_dotenv
from PIL import Image
from psycopg2.extras import RealDictCursor

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REDIS_HOST:      str = os.getenv("REDIS_HOST",         "redis")
REDIS_PORT:      int = int(os.getenv("REDIS_PORT",     "6379"))
REDIS_CHANNEL:   str = os.getenv("REDIS_FRAME_CHANNEL","frames")
REDIS_FRAME_KEY: str = "latest_frame"

POSTGRES_HOST: str = os.getenv("POSTGRES_HOST",     "timescaledb")
POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB:   str = os.getenv("POSTGRES_DB",       "eaglevision")
POSTGRES_USER: str = os.getenv("POSTGRES_USER",     "ev_user")
POSTGRES_PASS: str = os.getenv("POSTGRES_PASSWORD", "ev_pass")

DATABASE_URL: str = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASS}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

STATE_ACTIVE:   str = "ACTIVE"
STATE_INACTIVE: str = "INACTIVE"

REFRESH_INTERVAL: float = 1.0

# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------
LATEST_QUERY: str = """
SELECT DISTINCT ON (equipment_id)
    equipment_id,
    equipment_class,
    current_state,
    current_activity,
    motion_source,
    total_active_seconds,
    total_idle_seconds,
    total_tracked_seconds,
    util_percent,
    time
FROM equipment_logs
ORDER BY equipment_id, time DESC;
"""

logging.basicConfig(
    format="%(asctime)s [dashboard] %(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cached connections
# ---------------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def get_db_conn():
    """Return a cached psycopg2 connection, retrying up to 10 times."""
    for attempt in range(1, 11):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            logger.info("Dashboard connected to TimescaleDB.")
            return conn
        except psycopg2.OperationalError as exc:
            logger.warning("DB connect attempt %d/10: %s", attempt, exc)
            time.sleep(2)
    st.error("Cannot connect to TimescaleDB.")
    st.stop()


@st.cache_resource(show_spinner=False)
def get_redis():
    """Return a cached Redis client, or None if unavailable."""
    try:
        r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT,
            decode_responses=False,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        r.ping()
        logger.info("Dashboard connected to Redis.")
        return r
    except redis.RedisError as exc:
        logger.warning("Redis unavailable: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def fetch_latest_equipment(conn) -> list:
    """Query the most recent row per equipment_id."""
    try:
        if conn.closed:
            get_db_conn.clear()
            conn = get_db_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(LATEST_QUERY)
            return [dict(row) for row in cur.fetchall()]
    except psycopg2.DatabaseError as exc:
        logger.error("Query error: %s", exc)
        try:
            get_db_conn.clear()
        except Exception:
            pass
        return []


def get_latest_frame(r):
    """Read latest JPEG frame from Redis key; fall back to pub/sub."""
    if r is None:
        return None
    try:
        data = r.get(REDIS_FRAME_KEY)
        if data:
            return Image.open(io.BytesIO(data))
        pubsub = r.pubsub()
        pubsub.subscribe(REDIS_CHANNEL)
        frame_bytes = None
        for _ in range(15):
            msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.03)
            if msg and msg["type"] == "message":
                frame_bytes = msg["data"]
        pubsub.unsubscribe()
        pubsub.close()
        if frame_bytes:
            return Image.open(io.BytesIO(frame_bytes))
    except redis.RedisError as exc:
        logger.warning("Frame fetch error: %s", exc)
    return None


def fmt_seconds(seconds: float) -> str:
    """Format seconds as HH:MM:SS string."""
    s   = int(seconds)
    h   = s // 3600
    m   = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


# ---------------------------------------------------------------------------
# UI rendering
# ---------------------------------------------------------------------------

def render_video(r) -> None:
    """Render the live video feed panel."""
    st.subheader("📹 Live Analysis Feed")
    frame = get_latest_frame(r)
    if frame:
        st.image(frame, use_column_width=True)
    else:
        st.info("Searching for live stream…")


def render_equipment(conn) -> None:
    """Render equipment status cards with full time-analytics."""
    st.subheader("🏗️ Equipment Status")
    rows = fetch_latest_equipment(conn)

    if not rows:
        st.info("No data available.")
        return

    total    = len(rows)
    active   = sum(1 for row in rows if row["current_state"] == STATE_ACTIVE)
    avg_util = (
        sum(row["util_percent"] or 0.0 for row in rows) / total if total else 0.0
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Detected Equipment", total)
    c2.metric("Active Units",       active)
    c3.metric("Fleet Utilization",  f"{avg_util:.1f}%")

    st.markdown("---")

    for row in rows:
        eq_id      = row["equipment_id"]
        eq_class   = row["equipment_class"]
        state      = row["current_state"]
        activity   = row.get("current_activity") or "—"
        motion_src = row.get("motion_source")    or "—"
        util       = row.get("util_percent")     or 0.0
        working_s  = row.get("total_active_seconds")  or 0.0
        idle_s     = row.get("total_idle_seconds")     or 0.0
        ts         = row.get("time", "")

        state_color = "green" if state == STATE_ACTIVE else "red"

        st.markdown(f"**{eq_id}** ({eq_class})")
        st.markdown(f"Status: :{state_color}[{state}]")
        st.markdown(f"Activity: `{activity}` &nbsp;·&nbsp; Motion: `{motion_src}`",
                    unsafe_allow_html=True)

        col_w, col_i = st.columns(2)
        col_w.metric("⏱ Working", fmt_seconds(working_s))
        col_i.metric("💤 Idle",   fmt_seconds(idle_s))

        st.progress(int(min(util, 100)))
        st.caption(
            f"Utilization: {util:.1f}% | Last seen: {str(ts)[11:19]}"
        )
        st.divider()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Configure Streamlit page and run the render loop."""
    st.set_page_config(
        page_title="EagleVision",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    st.title("🦅 EagleVision — Real-time Fleet Monitor")
    st.markdown("---")

    db_conn  = get_db_conn()
    r_client = get_redis()

    col_vid, col_stats = st.columns([2, 1])

    with col_vid:
        render_video(r_client)

    with col_stats:
        render_equipment(db_conn)

    time.sleep(REFRESH_INTERVAL)
    st.rerun()


if __name__ == "__main__":
    main()