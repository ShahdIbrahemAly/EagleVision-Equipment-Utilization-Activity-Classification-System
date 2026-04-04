"""
Streamlit dashboard for EagleVision.
"""
import cv2, os, time, logging
from typing import Dict, List, Optional
import numpy as np
from dotenv import load_dotenv
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import redis
from redis.exceptions import ConnectionError, RedisError
import psycopg2
from psycopg2 import sql, OperationalError
from psycopg2.extras import DictCursor

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.host     = os.getenv('POSTGRES_HOST', 'timescaledb')
        self.port     = int(os.getenv('POSTGRES_PORT', '5432'))
        self.database = os.getenv('POSTGRES_DB', 'eaglevision')
        self.user     = os.getenv('POSTGRES_USER', 'ev_user')
        self.password = os.getenv('POSTGRES_PASSWORD', 'ev_pass')
        self.recent_seconds = int(os.getenv('DASHBOARD_RECENT_SECONDS', '10'))
        self.connection = None

    def connect(self) -> bool:
        try:
            self.connection = psycopg2.connect(
                host=self.host, port=self.port, database=self.database,
                user=self.user, password=self.password, cursor_factory=DictCursor)
            return True
        except OperationalError as e:
            logger.error(f"DB connect failed: {e}")
            return False

    def get_latest_stats(self) -> List[Dict]:
        # reconnect if dropped
        if not self.connection or self.connection.closed:
            self.connect()
        if not self.connection:
            return []
        try:
            with self.connection.cursor() as cur:
                if self.recent_seconds > 0:
                    cur.execute("""
                        SELECT DISTINCT ON (equipment_id)
                            equipment_id, equipment_class, current_state,
                            current_activity, motion_source, util_percent,
                            active_seconds, idle_seconds, time
                        FROM equipment_events
                        WHERE time >= NOW() - interval %s
                        ORDER BY equipment_id, time DESC;
                    """, (f'{self.recent_seconds} seconds',))
                else:
                    cur.execute("""
                        SELECT DISTINCT ON (equipment_id)
                            equipment_id, equipment_class, current_state,
                            current_activity, motion_source, util_percent,
                            active_seconds, idle_seconds, time
                        FROM equipment_events
                        ORDER BY equipment_id, time DESC;
                    """)

                stats = [dict(r) for r in cur.fetchall()]

            return stats
        except Exception as e:
            logger.error(f"get_latest_stats failed: {e}")
            try: self.connection.rollback()
            except: pass
            return []

    def close(self):
        if self.connection:
            try: self.connection.close()
            except: pass
            self.connection = None


class RedisClient:
    def __init__(self):
        self.host    = os.getenv('REDIS_HOST', 'redis')
        self.port    = int(os.getenv('REDIS_PORT', '6379'))
        self.client  = None

    def connect(self) -> bool:
        try:
            self.client = redis.Redis(host=self.host, port=self.port,
                                      decode_responses=False,
                                      socket_connect_timeout=5,
                                      socket_timeout=5)
            self.client.ping()
            return True
        except Exception as e:
            logger.error(f"Redis connect failed: {e}")
            return False

    def get_frame(self) -> Optional[bytes]:
        if not self.client:
            if not self.connect():
                return None
        try:
            return self.client.get('latest_frame')
        except Exception:
            self.client = None
            self.connect()
            return None

    def get_ts(self) -> Optional[str]:
        if not self.client:
            if not self.connect():
                return None
        try:
            v = self.client.get('latest_frame_ts')
            return v.decode() if v else None
        except Exception:
            self.client = None
            self.connect()
            return None


# ── session-state helpers ─────────────────────────────────────────────────────

def _init_session_state() -> None:
    if 'db' not in st.session_state:
        st.session_state.db = DatabaseManager()
        st.session_state.db_ok = st.session_state.db.connect()
    if 'redis' not in st.session_state:
        st.session_state.redis = RedisClient()
        st.session_state.redis_ok = st.session_state.redis.connect()
    if 'cached_frame_bytes' not in st.session_state:
        st.session_state.cached_frame_bytes = None
        st.session_state.cached_frame_ts = None


def _get_db() -> DatabaseManager:
    return st.session_state.db


def _get_redis() -> RedisClient:
    return st.session_state.redis


def _get_cached_frame() -> Optional[bytes]:
    return st.session_state.get('cached_frame_bytes')


def _set_cached_frame(data: bytes):
    st.session_state.cached_frame_bytes = data
    st.session_state.cached_frame_ts = time.time()


# ── UI helpers ────────────────────────────────────────────────────────────────
def render_equipment_card(eq: Dict):
    state    = eq.get('current_state', 'INACTIVE')
    sid      = eq.get('equipment_id',  'Unknown')
    cls      = eq.get('equipment_class', 'Unknown')
    activity = eq.get('current_activity', 'WAITING')
    motion   = eq.get('motion_source', 'none')
    util     = eq.get('util_percent', 0.0)
    active_s = eq.get('active_seconds', 0.0)
    idle_s   = eq.get('idle_seconds', 0.0)

    st.markdown(f"### {sid} — {cls}")
    st.markdown("🟢 **ACTIVE**" if state == "ACTIVE" else "🔴 **INACTIVE**")
    c1, c2 = st.columns(2)
    with c1:
        st.metric("Utilization", f"{util:.1f}%")
        st.metric("Active time", f"{active_s:.0f}s")
    with c2:
        st.metric("Idle time", f"{idle_s:.0f}s")
    st.caption(f"Activity: {activity} | Motion: {motion}")
    st.divider()


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    st.set_page_config(page_title="EagleVision", layout="wide",
                       initial_sidebar_state="expanded")
    st.title("🦅 EagleVision")
    st.markdown("*Construction Equipment Utilization Monitoring*")

    _init_session_state()
    db    = _get_db()
    rcli  = _get_redis()

    # Refresh controls
    st.session_state.setdefault('auto_refresh', True)
    st.session_state.setdefault('refresh_interval', 5)
    auto_refresh = st.session_state.auto_refresh
    selected_interval = st.session_state.refresh_interval

    refresh_ms = int(os.getenv('DASHBOARD_REFRESH_MS', '5000'))
    if auto_refresh:
        st_autorefresh(interval=selected_interval * 1000, key="ar")
    st.caption(f"Auto-refresh: {'ON' if auto_refresh else 'OFF'} ({selected_interval}s), equipment in last {db.recent_seconds}s.")

    col_video, col_stats = st.columns([2, 1])

    # ── video feed ────────────────────────────────────────────────────────────
    with col_video:
        st.markdown("### 📹 Live Video Feed")
        img_ph  = st.empty()
        stat_ph = st.empty()

        frame_bytes = _get_cached_frame()    # always use cache → no blank flash
        new_bytes = rcli.get_frame()
        decoded = None

        if new_bytes is not None and new_bytes != frame_bytes:
            try:
                arr = np.frombuffer(new_bytes, np.uint8)
                tmp = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                if tmp is None:
                    raise ValueError("decode returned None")
                decoded = cv2.cvtColor(tmp, cv2.COLOR_BGR2RGB)
                _set_cached_frame(new_bytes)
                frame_bytes = new_bytes
            except Exception as e:
                logger.warning(f"Invalid new frame received, keeping cached frame: {e}")

        if decoded is None and frame_bytes:
            try:
                arr = np.frombuffer(frame_bytes, np.uint8)
                tmp = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                if tmp is None:
                    raise ValueError("decode returned None")
                decoded = cv2.cvtColor(tmp, cv2.COLOR_BGR2RGB)
            except Exception as e:
                logger.warning(f"Cached frame decode failed: {e}")
                decoded = None

        if decoded is not None:
            try:
                try:
                    img_ph.image(decoded, channels="RGB", use_container_width=True)
                except TypeError:
                    img_ph.image(decoded, channels="RGB")

                ts = rcli.get_ts()
                try:
                    ts_str = time.strftime('%H:%M:%S', time.localtime(float(ts)))
                except Exception:
                    ts_str = ts or "—"
                stat_ph.markdown(f"**Last frame:** {ts_str}")
            except Exception as e:
                img_ph.error(f"Frame draw failed: {e}")
        else:
            cached_len = len(frame_bytes) if frame_bytes else 0
            conn_status = "connected" if st.session_state.get('redis_ok', False) else "disconnected"
            img_ph.info(f"⏳ Waiting for video feed… (cached={cached_len}, redis={conn_status})")

    # ── equipment status ──────────────────────────────────────────────────────
    with col_stats:
        st.markdown("### 📊 Equipment Status")
        stats = db.get_latest_stats()
        stats.sort(key=lambda x: x.get('equipment_id', ''))
        sp = st.empty()
        if not stats:
            sp.info("⏳ Waiting for first detections…")
        else:
            with sp.container():
                for eq in stats:
                    render_equipment_card(eq)

    # ── sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### 🔄 Refresh Controls")
        auto_refresh = st.toggle("Auto-refresh", value=st.session_state.get('auto_refresh', True), key="toggle_ar")
        selected_interval = st.slider("Refresh interval (s)", 1, 30, st.session_state.get('refresh_interval', 5), key="slider_interval")
        if st.button("🔄 Manual Refresh", key="manual_refresh"):
            st.rerun()
        st.session_state.auto_refresh = auto_refresh
        st.session_state.refresh_interval = selected_interval
        
        st.markdown("---")
        st.markdown("### 📋 System Information")
        db_ok    = st.session_state.get('db_ok', False)
        redis_ok = st.session_state.get('redis_ok', False)
        st.markdown("**Connections:**")
        st.markdown("✅ Database" if db_ok    else "❌ Database")
        st.markdown("✅ Redis"    if redis_ok else "❌ Redis")
        st.markdown("---")

        if stats:
            total  = len(stats)
            active = sum(1 for s in stats if s.get('current_state') == 'ACTIVE')
            avg_u  = sum(s.get('util_percent', 0) for s in stats) / total
            st.markdown("**Summary:**")
            st.metric("Total Equipment",  total)
            st.metric("Active Equipment", active)
            st.metric("Avg Utilization",  f"{avg_u:.1f}%")

        st.markdown("---")
        cached_ts = st.session_state.get('cached_frame_ts')
        if cached_ts:
            st.markdown("**Last Updated:**")
            st.markdown(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cached_ts)))


if __name__ == "__main__":
    main()