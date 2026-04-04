"""
Streamlit dashboard for EagleVision.
Displays live video feed and equipment utilization statistics.
"""
import cv2
import os
import sys
import time
import logging
from typing import Dict, List, Optional
import io
import numpy as np
from dotenv import load_dotenv
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import redis
from redis.exceptions import ConnectionError, RedisError
import psycopg2
from psycopg2 import sql, OperationalError
from psycopg2.extras import DictCursor

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if os.getenv('DEBUG', '0') == '1' else logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

cached_frame_bytes: Optional[bytes] = None
cached_frame_ts: Optional[str] = None

# Application-level backend components
db_manager: Optional['DatabaseManager'] = None
redis_subscriber: Optional['RedisFrameSubscriber'] = None

class DatabaseManager:
    """Database manager for TimescaleDB operations."""
    
    def __init__(self):
        """Initialize database manager."""
        self.host = os.getenv('POSTGRES_HOST', 'timescaledb')
        self.port = int(os.getenv('POSTGRES_PORT', '5432'))
        self.database = os.getenv('POSTGRES_DB', 'eaglevision')
        self.user = os.getenv('POSTGRES_USER', 'ev_user')
        self.password = os.getenv('POSTGRES_PASSWORD', 'ev_pass')
        self.connection = None
    
    def connect(self) -> bool:
        """
        Connect to database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=DictCursor
            )
            return True
        except OperationalError as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def get_latest_stats(self) -> List[Dict]:
        """
        Get latest statistics for all equipment.
        
        Returns:
            List of dictionaries with latest stats per equipment
        """
        if not self.connection:
            return []
        
        try:
            query = sql.SQL("""
                SELECT DISTINCT ON (equipment_id)
                    equipment_id, equipment_class, current_state, current_activity,
                    motion_source, util_percent, active_seconds, idle_seconds, time
                FROM equipment_events
                ORDER BY equipment_id, time DESC;
            """)
            
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            
            return [dict(row) for row in results]
        
        except Exception as e:
            logger.error(f"Failed to get latest stats: {e}")
            return []
    
    def close(self) -> None:
        """Close database connection."""
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
            finally:
                self.connection = None

class RedisFrameSubscriber:
    """Redis subscriber for video frames."""
    
    def __init__(self):
        """Initialize Redis subscriber."""
        self.redis_host = os.getenv('REDIS_HOST', 'redis')
        self.redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.channel = os.getenv('REDIS_FRAME_CHANNEL', 'frames')
        self.redis_client = None
        self.pubsub = None
    
    def connect(self) -> bool:
        """
        Connect to Redis.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=False,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            # Test connection
            self.redis_client.ping()
            
            # Set up pubsub
            self.pubsub = self.redis_client.pubsub()
            self.pubsub.subscribe(self.channel)
            
            return True
        
        except (ConnectionError, RedisError) as e:
            logger.error(f"Redis connection failed: {e}")
            return False
    
    def get_latest_frame(self) -> Optional[bytes]:
        """
        Get the latest frame from Redis.
        
        Returns:
            Frame bytes or None if no frame available
        """
        if not self.redis_client:
            return None
        
        try:
            frame_data = self.redis_client.get('latest_frame')
            
            if frame_data is not None:
                return frame_data
            else:
                return None
        
        except Exception as e:
            logger.error(f"Error getting frame from Redis: {e}")
            return None

    def get_latest_frame_timestamp(self) -> Optional[str]:
        """
        Get the timestamp for the latest frame.
        
        Returns:
            UNIX timestamp string or None if not available
        """
        if not self.redis_client:
            return None
        
        try:
            ts_data = self.redis_client.get('latest_frame_ts')
            if ts_data is not None:
                return ts_data.decode('utf-8') if isinstance(ts_data, bytes) else str(ts_data)
            return None
        except Exception as e:
            logger.error(f"Error getting frame timestamp from Redis: {e}")
            return None
    
    def close(self) -> None:
        """Close Redis connection."""
        if self.pubsub:
            try:
                self.pubsub.close()
            except Exception as e:
                logger.error(f"Error closing Redis pubsub: {e}")
        
        if self.redis_client:
            try:
                self.redis_client.close()
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")

def render_equipment_card(equipment_data: Dict) -> None:
    """
    Render equipment status card.
    
    Args:
        equipment_data: Equipment statistics dictionary
    """
    equipment_id = equipment_data.get('equipment_id', 'Unknown')
    equipment_class = equipment_data.get('equipment_class', 'Unknown')
    current_state = equipment_data.get('current_state', 'INACTIVE')
    current_activity = equipment_data.get('current_activity', 'WAITING')
    motion_source = equipment_data.get('motion_source', 'none')
    util_percent = equipment_data.get('util_percent', 0.0)
    active_seconds = equipment_data.get('active_seconds', 0.0)
    idle_seconds = equipment_data.get('idle_seconds', 0.0)
    
    # Equipment header
    st.markdown(f"### {equipment_id} — {equipment_class}")
    
    # State badge
    if current_state == "ACTIVE":
        st.markdown("🟢 **ACTIVE**")
    else:
        st.markdown("🔴 **INACTIVE**")
    
    # Metrics
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Utilization", f"{util_percent:.1f}%")
        st.metric("Active time", f"{active_seconds:.0f}s")
    with col2:
        st.metric("Idle time", f"{idle_seconds:.0f}s")
    
    # Activity and motion info
    st.caption(f"Activity: {current_activity} | Motion: {motion_source}")
    
    st.divider()

def main():
    """Main Streamlit application."""
    # Page configuration
    st.set_page_config(
        page_title="EagleVision",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Title
    st.title("🦅 EagleVision")
    st.markdown("*Construction Equipment Utilization Monitoring*")
    
    # Initialize session state
    global db_manager, redis_subscriber

    if db_manager is None:
        db_manager = DatabaseManager()

    if redis_subscriber is None:
        redis_subscriber = RedisFrameSubscriber()

    if 'connections_initialized' not in st.session_state:
        st.session_state.connections_initialized = False
        st.session_state.db_connected = False
        st.session_state.redis_connected = False

    # Initialize database and Redis connections only once
    if not st.session_state.connections_initialized:
        db_ok, redis_ok = initialize_connections()
        st.session_state.db_connected = db_ok
        st.session_state.redis_connected = redis_ok
        st.session_state.connections_initialized = True
    
    # Auto-refresh every second
    st_autorefresh(interval=1000, key="refresh")
    
    # Main layout: 2 columns
    col_video, col_stats = st.columns([2, 1])
    last_updated = None
    
    with col_video:
        st.markdown("### 📹 Live Video Feed")
        
        # Try to get latest frame
        global cached_frame_bytes, cached_frame_ts
        frame_bytes = redis_subscriber.get_latest_frame() if redis_subscriber else None
        frame_ts = redis_subscriber.get_latest_frame_timestamp() if redis_subscriber else None

        if frame_bytes is not None:
            cached_frame_bytes = frame_bytes
            cached_frame_ts = frame_ts
        else:
            frame_bytes = cached_frame_bytes
            frame_ts = cached_frame_ts

        image_placeholder = st.empty()
        status_placeholder = st.empty()

        if frame_bytes:
            try:
                # Decode image from bytes
                nparr = np.frombuffer(frame_bytes, np.uint8)
                decoded_frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

                if decoded_frame is None:
                    raise ValueError("Failed to decode frame data")

                decoded_frame = cv2.cvtColor(decoded_frame, cv2.COLOR_BGR2RGB)
                image_placeholder.image(decoded_frame, channels="RGB", use_column_width=True)

                last_updated = frame_ts if frame_ts else time.strftime('%Y-%m-%d %H:%M:%S')
                status_placeholder.markdown(f"**Last frame:** {last_updated}")

            except Exception as e:
                image_placeholder.error(f"Error displaying frame: {e}")
                status_placeholder.markdown("#### Corrupt frame data")
                last_updated = None
        else:
            image_placeholder.info("Waiting for video feed...")
            status_placeholder.markdown("#### No video feed available")
            last_updated = None

    # Store last_updated for sidebar (avoid modifying session state during render)
    current_last_updated = last_updated
    
    with col_stats:
        st.markdown("### 📊 Equipment Status")
        
        # Get latest stats from database
        stats = db_manager.get_latest_stats() if db_manager else []
        stats_placeholder = st.empty()
        
        if not stats:
            stats_placeholder.info("Waiting for first detections...")
        else:
            # Sort by equipment_id for consistent ordering
            stats.sort(key=lambda x: x.get('equipment_id', ''))
            
            with stats_placeholder.container():
                for equipment_data in stats:
                    render_equipment_card(equipment_data)
    
    # Sidebar with system info
    with st.sidebar:
        st.markdown("### 📋 System Information")
        
        # Connection status
        db_connected = st.session_state.db_manager.connection is not None
        redis_connected = st.session_state.redis_subscriber.redis_client is not None
        
        st.markdown("**Connections:**")
        if db_connected:
            st.markdown("✅ Database")
        else:
            st.markdown("❌ Database")
        
        if redis_connected:
            st.markdown("✅ Redis")
        else:
            st.markdown("❌ Redis")
        
        st.markdown("---")
        
        # Statistics
        if stats:
            total_equipment = len(stats)
            active_equipment = sum(1 for s in stats if s.get('current_state') == 'ACTIVE')
            avg_utilization = sum(s.get('util_percent', 0) for s in stats) / total_equipment
            
            st.markdown("**Summary:**")
            st.metric("Total Equipment", total_equipment)
            st.metric("Active Equipment", active_equipment)
            st.metric("Avg Utilization", f"{avg_utilization:.1f}%")
        
        st.markdown("---")
        st.markdown("**Last Updated:**")

        if current_last_updated:
            # Use backend timestamp if available (unix seconds) or human readable
            try:
                epoch = float(current_last_updated)
                human = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch))
                st.markdown(f"{human}")
            except Exception:
                st.markdown(f"{current_last_updated}")
        else:
            st.markdown("No frame yet")

def initialize_connections() -> tuple[bool, bool]:
    """Initialize database and Redis connections."""
    db_ok = False
    redis_ok = False

    # Initialize database only if not already connected
    if not hasattr(st.session_state.db_manager, 'connection') or st.session_state.db_manager.connection is None:
        db_ok = st.session_state.db_manager.connect()
    else:
        db_ok = True
    
    # Initialize Redis only if not already connected
    if not hasattr(st.session_state.redis_subscriber, 'redis_client') or st.session_state.redis_subscriber.redis_client is None:
        redis_ok = st.session_state.redis_subscriber.connect()
    else:
        redis_ok = True

    return db_ok, redis_ok

# Initialize connections on first run
if __name__ == "__main__":
    main()
