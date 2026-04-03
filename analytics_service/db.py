"""
Database interface for TimescaleDB operations.
Handles connection management and equipment event storage/retrieval.
"""

import os
import logging
import time
from typing import List, Dict, Optional
import psycopg2
from psycopg2 import sql, OperationalError
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class TimescaleDB:
    """TimescaleDB interface for equipment events."""
    
    def __init__(self):
        """Initialize database connection."""
        self.logger = logging.getLogger(__name__)
        
        # Database configuration
        self.host = os.getenv('POSTGRES_HOST', 'timescaledb')
        self.port = int(os.getenv('POSTGRES_PORT', '5432'))
        self.database = os.getenv('POSTGRES_DB', 'eaglevision')
        self.user = os.getenv('POSTGRES_USER', 'ev_user')
        self.password = os.getenv('POSTGRES_PASSWORD', 'ev_pass')
        
        self.connection = None
        
    def connect_with_retry(self, max_attempts: int = 30, retry_delay: float = 2.0) -> bool:
        """
        Connect to database with retry logic.
        
        Args:
            max_attempts: Maximum connection attempts
            retry_delay: Delay between attempts in seconds
            
        Returns:
            True if connection successful, False otherwise
        """
        for attempt in range(1, max_attempts + 1):
            try:
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    cursor_factory=DictCursor
                )
                
                # Test connection
                with self.connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                
                self.logger.info(f"Connected to TimescaleDB at {self.host}:{self.port}")
                return True
                
            except OperationalError as e:
                self.logger.warning(f"DB connection attempt {attempt}/{max_attempts} failed: {e}")
                if attempt < max_attempts:
                    time.sleep(retry_delay)
                else:
                    self.logger.error("Failed to connect to database after all retries")
                    return False
        
        return False
    
    def init_db(self) -> bool:
        """
        Initialize database (verify TimescaleDB extension and connection).
        
        Returns:
            True if initialization successful, False otherwise
        """
        if not self.connection:
            self.logger.error("No database connection available")
            return False
        
        try:
            with self.connection.cursor() as cursor:
                # Verify TimescaleDB extension
                cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
                self.connection.commit()
                
                self.logger.info("Database initialized successfully")
                return True
                
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
            return False
    
    def insert_event(self, payload: Dict) -> bool:
        """
        Insert equipment event into database.
        
        Args:
            payload: Event payload dictionary
            
        Returns:
            True if insert successful, False otherwise
        """
        if not self.connection:
            self.logger.error("No database connection available")
            return False
        
        try:
            # Extract payload data
            utilization = payload.get('utilization', {})
            time_analytics = payload.get('time_analytics', {})
            
            # Build insert query with parameterized values
            query = sql.SQL("""
                INSERT INTO equipment_events (
                    time, frame_id, equipment_id, equipment_class,
                    current_state, current_activity, motion_source,
                    util_percent, active_seconds, idle_seconds
                ) VALUES (
                    NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """)
            
            values = (
                payload.get('frame_id'),
                payload.get('equipment_id'),
                payload.get('equipment_class'),
                utilization.get('current_state'),
                utilization.get('current_activity'),
                utilization.get('motion_source'),
                time_analytics.get('utilization_percent'),
                time_analytics.get('total_active_seconds'),
                time_analytics.get('total_idle_seconds')
            )
            
            with self.connection.cursor() as cursor:
                cursor.execute(query, values)
                self.connection.commit()
            
            self.logger.debug(f"Inserted event for {payload.get('equipment_id')}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to insert event: {e}")
            # Don't rollback on error to avoid affecting other transactions
            return False
    
    def get_latest_stats(self) -> List[Dict]:
        """
        Get latest statistics for all equipment.
        
        Returns:
            List of dictionaries with latest stats per equipment
        """
        if not self.connection:
            self.logger.error("No database connection available")
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
            
            # Convert to list of dictionaries
            stats = [dict(row) for row in results]
            
            self.logger.debug(f"Retrieved stats for {len(stats)} equipment")
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get latest stats: {e}")
            return []
    
    def close(self) -> None:
        """Close database connection."""
        if self.connection:
            try:
                self.connection.close()
                self.logger.info("Database connection closed")
            except Exception as e:
                self.logger.error(f"Error closing database connection: {e}")
            finally:
                self.connection = None
