import logging
import time
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection
from dotenv import load_dotenv
import os

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.host = os.getenv("POSTGRES_HOST")
        self.port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = os.getenv("POSTGRES_DB")
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")
        self.connection: Optional[connection] = None

    def connect_with_retry(self, max_attempts: int = 30, retry_delay: int = 2) -> bool:
        for attempt in range(1, max_attempts + 1):
            try:
                self.connection = psycopg2.connect(
                    host=self.host, port=self.port, database=self.database,
                    user=self.user, password=self.password
                )
                return True
            except Exception as e:
                time.sleep(retry_delay)
        return False

    def init_database(self) -> bool:
        try:
            if not self.connection: return False
            with self.connection.cursor() as cursor:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'equipment_logs'
                    );
                """)
                if not cursor.fetchone()[0]:
                    self.logger.error("equipment_logs table not found")
                    return False
            self.connection.commit()
            return True
        except Exception as e:
            self.logger.error(f"DB Init failed: {e}")
            return False

    def insert_event(self, payload: Dict[str, Any]) -> bool:
        """
        Insert flat equipment event into equipment_logs.
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO equipment_logs (
                        frame_id, equipment_id, equipment_class,
                        current_state, current_activity, motion_source, util_percent
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    payload["frame_id"], payload["equipment_id"], payload["equipment_class"],
                    payload["current_state"], payload["current_activity"], 
                    payload["motion_source"], payload["util_percent"]
                ))
            self.connection.commit()
            print(f"[DB SAVED] id={payload['equipment_id']} util={payload['util_percent']:.1f}%")
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert event: {e}")
            self.connection.rollback()
            return False

    def get_latest_stats(self) -> List[Dict[str, Any]]:
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT DISTINCT ON (equipment_id) *
                    FROM equipment_logs
                    ORDER BY equipment_id, time DESC;
                """)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            return []

    def close(self) -> None:
        if self.connection: self.connection.close()