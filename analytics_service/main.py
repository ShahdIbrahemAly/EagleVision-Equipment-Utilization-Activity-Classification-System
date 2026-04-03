"""
Main analytics service for EagleVision.
Consumes equipment events from Kafka and stores them in TimescaleDB.
"""

import os
import sys
import signal
import logging
import time
import threading
from dotenv import load_dotenv

# Import local modules
from db import TimescaleDB
from consumer import EquipmentEventConsumer

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if os.getenv('DEBUG', '0') == '1' else logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

class AnalyticsService:
    """Main analytics service."""
    
    def __init__(self):
        """Initialize analytics service."""
        self.logger = logging.getLogger(__name__)
        
        # Kafka configuration
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'equipment-events')
        self.kafka_group_id = os.getenv('KAFKA_GROUP_ID', 'analytics-group')
        
        # Initialize components
        self.db = None
        self.consumer = None
        self.shutdown_event = threading.Event()
        
    def initialize(self) -> bool:
        """
        Initialize all components.
        
        Returns:
            True if initialization successful, False otherwise
        """
        try:
            # Initialize database connection
            self.db = TimescaleDB()
            
            if not self.db.connect_with_retry():
                self.logger.error("Failed to connect to database")
                return False
            
            if not self.db.init_db():
                self.logger.error("Failed to initialize database")
                return False
            
            # Initialize Kafka consumer
            self.consumer = EquipmentEventConsumer(
                self.kafka_bootstrap_servers,
                self.kafka_topic,
                self.kafka_group_id
            )
            
            if not self.consumer.connect_with_retry():
                self.logger.error("Failed to connect to Kafka")
                return False
            
            self.logger.info("Analytics service initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Initialization failed: {e}")
            return False
    
    def run(self) -> None:
        """Main service loop."""
        if not self.initialize():
            self.logger.error("Failed to initialize analytics service")
            sys.exit(1)
        
        self.logger.info("Starting analytics service...")
        
        try:
            # Run consumer in main thread
            self.consumer.consume_messages(self.db, self.shutdown_event)
            
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        except Exception as e:
            self.logger.error(f"Error in service loop: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self) -> None:
        """Clean up resources."""
        self.logger.info("Cleaning up analytics service...")
        
        # Signal shutdown
        self.shutdown_event.set()
        
        if self.consumer:
            self.consumer.close()
        
        if self.db:
            self.db.close()
        
        self.logger.info("Analytics service cleanup complete")

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info("Received shutdown signal")
    sys.exit(0)

def main():
    """Main entry point."""
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run service
    service = AnalyticsService()
    service.run()

if __name__ == "__main__":
    main()
