"""
Kafka consumer for equipment events.
Consumes messages from CV service and stores them in TimescaleDB.
"""

import os
import json
import logging
import signal
import sys
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka.error import ConsumeError as DeserializationError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if os.getenv('DEBUG', '0') == '1' else logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

class EquipmentEventConsumer:
    """Kafka consumer for equipment utilization events."""
    
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        """
        Initialize Kafka consumer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic to consume from
            group_id: Consumer group ID
        """
        self.logger = logging.getLogger(__name__)
        self.topic = topic
        self.consumer = None
        
        # Consumer configuration
        config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 3000,
            'max.poll.records': 100,
            'fetch.min.bytes': 1,
            'fetch.wait.max.ms': 500
        }
        
        try:
            self.consumer = Consumer(config)
            self.logger.info(f"Kafka consumer initialized for topic: {topic}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise
    
    def connect_with_retry(self, max_attempts: int = 30, retry_delay: float = 2.0) -> bool:
        """
        Connect to Kafka with retry logic.
        
        Args:
            max_attempts: Maximum connection attempts
            retry_delay: Delay between attempts in seconds
            
        Returns:
            True if connection successful, False otherwise
        """
        for attempt in range(1, max_attempts + 1):
            try:
                # Subscribe to topic
                self.consumer.subscribe([self.topic])
                
                # Test connection by polling briefly
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    # No message but connection is working
                    pass
                elif msg.error():
                    raise KafkaException(msg.error())
                
                self.logger.info(f"Connected to Kafka at {self.consumer._bootstrap_servers}")
                return True
                
            except Exception as e:
                self.logger.warning(f"Kafka connection attempt {attempt}/{max_attempts} failed: {e}")
                if attempt < max_attempts:
                    import time
                    time.sleep(retry_delay)
                else:
                    self.logger.error("Failed to connect to Kafka after all retries")
                    return False
        
        return False
    
    def consume_messages(self, db_handler, shutdown_event) -> None:
        """
        Consume messages from Kafka and store in database.
        
        Args:
            db_handler: Database handler instance
            shutdown_event: Event to signal shutdown
        """
        self.logger.info("Starting message consumption loop...")
        
        try:
            while not shutdown_event.is_set():
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        continue
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                try:
                    # Deserialize message
                    payload = json.loads(msg.value().decode('utf-8'))
                    
                    # Validate payload structure
                    if not self._validate_payload(payload):
                        self.logger.warning(f"Invalid payload structure: {payload}")
                        continue
                    
                    # Store in database
                    success = db_handler.insert_event(payload)
                    if success:
                        self.logger.debug(f"Processed event for {payload.get('equipment_id')}")
                    else:
                        self.logger.warning(f"Failed to store event for {payload.get('equipment_id')}")
                
                except DeserializationError as e:
                    self.logger.error(f"Message deserialization failed: {e}")
                    continue
                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON decode error: {e}")
                    continue
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    continue
        
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        except Exception as e:
            self.logger.error(f"Error in consumption loop: {e}")
        finally:
            self.close()
    
    def _validate_payload(self, payload: Dict) -> bool:
        """
        Validate payload structure.
        
        Args:
            payload: Message payload dictionary
            
        Returns:
            True if payload is valid, False otherwise
        """
        required_fields = [
            'frame_id', 'equipment_id', 'equipment_class', 'timestamp',
            'utilization', 'time_analytics'
        ]
        
        for field in required_fields:
            if field not in payload:
                return False
        
        # Check nested structures
        utilization = payload.get('utilization', {})
        required_util_fields = ['current_state', 'current_activity', 'motion_source']
        
        for field in required_util_fields:
            if field not in utilization:
                return False
        
        time_analytics = payload.get('time_analytics', {})
        required_time_fields = ['total_tracked_seconds', 'total_active_seconds', 
                               'total_idle_seconds', 'utilization_percent']
        
        for field in required_time_fields:
            if field not in time_analytics:
                return False
        
        return True
    
    def close(self) -> None:
        """Close consumer and clean up resources."""
        if self.consumer:
            try:
                self.consumer.close()
                self.logger.info("Kafka consumer closed")
            except Exception as e:
                self.logger.error(f"Error closing Kafka consumer: {e}")
            finally:
                self.consumer = None
