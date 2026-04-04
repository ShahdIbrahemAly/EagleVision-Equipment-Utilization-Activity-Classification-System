"""
Kafka consumer for equipment events.
Consumes messages from CV service and stores them in TimescaleDB.
"""

import os
import json
import logging
import signal
import sys
import time
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient
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
            'fetch.min.bytes': 1,
            'fetch.wait.max.ms': 500
        }

        try:
            self.consumer = Consumer(config)
            self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
            self.logger.info(f"Kafka consumer initialized for topic: {topic}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise

    def _wait_for_topic(self, timeout: float = 10.0) -> bool:
        """
        Wait for the Kafka topic to become available.
        """
        try:
            metadata = self.admin_client.list_topics(timeout=timeout)
            return self.topic in metadata.topics
        except Exception as e:
            self.logger.warning(f"Topic availability check failed: {e}")
            return False

    def connect_with_retry(self, max_attempts: int = 30, retry_delay: float = 2.0) -> bool:
        """
        Connect to Kafka with retry logic.
        """
        for attempt in range(1, max_attempts + 1):
            try:
                if not self._wait_for_topic(timeout=5.0):
                    raise KafkaException(f"Kafka topic not available: {self.topic}")

                # Subscribe to topic after it is available
                self.consumer.subscribe([self.topic])
                msg = self.consumer.poll(timeout=1.0)

                if msg is not None and msg.error():
                    raise KafkaException(msg.error())

                self.logger.info(f"Connected to Kafka at {self.topic}")
                return True
            except Exception as e:
                self.logger.warning(f"Kafka connection attempt {attempt}/{max_attempts} failed: {e}")
                if attempt < max_attempts:
                    time.sleep(retry_delay)
                else:
                    self.logger.error("Failed to connect to Kafka after all retries")
                    return False
        return False

    def consume_messages(self, db_handler, shutdown_event) -> None:
        """
        Consume messages from Kafka and store in database.
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

                except (DeserializationError, json.JSONDecodeError) as e:
                    self.logger.error(f"Message decoding failed: {e}")
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
        
        return all(field in utilization for field in required_util_fields)

    def close(self):
        """Close the consumer."""
        if self.consumer:
            self.consumer.close()
            self.logger.info("Kafka consumer closed.")