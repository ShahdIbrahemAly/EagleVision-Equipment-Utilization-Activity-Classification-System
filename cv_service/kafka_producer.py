"""
Computer Vision Service - Kafka Producer Module

This module handles publishing equipment detection events to Kafka
for downstream analytics processing.
"""

import json
import logging
import signal
import sys
from typing import Dict, Any
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic

class KafkaProducer:
    """
    Kafka producer for equipment detection events.
    
    Publishes structured JSON messages to the equipment-events topic
    for consumption by the analytics service.
    """
    
    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialize Kafka producer and ensure topic exists.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic name for equipment events
        """
        self.logger = logging.getLogger(__name__)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.admin_client = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self._initialize()
    
    def _initialize(self) -> None:
        """Initialize producer and create topic if needed."""
        try:
            # Create admin client for topic management
            admin_config = {"bootstrap.servers": self.bootstrap_servers}
            self.admin_client = AdminClient(admin_config)
            
            # Create topic if it doesn't exist
            self._ensure_topic_exists()
            
            # Initialize producer
            producer_config = {
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": "cv-service-producer"
            }
            self.producer = Producer(producer_config)
            
            self.logger.info(f"Kafka producer initialized for topic: {self.topic}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def _ensure_topic_exists(self) -> None:
        """Create the equipment-events topic if it doesn't exist."""
        try:
            # Check if topic exists
            metadata = self.admin_client.list_topics(timeout=10)
            if self.topic in metadata.topics:
                self.logger.info(f"Topic {self.topic} already exists")
                return
            
            # Create topic
            topic_spec = NewTopic(
                self.topic,
                num_partitions=3,
                replication_factor=1
            )
            
            future = self.admin_client.create_topics([topic_spec])
            
            # Wait for creation
            for topic_name, future_result in future.items():
                try:
                    future_result.result()  # Will raise exception if failed
                    self.logger.info(f"Created topic: {topic_name}")
                except KafkaException as e:
                    if "ALREADY_EXISTS" in str(e):
                        self.logger.info(f"Topic {topic_name} already exists")
                    else:
                        raise
            
        except Exception as e:
            self.logger.error(f"Failed to ensure topic exists: {e}")
            raise
    
    def send(self, payload: Dict[str, Any]) -> None:
        """
        Send equipment event to Kafka.
        
        Args:
            payload: Equipment event payload dictionary
        """
        try:
            if not self.producer:
                raise RuntimeError("Producer not initialized")
            
            # Extract equipment_id for partition key
            equipment_id = payload.get("equipment_id", "unknown")
            key = equipment_id.encode()
            
            # Serialize payload
            value = json.dumps(payload).encode()
            
            # Send message
            self.producer.produce(
                self.topic,
                key=key,
                value=value,
                callback=self._delivery_callback
            )
            
            # Poll for delivery reports
            self.producer.poll(0)
            
        except Exception as e:
            self.logger.error(f"Failed to send message to Kafka: {e}")
    
    def _delivery_callback(self, err, msg) -> None:
        """
        Callback for message delivery reports.
        
        Args:
            err: Delivery error (None if successful)
            msg: Message object
        """
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def flush(self) -> None:
        """Flush all pending messages."""
        if self.producer:
            try:
                self.producer.flush()
                self.logger.info("Kafka producer flushed")
            except Exception as e:
                self.logger.error(f"Failed to flush Kafka producer: {e}")
    
    def _signal_handler(self, signum, frame) -> None:
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.flush()
        sys.exit(0)
    
    def close(self) -> None:
        """Close the producer and cleanup resources."""
        self.flush()
        if self.producer:
            self.producer.close()
        if self.admin_client:
            self.admin_client.close()
