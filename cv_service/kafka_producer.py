"""
Kafka producer for equipment events.
Handles connection management and message publishing.
"""

import json
import logging
import signal
import sys
from typing import Dict, Any
from confluent_kafka import Producer, KafkaException, KafkaError

class EquipmentKafkaProducer:
    """Kafka producer for equipment utilization events."""
    
    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic name for equipment events
        """
        self.logger = logging.getLogger(__name__)
        self.topic = topic
        self.producer = None
        
        # Producer configuration
        config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'cv-service-producer',
            'queue.buffering.max.messages': 10000,
            'queue.buffering.max.ms': 1000,
            'batch.num.messages': 100,
            'compression.type': 'snappy'
        }
        
        try:
            self.producer = Producer(config)
            self.logger.info(f"Kafka producer initialized for topic: {topic}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def send(self, payload: Dict[str, Any]) -> bool:
        """
        Send a message to Kafka.
        
        Args:
            payload: Event payload dictionary
            
        Returns:
            True if message sent successfully, False otherwise
        """
        try:
            # Convert payload to JSON
            message_json = json.dumps(payload, separators=(',', ':'))
            
            # Create message with equipment_id as key for partitioning
            key = payload.get('equipment_id', 'unknown').encode('utf-8')
            value = message_json.encode('utf-8')
            
            # Produce message
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                callback=self._delivery_callback
            )
            
            # Poll to serve delivery callbacks
            self.producer.poll(0)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send message to Kafka: {e}")
            return False
    
    def _delivery_callback(self, error: KafkaError, message) -> None:
        """Callback for message delivery reports."""
        if error is not None:
            self.logger.error(f"Message delivery failed: {error}")
        else:
            self.logger.debug(f"Message delivered to {message.topic()} [{message.partition()}]")
    
    def flush(self, timeout: float = 10.0) -> None:
        """
        Flush pending messages.
        
        Args:
            timeout: Timeout in seconds
        """
        if self.producer:
            try:
                self.producer.flush(timeout)
                self.logger.info("Kafka producer flushed successfully")
            except Exception as e:
                self.logger.error(f"Error flushing Kafka producer: {e}")
    
    def close(self) -> None:
        """Close the producer and clean up resources."""
        if self.producer:
            try:
                self.flush()
                self.logger.info("Kafka producer closed")
            except Exception as e:
                self.logger.error(f"Error closing Kafka producer: {e}")
            finally:
                self.producer = None

# Global producer instance for signal handling
_producer_instance = None

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global _producer_instance
    if _producer_instance:
        _producer_instance.close()
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
