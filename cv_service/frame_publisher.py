"""
Computer Vision Service - Frame Publisher Module

This module handles publishing annotated video frames to Redis
for real-time dashboard display.
"""

import logging
import time
from typing import Optional
import cv2
import redis
from redis.exceptions import ConnectionError, RedisError

class FramePublisher:
    """
    Redis publisher for annotated video frames.
    
    Publishes JPEG-encoded frames to a Redis channel for consumption
    by the dashboard service.
    """
    
    def __init__(self, redis_host: str, redis_port: int, channel: str):
        """
        Initialize Redis frame publisher.
        
        Args:
            redis_host: Redis server host
            redis_port: Redis server port
            channel: Redis pub/sub channel name
        """
        self.logger = logging.getLogger(__name__)
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.channel = channel
        self.redis_client = None
        self.retry_count = 0
        self.max_retries = 10
        
        self._connect_with_retry()
    
    def _connect_with_retry(self) -> None:
        """Connect to Redis with retry logic."""
        while self.retry_count < self.max_retries:
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
                self.logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}")
                return
                
            except (ConnectionError, RedisError) as e:
                self.retry_count += 1
                self.logger.warning(f"Redis connection attempt {self.retry_count}/{self.max_retries} failed: {e}")
                if self.retry_count < self.max_retries:
                    time.sleep(2)
                else:
                    self.logger.error("Failed to connect to Redis after maximum retries")
                    raise
    
    def publish(self, frame_bgr) -> None:
        """
        Publish annotated frame to Redis channel.
        
        Args:
            frame_bgr: BGR frame to publish
        """
        try:
            if not self.redis_client:
                raise RuntimeError("Redis client not connected")
            
            # Encode frame as JPEG
            success, buffer = cv2.imencode(
                ".jpg", 
                frame_bgr, 
                [cv2.IMWRITE_JPEG_QUALITY, 70]
            )
            
            if not success:
                self.logger.error("Failed to encode frame as JPEG")
                return

            # Publish frame bytes via pub/sub AND store as latest-frame key for the dashboard
            frame_bytes = buffer.tobytes()
            self.redis_client.publish(self.channel, frame_bytes)
            self.redis_client.set(self.channel, frame_bytes)
            
        except Exception as e:
            self.logger.error(f"Failed to publish frame to Redis: {e}")
            # Attempt to reconnect
            try:
                self._connect_with_retry()
            except:
                pass
    
    def close(self) -> None:
        """Close Redis connection."""
        if self.redis_client:
            try:
                self.redis_client.close()
                self.logger.info("Redis connection closed")
            except Exception as e:
                self.logger.error(f"Error closing Redis connection: {e}")
