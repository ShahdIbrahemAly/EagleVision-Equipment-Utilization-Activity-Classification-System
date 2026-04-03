"""
Redis frame publisher for streaming annotated video frames to dashboard.
"""

import logging
import time
from typing import Optional
import cv2
import redis
from redis.exceptions import ConnectionError, RedisError

class FramePublisher:
    """Publishes video frames to Redis for dashboard consumption."""
    
    def __init__(self, redis_host: str, redis_port: int, channel: str, 
                 max_retries: int = 10):
        """
        Initialize frame publisher.
        
        Args:
            redis_host: Redis server host
            redis_port: Redis server port
            channel: Redis pub/sub channel name
            max_retries: Maximum connection retry attempts
        """
        self.logger = logging.getLogger(__name__)
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.channel = channel
        self.max_retries = max_retries
        self.r = None
        
        # Initialize Redis connection with retry
        self._connect_with_retry()
    
    def _connect_with_retry(self) -> bool:
        """
        Connect to Redis with retry logic.
        
        Returns:
            True if connection successful, False otherwise
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                self.r = redis.Redis(
                    host=self.redis_host,
                    port=self.redis_port,
                    decode_responses=False,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                
                # Test connection
                self.r.ping()
                self.logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}")
                return True
                
            except (ConnectionError, RedisError) as e:
                self.logger.warning(f"Redis connection attempt {attempt}/{self.max_retries} failed: {e}")
                if attempt < self.max_retries:
                    time.sleep(2)
                else:
                    self.logger.error("Failed to connect to Redis after all retries")
                    return False
        
        return False
    
    def publish(self, frame_bgr) -> bool:
        """
        Publish a frame to Redis channel.
        
        Args:
            frame_bgr: BGR image frame as numpy array
            
        Returns:
            True if published successfully, False otherwise
        """
        if self.r is None:
            self.logger.error("Redis connection not available")
            return False
        
        try:
            # Encode frame as JPEG
            success, buffer = cv2.imencode(
                ".jpg", 
                frame_bgr, 
                [cv2.IMWRITE_JPEG_QUALITY, 70]
            )
            
            if not success:
                self.logger.error("Failed to encode frame as JPEG")
                return False
            
            # Publish to Redis channel
            frame_bytes = buffer.tobytes()
            result = self.r.publish(self.channel, frame_bytes)
            
            if result > 0:
                self.logger.debug(f"Frame published to {result} subscribers")
            else:
                self.logger.debug("No subscribers for frame channel")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error publishing frame: {e}")
            # Try to reconnect on error
            self._connect_with_retry()
            return False
    
    def close(self) -> None:
        """Close Redis connection."""
        if self.r:
            try:
                self.r.close()
                self.logger.info("Redis connection closed")
            except Exception as e:
                self.logger.error(f"Error closing Redis connection: {e}")
            finally:
                self.r = None
