"""
Main computer vision service for EagleVision.
Processes video frames to detect and track construction equipment.
"""
import torch
from functools import partial
torch.load = partial(torch.load, weights_only=False)

import cv2
from ultralytics import YOLO

import os
import sys
import signal
import logging
import time
import json
from typing import Dict, Optional
import cv2
import numpy as np
from dotenv import load_dotenv


# Import local modules
from detector import EquipmentDetector
from motion_analyzer import MotionAnalyzer
from activity_classifier import ActivityClassifier
from kafka_producer import EquipmentKafkaProducer, _producer_instance
from frame_publisher import FramePublisher

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if os.getenv('DEBUG', '0') == '1' else logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
ACTIVE = "ACTIVE"
INACTIVE = "INACTIVE"

class EquipmentTracker:
    """Tracks time analytics for each equipment instance."""
    
    def __init__(self):
        """Initialize tracker."""
        self.equipment_data: Dict[str, Dict] = {}
    
    def update_time_tracking(self, equipment_id: str, current_state: str, 
                           delta_time: float) -> Dict[str, float]:
        """
        Update time tracking for equipment.
        
        Args:
            equipment_id: Equipment identifier
            current_state: Current activity state (ACTIVE/INACTIVE)
            delta_time: Time delta in seconds
            
        Returns:
            Dictionary with updated time analytics
        """
        current_time = time.time()
        
        # Initialize equipment data if new
        if equipment_id not in self.equipment_data:
            self.equipment_data[equipment_id] = {
                'total_tracked_seconds': 0.0,
                'total_active_seconds': 0.0,
                'total_idle_seconds': 0.0,
                'last_state': current_state,
                'last_timestamp': current_time
            }
        
        data = self.equipment_data[equipment_id]
        
        # Update time tracking
        data['total_tracked_seconds'] += delta_time
        
        if data['last_state'] == ACTIVE:
            data['total_active_seconds'] += delta_time
        else:
            data['total_idle_seconds'] += delta_time
        
        # Update state and timestamp
        data['last_state'] = current_state
        data['last_timestamp'] = current_time
        
        # Calculate utilization percentage
        if data['total_tracked_seconds'] > 0:
            utilization = (data['total_active_seconds'] / data['total_tracked_seconds']) * 100
        else:
            utilization = 0.0
        
        return {
            'total_tracked_seconds': round(data['total_tracked_seconds'], 1),
            'total_active_seconds': round(data['total_active_seconds'], 1),
            'total_idle_seconds': round(data['total_idle_seconds'], 1),
            'utilization_percent': round(utilization, 1)
        }

class CVService:
    """Main computer vision service."""
    
    def __init__(self):
        """Initialize CV service."""
        self.logger = logging.getLogger(__name__)
        
        # Load configuration
        self.video_source = os.getenv('VIDEO_SOURCE', 'data/input.mp4')
        self.confidence_threshold = float(os.getenv('CONFIDENCE_THRESHOLD', '0.45'))
        self.motion_threshold = float(os.getenv('MOTION_THRESHOLD', '2.5'))
        self.frame_skip = int(os.getenv('FRAME_SKIP', '2'))
        self.target_fps = int(os.getenv('TARGET_FPS', '15'))
        self.yolo_model = os.getenv('YOLO_MODEL', 'yolov8n.pt')
        
        # Kafka configuration
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'equipment-events')
        
        # Redis configuration
        self.redis_host = os.getenv('REDIS_HOST', 'redis')
        self.redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.redis_channel = os.getenv('REDIS_FRAME_CHANNEL', 'frames')
        
        # Initialize components
        self.detector = None
        self.motion_analyzer = None
        self.activity_classifier = None
        self.kafka_producer = None
        self.frame_publisher = None
        self.equipment_tracker = EquipmentTracker()
        
        # Video processing state
        self.cap = None
        self.frame_id = 0
        self.prev_frame = None
        self.actual_fps = self.target_fps
        self.target_width = None
        self.target_height = None
        
    def initialize(self) -> bool:
        """
        Initialize all components.
        
        Returns:
            True if initialization successful, False otherwise
        """
        try:
            # Check if video file exists
            if not os.path.exists(self.video_source):
                self.logger.warning(f"Video file not found: {self.video_source}")
                self.logger.info("Attempting to generate test video automatically...")
                
                # Create data directory if it doesn't exist
                video_dir = os.path.dirname(self.video_source) or "data"
                os.makedirs(video_dir, exist_ok=True)
                
                # Auto-generate test video
                try:
                    from generate_test_video import generate_test_video
                    self.logger.info(f"Generating test video to {self.video_source}...")
                    generate_test_video(self.video_source, duration=60)
                    self.logger.info(f"Test video generated successfully: {self.video_source}")
                except Exception as gen_error:
                    self.logger.error(f"Failed to generate test video: {gen_error}")
                    return False
            
            # Initialize video capture
            self.cap = cv2.VideoCapture(self.video_source)
            if not self.cap.isOpened():
                self.logger.error(f"Failed to open video: {self.video_source}")
                return False
            
            # Get video properties
            self.actual_fps = self.cap.get(cv2.CAP_PROP_FPS)
            if self.actual_fps == 0:
                self.actual_fps = self.target_fps
            
            width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            
            self.logger.info(f"Video opened: {self.video_source} (FPS: {self.actual_fps}, Resolution: {width}x{height})")
            
            # Resize frames if they are too large to speed up processing
            self.target_width = width
            self.target_height = height
            if width > 800 or height > 600:
                self.logger.info(f"Downscaling video from {width}x{height} to 800x600 for faster processing")
                self.target_width = 800
                self.target_height = 600
            
            # Initialize detector
            self.detector = EquipmentDetector(
                self.yolo_model, 
                self.confidence_threshold
            )
            
            # Initialize motion analyzer
            self.motion_analyzer = MotionAnalyzer(self.motion_threshold)
            
            # Initialize activity classifier
            self.activity_classifier = ActivityClassifier()
            
            # Initialize Kafka producer
            self.kafka_producer = EquipmentKafkaProducer(
                self.kafka_bootstrap_servers,
                self.kafka_topic
            )
            
            # Set global producer instance for signal handling
            global _producer_instance
            _producer_instance = self.kafka_producer
            
            # Initialize frame publisher
            self.frame_publisher = FramePublisher(
                self.redis_host,
                self.redis_port,
                self.redis_channel
            )
            
            self.logger.info("CV service initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Initialization failed: {e}")
            return False
    
    def format_timestamp(self, frame_number: int) -> str:
        """
        Format timestamp from frame number.
        
        Args:
            frame_number: Current frame number
            
        Returns:
            Formatted timestamp string
        """
        seconds = frame_number / self.actual_fps
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:06.3f}"
    
    def draw_detections(self, frame: np.ndarray, detections: list) -> np.ndarray:
        """
        Draw detection boxes and labels on frame.
        
        Args:
            frame: Input frame
            detections: List of detection objects
            
        Returns:
            Annotated frame
        """
        annotated = frame.copy()
        
        for detection in detections:
            x1, y1, x2, y2 = detection.bbox
            
            # Get color based on state (green for active, red for inactive)
            # This will be updated after motion analysis
            color = (0, 255, 0)  # Default green
            
            # Draw bounding box
            cv2.rectangle(annotated, (x1, y1), (x2, y2), color, 2)
            
            # Draw label background
            label = f"{detection.equipment_id} | {detection.class_name}"
            label_size = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 2)[0]
            cv2.rectangle(annotated, (x1, y1 - 25), (x1 + label_size[0], y1), color, -1)
            
            # Draw label text
            cv2.putText(annotated, label, (x1, y1 - 8), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 2)
        
        return annotated
    
    def process_frame(self, frame: np.ndarray) -> None:
        """
        Process a single video frame.
        
        Args:
            frame: Input BGR frame
        """
        try:
            # Resize frame if needed for faster processing
            if hasattr(self, 'target_width') and (frame.shape[1] != self.target_width or frame.shape[0] != self.target_height):
                frame = cv2.resize(frame, (self.target_width, self.target_height), interpolation=cv2.INTER_LINEAR)
            
            # Convert to grayscale for motion analysis
            gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            
            # Run detection
            detections = self.detector.detect(frame)
            
            if not detections:
                # No detections found, publish frame without annotations
                if self.frame_publisher:
                    self.frame_publisher.publish(frame)
                return
            
            annotated_frame = frame.copy()
            
            # Process each detection
            for detection in detections:
                # Analyze motion
                state, motion_source, upper_mag, lower_mag = self.motion_analyzer.analyze(
                    frame, detection.bbox, self.prev_frame
                )
                
                # Classify activity
                activity = self.activity_classifier.classify(
                    detection.track_id, motion_source, 
                    np.zeros((10, 10, 2)), detection.bbox  # Dummy flow field for now
                )
                
                # Update time tracking
                delta_time = 1.0 / self.actual_fps
                time_analytics = self.equipment_tracker.update_time_tracking(
                    detection.equipment_id, state, delta_time
                )
                
                # Build Kafka payload
                payload = {
                    "frame_id": self.frame_id,
                    "equipment_id": detection.equipment_id,
                    "equipment_class": detection.class_name,
                    "timestamp": self.format_timestamp(self.frame_id),
                    "utilization": {
                        "current_state": state,
                        "current_activity": activity,
                        "motion_source": motion_source
                    },
                    "time_analytics": time_analytics
                }
                
                # Send to Kafka
                if self.kafka_producer:
                    self.kafka_producer.send(payload)
                
                # Draw detection on annotated frame
                x1, y1, x2, y2 = detection.bbox
                color = (0, 255, 0) if state == ACTIVE else (0, 0, 255)
                
                # Draw bounding box
                cv2.rectangle(annotated_frame, (x1, y1), (x2, y2), color, 2)
                
                # Draw label with utilization
                label = f"{detection.equipment_id} | {activity} | {time_analytics['utilization_percent']:.0f}%"
                label_size = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 2)[0]
                cv2.rectangle(annotated_frame, (x1, y1 - 25), (x1 + label_size[0], y1), color, -1)
                cv2.putText(annotated_frame, label, (x1, y1 - 8), 
                           cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 2)
            
            # Publish annotated frame
            if self.frame_publisher:
                self.frame_publisher.publish(annotated_frame)
            
            # Update previous frame
            self.prev_frame = gray_frame
            
        except Exception as e:
            self.logger.error(f"Error processing frame {self.frame_id}: {e}")
    
    def run(self) -> None:
        """Main processing loop."""
        if not self.initialize():
            self.logger.error("Failed to initialize CV service")
            sys.exit(1)
        
        self.logger.info("Starting video processing loop...")
        
        try:
            while True:
                ret, frame = self.cap.read()
                
                if not ret:
                    # End of video, loop back to start
                    self.logger.info("Video ended, restarting from beginning...")
                    self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    self.frame_id = 0
                    continue
                
                # Skip frames if configured
                if self.frame_id % self.frame_skip != 0:
                    self.frame_id += 1
                    continue
                
                # Process frame
                self.process_frame(frame)
                self.frame_id += 1
                
                # Control frame rate
                time.sleep(1.0 / self.target_fps)
                
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        except Exception as e:
            self.logger.error(f"Error in processing loop: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self) -> None:
        """Clean up resources."""
        self.logger.info("Cleaning up CV service...")
        
        if self.cap:
            self.cap.release()
        
        if self.kafka_producer:
            self.kafka_producer.close()
        
        if self.frame_publisher:
            self.frame_publisher.close()
        
        self.logger.info("CV service cleanup complete")

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
    service = CVService()
    service.run()

if __name__ == "__main__":
    main()
