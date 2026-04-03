"""
Computer vision equipment detector using YOLOv8 with ByteTrack.
Provides object detection and tracking for construction equipment.
"""

import os
import logging
from typing import List, Dict, Tuple, Optional
import cv2
import numpy as np
from ultralytics import YOLO

# Equipment class mapping for COCO dataset
EQUIPMENT_CLASSES = {
    2: "car",      # Car as proxy for light equipment
    5: "bus",      # Bus as proxy for transport equipment
    7: "truck"     # Truck as proxy for dump_truck/excavator
}

# Activity and state constants
ACTIVE = "ACTIVE"
INACTIVE = "INACTIVE"

class Detection:
    """Represents a single equipment detection."""
    
    def __init__(self, bbox: Tuple[int, int, int, int], track_id: int, 
                 class_name: str, confidence: float, equipment_id: str):
        self.bbox = bbox  # (x1, y1, x2, y2)
        self.track_id = track_id
        self.class_name = class_name
        self.confidence = confidence
        self.equipment_id = equipment_id

class EquipmentDetector:
    """YOLOv8-based equipment detector with ByteTrack tracking."""
    
    def __init__(self, model_path: str, confidence_threshold: float = 0.45):
        """
        Initialize the detector.
        
        Args:
            model_path: Path to YOLO model file
            confidence_threshold: Minimum confidence for detections
        """
        self.logger = logging.getLogger(__name__)
        self.confidence_threshold = confidence_threshold
        
        self.logger.info(f"Loading YOLO model: {model_path}")
        self.model = YOLO(model_path)
        
        # Map detected classes to equipment types
        self.class_mapping = {
            "car": "light_equipment",    # Light mobile equipment
            "bus": "transport_equipment", # Transport/support equipment
            "truck": "excavator"         # Use truck as proxy for excavator
        }
        
    def detect(self, frame: np.ndarray) -> List[Detection]:
        """
        Detect and track equipment in a frame.
        
        Args:
            frame: Input BGR frame
            
        Returns:
            List of Detection objects
        """
        try:
            # Run tracking with ByteTrack
            results = self.model.track(
                source=frame,
                tracker="bytetrack.yaml",
                persist=True,
                conf=self.confidence_threshold,
                verbose=False
            )
            
            detections = []
            
            if results and len(results) > 0 and results[0].boxes is not None:
                boxes = results[0].boxes
                
                for i in range(len(boxes)):
                    # Get bounding box
                    bbox = boxes.xyxy[i].cpu().numpy().astype(int)
                    x1, y1, x2, y2 = bbox
                    
                    # Get track ID
                    track_id = int(boxes.id[i].cpu().numpy()) if boxes.id is not None else -1
                    
                    # Get class and confidence
                    class_id = int(boxes.cls[i].cpu().numpy())
                    confidence = float(boxes.conf[i].cpu().numpy())
                    
                    # Filter to equipment classes only
                    if class_id in EQUIPMENT_CLASSES:
                        class_name = EQUIPMENT_CLASSES[class_id]
                        
                        # Map to equipment type
                        equipment_type = self.class_mapping.get(class_name, class_name)
                        
                        # Generate equipment ID based on type
                        if equipment_type == "excavator":
                            equipment_id = f"EX-{track_id:03d}"
                        elif equipment_type == "transport_equipment":
                            equipment_id = f"TR-{track_id:03d}"
                        elif equipment_type == "light_equipment":
                            equipment_id = f"LE-{track_id:03d}"
                        else:
                            equipment_id = f"DT-{track_id:03d}"
                        
                        detection = Detection(
                            bbox=(x1, y1, x2, y2),
                            track_id=track_id,
                            class_name=equipment_type,
                            confidence=confidence,
                            equipment_id=equipment_id
                        )
                        detections.append(detection)
            
            return detections
            
        except Exception as e:
            self.logger.error(f"Error in detection: {e}")
            return []
