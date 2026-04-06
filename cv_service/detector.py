"""
Computer Vision Service - Equipment Detector Module

This module provides YOLOv8-based equipment detection with ByteTrack tracking
for construction site monitoring.
"""

import os
import logging
from typing import List, Dict, Tuple, Optional
import cv2
import numpy as np
import torch
from ultralytics import YOLO
from ultralytics.nn.modules import Conv
from ultralytics.nn.tasks import DetectionModel

# Equipment type constants
EXCAVATOR = "excavator"
DUMP_TRUCK = "dump_truck"

# COCO class mapping for construction equipment
COCO_EQUIPMENT_CLASSES = {
    2: "car",      # Car
    5: "bus",      # Bus  
    7: "truck",    # Truck
}

class EquipmentDetector:
    """
    YOLOv8-based equipment detector with ByteTrack tracking.
    
    Detects and tracks construction equipment (excavators, dump trucks)
    in video frames using YOLOv8 model with ByteTrack persistence.
    """
    
    def __init__(self, model_path: str, confidence_threshold: float = 0.45):
        """
        Initialize the detector with YOLO model.
        
        Args:
            model_path: Path to YOLO model file (e.g., "yolov8n.pt")
            confidence_threshold: Minimum confidence for detections
        """
        self.logger = logging.getLogger(__name__)
        self.confidence_threshold = confidence_threshold
        
        try:
            # Handle PyTorch 2.6+ weights_only security restriction
            safe_classes = [DetectionModel, Conv, torch.nn.modules.container.Sequential]
            if hasattr(torch.serialization, "add_safe_globals"):
                torch.serialization.add_safe_globals(safe_classes)
            
            if hasattr(torch.serialization, "safe_globals"):
                with torch.serialization.safe_globals(safe_classes):
                    self.model = YOLO(model_path)
            else:
                self.model = YOLO(model_path)

            self.logger.info(f"Loaded YOLOv8 model: {model_path}")
        except Exception as e:
            self.logger.error(f"Failed to load YOLO model: {e}")
            raise
    
    def detect(self, frame: np.ndarray) -> List[Dict]:
        """
        Detect and track equipment in a frame.
        
        Args:
            frame: Input BGR frame
            
        Returns:
            List of detection dictionaries with bbox, track_id, class_name, confidence
        """
        try:
            # Run tracking with ByteTrack
            results = self.model.track(
                frame, 
                tracker="bytetrack.yaml", 
                persist=True,
                conf=self.confidence_threshold
            )
            
            detections = []
            
            if results and len(results) > 0 and results[0].boxes is not None:
                boxes = results[0].boxes
                
                for i in range(len(boxes)):
                    # Get bounding box coordinates
                    bbox = boxes.xyxy[i].cpu().numpy().astype(int)
                    x1, y1, x2, y2 = bbox
                    
                    # Get track ID (if available)
                    if hasattr(boxes, 'id') and boxes.id is not None:
                        track_id = int(boxes.id[i].cpu().numpy())
                    else:
                        track_id = i  # Fallback to index
                    
                    # Get class and confidence
                    class_id = int(boxes.cls[i].cpu().numpy())
                    confidence = float(boxes.conf[i].cpu().numpy())
                    
                    # Map to equipment types
                    equipment_class = self._map_to_equipment_class(class_id)
                    if equipment_class:
                        equipment_id = self._generate_equipment_id(track_id, equipment_class)
                        
                        detection = {
                            "bbox": (x1, y1, x2, y2),
                            "track_id": track_id,
                            "equipment_id": equipment_id,
                            "equipment_class": equipment_class,
                            "confidence": confidence
                        }
                        detections.append(detection)
            
            return detections
            
        except Exception as e:
            self.logger.error(f"Detection failed: {e}")
            return []
    
    def predict(self, frame: np.ndarray) -> List[Dict]:
        """
        Run YOLO prediction on a frame.
        
        Args:
            frame: Input BGR frame
            
        Returns:
            List of detection dictionaries with bbox, class_id, class_name, confidence
        """
        try:
            # Run prediction with specified parameters
            results = self.model.predict(
                source=frame, 
                conf=0.2, 
                imgsz=640, 
                classes=[2, 5, 7]
            )
            
            detections = []
            
            if results and len(results) > 0 and results[0].boxes is not None:
                boxes = results[0].boxes
                
                for i in range(len(boxes)):
                    # Get bounding box coordinates
                    bbox = boxes.xyxy[i].cpu().numpy().astype(int).tolist()
                    
                    # Get class and confidence
                    class_id = int(boxes.cls[i].cpu().numpy())
                    confidence = float(boxes.conf[i].cpu().numpy())
                    
                    # Get equipment class
                    equipment_class = self._map_to_equipment_class(class_id)
                    if equipment_class:
                        detection = {
                            "bbox": bbox,
                            "class_id": class_id,
                            "equipment_class": equipment_class,
                            "confidence": confidence
                        }
                        detections.append(detection)
            
            return detections
            
        except Exception as e:
            self.logger.error(f"Prediction failed: {e}")
            return []
    
    def _map_to_equipment_class(self, class_id: int) -> Optional[str]:
        """
        Map COCO class ID to equipment type.
        
        Args:
            class_id: COCO class ID
            
        Returns:
            Equipment class string or None if not relevant
        """
        return COCO_EQUIPMENT_CLASSES.get(class_id)
    
    def _generate_equipment_id(self, track_id: int, equipment_class: str) -> str:
        """
        Generate equipment ID based on track ID and class.
        
        Args:
            track_id: ByteTrack track ID
            equipment_class: Type of equipment
            
        Returns:
            Formatted equipment ID string
        """
        if equipment_class == EXCAVATOR:
            return f"EX-{track_id:03d}"
        else:  # DUMP_TRUCK
            return f"DT-{track_id:03d}"
