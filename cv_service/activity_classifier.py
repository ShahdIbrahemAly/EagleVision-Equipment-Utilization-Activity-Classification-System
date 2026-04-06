"""
Computer Vision Service - Activity Classifier Module

This module provides rule-based activity classification for construction equipment
based on motion patterns and optical flow analysis.
"""

import logging
from typing import Dict, Optional, Tuple
import numpy as np

# Activity constants
DIGGING = "DIGGING"
SWINGING = "SWINGING"
DUMPING = "DUMPING"
WAITING = "WAITING"

# Debounce constant
DEBOUNCE_FRAMES = 3

class ActivityClassifier:
    """
    Rule-based activity classifier for construction equipment.
    
    Uses motion analysis and flow patterns to classify equipment activities
    like digging, swinging, dumping, and waiting.
    """
    
    def __init__(self):
        """Initialize the activity classifier."""
        self.logger = logging.getLogger(__name__)
        
        # Per-track state tracking
        self.track_states: Dict[int, Dict] = {}
    
    def classify(self, track_id: int, motion_source: str, 
                 flow_field_crop: np.ndarray, bbox: Tuple[int, int, int, int]) -> str:
        """
        Classify equipment activity based on motion and flow patterns.
        
        Args:
            track_id: Equipment track ID
            motion_source: Motion source from motion analyzer
            flow_field_crop: Optical flow field within bounding box
            bbox: Bounding box coordinates
            
        Returns:
            Classified activity string
        """
        try:
            # Initialize track state if not exists
            if track_id not in self.track_states:
                self.track_states[track_id] = {
                    "last_activity": WAITING,
                    "pending_activity": WAITING,
                    "debounce_count": 0
                }
            
            track_state = self.track_states[track_id]
            
            # Classify current activity based on motion
            candidate_activity = self._classify_motion_activity(motion_source, flow_field_crop)
            
            # Apply debouncing
            if candidate_activity == track_state["pending_activity"]:
                track_state["debounce_count"] += 1
                if track_state["debounce_count"] >= DEBOUNCE_FRAMES:
                    track_state["last_activity"] = candidate_activity
                    track_state["debounce_count"] = 0
            else:
                track_state["pending_activity"] = candidate_activity
                track_state["debounce_count"] = 0
            
            return track_state["last_activity"]
            
        except Exception as e:
            self.logger.error(f"Activity classification failed: {e}")
            return WAITING
    
    def _classify_motion_activity(self, motion_source: str, flow_field: np.ndarray) -> str:
        """
        Classify activity based on motion source and flow patterns.
        
        Args:
            motion_source: Motion source from motion analyzer
            flow_field: Optical flow field
            
        Returns:
            Candidate activity string
        """
        # If no motion, equipment is waiting
        if motion_source == "none":
            return WAITING
        
        # Analyze flow patterns for active equipment
        if flow_field is None or flow_field.size == 0:
            return WAITING
        
        h = flow_field.shape[0]
        if h < 2:
            return DIGGING  # Default for small regions
        
        # Focus on upper zone for arm activity analysis
        upper_zone = flow_field[:h//2, :]
        
        # Calculate mean flow vectors in upper zone
        vx_mean = float(np.mean(upper_zone[..., 0]))
        vy_mean = float(np.mean(upper_zone[..., 1]))
        
        abs_vx = abs(vx_mean)
        abs_vy = abs(vy_mean)
        
        # Apply classification rules in order
        if abs_vy > abs_vx * 1.5 and vy_mean > 0:
            # Strong downward motion (positive vy in image coordinates)
            return DIGGING
        elif abs_vx > abs_vy * 1.2:
            # Strong horizontal motion
            return SWINGING
        elif abs_vy > abs_vx * 1.5 and vy_mean < 0:
            # Strong upward motion (negative vy in image coordinates)
            return DUMPING
        else:
            # Default active motion
            return DIGGING
    
    def reset_track(self, track_id: int) -> None:
        """
        Reset tracking state for a specific track ID.
        
        Args:
            track_id: Track ID to reset
        """
        if track_id in self.track_states:
            del self.track_states[track_id]
