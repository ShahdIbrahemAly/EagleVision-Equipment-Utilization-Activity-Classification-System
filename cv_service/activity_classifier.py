"""
Activity classifier for equipment based on motion patterns.
Uses rule-based state machine with debouncing.
"""

import logging
from typing import Dict, Optional
import numpy as np

# Activity constants
DIGGING = "DIGGING"
SWINGING = "SWINGING"
DUMPING = "DUMPING"
WAITING = "WAITING"

# Debounce constant
DEBOUNCE_FRAMES = 3

class ActivityClassifier:
    """Classifies equipment activity based on motion patterns."""
    
    def __init__(self):
        """Initialize activity classifier."""
        self.logger = logging.getLogger(__name__)
        
        # Per-track state tracking
        self.track_states: Dict[int, Dict] = {}
        
    def classify(self, track_id: int, motion_source: str, 
                 flow_field_crop: np.ndarray, bbox: tuple) -> str:
        """
        Classify activity for a tracked equipment.
        
        Args:
            track_id: Unique track identifier
            motion_source: Source of motion (arm_only, tracks_only, etc.)
            flow_field_crop: Optical flow field for the cropped region
            bbox: Bounding box coordinates
            
        Returns:
            Activity string (DIGGING, SWINGING, DUMPING, WAITING)
        """
        try:
            # Initialize track state if new
            if track_id not in self.track_states:
                self.track_states[track_id] = {
                    'last_activity': WAITING,
                    'pending_activity': WAITING,
                    'pending_count': 0
                }
            
            track_state = self.track_states[track_id]
            
            # Determine candidate activity based on motion
            if motion_source == "none":
                candidate = WAITING
            else:
                # Analyze flow vectors in upper zone for activity classification
                h, w = flow_field_crop.shape[:2]
                upper_zone = flow_field_crop[:h//2, :]
                
                # Calculate mean flow vectors
                vy_mean = float(np.mean(upper_zone[..., 1])) if upper_zone.size > 0 else 0.0
                vx_mean = float(np.mean(upper_zone[..., 0])) if upper_zone.size > 0 else 0.0
                
                abs_vx = abs(vx_mean)
                abs_vy = abs(vy_mean)
                
                # Apply activity rules in order
                if abs_vy > abs_vx * 1.5 and vy_mean > 0:
                    candidate = DIGGING
                elif abs_vx > abs_vy * 1.2:
                    candidate = SWINGING
                elif abs_vy > abs_vx * 1.5 and vy_mean < 0:
                    candidate = DUMPING
                else:
                    candidate = DIGGING  # Default active activity
            
            # Debounce: require same signal for N frames before updating
            if candidate == track_state['pending_activity']:
                track_state['pending_count'] += 1
            else:
                track_state['pending_activity'] = candidate
                track_state['pending_count'] = 1
            
            # Update activity if debounce threshold reached
            if track_state['pending_count'] >= DEBOUNCE_FRAMES:
                track_state['last_activity'] = candidate
                track_state['pending_count'] = 0
            
            return track_state['last_activity']
            
        except Exception as e:
            self.logger.error(f"Error in activity classification: {e}")
            return WAITING
