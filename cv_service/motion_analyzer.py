"""
Motion analyzer using zone-based optical flow for equipment activity detection.
"""

import os
import logging
from typing import Tuple, Optional
import cv2
import numpy as np

# Motion source constants
FULL_BODY = "full_body"
ARM_ONLY = "arm_only"
TRACKS_ONLY = "tracks_only"
NONE = "none"

# Activity and state constants
ACTIVE = "ACTIVE"
INACTIVE = "INACTIVE"

class MotionAnalyzer:
    """Analyzes motion in equipment bounding boxes using optical flow."""
    
    def __init__(self, motion_threshold: float = 2.5):
        """
        Initialize motion analyzer.
        
        Args:
            motion_threshold: Threshold for detecting significant motion
        """
        self.logger = logging.getLogger(__name__)
        self.motion_threshold = motion_threshold
        self.prev_frame = None
        
    def analyze(self, frame: np.ndarray, bbox: Tuple[int, int, int, int], 
                prev_frame: Optional[np.ndarray] = None) -> Tuple[str, str, float, float]:
        """
        Analyze motion in a bounding box region.
        
        Args:
            frame: Current BGR frame
            bbox: Bounding box (x1, y1, x2, y2)
            prev_frame: Previous grayscale frame for optical flow
            
        Returns:
            Tuple of (state, motion_source, upper_magnitude, lower_magnitude)
        """
        try:
            x1, y1, x2, y2 = bbox
            
            # Convert current frame to grayscale
            gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            
            if prev_frame is None:
                # Store current frame for next iteration
                self.prev_frame = gray_frame
                return INACTIVE, NONE, 0.0, 0.0
            
            # Crop regions from current and previous frames
            curr_crop = gray_frame[y1:y2, x1:x2]
            prev_crop = prev_frame[y1:y2, x1:x2]
            
            # Skip if crops are too small
            if curr_crop.size == 0 or prev_crop.size == 0:
                return INACTIVE, NONE, 0.0, 0.0
            
            # Ensure same size
            if curr_crop.shape != prev_crop.shape:
                return INACTIVE, NONE, 0.0, 0.0
            
            # Calculate optical flow
            flow = cv2.calcOpticalFlowFarneback(
                prev_crop, curr_crop, None,
                pyr_scale=0.5, levels=3, winsize=15, iterations=3,
                poly_n=5, poly_sigma=1.2, flags=0
            )
            
            # Calculate flow magnitude
            magnitude = np.sqrt(flow[..., 0]**2 + flow[..., 1]**2)
            
            # Split vertically (upper = arm/body, lower = tracks)
            h, w = magnitude.shape
            upper_region = magnitude[:h//2, :]
            lower_region = magnitude[h//2:, :]
            
            # Calculate mean magnitudes
            upper_mag = float(np.mean(upper_region)) if upper_region.size > 0 else 0.0
            lower_mag = float(np.mean(lower_region)) if lower_region.size > 0 else 0.0
            
            # Determine motion source and state
            if upper_mag > self.motion_threshold and lower_mag > self.motion_threshold:
                motion_source = FULL_BODY
                state = ACTIVE
            elif upper_mag > self.motion_threshold:
                motion_source = ARM_ONLY
                state = ACTIVE
            elif lower_mag > self.motion_threshold:
                motion_source = TRACKS_ONLY
                state = ACTIVE
            else:
                motion_source = NONE
                state = INACTIVE
            
            # Store current frame for next iteration
            self.prev_frame = gray_frame
            
            return state, motion_source, upper_mag, lower_mag
            
        except Exception as e:
            self.logger.error(f"Error in motion analysis: {e}")
            return INACTIVE, NONE, 0.0, 0.0
