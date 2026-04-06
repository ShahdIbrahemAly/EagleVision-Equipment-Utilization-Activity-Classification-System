"""
Computer Vision Service - Motion Analyzer Module

This module provides zone-based optical flow analysis for detecting equipment
motion patterns and activity states.
"""

import os
import logging
from typing import Tuple, Optional
import cv2
import numpy as np

# Motion state constants
ACTIVE = "ACTIVE"
INACTIVE = "INACTIVE"

# Motion source constants
FULL_BODY = "full_body"
ARM_ONLY = "arm_only"
TRACKS_ONLY = "tracks_only"
NONE = "none"

class MotionAnalyzer:
    """
    Zone-based optical flow analyzer for equipment motion detection.
    
    Analyzes motion patterns within bounding boxes to determine if equipment
    is active or inactive and identifies motion sources (arm, tracks, etc.).
    """
    
    def __init__(self, motion_threshold: float = 2.5):
        """
        Initialize the motion analyzer.
        
        Args:
            motion_threshold: Threshold for optical flow magnitude
        """
        self.logger = logging.getLogger(__name__)
        self.motion_threshold = motion_threshold
        self.prev_frame = None
    
    def analyze(self, frame: np.ndarray, bbox: Tuple[int, int, int, int], 
                prev_frame: Optional[np.ndarray] = None) -> Tuple[str, str, float, float]:
        """
        Analyze motion within a bounding box using optical flow.
        
        Args:
            frame: Current BGR frame
            bbox: Bounding box (x1, y1, x2, y2)
            prev_frame: Previous grayscale frame (optional, uses stored if None)
            
        Returns:
            Tuple of (state, motion_source, upper_magnitude, lower_magnitude)
        """
        try:
            # Convert current frame to grayscale
            gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            
            # Use provided previous frame or stored one
            if prev_frame is not None:
                prev_gray = prev_frame
            elif self.prev_frame is not None:
                prev_gray = self.prev_frame
            else:
                # No previous frame available
                return INACTIVE, NONE, 0.0, 0.0
            
            # Extract bounding box regions
            x1, y1, x2, y2 = bbox
            curr_crop = gray_frame[y1:y2, x1:x2]
            prev_crop = prev_gray[y1:y2, x1:x2]
            
            # Skip if regions are too small
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
            
            # Split vertically for zone analysis
            h = magnitude.shape[0]
            if h < 2:
                return INACTIVE, NONE, 0.0, 0.0
            
            upper_zone = magnitude[:h//2, :]
            lower_zone = magnitude[h//2:, :]
            
            # Calculate average magnitudes
            upper_mag = float(np.mean(upper_zone))
            lower_mag = float(np.mean(lower_zone))
            
            # Determine motion state and source
            state, motion_source = self._classify_motion(upper_mag, lower_mag)
            
            return state, motion_source, upper_mag, lower_mag
            
        except Exception as e:
            self.logger.error(f"Motion analysis failed: {e}")
            return INACTIVE, NONE, 0.0, 0.0
    
    def _classify_motion(self, upper_mag: float, lower_mag: float) -> Tuple[str, str]:
        """
        Classify motion state and source based on zone magnitudes.
        
        Args:
            upper_mag: Average magnitude in upper zone
            lower_mag: Average magnitude in lower zone
            
        Returns:
            Tuple of (state, motion_source)
        """
        threshold = self.motion_threshold
        
        if upper_mag > threshold and lower_mag > threshold:
            return ACTIVE, FULL_BODY
        elif upper_mag > threshold:
            return ACTIVE, ARM_ONLY
        elif lower_mag > threshold:
            return ACTIVE, TRACKS_ONLY
        else:
            return INACTIVE, NONE
    
    def update_previous_frame(self, frame: np.ndarray) -> None:
        """
        Update the stored previous frame.
        
        Args:
            frame: Current frame to store as previous for next iteration
        """
        self.prev_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
