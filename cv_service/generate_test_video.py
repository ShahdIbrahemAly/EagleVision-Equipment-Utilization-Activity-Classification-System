"""
Generate test video for EagleVision demo when no real footage is available.
Creates a synthetic video with moving rectangles simulating equipment.
"""

import cv2
import numpy as np
import os

def generate_test_video(output_path: str = "data/input.mp4", duration: int = 30):
    """
    Generate a test video with simulated equipment movement.
    
    Args:
        output_path: Output video file path
        duration: Video duration in seconds
    """
    # Video properties
    width, height = 640, 480
    fps = 15
    total_frames = duration * fps
    
    # Create video writer
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
    
    print(f"Generating test video: {output_path}")
    print(f"Duration: {duration}s, FPS: {fps}, Frames: {total_frames}")
    
    for frame_num in range(total_frames):
        # Create black background
        frame = np.zeros((height, width, 3), dtype=np.uint8)
        
        # Add some background texture
        frame += 20  # Dark gray background
        
        # Simulate excavator movement
        # Base position oscillates horizontally (simulating swing)
        base_x = int(width/2 + 150 * np.sin(frame_num * 0.05))
        base_y = int(height * 0.7)
        
        # Arm position oscillates vertically (simulating digging)
        arm_top_y = int(base_y - 100 * (1 + np.sin(frame_num * 0.1)))
        
        # Draw tracks (lower rectangle)
        track_color = (50, 50, 50)  # Dark gray
        cv2.rectangle(frame, (base_x - 40, base_y), (base_x + 40, base_y + 30), track_color, -1)
        
        # Draw body (middle rectangle)
        body_color = (100, 100, 100)  # Gray
        cv2.rectangle(frame, (base_x - 30, base_y - 40), (base_x + 30, base_y), body_color, -1)
        
        # Draw arm (line from body to top)
        arm_color = (150, 150, 150)  # Light gray
        cv2.line(frame, (base_x, base_y - 20), (base_x + 50, arm_top_y), arm_color, 8)
        
        # Draw bucket (small rectangle at arm end)
        bucket_color = (80, 80, 80)  # Medium gray
        cv2.rectangle(frame, (base_x + 40, arm_top_y - 10), (base_x + 60, arm_top_y + 10), bucket_color, -1)
        
        # Add frame number
        cv2.putText(frame, f"Frame: {frame_num}", (10, 30), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
        
        # Add timestamp
        seconds = frame_num / fps
        timestamp = f"{int(seconds//60):02d}:{int(seconds%60):02d}.{int((seconds%1)*1000):03d}"
        cv2.putText(frame, timestamp, (10, 60), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
        
        out.write(frame)
        
        # Progress indicator
        if frame_num % 50 == 0:
            print(f"  Frame {frame_num}/{total_frames} ({100*frame_num/total_frames:.1f}%)")
    
    out.release()
    print(f"Test video generated successfully: {output_path}")

if __name__ == "__main__":
    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)
    
    # Generate test video
    generate_test_video("data/input.mp4", duration=30)
