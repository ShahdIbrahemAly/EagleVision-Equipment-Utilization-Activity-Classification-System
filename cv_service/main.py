"""
Main CV service for EagleVision.
"""
import torch
from functools import partial
torch.load = partial(torch.load, weights_only=False)

import cv2
import os, sys, signal, logging, time
from typing import Dict, Optional
import numpy as np
from dotenv import load_dotenv

from detector import EquipmentDetector
from motion_analyzer import MotionAnalyzer
from activity_classifier import ActivityClassifier
from kafka_producer import EquipmentKafkaProducer, _producer_instance
from frame_publisher import FramePublisher

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG if os.getenv('DEBUG','0')=='1' else logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

ACTIVE   = "ACTIVE"
INACTIVE = "INACTIVE"


class EquipmentTracker:
    def __init__(self):
        self.data: Dict[str, Dict] = {}

    def update(self, equipment_id: str, state: str, delta: float) -> Dict:
        if equipment_id not in self.data:
            self.data[equipment_id] = dict(total=0.0, active=0.0, idle=0.0,
                                           last_state=state)
        d = self.data[equipment_id]
        d['total'] += delta
        if d['last_state'] == ACTIVE:
            d['active'] += delta
        else:
            d['idle'] += delta
        d['last_state'] = state
        util = (d['active'] / d['total'] * 100) if d['total'] > 0 else 0.0
        return {
            'total_tracked_seconds': round(d['total'], 1),
            'total_active_seconds':  round(d['active'], 1),
            'total_idle_seconds':    round(d['idle'], 1),
            'utilization_percent':   round(util, 1),
        }


class CVService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

        self.video_source         = os.getenv('VIDEO_SOURCE', 'data/input.mp4')
        self.confidence_threshold = float(os.getenv('CONFIDENCE_THRESHOLD', '0.45'))
        self.motion_threshold     = float(os.getenv('MOTION_THRESHOLD', '2.5'))
        self.frame_skip           = int(os.getenv('FRAME_SKIP', '2'))
        self.target_fps           = int(os.getenv('TARGET_FPS', '15'))
        self.yolo_model           = os.getenv('YOLO_MODEL', 'yolov8n.pt')
        self.kafka_bootstrap      = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.kafka_topic          = os.getenv('KAFKA_TOPIC', 'equipment-events')
        self.redis_host           = os.getenv('REDIS_HOST', 'redis')
        self.redis_port           = int(os.getenv('REDIS_PORT', '6379'))
        self.redis_channel        = os.getenv('REDIS_FRAME_CHANNEL', 'frames')

        self.detector            = None
        self.motion_analyzer     = None
        self.activity_classifier = None
        self.kafka_producer      = None
        self.frame_publisher     = None
        self.tracker             = EquipmentTracker()

        self.cap        = None
        self.frame_id   = 0
        self.prev_frame = None
        self.actual_fps = self.target_fps
        self.target_w   = None
        self.target_h   = None

    def initialize(self) -> bool:
        try:
            if not os.path.exists(self.video_source):
                video_dir = os.path.dirname(self.video_source) or "data"
                os.makedirs(video_dir, exist_ok=True)
                from generate_test_video import generate_test_video
                generate_test_video(self.video_source, duration=60)

            self.cap = cv2.VideoCapture(self.video_source)
            if not self.cap.isOpened():
                self.logger.error(f"Cannot open: {self.video_source}")
                return False

            self.actual_fps = self.cap.get(cv2.CAP_PROP_FPS) or self.target_fps
            w = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            h = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            self.target_w, self.target_h = (800, 600) if (w > 800 or h > 600) else (w, h)
            self.logger.info(f"Video {w}x{h}@{self.actual_fps}fps → {self.target_w}x{self.target_h}")

            self.detector            = EquipmentDetector(self.yolo_model, self.confidence_threshold)
            self.motion_analyzer     = MotionAnalyzer(self.motion_threshold)
            self.activity_classifier = ActivityClassifier()
            self.kafka_producer      = EquipmentKafkaProducer(self.kafka_bootstrap, self.kafka_topic)
            global _producer_instance
            _producer_instance = self.kafka_producer
            self.frame_publisher = FramePublisher(self.redis_host, self.redis_port, self.redis_channel)

            self.logger.info("CV service initialized OK")
            return True
        except Exception as e:
            self.logger.error(f"Init failed: {e}")
            return False

    def _ts(self, n: int) -> str:
        s = n / self.actual_fps
        return f"{int(s//3600):02d}:{int((s%3600)//60):02d}:{s%60:06.3f}"

    def process_frame(self, frame: np.ndarray) -> None:
        try:
            if frame.shape[1] != self.target_w or frame.shape[0] != self.target_h:
                frame = cv2.resize(frame, (self.target_w, self.target_h),
                                   interpolation=cv2.INTER_LINEAR)

            gray       = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            detections = self.detector.detect(frame)

            # ✅ Always publish every frame — prevents blank feed
            if not detections:
                if self.frame_publisher:
                    self.frame_publisher.publish(frame)
                self.prev_frame = gray
                return

            annotated = frame.copy()
            # ✅ Correct delta: accounts for skipped frames
            delta = self.frame_skip / self.actual_fps

            for det in detections:
                # stable ID already set inside detector.py
                sid = det.equipment_id

                state, motion_src, _, _ = self.motion_analyzer.analyze(
                    frame, det.bbox, self.prev_frame)
                activity = self.activity_classifier.classify(
                    det.track_id, motion_src, np.zeros((10,10,2)), det.bbox)
                analytics = self.tracker.update(sid, state, delta)

                if self.kafka_producer:
                    self.kafka_producer.send({
                        "frame_id":        self.frame_id,
                        "equipment_id":    sid,
                        "equipment_class": det.class_name,
                        "timestamp":       self._ts(self.frame_id),
                        "utilization": {
                            "current_state":    state,
                            "current_activity": activity,
                            "motion_source":    motion_src,
                        },
                        "time_analytics": analytics,
                    })

                x1, y1, x2, y2 = det.bbox
                color = (0, 255, 0) if state == ACTIVE else (0, 0, 255)
                cv2.rectangle(annotated, (x1,y1), (x2,y2), color, 2)
                lbl = f"{sid} | {activity} | {analytics['utilization_percent']:.0f}%"
                lsz = cv2.getTextSize(lbl, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 2)[0]
                cv2.rectangle(annotated, (x1, y1-25), (x1+lsz[0], y1), color, -1)
                cv2.putText(annotated, lbl, (x1, y1-8),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255,255,255), 2)

            if self.frame_publisher:
                self.frame_publisher.publish(annotated)

            self.prev_frame = gray

        except Exception as e:
            self.logger.error(f"Frame {self.frame_id} error: {e}")

    def run(self) -> None:
        if not self.initialize():
            sys.exit(1)

        self.logger.info("Starting loop…")

        # ✅ FIX: الفيديو الأصلي عنده FPS معين (مثلاً 25fps)
        # لما بنعمل frame_skip=2، كل 2 فريم بنشتغل على 1
        # يعني بنعالج 25/2 = 12.5 فريم في الثانية
        # السليب المناسب = 1 / (actual_fps / frame_skip)
        effective_fps      = self.actual_fps / max(self.frame_skip, 1)
        playback_interval  = 1.0 / effective_fps
        self.logger.info(
            f"actual_fps={self.actual_fps}, frame_skip={self.frame_skip} "
            f"→ effective={effective_fps:.1f}fps, interval={playback_interval*1000:.0f}ms"
        )

        try:
            while True:
                t0  = time.time()
                ret, frame = self.cap.read()

                if not ret:
                    self.logger.info("Video loop")
                    self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    self.frame_id   = 0
                    self.prev_frame = None
                    continue

                # skip some frames for detection, but still publish them to keep the feed smooth
                if self.frame_skip > 1 and (self.frame_id % self.frame_skip != 0):
                    if self.frame_publisher:
                        self.frame_publisher.publish(frame)
                    self.frame_id += 1
                    continue

                self.process_frame(frame)
                self.frame_id += 1

                # ✅ Sleep only remaining time → steady playback speed
                elapsed = time.time() - t0
                wait    = playback_interval - elapsed
                if wait > 0:
                    time.sleep(wait)

        except KeyboardInterrupt:
            pass
        except Exception as e:
            self.logger.error(f"Loop error: {e}")
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        if self.cap:             self.cap.release()
        if self.kafka_producer:  self.kafka_producer.close()
        if self.frame_publisher: self.frame_publisher.close()
        self.logger.info("Cleanup done")


def signal_handler(signum, frame):
    logger.info("Shutdown")
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT,  signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    CVService().run()

if __name__ == "__main__":
    main()