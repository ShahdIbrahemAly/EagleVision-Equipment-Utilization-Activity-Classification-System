"""
Equipment detector using YOLOv8 with ByteTrack + stable ID mapping.
"""

import os
import logging
from typing import List, Dict, Tuple, Optional
import cv2
import numpy as np
from ultralytics import YOLO

EQUIPMENT_CLASSES = {
    2: "car",
    5: "bus",
    7: "truck"
}

ACTIVE   = "ACTIVE"
INACTIVE = "INACTIVE"


class Detection:
    def __init__(self, bbox, track_id, class_name, confidence, equipment_id):
        self.bbox         = bbox          # (x1, y1, x2, y2)
        self.track_id     = track_id
        self.class_name   = class_name
        self.confidence   = confidence
        self.equipment_id = equipment_id


# ──────────────────────────────────────────────────────────────────────────────
#  StableIDMapper
#  Root cause of the "15 machines" bug:
#    detector.py was doing  equipment_id = f"EX-{track_id:03d}"
#    YOLO increments track_id every time it loses + re-finds an object,
#    so one real truck becomes EX-003, EX-007, EX-011 …  → 15 rows in DB.
#
#  Fix: map every new track_id to a stable ID using IoU overlap with known
#  positions.  Only create a genuinely new ID when there is no overlap.
# ──────────────────────────────────────────────────────────────────────────────
class StableIDMapper:

    def __init__(self, iou_threshold: float = 0.25, max_missing_frames: int = 45):
        self.iou_threshold      = iou_threshold
        self.max_missing_frames = max_missing_frames
        self.known: Dict[str, Dict]  = {}   # stable_id → info
        self.cache: Dict[int, str]   = {}   # yolo track_id → stable_id
        self.counters: Dict[str, int] = {}  # class → counter
        self.frame_num = 0

    @staticmethod
    def _iou(a: Tuple, b: Tuple) -> float:
        ax1, ay1, ax2, ay2 = a
        bx1, by1, bx2, by2 = b
        ix1 = max(ax1, bx1); iy1 = max(ay1, by1)
        ix2 = min(ax2, bx2); iy2 = min(ay2, by2)
        inter = max(0, ix2-ix1) * max(0, iy2-iy1)
        if inter == 0:
            return 0.0
        union = (ax2-ax1)*(ay2-ay1) + (bx2-bx1)*(by2-by1) - inter
        return inter / union if union > 0 else 0.0

    def _prefix(self, cls: str) -> str:
        return {'excavator': 'EX', 'truck': 'TR', 'transport_equipment': 'TR',
                'light_equipment': 'LE', 'bus': 'BU', 'car': 'CA'
                }.get(cls.lower(), cls[:2].upper())

    def _new_id(self, cls: str) -> str:
        n = self.counters.get(cls, 0)
        self.counters[cls] = n + 1
        return f"{self._prefix(cls)}-{n:03d}"

    def _class_group(self, cls: str) -> str:
        return 'vehicle' if cls.lower() in {
            'excavator', 'truck', 'transport_equipment',
            'light_equipment', 'bus', 'car'
        } else cls.lower()

    def get_stable_id(self, track_id: int, bbox: Tuple, raw_class: str, class_name: str) -> str:
        self.frame_num += 1
        group = self._class_group(class_name)

        # 1. Cached mapping for this session
        if track_id in self.cache:
            sid = self.cache[track_id]
            if sid in self.known:
                self.known[sid]['bbox']      = bbox
                self.known[sid]['last_seen'] = self.frame_num
                self.known[sid]['raw_class'] = raw_class
                self.known[sid]['class_name'] = class_name
            return sid

        # 2. IoU match with known equipment in the same broad group
        best_sid, best_iou = None, self.iou_threshold
        for sid, info in self.known.items():
            if self._class_group(info['class_name']) != group:
                continue
            if self.frame_num - info['last_seen'] > self.max_missing_frames:
                continue
            iou = self._iou(bbox, info['bbox'])
            if iou > best_iou:
                best_iou, best_sid = iou, sid

        if best_sid:
            self.cache[track_id] = best_sid
            self.known[best_sid].update(
                bbox=bbox,
                last_seen=self.frame_num,
                track_id=track_id,
                raw_class=raw_class,
                class_name=class_name,
            )
            return best_sid

        # 3. Truly new machine
        sid = self._new_id(class_name)
        self.known[sid] = dict(
            bbox=bbox,
            class_name=class_name,
            raw_class=raw_class,
            last_seen=self.frame_num,
            track_id=track_id,
        )
        self.cache[track_id] = sid
        logging.getLogger(__name__).info(
            f"New equipment registered: {sid}  (yolo_track_id={track_id})")
        return sid

    def cleanup_stale(self) -> None:
        cutoff = self.max_missing_frames * 2
        stale = [s for s, i in self.known.items()
                 if self.frame_num - i['last_seen'] > cutoff]
        for s in stale:
            del self.known[s]
        self.cache = {k: v for k, v in self.cache.items() if v in self.known}

    @property
    def active_count(self) -> int:
        return len(self.known)


# ──────────────────────────────────────────────────────────────────────────────
class EquipmentDetector:

    def __init__(self, model_path: str, confidence_threshold: float = 0.45):
        self.logger = logging.getLogger(__name__)
        self.confidence_threshold = confidence_threshold
        self.logger.info(f"Loading YOLO model: {model_path}")
        self.model = YOLO(model_path)

        self.class_mapping = {
            "car":   "light_equipment",
            "bus":   "transport_equipment",
            "truck": "excavator",
        }

        
        self.id_mapper = StableIDMapper(
            iou_threshold=float(os.getenv('IOU_THRESHOLD', '0.10')),
            max_missing_frames=int(os.getenv('MAX_MISSING_FRAMES', '45'))
        )
        self._frame_count = 0

    def detect(self, frame: np.ndarray) -> List[Detection]:
        self._frame_count += 1

        try:
            results = self.model.track(
                source=frame,
                tracker="bytetrack.yaml",
                persist=True,          # ← critical: keep ByteTrack state alive
                conf=self.confidence_threshold,
                verbose=False
            )

            detections = []

            if results and results[0].boxes is not None:
                boxes = results[0].boxes

                for i in range(len(boxes)):
                    bbox      = tuple(boxes.xyxy[i].cpu().numpy().astype(int))
                    x1,y1,x2,y2 = bbox

                    # track_id may be None if ByteTrack hasn't confirmed yet
                    if boxes.id is None:
                        continue
                    track_id  = int(boxes.id[i].cpu().numpy())
                    class_id  = int(boxes.cls[i].cpu().numpy())
                    confidence= float(boxes.conf[i].cpu().numpy())

                    if class_id not in EQUIPMENT_CLASSES:
                        continue

                    raw_class    = EQUIPMENT_CLASSES[class_id]
                    equip_class  = self.class_mapping.get(raw_class, raw_class)

                    # ✅ FIX: stable ID — never uses raw track_id as equipment_id
                    stable_id = self.id_mapper.get_stable_id(
                        track_id, (x1, y1, x2, y2), raw_class, equip_class)

                    detections.append(Detection(
                        bbox=(x1, y1, x2, y2),
                        track_id=track_id,
                        class_name=equip_class,
                        confidence=confidence,
                        equipment_id=stable_id,
                    ))

            # ✅ FIX: NO fallback detection.
            # The old _fallback_detection() created a fake EX-000 in EVERY frame
            # when YOLO found nothing → that's what filled the DB with ghost rows.
            # If there are no real detections just return empty list.

            # Periodic cleanup
            if self._frame_count % 150 == 0:
                self.id_mapper.cleanup_stale()

            return detections

        except Exception as e:
            self.logger.error(f"Detection error: {e}")
            return []