"""
cv_service/main.py

EagleVision Computer Vision Service.
Generates a synthetic construction-site scene (Cat-395 excavator + Cat-777
dump truck), streams JPEG frames to Redis, and publishes the full assessment
Kafka payload format including activity classification, motion_source, and
time-analytics per equipment unit.
"""

import json
import logging
import math
import os
import signal
import time

import cv2
import numpy as np
import redis
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER: str  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC: str   = os.getenv("KAFKA_TOPIC", "equipment-events")
REDIS_HOST: str    = os.getenv("REDIS_HOST", "redis")
REDIS_PORT: int    = int(os.getenv("REDIS_PORT", "6379"))
REDIS_CHANNEL: str = os.getenv("REDIS_FRAME_CHANNEL", "frames")
REDIS_FRAME_KEY: str = "latest_frame"
REDIS_FRAME_TTL: int = 5
TARGET_FPS: int    = int(os.getenv("TARGET_FPS", "15"))
KAFKA_EVERY_N: int = 5          # send Kafka event every N frames

# ---------------------------------------------------------------------------
# State / Activity constants  (matches assessment payload spec)
# ---------------------------------------------------------------------------
STATE_ACTIVE: str       = "ACTIVE"
STATE_INACTIVE: str     = "INACTIVE"

ACTIVITY_DIGGING: str   = "DIGGING"
ACTIVITY_SWINGING: str  = "SWINGING"
ACTIVITY_DUMPING: str   = "DUMPING"
ACTIVITY_WAITING: str   = "WAITING"
ACTIVITY_TRAVELLING: str = "TRAVELLING"

MOTION_ARM_ONLY: str    = "arm_only"
MOTION_FULL_BODY: str   = "full_body"
MOTION_NONE: str        = "none"

# ---------------------------------------------------------------------------
# Colour palette (BGR)
# ---------------------------------------------------------------------------
SKY_TOP     = (180, 140, 100)
SKY_BOT     = (210, 190, 150)
GROUND_DARK = (40,  55,  70)
GROUND_MID  = (55,  75,  90)
DUST_COLOR  = (160, 170, 180)
CAT_YELLOW  = (0, 180, 255)
CAT_DARK    = (0,  80, 140)
STEEL_GRAY  = (120, 120, 120)
DARK_STEEL  = (60,  60,  60)
GLASS_COLOR = (200, 210, 140)
DIRT_COLOR  = (40,  80, 120)
ROCK_COLOR  = (80,  90, 100)
SMOKE_COLOR = (210, 210, 220)
PIT_COLOR   = (25,  35,  45)

logging.basicConfig(
    format="%(asctime)s [cv_service] %(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-equipment time tracker
# ---------------------------------------------------------------------------

class EquipmentTracker:
    """Accumulates active / idle seconds for one piece of equipment."""

    def __init__(self, equip_id: str) -> None:
        self.equip_id = equip_id
        self.total_tracked: float = 0.0
        self.total_active:  float = 0.0
        self.total_idle:    float = 0.0
        self._last_tick:    float = time.time()

    def tick(self, state: str) -> None:
        """Call once per frame with the current STATE_ACTIVE / STATE_INACTIVE."""
        now = time.time()
        delta = now - self._last_tick
        self._last_tick = now
        self.total_tracked += delta
        if state == STATE_ACTIVE:
            self.total_active += delta
        else:
            self.total_idle += delta

    @property
    def utilization_percent(self) -> float:
        if self.total_tracked == 0:
            return 0.0
        return round(self.total_active / self.total_tracked * 100, 1)

    def time_analytics(self) -> dict:
        return {
            "total_tracked_seconds": round(self.total_tracked, 1),
            "total_active_seconds":  round(self.total_active,  1),
            "total_idle_seconds":    round(self.total_idle,    1),
            "utilization_percent":   self.utilization_percent,
        }


# ---------------------------------------------------------------------------
# Activity classifier  (rule-based from animation phase)
# ---------------------------------------------------------------------------

def classify_excavator(dig_phase: float) -> tuple[str, str, str]:
    """
    Derive activity, state, and motion_source from the excavator dig phase.

    Returns (activity, state, motion_source).
    The excavator arm is always moving so state is always ACTIVE;
    motion_source is arm_only because the tracks never translate.
    """
    phase_norm = dig_phase % (2 * math.pi) / (2 * math.pi)   # 0–1
    if phase_norm < 0.30:
        activity = ACTIVITY_DIGGING
    elif phase_norm < 0.55:
        activity = ACTIVITY_SWINGING
    elif phase_norm < 0.70:
        activity = ACTIVITY_DUMPING
    else:
        activity = ACTIVITY_DIGGING
    return activity, STATE_ACTIVE, MOTION_ARM_ONLY


def classify_truck(phase: int) -> tuple[str, str, str]:
    """
    Derive activity, state, and motion_source from the truck animation phase.

    Returns (activity, state, motion_source).
    """
    if phase < 80:                      # approaching
        return ACTIVITY_TRAVELLING, STATE_ACTIVE, MOTION_FULL_BODY
    elif phase < 90:                    # just arrived
        return ACTIVITY_WAITING, STATE_INACTIVE, MOTION_NONE
    elif phase < 160:                   # being loaded
        return ACTIVITY_DUMPING, STATE_ACTIVE, MOTION_ARM_ONLY
    elif phase < 170:                   # departing pause
        return ACTIVITY_WAITING, STATE_INACTIVE, MOTION_NONE
    else:                               # reversing out
        return ACTIVITY_TRAVELLING, STATE_ACTIVE, MOTION_FULL_BODY


# ---------------------------------------------------------------------------
# Drawing helpers
# ---------------------------------------------------------------------------

def filled_poly(img, pts, color):
    cv2.fillPoly(img, [np.array(pts, np.int32)], color)


def outlined_poly(img, pts, color, border=(0, 0, 0), t=1):
    filled_poly(img, pts, color)
    cv2.polylines(img, [np.array(pts, np.int32)], True, border, t)


def draw_wheel(img, cx, cy, r, color=DARK_STEEL):
    cv2.circle(img, (cx, cy), r,     color,     -1)
    cv2.circle(img, (cx, cy), r,     STEEL_GRAY, 1)
    cv2.circle(img, (cx, cy), r // 2, STEEL_GRAY, -1)
    cv2.circle(img, (cx, cy), r // 2, color,      1)
    for a in range(0, 360, 60):
        rad = math.radians(a)
        x2 = int(cx + (r * 0.75) * math.cos(rad))
        y2 = int(cy + (r * 0.75) * math.sin(rad))
        cv2.line(img, (cx, cy), (x2, y2), DARK_STEEL, 1)


def draw_hydraulic(img, x1, y1, x2, y2, w=5):
    cv2.line(img, (x1, y1), (x2, y2), STEEL_GRAY, w)
    cv2.line(img, (x1, y1), (x2, y2), (200, 200, 200), 2)


# ---------------------------------------------------------------------------
# Scene elements
# ---------------------------------------------------------------------------

def draw_background(img, W, H, dust_alpha, frame_num):
    for y in range(H // 2):
        t = y / (H // 2)
        c = tuple(int(SKY_TOP[i] * (1 - t) + SKY_BOT[i] * t) for i in range(3))
        cv2.line(img, (0, y), (W, y), c, 1)

    hill_pts = [(0, H // 2)]
    for x in range(0, W + 1, 20):
        hy = int(H // 2 - 40 * math.sin(x * 0.018 + 1) - 25 * math.sin(x * 0.031))
        hill_pts.append((x, hy))
    hill_pts.append((W, H // 2))
    filled_poly(img, hill_pts, GROUND_MID)

    ground_y = int(H * 0.52)
    cv2.rectangle(img, (0, ground_y), (W, H), GROUND_DARK, -1)

    pit_pts = [
        (int(W * 0.22), ground_y),   (int(W * 0.20), ground_y + 60),
        (int(W * 0.18), ground_y + 110), (int(W * 0.22), ground_y + 130),
        (int(W * 0.55), ground_y + 130), (int(W * 0.60), ground_y + 110),
        (int(W * 0.58), ground_y + 60),  (int(W * 0.56), ground_y),
    ]
    filled_poly(img, pit_pts, PIT_COLOR)

    rng = np.random.default_rng(frame_num // 8)
    for _ in range(18):
        rx = int(rng.integers(int(W * 0.22), int(W * 0.54)))
        ry = int(rng.integers(ground_y + 80, ground_y + 128))
        rr = int(rng.integers(3, 10))
        col = tuple(int(x) for x in rng.integers(30, 90, 3).tolist())
        cv2.circle(img, (rx, ry), rr, col, -1)

    if dust_alpha > 0.05:
        dust = np.zeros_like(img)
        rng2 = np.random.default_rng(frame_num // 3)
        for _ in range(int(40 * dust_alpha)):
            dx = int(rng2.integers(int(W * 0.15), int(W * 0.65)))
            dy = int(rng2.integers(ground_y - 20, ground_y + 80))
            dr = int(rng2.integers(4, 20))
            cv2.circle(dust, (dx, dy), dr, DUST_COLOR, -1)
        img[:] = cv2.addWeighted(img, 1.0, dust, dust_alpha * 0.55, 0)


def draw_excavator(img, W, H, frame_num):
    """Draw excavator and return detection dict with activity info."""
    ground_y = int(H * 0.52)
    base_x   = int(W * 0.48)
    base_y   = ground_y + 10

    track_w, track_h = 110, 28
    track_x = base_x - track_w // 2
    track_y = base_y + 55

    cv2.rectangle(img, (track_x, track_y), (track_x + track_w, track_y + track_h), DARK_STEEL, -1)
    cv2.rectangle(img, (track_x, track_y), (track_x + track_w, track_y + track_h), STEEL_GRAY, 1)

    link_offset = (frame_num * 3) % 12
    for lx in range(track_x, track_x + track_w, 12):
        ox = (lx + link_offset) % (track_x + track_w)
        if track_x <= ox <= track_x + track_w - 4:
            cv2.rectangle(img, (ox, track_y + 2), (ox + 9, track_y + track_h - 2), STEEL_GRAY, 1)

    draw_wheel(img, track_x + 14,           track_y + track_h // 2, 14, DARK_STEEL)
    draw_wheel(img, track_x + track_w - 14, track_y + track_h // 2, 14, DARK_STEEL)
    for rx in [track_x + 40, track_x + 66, track_x + 90]:
        draw_wheel(img, rx, track_y + track_h // 2, 7, DARK_STEEL)

    house_pts = [
        (base_x - 50, base_y + 55), (base_x - 52, base_y + 10),
        (base_x + 52, base_y + 10), (base_x + 50, base_y + 55),
    ]
    outlined_poly(img, house_pts, CAT_YELLOW, CAT_DARK)

    hood_pts = [
        (base_x - 10, base_y + 10), (base_x - 8,  base_y - 18),
        (base_x + 50, base_y - 18), (base_x + 50, base_y + 10),
    ]
    outlined_poly(img, hood_pts, CAT_YELLOW, CAT_DARK)

    ex_x, ex_y = base_x + 42, base_y - 18
    cv2.rectangle(img, (ex_x, ex_y - 22), (ex_x + 6, ex_y), DARK_STEEL, -1)
    smoke_alpha = 0.3 + 0.2 * math.sin(frame_num * 0.15)
    for si in range(4):
        sy = ex_y - 30 - si * 14
        sr = 5 + si * 4
        sx = ex_x + 3 + si * 3
        overlay = img.copy()
        cv2.circle(overlay, (sx, sy), sr, SMOKE_COLOR, -1)
        img[:] = cv2.addWeighted(img, 1 - smoke_alpha * 0.3, overlay, smoke_alpha * 0.3, 0)

    cab_pts = [
        (base_x - 52, base_y + 10), (base_x - 52, base_y - 30),
        (base_x - 10, base_y - 34), (base_x - 8,  base_y + 10),
    ]
    outlined_poly(img, cab_pts, CAT_YELLOW, CAT_DARK)

    glass_pts = [
        (base_x - 49, base_y + 8),  (base_x - 49, base_y - 26),
        (base_x - 13, base_y - 30), (base_x - 11, base_y + 8),
    ]
    filled_poly(img, glass_pts, GLASS_COLOR)
    cv2.rectangle(img, (base_x - 52, base_y + 40), (base_x + 52, base_y + 48), CAT_DARK, -1)

    dig_phase   = (frame_num * 0.04) % (2 * math.pi)
    swing       = math.sin(dig_phase)
    boom_base   = (base_x - 30, base_y + 5)
    boom_angle  = -0.9 + 0.25 * swing
    boom_len    = 120
    boom_tip    = (
        int(boom_base[0] + boom_len * math.sin(boom_angle)),
        int(boom_base[1] - boom_len * math.cos(boom_angle)),
    )
    stick_angle = boom_angle - 0.5 + 0.4 * math.sin(dig_phase + 1.0)
    stick_len   = 90
    stick_tip   = (
        int(boom_tip[0] + stick_len * math.sin(stick_angle)),
        int(boom_tip[1] - stick_len * math.cos(stick_angle)),
    )
    bucket_angle = stick_angle + 0.8 - 0.6 * math.sin(dig_phase + 1.8)
    bucket_len   = 35
    bucket_tip   = (
        int(stick_tip[0] + bucket_len * math.sin(bucket_angle)),
        int(stick_tip[1] - bucket_len * math.cos(bucket_angle)),
    )

    draw_hydraulic(img, boom_base[0] + 10, boom_base[1], int(boom_tip[0] - 15), int(boom_tip[1] + 20), 4)
    draw_hydraulic(img, boom_tip[0],  boom_tip[1],  int(stick_tip[0] + 5),  int(stick_tip[1] + 10), 3)
    draw_hydraulic(img, stick_tip[0], stick_tip[1], int(bucket_tip[0] + 5), int(bucket_tip[1]),      3)

    cv2.line(img, boom_base, boom_tip,  CAT_YELLOW, 14)
    cv2.line(img, boom_base, boom_tip,  CAT_DARK,    2)
    cv2.line(img, boom_tip,  stick_tip, CAT_YELLOW, 10)
    cv2.line(img, boom_tip,  stick_tip, CAT_DARK,    2)

    bp = bucket_tip
    bw = 24
    bucket_pts = [
        stick_tip,
        (bp[0] - bw,     bp[1] - 5),
        (bp[0] - bw + 5, bp[1] + 18),
        (bp[0] + bw - 5, bp[1] + 18),
        (bp[0] + bw,     bp[1] - 5),
    ]
    outlined_poly(img, bucket_pts, DARK_STEEL, STEEL_GRAY, 1)

    if math.sin(dig_phase + 1.8) > 0.2:
        dirt_pts = [
            (bp[0] - bw + 6, bp[1] + 2),  (bp[0] - bw + 8, bp[1] + 16),
            (bp[0] + bw - 8, bp[1] + 16), (bp[0] + bw - 6, bp[1] + 2),
        ]
        filled_poly(img, dirt_pts, DIRT_COLOR)

    for pt in [boom_base, boom_tip, stick_tip]:
        cv2.circle(img, pt, 5, STEEL_GRAY, -1)
        cv2.circle(img, pt, 5, DARK_STEEL,  1)

    activity, state, motion_src = classify_excavator(dig_phase)
    util = round(75 + 20 * abs(math.sin(dig_phase)), 1)

    return {
        "id":            "EXC-001",
        "class":         "excavator",
        "state":         state,
        "activity":      activity,
        "motion_source": motion_src,
        "util":          util,
        "dig_phase":     dig_phase,
    }


def draw_dump_truck(img, W, H, frame_num):
    """Draw dump truck and return detection dict with activity info."""
    ground_y = int(H * 0.52)
    cycle    = 240
    phase    = frame_num % cycle

    if phase < 80:
        tx = int(W * 0.02 + phase * 1.6)
    elif phase < 160:
        tx = int(W * 0.02 + 80 * 1.6)
    else:
        tx = int(W * 0.02 + (240 - phase) * 1.6)

    ty      = ground_y + 5
    truck_w = 160
    truck_h = 50
    cab_w   = 48
    cab_h   = 62
    wheel_r = 20

    tilt = 0.0
    if 100 < phase < 155:
        tilt = 0.06 * math.sin((phase - 100) * math.pi / 55)

    body_pts_base = np.array([
        [tx,          ty],
        [tx + truck_w, ty],
        [tx + truck_w, ty + truck_h],
        [tx,          ty + truck_h],
    ], dtype=np.float32)

    pivot  = np.array([tx + truck_w, ty + truck_h // 2], dtype=np.float32)
    cos_t, sin_t = math.cos(tilt), math.sin(tilt)
    body_pts = []
    for p in body_pts_base:
        d  = p - pivot
        rp = np.array([d[0] * cos_t - d[1] * sin_t,
                       d[0] * sin_t + d[1] * cos_t]) + pivot
        body_pts.append((int(rp[0]), int(rp[1])))

    outlined_poly(img, body_pts, CAT_YELLOW, CAT_DARK, 2)

    load_level = 0.4
    if 90 < phase < 160:
        load_level = 0.4 + 0.5 * min(1.0, (phase - 90) / 40)
    dirt_top = ty + int(truck_h * (1 - load_level))
    dirt_pts = [
        body_pts[0],
        (body_pts[0][0], dirt_top),
        (body_pts[1][0], dirt_top - int(6 * math.sin((phase - 90) * 0.15))),
        body_pts[1],
    ]
    if load_level > 0.41:
        filled_poly(img, dirt_pts, DIRT_COLOR)
        rng = np.random.default_rng(phase // 5)
        for _ in range(8):
            rx = int(rng.integers(tx + 10, tx + truck_w - 10))
            ry = int(rng.integers(dirt_top - 8, dirt_top + 2))
            rr = int(rng.integers(3, 8))
            cv2.circle(img, (rx, ry), rr, ROCK_COLOR, -1)

    cab_x   = tx + truck_w
    cab_pts = [
        (cab_x,            ty + truck_h),
        (cab_x,            ty + truck_h - cab_h),
        (cab_x + cab_w - 8, ty + truck_h - cab_h),
        (cab_x + cab_w,    ty + truck_h - cab_h + 18),
        (cab_x + cab_w,    ty + truck_h),
    ]
    outlined_poly(img, cab_pts, CAT_YELLOW, CAT_DARK, 2)

    ws_pts = [
        (cab_x + 4,        ty + truck_h - cab_h + 4),
        (cab_x + cab_w - 10, ty + truck_h - cab_h + 20),
        (cab_x + cab_w - 4,  ty + truck_h - cab_h + 36),
        (cab_x + 4,          ty + truck_h - cab_h + 36),
    ]
    filled_poly(img, ws_pts, GLASS_COLOR)

    wheel_y = ty + truck_h + wheel_r - 4
    for wx in [tx + 25, tx + truck_w - 25, cab_x + 15, cab_x + cab_w - 10]:
        draw_wheel(img, wx, wheel_y, wheel_r)

    ex_x = cab_x + cab_w - 5
    ex_y = ty + truck_h - cab_h - 5
    cv2.rectangle(img, (ex_x, ex_y - 18), (ex_x + 5, ex_y), DARK_STEEL, -1)
    if 90 < phase < 165:
        for si in range(3):
            overlay = img.copy()
            cv2.circle(overlay, (ex_x + 2, ex_y - 25 - si * 12), 4 + si * 3, SMOKE_COLOR, -1)
            img[:] = cv2.addWeighted(img, 0.85, overlay, 0.15, 0)

    activity, state, motion_src = classify_truck(phase)
    util = 60.0 if (phase < 80 or phase > 160) else 95.0

    return {
        "id":            "TRK-001",
        "class":         "truck",
        "state":         state,
        "activity":      activity,
        "motion_source": motion_src,
        "util":          util,
    }


def draw_hud(img, W, H, frame_num, detections):
    """Overlay HUD: frame counter, timestamp, and per-equipment label with activity."""
    header = f"EagleVision | Frame {frame_num:05d}"
    cv2.putText(img, header, (10, 22),
                cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 255, 255), 1, cv2.LINE_AA)
    ts = time.strftime("%H:%M:%S")
    cv2.putText(img, ts, (W - 80, 22),
                cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 255, 255), 1, cv2.LINE_AA)
    y = 45
    for d in detections:
        label = f"{d['id']} [{d['class']}] {d['activity']} {d['util']:.0f}%"
        cv2.putText(img, label, (10, y),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.48, (0, 255, 120), 1, cv2.LINE_AA)
        y += 18


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def ensure_topic(broker: str, topic: str) -> None:
    """Create Kafka topic if it does not already exist."""
    admin = AdminClient({"bootstrap.servers": broker})
    meta  = admin.list_topics(timeout=10)
    if topic not in meta.topics:
        futures = admin.create_topics(
            [NewTopic(topic, num_partitions=3, replication_factor=1)])
        for t, fut in futures.items():
            try:
                fut.result()
                logger.info("Created Kafka topic: %s", t)
            except Exception as exc:
                if "ALREADY_EXISTS" not in str(exc):
                    raise
    else:
        logger.info("Kafka topic ready: %s", topic)


def send_event(
    producer: Producer,
    topic: str,
    detection: dict,
    frame_id: int,
    tracker: EquipmentTracker,
    timestamp: str,
) -> None:
    """Produce the full assessment-format JSON payload to Kafka."""
    payload = {
        "frame_id":       frame_id,
        "equipment_id":   detection["id"],
        "equipment_class": detection["class"],
        "timestamp":      timestamp,
        "utilization": {
            "current_state":    detection["state"],
            "current_activity": detection["activity"],
            "motion_source":    detection["motion_source"],
        },
        "time_analytics": tracker.time_analytics(),
    }
    producer.produce(
        topic,
        key=detection["id"].encode(),
        value=json.dumps(payload).encode(),
    )
    producer.poll(0)


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def publish_frame(r: redis.Redis, channel: str, frame) -> None:
    """Encode frame as JPEG and store/publish to Redis."""
    try:
        _, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        data = buf.tobytes()
        r.set(REDIS_FRAME_KEY, data, ex=REDIS_FRAME_TTL)
        r.publish(channel, data)
    except redis.RedisError as exc:
        logger.warning("Redis publish error: %s", exc)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run() -> None:
    """Initialise all connections and start the frame-generation loop."""
    logger.info("Starting CV service...")
    logger.info("Mock detection system active — activity classification enabled")

    ensure_topic(KAFKA_BROKER, KAFKA_TOPIC)

    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "cv-service",
        "linger.ms": 50,
    })

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

    # Per-equipment time trackers
    trackers: dict[str, EquipmentTracker] = {
        "EXC-001": EquipmentTracker("EXC-001"),
        "TRK-001": EquipmentTracker("TRK-001"),
    }

    W, H     = 640, 480
    frame_id = 0
    running  = {"val": True}

    def _sig(*_) -> None:
        running["val"] = False

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)

    logger.info("Processing frame loop started at %d FPS", TARGET_FPS)

    while running["val"]:
        t0 = time.time()
        frame_id += 1

        img  = np.zeros((H, W, 3), dtype=np.uint8)
        dust = 0.15 + 0.10 * math.sin(frame_id * 0.05)

        draw_background(img, W, H, dust, frame_id)
        detections = [
            draw_dump_truck(img, W, H, frame_id),
            draw_excavator(img, W, H, frame_id),
        ]
        draw_hud(img, W, H, frame_id, detections)
        publish_frame(r, REDIS_CHANNEL, img)

        # Update time trackers every frame
        for det in detections:
            trackers[det["id"]].tick(det["state"])

        # Send Kafka events every N frames
        if frame_id % KAFKA_EVERY_N == 0:
            ts = time.strftime("%H:%M:%S.000")
            for det in detections:
                send_event(producer, KAFKA_TOPIC, det, frame_id, trackers[det["id"]], ts)
            logger.info("Processing frame %d — sent %d Kafka events", frame_id, len(detections))

        elapsed = time.time() - t0
        time.sleep(max(0.0, 1.0 / TARGET_FPS - elapsed))

    producer.flush()
    logger.info("CV service stopped.")


if __name__ == "__main__":
    run()