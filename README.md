# 🦅 EagleVision

Real-time construction equipment utilization monitoring system using computer vision and microservices architecture.

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Video File    │───▶│   cv_service    │───▶│     Kafka       │
│  (input.mp4)    │    │  (YOLOv8 + CV)  │    │   (events)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Dashboard     │◀───│ analytics_svc   │◀───│  TimescaleDB    │
│  (Streamlit)    │    │  (Consumer)     │    │   (storage)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       ▲
        │                       │
        ▼                       │
┌─────────────────┐              │
│     Redis       │◀─────────────┘
│  (frame pub/sub)│
└─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- 8GB+ RAM recommended
- Python 3.11+ (for local development)

### 1. Setup

```bash
# Clone the repository
git clone <repository-url>
cd eaglevision

# Copy environment configuration
cp .env.example .env

# Place test video (see Getting Test Data section)
# or generate synthetic test video
cd cv_service
python generate_test_video.py
cd ..
```

### 2. Run

```bash
# Start all services
docker compose up --build

# Open dashboard in browser
# Navigate to: http://localhost:8501
```

### 3. Expected Behavior

- **cv_service**: Logs "Loaded YOLOv8 model", "Processing frame N"
- **analytics_service**: Logs "Connected to DB", "Consumed message: EX-001"
- **Dashboard**: Shows live video feed + equipment utilization cards

## 📹 Getting Test Data

### Option 1: Roberts & Golparvar-Fard Dataset (Recommended)

Download the benchmark dataset:
```bash
# URL: https://data.mendeley.com/datasets/fyw6ps2d2j/1
# Place any .mp4 from the dataset at:
cp your-video.mp4 cv_service/data/input.mp4
```

### Option 2: YouTube Video

Download excavator footage:
```bash
# Install yt-dlp
pip install yt-dlp

# Download video
yt-dlp -o cv_service/data/input.mp4 "<youtube-url>"
# Search: "excavator digging loading dump truck site camera"
```

### Option 3: Synthetic Test Video

Generate test video programmatically:
```bash
cd cv_service
python generate_test_video.py
# Creates: data/input.mp4 (30s, simulated excavator movement)
```

## 🔧 Configuration

### Environment Variables (.env)

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=equipment-events
KAFKA_GROUP_ID=analytics-group

# TimescaleDB
POSTGRES_HOST=timescaledb
POSTGRES_PORT=5432
POSTGRES_DB=eaglevision
POSTGRES_USER=ev_user
POSTGRES_PASSWORD=ev_pass

# Redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_FRAME_CHANNEL=frames

# CV Service
VIDEO_SOURCE=data/input.mp4
CONFIDENCE_THRESHOLD=0.45
MOTION_THRESHOLD=2.5
FRAME_SKIP=2
TARGET_FPS=15

# Model
YOLO_MODEL=yolov8n.pt
```

### Model Customization

To use a custom fine-tuned YOLO model:

1. Place model file in `cv_service/`
2. Update `.env`: `YOLO_MODEL=your-custom-model.pt`
3. Update class mapping in `cv_service/detector.py`

**Note**: COCO dataset doesn't have dedicated excavator class. The system uses truck class (ID 7) as proxy. For production use, train a custom model on construction equipment data.

## 🧠 Technical Details

### Computer Vision Pipeline

1. **Detection**: YOLOv8 with ByteTrack for object tracking
2. **Motion Analysis**: Zone-based optical flow (Farneback algorithm)
   - Upper zone: Arm/body movement
   - Lower zone: Tracks movement
3. **Activity Classification**: Rule-based state machine
   - **DIGGING**: Vertical downward motion (vy > vx * 1.5, vy > 0)
   - **SWINGING**: Horizontal motion (vx > vy * 1.2)
   - **DUMPING**: Vertical upward motion (vy > vx * 1.5, vy < 0)
   - **WAITING**: No significant motion
4. **Debouncing**: 3-frame stability requirement for state changes

### Data Flow

```
Frame → Detection → Motion Analysis → Activity Classification → Time Tracking → Kafka Event
                                                                                     ↓
                                                                                 TimescaleDB
                                                                                     ↓
                                                                                 Dashboard
```

### Kafka Event Schema

```json
{
  "frame_id": 450,
  "equipment_id": "EX-001",
  "equipment_class": "excavator",
  "timestamp": "00:00:15.000",
  "utilization": {
    "current_state": "ACTIVE",
    "current_activity": "DIGGING",
    "motion_source": "arm_only"
  },
  "time_analytics": {
    "total_tracked_seconds": 15.0,
    "total_active_seconds": 12.5,
    "total_idle_seconds": 2.5,
    "utilization_percent": 83.3
  }
}
```

## 📊 Dashboard Features

- **Live Video Feed**: Real-time annotated video via Redis pub/sub
- **Equipment Cards**: Status, utilization %, active/idle time
- **Activity Tracking**: Current activity and motion source
- **System Status**: Database and Redis connection health
- **Auto-refresh**: 1-second updates

## 🐛 Troubleshooting

### Common Issues

1. **Video not found**
   ```
   ERROR: Video file not found: cv_service/data/input.mp4
   ```
   **Solution**: Place video file or run `generate_test_video.py`

2. **Kafka connection timeout**
   ```
   WARNING: Kafka connection attempt X/30 failed
   ```
   **Solution**: Wait for Kafka to fully start (can take 30-60s)

3. **Database connection failed**
   ```
   ERROR: Failed to connect to database
   ```
   **Solution**: Check TimescaleDB container logs

4. **No detections**
   ```
   DEBUG: No detections found
   ```
   **Solution**: 
   - Lower `CONFIDENCE_THRESHOLD` in .env
   - Use video with clear equipment visibility
   - Try custom-trained model

### Debug Mode

Enable debug logging:
```bash
# Add to .env
DEBUG=1

# Or set environment variable
export DEBUG=1
docker compose up --build
```

### Performance Tuning

- **Frame Processing**: Adjust `FRAME_SKIP` to reduce CPU load
- **Detection Accuracy**: Tune `CONFIDENCE_THRESHOLD`
- **Motion Sensitivity**: Adjust `MOTION_THRESHOLD`
- **Output FPS**: Modify `TARGET_FPS`

## 🔒 Known Limitations

1. **Model Accuracy**: Using COCO truck class as excavator proxy
2. **Lighting Sensitivity**: Optical flow affected by lighting changes
3. **Occlusion Handling**: Basic tracking, may lose equipment on occlusion
4. **Scale Sensitivity**: Performance varies with camera distance/angle
5. **Single Video Source**: Processes one video file at a time

## 🚧 Production Considerations

1. **Custom Model Training**: Train YOLO on construction equipment dataset
2. **Multi-Camera Support**: Extend for multiple video sources
3. **Alert System**: Add notifications for low utilization
4. **Historical Analytics**: Extended reporting and trend analysis
5. **Security**: Add authentication and authorization
6. **Scalability**: Horizontal scaling for multiple sites

## 📁 Project Structure

```
eaglevision/
├── docker-compose.yml          # Service orchestration
├── .env.example               # Environment template
├── README.md                  # This file
├── .gitignore                 # Git ignore rules
│
├── cv_service/                # Computer vision microservice
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                # Entry point
│   ├── detector.py            # YOLOv8 + ByteTrack
│   ├── motion_analyzer.py     # Optical flow analysis
│   ├── activity_classifier.py # Activity state machine
│   ├── kafka_producer.py      # Event publishing
│   ├── frame_publisher.py     # Redis video streaming
│   ├── generate_test_video.py # Synthetic video generator
│   └── data/
│       └── .gitkeep
│
├── analytics_service/         # Analytics microservice
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                # Entry point
│   ├── consumer.py            # Kafka consumer
│   └── db.py                   # TimescaleDB interface
│
├── dashboard/                 # Streamlit UI
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py                  # Dashboard application
│
└── db/                        # Database initialization
    └── init.sql                # Schema + hypertable creation
```




