# Use python 3.11 slim as base image [cite: 1]
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies for OpenCV and graphics
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Upgrade pip and install dependencies [cite: 2]
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Ensure data directory exists [cite: 3]
RUN mkdir -p /app/data

# Start the computer vision service
CMD ["python", "main.py"]