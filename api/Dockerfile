# Path: api/Dockerfile
# Purpose: Standard Python Container for Cloud Run FastAPI application deployment
# Idempotent: true
# Dependencies: requirements.txt, main.py, rag_service.py

FROM python:3.11-slim

WORKDIR /app

# Upgrade pip and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY main.py .
COPY rag_service.py .

# Standardize PORT per Cloud Run specs, fallback to 8080 default
ENV PORT=8080

# Ensure Uvicorn binds to the correct port securely via Cloud Run native syntax
CMD uvicorn main:app --host 0.0.0.0 --port $PORT
