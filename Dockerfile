FROM python:3.11-slim

# Work inside /app
WORKDIR /app

# Prevent Python from writing .pyc files and use unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install system deps (minimal; composite doesn't need DB client libs)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Cloud Run expects the container to listen on $PORT (default 8080)
EXPOSE 8080

# Run the FastAPI app with Uvicorn
CMD uvicorn main:app --host 0.0.0.0 --port $PORT
