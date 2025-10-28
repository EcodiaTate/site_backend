# === Canonical Ecodia site_backend Dockerfile ===
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (keep lean)
RUN apt-get update -y && apt-get install -y --no-install-recommends build-essential \
  && rm -rf /var/lib/apt/lists/*

# Python deps first for better caching
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# App code: repo root -> /app so package is /app/site_backend
COPY . .

# Start the ASGI app
# - Cloud Run will set PORT (you can keep your service at 8000 if you prefer)
# - Locally, it defaults to 8000
CMD sh -c 'uvicorn site_backend.main:app --host 0.0.0.0 --port ${PORT:-8000} --lifespan off'
