# === canonical for repo-root-as-app ===
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# minimal build deps
RUN apt-get update -y && apt-get install -y --no-install-recommends build-essential \
  && rm -rf /var/lib/apt/lists/*

# deps first for cache
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# copy the WHOLE repo into /app  (your app files live directly here)
COPY . .

# start: import "main:app" from /app, listen on PORT (8000 locally by default)
CMD sh -c 'uvicorn main:app --app-dir /app --host 0.0.0.0 --port ${PORT:-8000} --lifespan off'
