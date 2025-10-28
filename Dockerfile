FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

RUN apt-get update -y && apt-get install -y --no-install-recommends build-essential \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ⬅️ copy repo to /app so /app/site_backend exists
COPY . .

# optional: make it explicit
ENV PYTHONPATH=/app

CMD sh -c 'uvicorn site_backend.main:app --host 0.0.0.0 --port ${PORT:-8000} --lifespan off'
