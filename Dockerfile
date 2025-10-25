FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app

RUN apt-get update -y && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# requirements.txt is in site_backend (your current working dir), copy it in
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# ⬅️ key change: copy the CURRENT DIR into /app/site_backend
COPY . /app/site_backend

# launch uvicorn against the package path
CMD ["sh","-c","python -m uvicorn site_backend.main:app --host 0.0.0.0 --port ${PORT:-8080}"]
