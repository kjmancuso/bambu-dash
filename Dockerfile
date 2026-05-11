FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app.py .
COPY templates/ ./templates/

RUN useradd --create-home --uid 1000 app
USER app

EXPOSE 5000

# Single worker = single CameraBroker shared across MJPEG subscribers.
# Threads handle concurrent /api/status, /stream.mjpg, and /healthz callers.
# timeout=0 disables gunicorn's request timeout (MJPEG streams are long-lived).
CMD ["gunicorn", \
     "--bind", "0.0.0.0:5000", \
     "--workers", "1", \
     "--threads", "16", \
     "--worker-class", "gthread", \
     "--timeout", "0", \
     "--access-logfile", "-", \
     "app:app"]
