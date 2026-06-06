# Build context is the repo root (set in docker-compose).
FROM python:3.12-slim

WORKDIR /app

COPY ui/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY schemas /app/schemas
COPY ui/ /app/

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PORT=8088

EXPOSE 8088

CMD ["python", "app.py"]
