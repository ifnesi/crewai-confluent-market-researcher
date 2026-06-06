# Build context is the repo root (set in docker-compose).
FROM python:3.12-slim

WORKDIR /app

COPY agents/report_creator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY schemas /app/schemas
COPY agents/report_creator/ /app/

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    AGENT_NAME=scribe-report-creator

CMD ["python", "main.py"]
