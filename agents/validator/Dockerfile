# Build context is the repo root (set in docker-compose).
FROM python:3.12-slim

WORKDIR /app

COPY agents/validator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY schemas /app/schemas
COPY agents/validator/ /app/

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    AGENT_NAME=auditor-validator

CMD ["python", "main.py"]
