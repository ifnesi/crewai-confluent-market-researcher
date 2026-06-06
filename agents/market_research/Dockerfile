# Build context is the repo root (set in docker-compose) so we can copy the
# shared common/ package and the schemas alongside the agent code.
FROM python:3.12-slim

WORKDIR /app

COPY agents/market_research/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY schemas /app/schemas
COPY agents/market_research/ /app/

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    AGENT_NAME=scout-market-research

CMD ["python", "main.py"]
