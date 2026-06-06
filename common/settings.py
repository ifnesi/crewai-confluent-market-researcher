"""Central configuration, read from the environment with container-friendly
defaults. Hostnames default to the docker-compose service names, so the agents
and UI work out of the box on the compose network; override via env for local
runs.
"""
from __future__ import annotations

import os

# --- Kafka / Schema Registry -------------------------------------------------
# Inside the compose network the broker advertises broker:29092 (PLAINTEXT);
# from the host it is localhost:9092.
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
SCHEMA_REGISTRY_URL: str = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

# --- Topics (single partition each; key = username UTF-8, value = Avro) ------
TOPIC_UI_REQUEST: str = "crewai-ui-request-report"
TOPIC_MARKET_RESEARCH: str = "crewai-agent-market-research"
TOPIC_RESEARCH_READY: str = "crewai-agent-market-research-ready"
TOPIC_REPORT_READY: str = "crewai-agent-report-ready"
TOPIC_LOGS: str = "crewai-logs"

# --- Web search / MCP --------------------------------------------------------
SEARXNG_URL: str = os.getenv("SEARXNG_URL", "http://searxng:8080")
MCP_SERVER_URL: str = os.getenv("MCP_SERVER_URL", "http://mcp-server:8000/mcp")

# --- AWS Bedrock (consumed by LiteLLM / boto3) -------------------------------
# LiteLLM reads AWS_REGION_NAME; boto3 prefers AWS_DEFAULT_REGION. Mirror them.
AWS_REGION_NAME: str = os.getenv("AWS_REGION_NAME", "eu-west-1")
os.environ.setdefault("AWS_REGION_NAME", AWS_REGION_NAME)
os.environ.setdefault("AWS_DEFAULT_REGION", AWS_REGION_NAME)

# Model IDs are LiteLLM/Bedrock strings. In eu-west-1, Claude is reached through
# EU cross-region *inference profiles*, hence the `eu.` prefix. These defaults
# must match models you have enabled under Bedrock → Model access in eu-west-1;
# override per agent via the env vars below if your account differs.
BEDROCK_MODEL_RESEARCH: str = os.getenv(
    "BEDROCK_MODEL_RESEARCH", "bedrock/eu.anthropic.claude-sonnet-4-6"
)
BEDROCK_MODEL_VALIDATOR: str = os.getenv(
    "BEDROCK_MODEL_VALIDATOR", "bedrock/eu.anthropic.claude-sonnet-4-6"
)
BEDROCK_MODEL_REPORT: str = os.getenv(
    "BEDROCK_MODEL_REPORT", "bedrock/eu.anthropic.claude-opus-4-8"
)

# --- Choreography knobs ------------------------------------------------------
# Validator forces a report once the research has looped this many times.
MAX_RESEARCH_ITERATIONS: int = int(os.getenv("MAX_RESEARCH_ITERATIONS", "2"))

# Suggested authoritative sources injected into the research prompt.
SUGGESTED_SOURCES: list[str] = [
    "https://www.gartner.com",
    "https://www.mckinsey.com",
    "https://www.cbinsights.com",
    "https://www.crunchbase.com",
    "https://news.crunchbase.com",
    "https://techcrunch.com",
    "https://www.forrester.com",
    "https://hbr.org",
    "https://www.statista.com",
]
