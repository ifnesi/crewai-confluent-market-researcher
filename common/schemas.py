"""Locate and load the Avro schema (.avsc) for each topic.

The ``schemas/`` directory is copied into every image next to ``common/``; set
SCHEMA_DIR to override (e.g. when running from the host).
"""
from __future__ import annotations

import os
from pathlib import Path

from . import settings

SCHEMA_DIR: Path = Path(
    os.getenv("SCHEMA_DIR", str(Path(__file__).resolve().parent.parent / "schemas"))
)

# Topic -> .avsc filename (value schema; TopicNameStrategy subject is "<topic>-value").
TOPIC_SCHEMA_FILE: dict[str, str] = {
    settings.TOPIC_UI_REQUEST: "ui_request_report.avsc",
    settings.TOPIC_MARKET_RESEARCH: "agent_market_research.avsc",
    settings.TOPIC_RESEARCH_READY: "agent_market_research_ready.avsc",
    settings.TOPIC_REPORT_READY: "agent_report_ready.avsc",
    settings.TOPIC_LOGS: "logs.avsc",
}


def schema_path(topic: str) -> Path:
    try:
        return SCHEMA_DIR / TOPIC_SCHEMA_FILE[topic]
    except KeyError as exc:  # pragma: no cover - defensive
        raise KeyError(f"No Avro schema registered for topic {topic!r}") from exc


def load_schema_str(topic: str) -> str | None:
    """Return the raw Avro schema JSON string for a topic's value.

    Returns ``None`` for topics with no local .avsc (e.g. ``crewai-logs-stats``,
    whose schema is owned and registered by the Flink job). Consumers then
    deserialize against the writer schema resolved from Schema Registry by id,
    so no reader schema is needed; producers only ever target mapped topics.
    """
    fname = TOPIC_SCHEMA_FILE.get(topic)
    if fname is None:
        return None
    return (SCHEMA_DIR / fname).read_text(encoding="utf-8")
