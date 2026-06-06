"""Small shared helpers."""
from __future__ import annotations

import time
import uuid

# Avro strings are fine with large payloads, but keep a sane ceiling so a runaway
# prompt/response can't produce an unbounded Kafka message.
MAX_LOG_CHARS = 200_000


def now_ms() -> int:
    """Current time in epoch milliseconds (matches the Avro timestamp fields)."""
    return int(time.time() * 1000)


def new_report_id() -> str:
    # Short, readable correlation id (e.g. "3b94941b"). 8 hex chars is plenty
    # to disambiguate concurrent reports in this demo.
    return uuid.uuid4().hex[:8]


def clip(text: str, limit: int = MAX_LOG_CHARS) -> str:
    if text is None:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n…[truncated {len(text) - limit} chars]"
