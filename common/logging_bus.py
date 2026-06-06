"""Publish every LLM prompt and response to the ``crewai-logs`` topic.

CrewAI 1.x calls Bedrock through its *native* provider (boto3), not LiteLLM, so
we hook CrewAI's own event bus rather than a LiteLLM callback. The bus emits an
``LLMCallStartedEvent`` (prompt) and an ``LLMCallCompletedEvent`` (response +
token usage) around every model call.

Per-message context (which agent, which report_id, which username) travels via a
ContextVar the agent sets right before invoking its crew. CrewAI runs the crew
synchronously on the calling thread, so the ContextVar is visible to the events.
"""
from __future__ import annotations

import contextvars
import json
import logging
import os

from . import settings
from .kafka_io import KafkaAvro
from .util import clip, now_ms

log = logging.getLogger(__name__)

_ctx: contextvars.ContextVar[dict] = contextvars.ContextVar(
    "crewai_log_ctx",
    default={
        "agent_name": os.getenv("AGENT_NAME", "unknown-agent"),
        "report_id": "unknown",
        "username": "unknown",
    },
)

_kafka: KafkaAvro | None = None
_listener = None  # keep a strong reference so it isn't garbage-collected


def _producer() -> KafkaAvro:
    global _kafka
    if _kafka is None:
        _kafka = KafkaAvro()
    return _kafka


def set_log_context(*, agent_name: str, report_id: str, username: str) -> None:
    _ctx.set(
        {"agent_name": agent_name, "report_id": report_id, "username": username}
    )


def _emit(*, log_type: str, data: str, model: str, tokens: int | None) -> None:
    ctx = _ctx.get()
    record = {
        "agent_name": ctx["agent_name"],
        "report_id": ctx["report_id"],
        "timestamp": now_ms(),
        "type": log_type,  # "input" | "output"
        "data": clip(data),
        "tokens": tokens,
        "model": model,
    }
    try:
        _producer().produce(settings.TOPIC_LOGS, key=ctx["username"], value=record)
        _producer().flush(5)
    except Exception:  # noqa: BLE001 - logging must never break the agent
        log.exception("failed to publish to %s", settings.TOPIC_LOGS)


def _messages_to_text(messages) -> str:
    if isinstance(messages, str):
        return messages
    parts = []
    for m in messages or []:
        role = m.get("role", "?") if isinstance(m, dict) else "?"
        content = m.get("content", "") if isinstance(m, dict) else str(m)
        if isinstance(content, list):  # tool/multimodal content blocks
            content = json.dumps(content, default=str)
        parts.append(f"[{role}]\n{content}")
    return "\n\n".join(parts)


def _response_text(response) -> str:
    if response is None:
        return ""
    if isinstance(response, str):
        return response
    for attr in ("content", "text", "raw"):
        val = getattr(response, attr, None)
        if isinstance(val, str):
            return val
    return str(response)


def _estimate_tokens(text: str) -> int:
    """Rough token estimate for prompts (~4 characters per token).

    The LLMCallStartedEvent fires before the provider reports usage, so for the
    input side we approximate from the prompt size rather than leave it blank.
    """
    return max(1, round(len(text or "") / 4))


def _tokens(usage) -> int | None:
    if usage is None:
        return None
    for key in ("total_tokens", "completion_tokens"):
        if isinstance(usage, dict) and usage.get(key) is not None:
            return int(usage[key])
        val = getattr(usage, key, None)
        if val is not None:
            try:
                return int(val)
            except (TypeError, ValueError):
                return None
    return None


def install() -> None:
    """Register the Kafka logger with CrewAI's event bus. Call once at startup."""
    global _listener
    try:
        from crewai.events import (
            BaseEventListener,
            LLMCallCompletedEvent,
            LLMCallStartedEvent,
        )
    except Exception:  # noqa: BLE001
        log.warning("crewai.events not available; LLM logging disabled")
        return

    class _KafkaLogListener(BaseEventListener):
        def setup_listeners(self, crewai_event_bus):  # noqa: D401
            @crewai_event_bus.on(LLMCallStartedEvent)
            def _on_start(source, event):  # noqa: ANN001
                data = _messages_to_text(getattr(event, "messages", None))
                _emit(
                    log_type="input",
                    data=data,
                    model=str(getattr(event, "model", "")),
                    tokens=_estimate_tokens(data),
                )

            @crewai_event_bus.on(LLMCallCompletedEvent)
            def _on_done(source, event):  # noqa: ANN001
                data = _response_text(getattr(event, "response", None))
                # Prefer the provider's real usage; the native Bedrock event
                # often omits it, so fall back to estimating from the response.
                tokens = _tokens(getattr(event, "usage", None))
                if tokens is None:
                    tokens = _estimate_tokens(data)
                _emit(
                    log_type="output",
                    data=data,
                    model=str(getattr(event, "model", "")),
                    tokens=tokens,
                )

    _listener = _KafkaLogListener()
    log.info("CrewAI event-bus LLM logging installed")
