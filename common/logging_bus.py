"""Publish every agent activity — LLM prompts/responses and MCP tool calls — to
the ``crewai-logs`` topic.

CrewAI 1.x calls Bedrock through its *native* provider (boto3), not LiteLLM, so
we hook CrewAI's own event bus rather than a LiteLLM callback. The bus emits an
``LLMCallStartedEvent`` (prompt) and an ``LLMCallCompletedEvent`` (response +
real token usage) around every model call, and ``ToolUsage*Event``s around every
tool (MCP web_search) invocation.

Tokens come from the provider's real usage on the completed event — no character
estimate. Cost is derived from LiteLLM's maintained price map
(``litellm.cost_per_token``); we never hardcode a per-model price table, and the
cost is simply ``null`` when a (brand-new) model id isn't in the map yet.

Per-message context (which agent, which report_id, which username) travels via a
ContextVar the agent sets right before invoking its crew. CrewAI runs the crew
synchronously on the calling thread, so the ContextVar is visible to the events.
"""
from __future__ import annotations

import contextvars
import json
import logging
import os

from . import pricing, settings
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


def shutdown() -> None:
    """Flush the logging producer so buffered log records aren't lost on exit."""
    global _kafka
    if _kafka is not None:
        try:
            _kafka.close()
        except Exception:  # noqa: BLE001 - shutdown must not raise
            log.exception("error flushing log producer on shutdown")
        _kafka = None


def _emit(
    *,
    log_type: str,
    data: str,
    model: str,
    tokens: int | None = None,
    prompt_tokens: int | None = None,
    cost: float | None = None,
    tool_name: str | None = None,
) -> None:
    ctx = _ctx.get()
    record = {
        "agent_name": ctx["agent_name"],
        "report_id": ctx["report_id"],
        "timestamp": now_ms(),
        "type": log_type,  # input | output | tool_call | tool_result
        "data": clip(data),
        "tokens": tokens,            # completion tokens (output records)
        "prompt_tokens": prompt_tokens,  # real input tokens (output records)
        "cost": cost,               # USD from LiteLLM's price map, or null
        "tool_name": tool_name,     # e.g. "web_search" (tool records)
        "model": model,
    }
    try:
        _producer().produce(settings.TOPIC_LOGS, key=ctx["username"], value=record)
        _producer().flush(5)
    except Exception:  # noqa: BLE001 - logging must never break the agent
        log.exception("failed to publish to %s", settings.TOPIC_LOGS)


# --- text extraction ---------------------------------------------------------
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


def _tool_args_text(args) -> str:
    if args is None:
        return ""
    if isinstance(args, str):
        return args
    try:
        return json.dumps(args, default=str)
    except Exception:  # noqa: BLE001
        return str(args)


# --- real usage + cost -------------------------------------------------------
def _first(obj, *names):
    """First non-null of the given keys/attrs on a dict or object."""
    for n in names:
        if isinstance(obj, dict):
            if obj.get(n) is not None:
                return obj[n]
        else:
            v = getattr(obj, n, None)
            if v is not None:
                return v
    return None


def _int_or_none(v) -> int | None:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _usage(event) -> tuple[int | None, int | None]:
    """Return (prompt_tokens, completion_tokens) from a completed LLM event.

    CrewAI surfaces the provider's real usage either directly on the event or on
    the wrapped response. LiteLLM names them prompt_/completion_tokens; native
    Bedrock/Anthropic uses input_/output_tokens — accept both.
    """
    usage = getattr(event, "usage", None)
    if usage is None:
        resp = getattr(event, "response", None)
        usage = getattr(resp, "usage", None) if resp is not None else None
    if usage is None:
        return None, None
    # AWS Bedrock's Converse usage dict is camelCase (inputTokens/outputTokens);
    # LiteLLM/OpenAI/Anthropic use the snake_case variants — accept all.
    return (
        _int_or_none(_first(usage, "prompt_tokens", "input_tokens", "inputTokens")),
        _int_or_none(_first(usage, "completion_tokens", "output_tokens", "outputTokens")),
    )


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

    # Tool events are optional — guard separately so LLM logging still works if a
    # future CrewAI renames them.
    try:
        from crewai.events import (
            ToolUsageErrorEvent,
            ToolUsageFinishedEvent,
            ToolUsageStartedEvent,
        )
        tool_events = (ToolUsageStartedEvent, ToolUsageFinishedEvent, ToolUsageErrorEvent)
    except Exception:  # noqa: BLE001
        tool_events = None
        log.warning("crewai tool-usage events unavailable; MCP/tool logging disabled")

    class _KafkaLogListener(BaseEventListener):
        def setup_listeners(self, crewai_event_bus):  # noqa: D401
            @crewai_event_bus.on(LLMCallStartedEvent)
            def _on_start(source, event):  # noqa: ANN001
                # Live "asking the LLM" feedback; exact tokens aren't known until
                # the call completes, so they ride on the output record below.
                _emit(
                    log_type="input",
                    data=_messages_to_text(getattr(event, "messages", None)),
                    model=str(getattr(event, "model", "")),
                )

            @crewai_event_bus.on(LLMCallCompletedEvent)
            def _on_done(source, event):  # noqa: ANN001
                prompt_tokens, completion_tokens = _usage(event)
                model = str(getattr(event, "model", ""))
                _emit(
                    log_type="output",
                    data=_response_text(getattr(event, "response", None)),
                    model=model,
                    tokens=completion_tokens,
                    prompt_tokens=prompt_tokens,
                    cost=pricing.cost(model, prompt_tokens, completion_tokens),
                )

            if tool_events:
                ToolStarted, ToolFinished, ToolError = tool_events

                @crewai_event_bus.on(ToolStarted)
                def _on_tool_start(source, event):  # noqa: ANN001
                    _emit(
                        log_type="tool_call",
                        data=_tool_args_text(getattr(event, "tool_args", None)),
                        model="",
                        tool_name=str(getattr(event, "tool_name", "") or ""),
                    )

                @crewai_event_bus.on(ToolFinished)
                def _on_tool_done(source, event):  # noqa: ANN001
                    _emit(
                        log_type="tool_result",
                        data=_response_text(getattr(event, "output", None)),
                        model="",
                        tool_name=str(getattr(event, "tool_name", "") or ""),
                    )

                @crewai_event_bus.on(ToolError)
                def _on_tool_error(source, event):  # noqa: ANN001
                    _emit(
                        log_type="tool_result",
                        data="ERROR: " + str(getattr(event, "error", "")),
                        model="",
                        tool_name=str(getattr(event, "tool_name", "") or ""),
                    )

    _listener = _KafkaLogListener()
    log.info(
        "CrewAI event-bus logging installed (LLM%s)",
        " + tools" if tool_events else "",
    )
