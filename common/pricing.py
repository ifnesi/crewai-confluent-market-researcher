"""Per-call USD cost from LiteLLM's maintained model price map.

CrewAI 1.x calls Bedrock through its *native* provider (boto3), so LiteLLM isn't
installed and ``litellm.cost_per_token`` isn't available. We instead fetch the
*same* price JSON CrewAI references (``crewai.constants.JSON_URL``) once, cache it
for the process lifetime, and look the model up ourselves.

No prices are hardcoded here — the map is maintained upstream. Cost is ``0.0``
when the map can't be fetched (offline) or the model isn't in it (e.g. free
local Ollama models); tokens remain accurate regardless.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import urllib.request

log = logging.getLogger(__name__)

# Track the exact source CrewAI points at; fall back to the literal URL if the
# constant moves in a future CrewAI.
try:
    from crewai.constants import JSON_URL as _PRICE_URL
except Exception:  # noqa: BLE001
    _PRICE_URL = (
        "https://raw.githubusercontent.com/BerriAI/litellm/main/"
        "model_prices_and_context_window.json"
    )

_FETCH_TIMEOUT = 20
_lock = threading.Lock()
_prices: dict | None = None  # None = not loaded yet; {} = fetch failed/empty

# Cross-region inference-profile prefixes (eu./us./apac./global. …). The map keys
# usually include these (e.g. "eu.anthropic.claude-sonnet-4-6"), but we also try
# stripping them as a fallback.
_REGION_PREFIX_RE = re.compile(
    r"^(?:eu|us|apac|apne\d*|use\d*|usw\d*|au|jp|ca|sa|me|af|global|us-gov)\."
)


def _load() -> dict:
    global _prices
    if _prices is not None:
        return _prices
    with _lock:
        if _prices is not None:  # another thread won the race
            return _prices
        try:
            with urllib.request.urlopen(_PRICE_URL, timeout=_FETCH_TIMEOUT) as resp:
                _prices = json.load(resp)
            log.info("loaded %d model prices from %s", len(_prices), _PRICE_URL)
        except Exception:  # noqa: BLE001 - cost is best-effort, never fatal
            log.warning("could not fetch price map from %s; cost will be null", _PRICE_URL)
            _prices = {}
    return _prices


def _candidates(model: str) -> list[str]:
    m = (model or "").strip()
    if not m:
        return []
    bare = m[len("bedrock/"):] if m.startswith("bedrock/") else m
    out: list[str] = []
    for cand in (m, bare, f"bedrock/{bare}", _REGION_PREFIX_RE.sub("", bare)):
        if cand and cand not in out:
            out.append(cand)
    return out


def cost(model: str, prompt_tokens: int | None, completion_tokens: int | None) -> float:
    """USD cost for a call, or 0.0 if not derivable from the price map
    (offline, brand-new model, or a free local model such as Ollama)."""
    if not prompt_tokens and not completion_tokens:
        return 0.0
    prices = _load()
    if not prices:
        return 0.0
    for cand in _candidates(model):
        entry = prices.get(cand)
        if not isinstance(entry, dict):
            continue
        in_rate = entry.get("input_cost_per_token")
        out_rate = entry.get("output_cost_per_token")
        if in_rate is None and out_rate is None:
            continue
        total = (prompt_tokens or 0) * (in_rate or 0) + (completion_tokens or 0) * (out_rate or 0)
        return round(total, 8)
    return 0.0
