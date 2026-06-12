"""LLM factory for CrewAI, switchable between AWS Bedrock and Ollama.

CrewAI runs on LiteLLM, so the provider is encoded entirely in the model string:

* Bedrock — ``bedrock/eu.anthropic.claude-…`` plus AWS credentials in the
  environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION_NAME).
* Ollama  — ``ollama/<model>`` plus a base URL pointing at the Ollama server
  (``settings.OLLAMA_BASE_URL``).

``settings.LLM_PROVIDER`` selects one provider for all three agents. Centralising
the construction here keeps model choice and defaults consistent across agents.
"""
from __future__ import annotations

from crewai import LLM

from . import settings


def make_llm(model_id: str, *, max_tokens: int = 8192) -> LLM:
    """Build a CrewAI LLM bound to a Bedrock model id.

    Temperature/top_p are intentionally left at provider defaults — newer Claude
    models reject explicit sampling params, and prompting drives behaviour here.
    """
    return LLM(model=model_id, max_tokens=max_tokens)


def make_ollama_llm(model: str, *, max_tokens: int = 8192) -> LLM:
    """Build a CrewAI LLM bound to an Ollama model served at OLLAMA_BASE_URL."""
    return LLM(model=f"ollama/{model}", base_url=settings.OLLAMA_BASE_URL, max_tokens=max_tokens)


def _use_ollama() -> bool:
    return settings.LLM_PROVIDER == "OLLAMA"


def research_llm() -> LLM:
    if _use_ollama():
        return make_ollama_llm(settings.OLLAMA_MODEL_RESEARCH)
    return make_llm(settings.BEDROCK_MODEL_RESEARCH)


def validator_llm() -> LLM:
    if _use_ollama():
        return make_ollama_llm(settings.OLLAMA_MODEL_VALIDATOR, max_tokens=4096)
    return make_llm(settings.BEDROCK_MODEL_VALIDATOR, max_tokens=4096)


def report_llm() -> LLM:
    # The executive report is long-form; give the model generous output room.
    if _use_ollama():
        return make_ollama_llm(settings.OLLAMA_MODEL_REPORT, max_tokens=16000)
    return make_llm(settings.BEDROCK_MODEL_REPORT, max_tokens=16000)
