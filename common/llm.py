"""Bedrock-backed LLM factory for CrewAI.

CrewAI runs on LiteLLM, so a Bedrock model is just a model string of the form
``bedrock/eu.anthropic.claude-…`` plus AWS credentials in the environment
(AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION_NAME). Centralising the
construction here keeps model choice and defaults consistent across agents.
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


def research_llm() -> LLM:
    return make_llm(settings.BEDROCK_MODEL_RESEARCH)


def validator_llm() -> LLM:
    return make_llm(settings.BEDROCK_MODEL_VALIDATOR, max_tokens=4096)


def report_llm() -> LLM:
    # The executive report is long-form; give Opus generous output room.
    return make_llm(settings.BEDROCK_MODEL_REPORT, max_tokens=16000)
