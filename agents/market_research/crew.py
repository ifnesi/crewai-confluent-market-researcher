"""Scout — the market-research crew (a single agent).

Persona and instructions live in config.yaml (next to this file); this module
just loads them, fills in the runtime placeholders, and builds the crew.
"""
from __future__ import annotations

import os

import yaml
from crewai import Agent, Crew, Process, Task

from common import settings

_CFG = yaml.safe_load(
    open(os.path.join(os.path.dirname(__file__), "config.yaml"), encoding="utf-8")
)


def build_crew(tools, *, field: str, process: str, report_draft, report_feedback, llm) -> Crew:
    agent_cfg, task_cfg = _CFG["agent"], _CFG["task"]

    extra = ""
    if report_draft and report_feedback:
        # Revision pass: don't start over. Keep the sound parts of the prior
        # dossier and change only what the feedback calls out — cheaper and steadier.
        extra = (
            "\n\nREVISION PASS — a prior version of this dossier was reviewed by the "
            "validator and sent back. Do NOT start from scratch. Preserve the sections "
            "and claims that are already sound, and revise ONLY what the feedback calls "
            "out; run new web searches solely to address that feedback. Return the FULL, "
            "updated dossier in the same structure.\n\n"
            f"--- PRIOR DRAFT (revise, don't discard) ---\n{report_draft}\n\n"
            f"--- VALIDATOR FEEDBACK (address this) ---\n{report_feedback}\n"
        )
    elif report_feedback:
        # Feedback without a draft (shouldn't normally happen) — steer, don't rebuild.
        extra = (
            "\n\nIMPORTANT — a prior pass was sent back by the validator. Address "
            f"this feedback specifically and fill the gaps:\n{report_feedback}\n"
        )

    analyst = Agent(
        role=agent_cfg["role"].format(field=field, process=process),
        goal=agent_cfg["goal"].format(field=field, process=process),
        backstory=agent_cfg["backstory"],
        tools=tools,
        llm=llm,
        verbose=True,
        max_iter=agent_cfg["max_iter"],
        max_execution_time=agent_cfg["max_execution_time"],
        respect_context_window=True,
    )

    task = Task(
        description=task_cfg["description"].format(
            field=field,
            process=process,
            sources=", ".join(settings.SUGGESTED_SOURCES),
            extra=extra,
        ),
        expected_output=task_cfg["expected_output"],
        agent=analyst,
    )

    return Crew(
        agents=[analyst], tasks=[task], process=Process.sequential, verbose=True
    )
