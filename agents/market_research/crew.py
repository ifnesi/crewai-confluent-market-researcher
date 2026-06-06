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


def build_crew(tools, *, field: str, process: str, extra_context, llm) -> Crew:
    agent_cfg, task_cfg = _CFG["agent"], _CFG["task"]

    extra = ""
    if extra_context:
        extra = (
            "\n\nIMPORTANT — a prior pass was sent back by the validator. Address "
            f"this feedback specifically and fill the gaps:\n{extra_context}\n"
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
