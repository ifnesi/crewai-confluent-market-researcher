"""Scribe — the report-writing crew (a single agent).

Persona and the report structure live in config.yaml (next to this file); this
module loads them, fills in the runtime placeholders, and builds the crew.
"""
from __future__ import annotations

import os

import yaml
from crewai import Agent, Crew, Process, Task

_CFG = yaml.safe_load(
    open(os.path.join(os.path.dirname(__file__), "config.yaml"), encoding="utf-8")
)


def build_crew(tools, *, field: str, process: str, findings: str, references, llm) -> Crew:
    agent_cfg, task_cfg = _CFG["agent"], _CFG["task"]

    writer = Agent(
        role=agent_cfg["role"],
        goal=agent_cfg["goal"],
        backstory=agent_cfg["backstory"],
        tools=tools,
        llm=llm,
        verbose=True,
        max_iter=agent_cfg["max_iter"],
        max_execution_time=agent_cfg["max_execution_time"],
        respect_context_window=True,
    )

    refs = "\n".join(f"- {r}" for r in (references or [])) or "(none provided)"
    task = Task(
        description=task_cfg["description"].format(
            field=field, process=process, findings=findings, references=refs
        ),
        expected_output=task_cfg["expected_output"],
        agent=writer,
    )

    return Crew(
        agents=[writer], tasks=[task], process=Process.sequential, verbose=True
    )
