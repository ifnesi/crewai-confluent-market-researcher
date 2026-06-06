"""Auditor — the research-validation crew (a single agent).

Persona, instructions, and the exact verdict tokens live in config.yaml (next to
this file). The verdict tokens are also exported as module constants because the
consumer (main.py) parses the agent's reply for them to route the message.
"""
from __future__ import annotations

import os

import yaml
from crewai import Agent, Crew, Process, Task

_CFG = yaml.safe_load(
    open(os.path.join(os.path.dirname(__file__), "config.yaml"), encoding="utf-8")
)

VERDICT_PASS = _CFG["verdict"]["pass"]
VERDICT_REVISE = _CFG["verdict"]["revise"]


def build_crew(tools, *, field: str, process: str, findings: str, references, llm) -> Crew:
    agent_cfg, task_cfg = _CFG["agent"], _CFG["task"]

    auditor = Agent(
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
            field=field,
            process=process,
            findings=findings,
            references=refs,
            verdict_pass=VERDICT_PASS,
            verdict_revise=VERDICT_REVISE,
        ),
        expected_output=task_cfg["expected_output"].format(
            verdict_pass=VERDICT_PASS, verdict_revise=VERDICT_REVISE
        ),
        agent=auditor,
    )

    return Crew(
        agents=[auditor], tasks=[task], process=Process.sequential, verbose=True
    )
