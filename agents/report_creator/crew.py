"""Scribe — the report-writing crew (a single agent).

Turns validated research into a polished, executive-ready report in Markdown,
suitable for a Fortune 500 C-suite. May use web_search to fill a small gap, but
its job is synthesis and clear executive prose, not new research.
"""
from __future__ import annotations

from crewai import Agent, Crew, Process, Task

TASK_TEMPLATE = """\
Write an **executive-ready report** for the C-suite on the **{field}** field,
focused on the **{process}** process, based strictly on the validated research
below. Do not fabricate; if you add a clarifying fact, verify it with web_search
and cite it.

Audience: a VP of AI / C-level executive at a Fortune 500 company. They want
clarity, structure, and actionable insight — not a literature dump.

Produce Markdown with exactly this structure:

# {field} — {process}: Executive Market Briefing

**Prepared for:** C-Suite review

## Executive Summary
(4–7 crisp bullets a busy executive can read in 60 seconds.)

## Industry Landscape & Latest Improvements

## How Market Leaders Are Innovating

## Emerging Entrants to Watch

## Where Venture Capital Is Investing

## Strategic Implications & Recommendations
(Concrete, prioritized actions for the business.)

## References
(Bulleted list of the source URLs.)

VALIDATED RESEARCH
------------------
{findings}

SOURCE REFERENCES
-----------------
{references}
"""

EXPECTED_OUTPUT = (
    "A complete, well-formatted Markdown report following the exact section "
    "structure given, written for a C-suite reader, grounded in the provided "
    "research with a References section. Output only the report Markdown."
)


def build_crew(tools, *, field: str, process: str, findings: str, references, llm) -> Crew:
    writer = Agent(
        role="Executive Report Writer",
        goal=(
            "Transform validated research into a clear, structured, decision-"
            "ready executive briefing for a Fortune 500 C-suite."
        ),
        backstory=(
            "You are a former management consultant who writes the briefings "
            "executives actually read: sharp, structured, evidence-backed, and "
            "clear about what matters and what to do next. You write from the "
            "research provided and use web_search at most once, only if a key "
            "fact is missing — your job is to write, not to re-research."
        ),
        tools=tools,
        llm=llm,
        verbose=True,
        max_iter=4,
        max_execution_time=300,
        respect_context_window=True,
    )

    refs = "\n".join(f"- {r}" for r in (references or [])) or "(none provided)"
    task = Task(
        description=TASK_TEMPLATE.format(
            field=field, process=process, findings=findings, references=refs
        ),
        expected_output=EXPECTED_OUTPUT,
        agent=writer,
    )

    return Crew(
        agents=[writer], tasks=[task], process=Process.sequential, verbose=True
    )
