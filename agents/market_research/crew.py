"""Scout — the market-research crew (a single agent).

Given a field + process (and optionally validator feedback), it researches the
market using the MCP web_search tool and returns structured markdown findings
with a trailing References section of source URLs.
"""
from __future__ import annotations

from crewai import Agent, Crew, Process, Task

from common import settings

# The four questions the customer explicitly asked for (assignment.txt).
RESEARCH_BRIEF = """\
Produce comprehensive, decision-grade market research on the **{field}** field,
focused on the **{process}** process. Cover, with specifics and evidence:

1. Latest improvements and innovations in the industry for this process.
2. How market leaders are innovating (name companies, products, approaches).
3. New companies / entrants in this space (startups, recent launches).
4. Where venture capital is investing (rounds, amounts, investors, themes).

Search the web for current information — prioritise recent (last 12–24 months)
and authoritative sources. Useful starting points: {sources}.

For every non-obvious claim, capture the source URL. Be concrete: prefer figures,
dates, company names and deal sizes over generalities.

SEARCH DISCIPLINE — be decisive. Run a focused set of web searches (roughly 5–8
total across the four areas). As soon as you have enough to write each section
with a few concrete, sourced points, STOP searching and write the dossier. It
does not need to be exhaustive; do not chase every possible figure or keep
reformulating queries. Producing the report is more important than more searches.
{extra}
"""

EXPECTED_OUTPUT = """\
A well-structured markdown research dossier with these sections:
- ## Executive Snapshot (5–8 bullet takeaways)
- ## Latest Improvements & Innovations
- ## How Leaders Are Innovating
- ## New Entrants
- ## Where VCs Are Investing
- ## References  (a bulleted list of every source URL used)
Every section must be grounded in the searched sources and cite URLs inline.
"""


def build_crew(tools, *, field: str, process: str, extra_context, llm) -> Crew:
    extra = ""
    if extra_context:
        extra = (
            "\n\nIMPORTANT — a prior pass was sent back by the validator. Address "
            f"this feedback specifically and fill the gaps:\n{extra_context}\n"
        )

    analyst = Agent(
        role=f"Senior {field} Market Research Analyst",
        goal=(
            f"Deliver rigorous, well-sourced market intelligence on {process} "
            f"within {field}, suitable for a C-suite briefing."
        ),
        backstory=(
            "You are a meticulous but decisive industry analyst. You ground "
            "claims in current web sources, name names, and quote figures, "
            "never inventing facts. You run a focused set of searches and then "
            "synthesize — you do not run dozens of queries chasing every detail."
        ),
        tools=tools,
        llm=llm,
        verbose=True,
        max_iter=6,
        max_execution_time=300,
        respect_context_window=True,
    )

    task = Task(
        description=RESEARCH_BRIEF.format(
            field=field,
            process=process,
            sources=", ".join(settings.SUGGESTED_SOURCES),
            extra=extra,
        ),
        expected_output=EXPECTED_OUTPUT,
        agent=analyst,
    )

    return Crew(
        agents=[analyst],
        tasks=[task],
        process=Process.sequential,
        verbose=True,
    )
