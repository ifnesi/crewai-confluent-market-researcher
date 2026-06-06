"""Auditor — the research-validation crew (a single agent).

It judges whether the Scout's research is coherent and well-supported, spot-
checking cited URLs with the web_search tool. It ends its output with a machine-
readable verdict line so the consumer can route the message.
"""
from __future__ import annotations

from crewai import Agent, Crew, Process, Task

VERDICT_PASS = "VERDICT: PASS"
VERDICT_REVISE = "VERDICT: REVISE"

TASK_TEMPLATE = """\
You are auditing market research about the **{field}** field / **{process}**
process before it goes to a C-suite report writer.

Assess the research below on four axes:
1. Coverage — does it address latest improvements, how leaders innovate, new
   entrants, and where VCs invest?
2. Coherence — are the claims internally consistent and clearly written?
3. Evidence — are claims backed by credible, relevant source URLs? Spot-check a
   few of the cited URLs with the web_search tool to confirm they exist and
   support the claim.
4. Specificity — concrete names, figures, dates rather than vague generalities.

RESEARCH UNDER REVIEW
---------------------
{findings}

CITED REFERENCES
----------------
{references}

Decide, and bias strongly toward approval. The report writer can work with
imperfect research, so only send it back for a serious, fixable problem — for
example it is largely off-topic, cites almost no sources, or misses one of the
four areas entirely. Minor gaps, a few unverified figures, or "it could go
deeper" are NOT reasons to reject. When in doubt, approve.

- To approve (the normal outcome), end your reply with the exact line:
    {verdict_pass}
- Only for a serious, fixable problem, end your reply with the exact line:
    {verdict_revise}
  followed by a "FEEDBACK:" section listing exactly what must be fixed.
"""

EXPECTED_OUTPUT = (
    "A concise audit (a few short paragraphs or bullets) covering the four axes, "
    "ending with either '" + VERDICT_PASS + "' or '" + VERDICT_REVISE + "' plus a "
    "'FEEDBACK:' section when revision is needed."
)


def build_crew(tools, *, field: str, process: str, findings: str, references, llm) -> Crew:
    auditor = Agent(
        role="Research Validator",
        goal=(
            "Protect report quality by approving only coherent, well-evidenced "
            "research, and returning specific, actionable feedback otherwise."
        ),
        backstory=(
            "You are a rigorous but efficient fact-checking editor. You "
            "spot-check two or three cited URLs at most, then decide — you do "
            "not exhaustively re-verify everything. Your feedback is concrete."
        ),
        tools=tools,
        llm=llm,
        verbose=True,
        max_iter=5,
        max_execution_time=180,
        respect_context_window=True,
    )

    refs = "\n".join(f"- {r}" for r in (references or [])) or "(none provided)"
    task = Task(
        description=TASK_TEMPLATE.format(
            field=field,
            process=process,
            findings=findings,
            references=refs,
            verdict_pass=VERDICT_PASS,
            verdict_revise=VERDICT_REVISE,
        ),
        expected_output=EXPECTED_OUTPUT,
        agent=auditor,
    )

    return Crew(
        agents=[auditor], tasks=[task], process=Process.sequential, verbose=True
    )
