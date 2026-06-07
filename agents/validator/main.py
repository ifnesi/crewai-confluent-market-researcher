"""Auditor consumer: crewai-agent-market-research -> ready, or back to request.

Routing:
  * PASS, or the iteration cap (counter >= MAX_RESEARCH_ITERATIONS) is hit
        -> publish to crewai-agent-market-research-ready
  * otherwise
        -> re-publish to crewai-ui-request-report with extra_context (the
           feedback) and counter incremented, so Scout researches again.
"""
from __future__ import annotations

import logging
import re

import crew as validator_crew

from common import lifecycle
from common import llm as llm_factory
from common import logging_bus, settings
from common.kafka_io import KafkaAvro
from common.mcp_tools import open_mcp_tools
from common.util import now_ms

AGENT_NAME = "auditor-validator"
GROUP_ID = "auditor-validator"

log = logging.getLogger(AGENT_NAME)


def _extract_feedback(verdict_text: str) -> str:
    m = re.search(r"FEEDBACK:\s*(.+)", verdict_text, re.IGNORECASE | re.DOTALL)
    return m.group(1).strip() if m else verdict_text.strip()


def handle(kafka: KafkaAvro, key: str | None, value: dict) -> None:
    username = key or "unknown"
    report_id = value["report_id"]
    field = value["field"]
    process = value["process"]
    counter = int(value.get("counter", 0) or 0)
    findings = value.get("findings", "")
    references = value.get("references", []) or []

    log.info("validating report_id=%s counter=%s", report_id, counter)
    logging_bus.set_log_context(
        agent_name=AGENT_NAME, report_id=report_id, username=username
    )

    with open_mcp_tools() as tools:
        crew = validator_crew.build_crew(
            tools,
            field=field,
            process=process,
            findings=findings,
            references=references,
            llm=llm_factory.validator_llm(),
        )
        verdict_text = str(crew.kickoff())

    passed = validator_crew.VERDICT_PASS.upper() in verdict_text.upper()
    forced = counter >= settings.MAX_RESEARCH_ITERATIONS

    if passed or forced:
        note = (
            "Approved on merit."
            if passed
            else f"Forced through after {counter} iterations (cap reached)."
        )
        kafka.produce(
            settings.TOPIC_RESEARCH_READY,
            key=username,
            value={
                "report_id": report_id,
                "timestamp": now_ms(),
                "field": field,
                "process": process,
                "counter": counter,
                "findings": findings,
                "references": references,
                "validation_notes": note,
            },
        )
        log.info("PASS report_id=%s (passed=%s forced=%s)", report_id, passed, forced)
    else:
        feedback = _extract_feedback(verdict_text)
        kafka.produce(
            settings.TOPIC_UI_REQUEST,
            key=username,
            value={
                "report_id": report_id,
                "field": field,
                "process": process,
                "timestamp": now_ms(),
                "extra_context": feedback,
                "counter": counter + 1,
            },
        )
        log.info("REVISE report_id=%s -> re-request counter=%s", report_id, counter + 1)

    kafka.flush()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    logging_bus.install()
    kafka = KafkaAvro()
    # On SIGTERM/SIGINT: stop the consume loop and flush both producers.
    lifecycle.on_shutdown(kafka.close, logging_bus.shutdown)
    kafka.consume(
        settings.TOPIC_MARKET_RESEARCH,
        group_id=GROUP_ID,
        handler=lambda k, v: handle(kafka, k, v),
    )


if __name__ == "__main__":
    main()
