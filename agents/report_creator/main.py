"""Scribe consumer: crewai-agent-market-research-ready -> crewai-agent-report-ready.

The terminal agent in the choreography. Its output lands on the topic the UI
streams to the user.
"""
from __future__ import annotations

import logging

import crew as report_crew

from common import llm as llm_factory
from common import logging_bus, settings
from common.kafka_io import KafkaAvro
from common.mcp_tools import open_mcp_tools
from common.util import now_ms

AGENT_NAME = "scribe-report-creator"
GROUP_ID = "scribe-report-creator"

log = logging.getLogger(AGENT_NAME)


def handle(kafka: KafkaAvro, key: str | None, value: dict) -> None:
    username = key or "unknown"
    report_id = value["report_id"]
    field = value["field"]
    process = value["process"]
    findings = value.get("findings", "")
    references = value.get("references", []) or []

    log.info("writing report report_id=%s field=%s process=%s", report_id, field, process)
    logging_bus.set_log_context(
        agent_name=AGENT_NAME, report_id=report_id, username=username
    )

    with open_mcp_tools() as tools:
        crew = report_crew.build_crew(
            tools,
            field=field,
            process=process,
            findings=findings,
            references=references,
            llm=llm_factory.report_llm(),
        )
        report_md = str(crew.kickoff())

    kafka.produce(
        settings.TOPIC_REPORT_READY,
        key=username,
        value={
            "report_id": report_id,
            "timestamp": now_ms(),
            "field": field,
            "process": process,
            "report": report_md,
        },
    )
    kafka.flush()
    log.info("published report report_id=%s chars=%d", report_id, len(report_md))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    logging_bus.install()
    kafka = KafkaAvro()
    kafka.consume(
        settings.TOPIC_RESEARCH_READY,
        group_id=GROUP_ID,
        handler=lambda k, v: handle(kafka, k, v),
    )


if __name__ == "__main__":
    main()
