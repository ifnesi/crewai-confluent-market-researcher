"""Scout consumer: crewai-ui-request-report -> crewai-agent-market-research.

Choreographed, not orchestrated: this process simply reacts to request messages
(whether from the UI or a validator re-request) and emits its research onward.
"""
from __future__ import annotations

import logging
import re

import crew as research_crew

from common import llm as llm_factory
from common import logging_bus, settings
from common.kafka_io import KafkaAvro
from common.mcp_tools import open_mcp_tools
from common.util import now_ms

AGENT_NAME = "scout-market-research"
GROUP_ID = "scout-market-research"

# Pull source URLs out of the markdown to populate the references array.
_URL_RE = re.compile(r"https?://[^\s<>\")\]]+")

log = logging.getLogger(AGENT_NAME)


def handle(kafka: KafkaAvro, key: str | None, value: dict) -> None:
    username = key or "unknown"
    report_id = value["report_id"]
    field = value["field"]
    process = value["process"]
    counter = int(value.get("counter", 0) or 0)
    extra_context = value.get("extra_context")

    log.info("research request report_id=%s field=%s process=%s counter=%s",
             report_id, field, process, counter)
    logging_bus.set_log_context(
        agent_name=AGENT_NAME, report_id=report_id, username=username
    )

    with open_mcp_tools() as tools:
        crew = research_crew.build_crew(
            tools,
            field=field,
            process=process,
            extra_context=extra_context,
            llm=llm_factory.research_llm(),
        )
        result = crew.kickoff()

    findings = str(result)
    references = sorted(set(_URL_RE.findall(findings)))

    kafka.produce(
        settings.TOPIC_MARKET_RESEARCH,
        key=username,
        value={
            "report_id": report_id,
            "timestamp": now_ms(),
            "field": field,
            "process": process,
            "counter": counter,
            "findings": findings,
            "references": references,
        },
    )
    kafka.flush()
    log.info("published research report_id=%s refs=%d", report_id, len(references))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    logging_bus.install()
    kafka = KafkaAvro()
    kafka.consume(
        settings.TOPIC_UI_REQUEST,
        group_id=GROUP_ID,
        handler=lambda k, v: handle(kafka, k, v),
    )


if __name__ == "__main__":
    main()
