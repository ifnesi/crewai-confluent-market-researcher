#!/usr/bin/env bash
#
# kafka-bootstrap.sh — one-shot topic + schema setup, run as a docker service.
#
# Moved out of start_demo.sh so the rest of the stack (the Flink bootstrap in
# particular) can express a hard dependency on it via depends_on/condition.
# Creates every topic with a single partition (as the demo requires) and
# registers the Avro value schemas with Schema Registry. Runs in the cp-server
# image, which ships kafka-topics plus curl and python3 (used by
# register_schemas.sh).
#
set -euo pipefail

BROKER="${KAFKA_BOOTSTRAP_SERVERS:-broker:29092}"
SR_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"

echo "⏳ Waiting for broker at ${BROKER} ..."
until kafka-topics --bootstrap-server "$BROKER" --list >/dev/null 2>&1; do
  sleep 2
done
echo "✓ broker ready"

echo "⏳ Waiting for Schema Registry at ${SR_URL} ..."
until curl -fsS "${SR_URL}/subjects" >/dev/null 2>&1; do
  sleep 2
done
echo "✓ Schema Registry ready"

echo "▶ Creating Kafka topics (1 partition each) ..."
# crewai-logs-stats is the Flink-derived, dashboard-ready stream; create it up
# front so the Flink sink, the Elasticsearch connector and the UI can attach.
for t in crewai-ui-request-report crewai-agent-market-research \
         crewai-agent-market-research-ready crewai-agent-report-ready \
         crewai-logs crewai-logs-stats; do
  kafka-topics --bootstrap-server "$BROKER" \
    --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 \
    >/dev/null 2>&1 && echo "  ✓ $t" || echo "  ✗ $t"
done

echo "▶ Registering Avro schemas with Schema Registry ..."
# register_schemas.sh cds to its parent dir and reads schemas/<file>; with
# /scripts and /schemas mounted that resolves to /schemas.
SCHEMA_REGISTRY_URL="$SR_URL" bash /scripts/register_schemas.sh

echo "✅ kafka-setup complete."
