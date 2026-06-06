#!/usr/bin/env bash
#
# register_schemas.sh — register the Avro value schemas with Schema Registry.
#
# Producers auto-register on first publish, but registering up front means the
# schemas (and topics, once produced to) are visible in Control Center before
# any traffic flows. Idempotent: re-registering an identical schema is a no-op.
#
set -euo pipefail

cd "$(dirname "$0")/.."

SR_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
CT="Content-Type: application/vnd.schemaregistry.v1+json"

# file (under schemas/)                -> subject (TopicNameStrategy: <topic>-value)
declare -a MAP=(
  "ui_request_report.avsc|crewai-ui-request-report-value"
  "agent_market_research.avsc|crewai-agent-market-research-value"
  "agent_market_research_ready.avsc|crewai-agent-market-research-ready-value"
  "agent_report_ready.avsc|crewai-agent-report-ready-value"
  "logs.avsc|crewai-logs-value"
)

for entry in "${MAP[@]}"; do
  file="schemas/${entry%%|*}"
  subject="${entry##*|}"
  # Wrap the raw .avsc as {"schema": "<escaped json string>"} via python (no jq dep).
  payload=$(python3 -c "import json,sys;print(json.dumps({'schema':open(sys.argv[1]).read()}))" "$file")
  code=$(curl -s -o /tmp/sr_resp.json -w '%{http_code}' \
    -X POST -H "$CT" --data "$payload" \
    "$SR_URL/subjects/$subject/versions")
  if [[ "$code" == "200" ]]; then
    id=$(python3 -c "import json;print(json.load(open('/tmp/sr_resp.json')).get('id','?'))")
    echo "  ✓ $subject (schema id $id)"
  else
    echo "  ✗ $subject -> HTTP $code: $(cat /tmp/sr_resp.json)" >&2
    exit 1
  fi
done

echo "All schemas registered with $SR_URL"
