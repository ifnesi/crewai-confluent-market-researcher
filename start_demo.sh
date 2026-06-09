#!/usr/bin/env bash
#
# start_demo.sh — build and start the full CrewAI + Kafka demo stack.
#
# Brings up the Confluent Platform (broker, Schema Registry, Control Center,
# Prometheus, …) plus SearXNG, the MCP server, the three CrewAI agents and the
# Flask/React UI. Waits for the core services, registers Avro schemas (if a
# register script is present), and prints the service URLs.
#
set -euo pipefail

cd "$(dirname "$0")"

# --- preflight ---------------------------------------------------------------
if ! docker info >/dev/null 2>&1; then
  echo "❌ Docker does not appear to be running. Start Docker Desktop and retry." >&2
  exit 1
fi

if [ ! -f .env ]; then
  echo "❌ No .env found. Copy the template and add your AWS Bedrock credentials:" >&2
  echo "     cp .env_example .env" >&2
  echo "   then edit AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION_NAME." >&2
  exit 1
fi

# Load .env so the vars are available both to docker compose interpolation and,
# via `environment:` refs, to the agent containers. `set -a` exports everything;
# `source` tolerates the `export ` prefix already in the file.
set -a
# shellcheck disable=SC1091
source ./.env
set +a

# --- bring up the stack ------------------------------------------------------
echo "▶ Building and starting containers ..."
docker compose up -d --build

# --- wait for core services --------------------------------------------------
wait_for() {
  local name="$1" url="$2" tries="${3:-60}"
  printf "⏳ Waiting for %s " "$name"
  for ((i = 0; i < tries; i++)); do
    if curl -fsS "$url" >/dev/null 2>&1; then echo "✓"; return 0; fi
    printf "."
    sleep 2
  done
  echo " ✗"
  echo "   $name did not become ready at $url" >&2
  return 1
}

wait_for "Schema Registry" "http://localhost:8081/subjects"
wait_for "Control Center" "http://localhost:9021/" 60 || true

# Topics and Avro schemas are now created by the one-shot `kafka-setup` service
# (see docker-compose.yml); the Flink job depends on it. Wait for it to finish so
# the post-startup steps below are safe.
echo "⏳ Waiting for kafka-setup (topics + schemas) ..."
docker compose wait kafka-setup >/dev/null 2>&1 || true

# --- Elasticsearch: index template (stable field types for the dashboard) ----
wait_for "Elasticsearch" "http://localhost:9200" 90 || true
echo "▶ Applying Elasticsearch index template ..."
curl -fsS -X PUT "http://localhost:9200/_index_template/crewai-logs-stats" \
  -H 'Content-Type: application/json' \
  --data @connectors/es-index-template.json >/dev/null \
  && echo "  ✓ template crewai-logs-stats" || echo "  ✗ template (continuing)"

# --- Kafka Connect: deploy the Elasticsearch sink ----------------------------
# The connect container installs the Elasticsearch plugin via confluent-hub at
# startup; wait for it to register before deploying. PUT .../config is idempotent.
echo "⏳ Waiting for the Elasticsearch connector plugin ..."
for ((i = 0; i < 90; i++)); do
  if curl -fsS http://localhost:8083/connector-plugins 2>/dev/null | grep -q ElasticsearchSinkConnector; then
    break
  fi
  sleep 2
done
echo "▶ Deploying Elasticsearch sink connector ..."
curl -fsS -X PUT -H 'Content-Type: application/json' \
  --data @connectors/elastic_sink_observability.json \
  http://localhost:8083/connectors/elastic-sink-observability/config >/dev/null \
  && echo "  ✓ elastic-sink-observability" || echo "  ✗ connector deploy (continuing)"

# --- Kibana: import the observability dashboard ------------------------------
wait_for "Kibana" "http://localhost:5601/api/status" 120 || true
echo "▶ Importing Kibana dashboard ..."
curl -fsS -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  -F file=@kibana_dashboard.ndjson >/dev/null \
  && echo "  ✓ CrewAI Observability dashboard" || echo "  ✗ dashboard import (continuing)"

cat <<'EOF'

✅ Demo is up.

  UI (CrewAI report console) : http://localhost:8088   (flask-ui)
  Confluent Control Center   : http://localhost:9021
  Schema Registry            : http://localhost:8081
  Prometheus                 : http://localhost:9090
  Kafka Connect              : http://localhost:8083
  Flink Dashboard            : http://localhost:9081
  Elasticsearch              : http://localhost:9200
  Kibana (AI observability)  : http://localhost:5601/app/dashboards

  Agent logs:
    docker compose logs -f agent-market-research agent-validator agent-report-creator

  Stop the demo:
    ./stop_demo.sh             # preserves Kafka data
    ./stop_demo.sh --volumes   # also wipes volumes (fresh start next time)
EOF
