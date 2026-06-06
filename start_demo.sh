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

# --- create topics (1 partition each, as required) ---------------------------
echo "▶ Creating Kafka topics (1 partition each) ..."
for t in crewai-ui-request-report crewai-agent-market-research \
         crewai-agent-market-research-ready crewai-agent-report-ready crewai-logs; do
  docker compose exec -T broker kafka-topics --bootstrap-server broker:29092 \
    --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 \
    >/dev/null 2>&1 && echo "  ✓ $t" || echo "  ✗ $t"
done

# --- register Avro schemas (runs only once the helper exists) ----------------
if [ -x ./scripts/register_schemas.sh ]; then
  echo "▶ Registering Avro schemas with Schema Registry ..."
  ./scripts/register_schemas.sh
fi

cat <<'EOF'

✅ Demo is up.

  UI (CrewAI report console) : http://localhost:8088   (flask-ui)
  Confluent Control Center   : http://localhost:9021
  Schema Registry            : http://localhost:8081
  Prometheus                 : http://localhost:9090

  Agent logs:
    docker compose logs -f agent-market-research agent-validator agent-report-creator

  Stop the demo:
    ./stop_demo.sh             # preserves Kafka data
    ./stop_demo.sh --volumes   # also wipes volumes (fresh start next time)
EOF
