#!/usr/bin/env bash
#
# stop_demo.sh — stop the CrewAI + Kafka demo stack.
#
# Usage:
#   ./stop_demo.sh             Stop and remove containers; keep volumes (Kafka
#                              topics, schemas and offsets survive a restart).
#   ./stop_demo.sh --volumes   Also remove named volumes for a clean slate.
#
set -euo pipefail

cd "$(dirname "$0")"

# Note: no arrays here on purpose — macOS ships bash 3.2, where expanding an
# empty array under `set -u` ("${arr[@]}") aborts with "unbound variable".
case "${1:-}" in
  -v | --volumes)
    echo "▶ Stopping demo and removing volumes (Kafka data will be wiped) ..."
    docker compose down --volumes
    ;;
  "")
    echo "▶ Stopping demo (volumes preserved) ..."
    docker compose down
    ;;
  *)
    echo "Unknown option: $1" >&2
    echo "Usage: $0 [--volumes|-v]" >&2
    exit 1
    ;;
esac

echo "✅ Demo stopped."
