#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "main.py" ]; then
  shift
fi

if [ "$(id -u)" = "0" ]; then
  DOCKER=(docker)
else
  DOCKER=(sudo docker)
fi

if [ "$("${DOCKER[@]}" inspect -f '{{.State.Running}}' stock-analyzer 2>/dev/null || true)" != "true" ]; then
  (cd /opt/daily_stock_analysis && "${DOCKER[@]}" compose -f docker/docker-compose.yml up -d analyzer >/dev/null)
fi

exec "${DOCKER[@]}" exec -w /app stock-analyzer env \
  SCHEDULE_ENABLED=false \
  SCHEDULE_RUN_IMMEDIATELY=false \
  RUN_IMMEDIATELY=true \
  python main.py "$@"
