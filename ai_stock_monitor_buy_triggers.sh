#!/usr/bin/env bash
set -euo pipefail
cd /opt/ai-stock-combo

set -a
[ -f /opt/hermes-data/.env ] && . /opt/hermes-data/.env
[ -f /opt/ai-stock-combo/config.env ] && . /opt/ai-stock-combo/config.env
set +a

export PYTHONUNBUFFERED=1
python3 monitor_buy_triggers.py "$@"
