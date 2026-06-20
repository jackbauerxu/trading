#!/usr/bin/env bash
set -euo pipefail
cd /opt/ai-stock-combo

echo "AI选股组合状态"
echo "时间: $(date '+%F %T %Z')"
echo "Hermes: $(sudo systemctl is-active hermes-gateway.service 2>/dev/null || true)"
echo "Telegram代理: $(sudo systemctl is-active hermes-telegram-proxy.service 2>/dev/null || true)"
echo "股票池数量: $(python3 - <<'PY'
from pathlib import Path
from run_daily_pipeline import read_stock_pool
print(len(read_stock_pool(Path('stock_pool.yaml'))))
PY
)"
echo "A股数量: $(python3 - <<'PY'
import re
from pathlib import Path
from run_daily_pipeline import read_stock_pool
pool = read_stock_pool(Path('stock_pool.yaml'))
print(sum(1 for x in pool if re.fullmatch(r'\d{6}', x['symbol'])))
PY
)"

if [ -f outputs/final_top10.md ]; then
  echo "最近报告: /opt/ai-stock-combo/outputs/final_top10.md"
  echo "报告时间: $(python3 - <<'PY'
from pathlib import Path
import datetime as dt
p = Path('outputs/final_top10.md')
print(dt.datetime.fromtimestamp(p.stat().st_mtime).strftime('%F %T'))
PY
)"
else
  echo "最近报告: 暂无"
fi
