#!/usr/bin/env bash
set -e
set -x

[ ! -d "/dolt/investment_data" ] && echo "initializing dolt repo" && cd /dolt && dolt clone chenditc/investment_data
cd /dolt/investment_data
# 规范远程：强制将 origin 指向 DoltHub（避免误配到本地 3306）
set +e
dolt remote set-url origin chenditc/investment_data >/dev/null 2>&1 || true
set -e

# 尝试拉取；失败则离线继续
set +e
dolt pull || echo "[WARN] dolt pull failed; continue in offline mode"
set -e

# 尝试推送；失败则仅本地保留
set +e
dolt push || echo "[WARN] dolt push failed; changes remain local"
set -e

echo "Updating index weight (with Tushare→AKShare fallback)"
# 动态设置最近两周起始日，避免历史全量扫描
startdate=$(date -d "14 days ago" +%Y%m%d)
python3 /investment_data/tushare/dump_index_weight.py --start_date=$startdate || echo "[WARN] index_weight fallback engaged"
for file in $(ls /investment_data/tushare/index_weight/); 
do  
  dolt table import -u ts_index_weight /investment_data/tushare/index_weight/$file; 
done

echo "Updating index price (with Tushare→AKShare→Yahoo fallback)"
# 若未显式传参，脚本默认区间为最近两周
python3 /investment_data/tushare/dump_index_eod_price.py || echo "[WARN] index_eod_price fallback engaged"
for file in $(ls /investment_data/tushare/index/); 
do   
  dolt table import -u ts_a_stock_eod_price /investment_data/tushare/index/$file; 
done

echo "Updating stock price (with Tushare→AKShare fallback)"
dolt sql-server &
sleep 5 && python3 /investment_data/tushare/update_a_stock_eod_price_to_latest.py
killall dolt

dolt sql --file /investment_data/tushare/regular_update.sql

dolt add -A

status_output=$(dolt status)

# Check if the status output contains the "nothing to commit, working tree clean" message
if [[ $status_output == *"nothing to commit, working tree clean"* ]]; then
    echo "No changes to commit. Working tree is clean."
else
    echo "Changes found. Committing and pushing..."
    # Run the necessary commands
    dolt commit -m "Daily update"
    dolt push 
    echo "Changes committed and pushed."
fi

