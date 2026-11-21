#!/usr/bin/env bash
# 端到端演示：启动服务 -> 起轮次 -> SIGKILL 模拟崩溃 -> 重启自动续跑 -> 查询结果。
# 暂停/继续的同类演示见 scripts/verify_pause.sh。
set -u
BIN=./target/debug/drainage-collector
BASE=http://127.0.0.1:8080

pgrep -f "[t]arget/debug/drainage-collector" | xargs -r kill -9
sleep 1
rm -rf data

echo ">>> 启动第一个服务进程"
"$BIN" serve > /tmp/s1.log 2>&1 &
P1=$!
for _ in $(seq 1 25); do
  curl -s --max-time 1 "$BASE/health" >/dev/null 2>&1 && break
  sleep 1
done

echo ">>> 启动全量采集轮次"
curl -s -X POST "$BASE/runs/start" -H 'content-type: application/json' \
  -d '{"entry_id":1,"mode":"full"}'
echo
sleep 0.15
echo ">>> 崩溃前进度: $(curl -s "$BASE/runs/1/progress")"

echo ">>> SIGKILL 服务进程（模拟进程中途退出）"
kill -9 "$P1" 2>/dev/null || true
wait "$P1" 2>/dev/null || true
sleep 1

echo ">>> 重启服务（应自动续跑 run 1）"
"$BIN" serve > /tmp/s2.log 2>&1 &
P2=$!
for _ in $(seq 1 25); do
  curl -s --max-time 1 "$BASE/health" >/dev/null 2>&1 && break
  sleep 1
done
grep -E "恢复|轮次" /tmp/s2.log || true

for _ in $(seq 1 40); do
  st=$(curl -s "$BASE/runs/1/progress" | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])" 2>/dev/null)
  [ "${st:-}" != "running" ] && break
  sleep 0.3
done
echo ">>> 重启后续跑结果: $(curl -s "$BASE/runs/1/progress")"
echo ">>> 入口已核实可达设施:"
curl -s "$BASE/entries/1/reachable" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('  基于轮次', d['run_id'], '共', len(d['facilities']), '个设施')
for f in d['facilities']:
    print(f\"  - {f['station_id']}/{f['page_key']} rev={f['revision']} depth={f['depth']} {f.get('name') or ''}\")
"
kill "$P2" 2>/dev/null || true
exit 0
