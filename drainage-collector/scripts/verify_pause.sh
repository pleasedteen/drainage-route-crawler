#!/usr/bin/env bash
# 一条命令验证：暂停 -> 重启 -> 继续（真实进程 + 模拟站 + 文件型 SQLite）。
#
# 使用独立的临时目录与端口，不影响本地 data/ 与 8080。
# 依赖就绪时约 40-60 秒（含两次服务启动与轮次推进），满足 ≤80s 口径。
#
# 用法：./scripts/verify_pause.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -x /usr/bin/gcc ]; then
  export CC=/usr/bin/gcc
fi
export PATH="${HOME}/.cargo/bin:${PATH}"

BIN=./target/debug/drainage-collector
PORT=$((18000 + RANDOM % 1000))
WORK=$(mktemp -d)
trap 'kill ${P1:-} ${P2:-} ${P3:-} 2>/dev/null || true; rm -rf "$WORK"' EXIT
BASE="http://127.0.0.1:${PORT}"
DB="sqlite://${WORK}/pause.db"
CFG="${WORK}/config.toml"

# 站点放慢，确保暂停时轮次仍在进行；条目与演示图一致。
cat >"$CFG" <<EOF
[server]
listen = "127.0.0.1:${PORT}"

[database]
url = "${DB}"

[mock]
base_port = 9100

[[stations]]
id = "station-a"
name = "城东养护站"
base_url = "http://127.0.0.1:9100"
format = "html"
min_interval_ms = 120
max_inflight = 1
watermark_seq = 0

[[stations]]
id = "station-b"
name = "河西养护站"
base_url = "http://127.0.0.1:9101"
format = "json"
min_interval_ms = 120
max_inflight = 1
watermark_seq = 0

[[entry_points]]
station_id = "station-a"
page_key = "a0"
label = "城东入流口"
EOF

jget() { python3 -c 'import sys,json
d=json.load(sys.stdin)
for k in sys.argv[1:]:
    d=d[k]
print(d)' "$@"; }

start_server() {
  "$BIN" --config "$CFG" serve >"$WORK/$1.log" 2>&1 &
  if [ "$1" = "s1" ]; then P1=$!; else P2=$!; fi
  for _ in $(seq 1 60); do
    if curl -s --max-time 1 "$BASE/health" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "服务未能启动" >&2; cat "$WORK/$1.log" >&2; exit 1
}

echo "[1/6] 编译（依赖就绪后基本是空操作）..."
cargo build --frozen --quiet 2>/dev/null || cargo build --quiet

echo "[2/6] 启动第一个服务进程"
start_server s1
echo "      进度 $(curl -s -X POST "$BASE/runs/start" -H 'content-type: application/json' -d '{"entry_id":1,"mode":"full"}' | jget run_id) 号轮次已发起"

# 等轮次确认落库一部分页面（进度里的 done 非 0）。
for _ in $(seq 1 200); do
  DONE=$(curl -s "$BASE/runs/1/progress" | jget done)
  [ "$DONE" -ge 2 ] && break
  sleep 0.05
done
BEFORE=$(curl -s "$BASE/runs/1/progress")
echo "      暂停前进度: $BEFORE"

echo "[3/6] 确认暂停（站点若正在退避也应立即生效）"
set +e
PAUSE_RESP=$(curl -s -X POST "$BASE/runs/1/pause")
set -e
echo "      暂停响应: $PAUSE_RESP"
PAUSED_AT=$(echo "$PAUSE_RESP" | jget paused_at)
[ "$PAUSED_AT" != "None" ] || { echo "未记录暂停时刻"; exit 1; }
DONE_PAUSED=$(curl -s "$BASE/runs/1/progress" | jget done)
DONE_BEFORE=$(echo "$BEFORE" | jget done)
[ "$DONE_PAUSED" = "$DONE_BEFORE" ] || { echo "暂停改变了进度 $DONE_BEFORE -> $DONE_PAUSED"; exit 1; }
REACH_PAUSED=$(curl -s "$BASE/runs/1/reachable" | python3 -c "import sys,json;print(len(json.load(sys.stdin)))")

# 暂停后短时间进度与可达数不再变化；同入口不能另起轮次。
sleep 0.6
DONE_LATER=$(curl -s "$BASE/runs/1/progress" | jget done)
REACH_LATER=$(curl -s "$BASE/runs/1/reachable" | python3 -c "import sys,json;print(len(json.load(sys.stdin)))")
[ "$DONE_LATER" = "$DONE_PAUSED" ] && [ "$REACH_LATER" = "$REACH_PAUSED" ] \
  || { echo "暂停后进度仍在变化"; exit 1; }
DUP_CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/runs/start" \
  -H 'content-type: application/json' -d '{"entry_id":1,"mode":"full"}' || true)
[ "$DUP_CODE" = "409" ] || { echo "暂停中的入口不应允许另起轮次（got $DUP_CODE）"; exit 1; }
echo "      确认暂停：done=$DONE_PAUSED reachable=$REACH_PAUSED；同入口另起轮次被拒(409)"

echo "[4/6] SIGKILL 服务进程并重启（模拟进程退出）"
kill -9 "$P1" 2>/dev/null || true
wait "$P1" 2>/dev/null || true
start_server s2
AFTER_RESTART=$(curl -s "$BASE/runs/1/progress")
echo "      重启后进度: $AFTER_RESTART"
STATUS=$(echo "$AFTER_RESTART" | jget status)
[ "$STATUS" = "paused" ] || { echo "重启后暂停状态丢失（status=$STATUS）"; exit 1; }
DONE_RESTART=$(echo "$AFTER_RESTART" | jget done)
[ "$DONE_RESTART" = "$DONE_PAUSED" ] || { echo "重启后进度被推进 $DONE_PAUSED -> $DONE_RESTART"; exit 1; }
grep -q "已恢复未完成轮次" "$WORK/s2.log" && { echo "暂停轮次不应被自动恢复"; exit 1; } || true
DUP2=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/runs/start" \
  -H 'content-type: application/json' -d '{"entry_id":1,"mode":"full"}')
[ "$DUP2" = "409" ] || { echo "重启后同入口仍不应另起轮次（got $DUP2）"; exit 1; }
WM1=$(curl -s "$BASE/runs/1" | python3 -c "import sys,json;print(json.dumps(json.load(sys.stdin)['watermarks'],sort_keys=True))")
echo "      重启后仍为 paused，进度未变，同入口另起轮次仍被拒(409)"

echo "[5/6] 继续原轮次（沿用原 ID 与截止点）"
RESUME=$(curl -s -X POST "$BASE/runs/1/resume")
echo "      继续响应: $RESUME"
RID=$(echo "$RESUME" | jget run_id)
[ "$RID" = "1" ] || { echo "继续未沿用原轮次 ID（got $RID）"; exit 1; }
WM2=$(echo "$RESUME" | python3 -c "import sys,json;print(json.dumps(json.load(sys.stdin)['watermarks'],sort_keys=True))")
[ "$WM1" = "$WM2" ] || { echo "继续改变了各站截止点"; exit 1; }
REACH_AFTER_RESUME=$(curl -s "$BASE/runs/1/reachable" | python3 -c "import sys,json;print(len(json.load(sys.stdin)))")
[ "$REACH_AFTER_RESUME" -ge "$REACH_PAUSED" ] || { echo "继续后已核实结果丢失"; exit 1; }

for _ in $(seq 1 300); do
  ST=$(curl -s "$BASE/runs/1/progress" | jget status)
  [ "$ST" != "running" ] && break
  sleep 0.1
done
FINAL=$(curl -s "$BASE/runs/1/progress")
echo "      最终进度: $FINAL"
[ "$(echo "$FINAL" | jget status)" = "succeeded" ] || { echo "继续后轮次未完成"; exit 1; }
[ "$(echo "$FINAL" | jget reachable)" = "8" ] || { echo "继续后可达设施不完整"; exit 1; }
[ "$(echo "$FINAL" | jget run_id)" = "1" ] || { echo "继续后轮次 ID 变化"; exit 1; }

echo "[6/6] 校验继续后复用了已确认抓取（不重复抓同一修订）"
python3 - "$WORK/pause.db" <<'PY'
import sqlite3, sys
db = sqlite3.connect(sys.argv[1])
dup = db.execute(
    "SELECT COUNT(*) FROM (SELECT 1 FROM page_revisions "
    "GROUP BY station_id, page_key, revision HAVING COUNT(*) > 1)"
).fetchone()[0]
assert dup == 0, f"存在重复抓取修订 {dup} 行"
rows = db.execute("SELECT COUNT(*) FROM page_revisions").fetchone()[0]
print(f"      修订缓存 {rows} 行，无重复抓取")
PY

kill "$P2" 2>/dev/null || true
echo "================================================"
echo " 暂停 → 重启 → 继续 验证通过 ✔"
echo "================================================"
