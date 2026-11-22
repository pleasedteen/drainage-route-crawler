#!/usr/bin/env bash
# 一条命令验证历史轮次修复：
#   本地站点（真实 HTTP mock-server）+ 旧库夹具（预置被污染的历史轮次）-> 修复 -> 校验。
#
# 用法：
#   ./scripts/verify_repair.sh
#
# 覆盖：
#   1. 旧库夹具：一个“声称在早期截止点成功、结果却含截止点后设施”的历史轮次；
#   2. 按轮次号一条命令修复，用原截止点重新核实，结果一次切换；
#   3. 切换前：按轮次、按入口查询返回原已发布结果；
#   4. 修复后：轮次编号/顺序/截止点不变，误收设施消失，真设施保留；
#   5. 幂等：再次修复不新增轮次、不重抓。
set -euo pipefail
cd "$(dirname "$0")/.."

export RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}"
export CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
export PATH="${CARGO_HOME}/bin:${PATH}"
if [ -x /usr/bin/gcc ]; then export CC=/usr/bin/gcc; fi

WORK=$(mktemp -d)
BASE_PORT=${BASE_PORT:-19100}
MOCK_PID=
trap 'kill ${MOCK_PID:-} 2>/dev/null || true; rm -rf "$WORK"' EXIT

echo "==> [1/6] 构建二进制"
cargo build --bins >/dev/null

echo "==> [2/6] 启动本地站点（HTML:${BASE_PORT} / JSON:$((BASE_PORT+1)))"
./target/debug/mock-server --base-port "$BASE_PORT" >"$WORK/mock.log" 2>&1 &
MOCK_PID=$!
for _ in $(seq 1 50); do
  curl -s --max-time 1 "http://127.0.0.1:${BASE_PORT}/state" >/dev/null 2>&1 && break
  sleep 0.1
done

DB="$WORK/old.db"
CFG="$WORK/repair.toml"
cat >"$CFG" <<TOML
[server]
listen = "127.0.0.1:18080"

[database]
url = "sqlite://$DB"

# 无 [mock] 段 -> 使用真实 HTTP 客户端，打到本地站点。
[[stations]]
id = "station-a"
name = "城东养护站"
base_url = "http://127.0.0.1:${BASE_PORT}"
format = "html"
min_interval_ms = 5
max_inflight = 2
watermark_seq = 0

[[stations]]
id = "station-b"
name = "河西养护站"
base_url = "http://127.0.0.1:$((BASE_PORT+1))"
format = "json"
min_interval_ms = 5
max_inflight = 2
watermark_seq = 0

[[entry_points]]
station_id = "station-a"
page_key = "a0"
label = "城东入流口"
TOML

BIN="./target/debug/drainage-collector --config $CFG"

echo "==> [3/6] 造旧库夹具：跑一轮全量后把截止点回拨到早期"
$BIN start 1 --mode full --wait >/dev/null
# 用 sqlite 把该轮次声称的截止点改成 {a:4, b:7}：结果仍是最新图（误收 a4/a1r2/b2r2）。
python3 - "$DB" <<'PY'
import sqlite3, sys
db = sys.argv[1]
c = sqlite3.connect(db)
c.execute('UPDATE runs SET watermarks = ? WHERE id = 1',
          ('{"station-a":4,"station-b":7}',))
c.commit()
PY

echo "    修复前（按轮次）："
$BIN reachable 1 | python3 -c "import sys,json;d=json.load(sys.stdin);print('       共',len(d),'项:',sorted(f['page_key']+'@'+str(f['revision']) for f in d))"
echo "    修复前（按入口）："
$BIN entry-reachable 1 | tail -n +2 | python3 -c "import sys,json;d=json.load(sys.stdin);print('       共',len(d),'项')"

echo "==> [4/6] 失败不切换：注入站点故障，修复应失败且查询仍返回原结果"
curl -s -X POST "http://127.0.0.1:${BASE_PORT}/inject/fail?times=5" >/dev/null
if $BIN repair 1 >"$WORK/repair_fail.out" 2>"$WORK/repair_fail.err"; then
  echo "    修复在站点故障下不应成功" >&2; exit 1
fi
$BIN reachable 1 > "$WORK/before_switch.json"
$BIN repair-status 1 > "$WORK/failed_status.json"
python3 - "$WORK/before_switch.json" "$WORK/failed_status.json" <<'PY'
import json, sys
items = json.load(open(sys.argv[1]))
st = json.load(open(sys.argv[2]))
keys = sorted((f['station_id'], f['page_key'], f['revision']) for f in items)
assert len(items) == 8, f"失败修复不应改变已发布结果，仍应为 8 项: {keys}"
assert st['status'] == 'failed', f"修复状态应为 failed: {st}"
assert st['published_generation'] == 1, f"失败时不应切换代次: {st}"
print("       OK  修复失败，按轮次查询仍返回原 8 项，发布代次仍为 1（未切换）")
PY

echo "==> [5/6] 一条命令修复轮次 1（故障消除后接着修）"
REPAIR_OUT=$($BIN repair 1)
echo "$REPAIR_OUT" | python3 -c "import sys,json;d=json.load(sys.stdin);print('       generation=',d['generation'],'reachable=',d['reachable'])"

echo "==> [6/6] 校验"
$BIN repair 1 > "$WORK/repair2.json"   # 再执行一次：应为幂等空操作
$BIN reachable 1 > "$WORK/after.json"
$BIN progress 1 > "$WORK/progress.json"
$BIN repair-status 1 > "$WORK/status.json"

python3 - "$WORK/after.json" "$WORK/repair2.json" "$WORK/progress.json" "$WORK/status.json" <<'PY'
import json, sys
after = json.load(open(sys.argv[1]))
repair2 = json.load(open(sys.argv[2]))
progress = json.load(open(sys.argv[3]))
status = json.load(open(sys.argv[4]))

keys = sorted((f['station_id'], f['page_key'], f['revision']) for f in after)
assert len(after) == 7, f"预期 7 项可达，实际 {len(after)}: {keys}"
assert not any(p == 'a4' for _, p, _ in keys), f"a4 登记于截止点后，不应可达: {keys}"
assert ('station-a', 'a1', 1) in keys, f"a1 应回到截止点内 rev1: {keys}"
assert ('station-b', 'b2', 1) in keys, f"b2 应回到截止点内 rev1: {keys}"
assert repair2['already_repaired'] is True, f"再次修复应为幂等空操作: {repair2}"
assert progress['status'] == 'succeeded', f"轮次状态应保持 succeeded: {progress}"
assert progress['watermarks'] == {'station-a': 4, 'station-b': 7}, f"截止点不应被改动: {progress['watermarks']}"
assert status['status'] == 'succeeded' and status['generation'] == 2, f"修复状态异常: {status}"
print("       OK  修复后可达 7 项，误收的 a4 已消失，历史截止点/编号不变")
print("       OK  再次修复幂等（already_repaired=True）")
print("       OK  最终结果:", [f"{s}/{p}@{r}" for s, p, r in keys])
PY

echo
echo "全部校验通过 ✔"
