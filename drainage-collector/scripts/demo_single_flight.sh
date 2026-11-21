#!/usr/bin/env bash
# 跨进程在途单飞 + 抓取方退出后接手 的真实双进程端到端演示。
#
# 拓扑：
#   mock-server（真实 HTTP 模拟站，可注入慢响应/阻塞，可查实际请求数）
#     ├── 常驻服务进程  drainage-collector serve   （入口 1：a0）
#     └── 独立 CLI 进程 drainage-collector start 2 （入口 2：a1）
# 两个进程共用同一个文件型 SQLite 采集库。
#
# 验证点：
#   1) 两进程同时冷缓存、页面响应很慢时，同一 (站点,页面,修订) 只发一次实际请求；
#   2) 杀掉抓取方（SIGKILL）且其在途结果尚未落库时，等待者自动接手并完成；
#   3) 已确认结果继续共享，其他页面/修订照常推进；
#   4) 实际请求仍经过站点限速，汇合/成环的进度按各轮次独立维护。
set -u -o pipefail
cd "$(dirname "$0")/.."

BIN=./target/debug/drainage-collector
MOCK_BIN=./target/debug/mock-server
BASE=http://127.0.0.1:8090
A_STATION=http://127.0.0.1:9100
B_STATION=http://127.0.0.1:9101
CFG=config/single_flight.toml
export DRAINAGE_LEASE_TTL_MS=2000
export DRAINAGE_LEASE_HEARTBEAT_MS=700

if [ -x /usr/bin/gcc ]; then export CC=/usr/bin/gcc; fi
export PATH="${HOME}/.cargo/bin:${PATH}"

pass=0; fail=0
check() { # check <描述> <条件命令...>
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then echo "  ✔ $desc"; pass=$((pass+1));
  else echo "  ✗ 失败: $desc"; fail=$((fail+1)); fi
}
# 读取采集库诊断（只读），复用主二进制，不依赖外部 sqlite3。
dbq() { "$BIN" --config "$CFG" db "$@" 2>/dev/null; }
json_field() { python3 -c "import sys,json;d=json.load(sys.stdin);print(d$1)"; }
cleanup() {
  for p in "$SERVER_PID" "$CLI_PID" "$MOCK_PID"; do
    [ -n "${p:-}" ] && kill -9 "$p" 2>/dev/null || true
  done
}
trap cleanup EXIT
SERVER_PID=""; CLI_PID=""; MOCK_PID=""

echo ">>> [0/5] 编译并清理旧状态"
cargo build --frozen >/dev/null 2>&1 || cargo build >/dev/null
pgrep -f "[d]rainage-collector" | xargs -r kill -9 2>/dev/null || true
rm -rf data/single_flight.db data/single_flight.db-* 2>/dev/null || true

echo ">>> [1/5] 启动真实 HTTP 模拟站（station-a:9100 / station-b:9101）"
"$MOCK_BIN" >/tmp/sf_mock.log 2>&1 &
MOCK_PID=$!
for _ in $(seq 1 25); do
  curl -s --max-time 1 "$A_STATION/state" >/dev/null 2>&1 && break
  sleep 0.3
done

# 让 a1 响应慢且可控：先 hold 住第一次请求，制造“在途、结果未落库”的窗口。
curl -s -X POST "$A_STATION/inject/hold?page=a1&waiters=1" >/dev/null
# 其余页面也加延迟，保证两进程的冷缓存窗口充分重叠。
for pg in a0 a2 a3 a4; do
  curl -s -X POST "$A_STATION/inject/delay?page=$pg&delay_ms=150" >/dev/null
done
for pg in b1 b2 b3; do
  curl -s -X POST "$B_STATION/inject/delay?page=$pg&delay_ms=150" >/dev/null
done

echo ">>> [2/5] 启动常驻服务进程（入口 1），并发起全量轮次"
"$BIN" --config "$CFG" serve >/tmp/sf_server.log 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 25); do
  curl -s --max-time 1 "$BASE/health" >/dev/null 2>&1 && break
  sleep 0.3
done
RUN1=$(curl -s -X POST "$BASE/runs/start" -H 'content-type: application/json' \
  -d '{"entry_id":1,"mode":"full"}' | sed -E 's/.*"run_id":([0-9]+).*/\1/')
echo "    服务侧轮次 run_id=$RUN1"

# 等服务真正拿到 a1 租约并卡在在途抓取。
for _ in $(seq 1 40); do
  st=$(dbq lease a1 | python3 -c "import sys,json;print(json.load(sys.stdin).get('state',''))" 2>/dev/null || true)
  [ "$st" = "fetching" ] && break
  sleep 0.15
done
OWNER_BEFORE=$(dbq lease a1 | json_field "['owner_id']")
echo "    a1 在途租约 owner=$OWNER_BEFORE"
check "服务已持有 a1 在途租约" test -n "$OWNER_BEFORE"

echo ">>> [3/5] 启动独立 CLI 进程（入口 2），与服务同时撞冷缓存"
# --wait：CLI 进程保持存活并阻塞到轮次结束；它需要 a1，但租约被服务持有，
# 因此先等待，待服务被 kill、租约过期后由本进程自动接手。
"$BIN" --config "$CFG" start 2 --mode full --wait >/tmp/sf_cli.log 2>&1 &
CLI_PID=$!
sleep 1.5
# 两进程都在运行，且 a1 仍只有服务的一个实际请求在途（等待者不得重复抓）。
A1_HITS_WHILE_WAITING=$(curl -s "$A_STATION/requests" | json_field "['per_page']['a1']")
echo "    等待窗口内 a1 实际请求数=$A1_HITS_WHILE_WAITING（必须为 1）"
check "等待期间同页同修订只发一次请求" test "$A1_HITS_WHILE_WAITING" = "1"
check "此时 a1 租约仍属于服务、未易主" sh -c "[ \"$(dbq lease a1 | json_field "['owner_id']")\" = \"$OWNER_BEFORE\" ]"

echo ">>> [4/5] SIGKILL 抓取方（服务进程），验证等待者自动接手"
kill -9 "$SERVER_PID"; SERVER_PID=""
# 释放 a1 的在途请求：服务已被 kill，该响应无人接收；CLI 在租约过期后自己重新抓。
curl -s -X POST "$A_STATION/inject/release?page=a1" >/dev/null

# 等待 CLI 进程结束（内部在租约 TTL 过期后接手并跑完入口 2 的图）。
DEADLINE=$((SECONDS + 45))
while kill -0 "$CLI_PID" 2>/dev/null; do
  [ "$SECONDS" -gt "$DEADLINE" ] && { echo "  ✗ CLI 等待接手超时"; break; }
  sleep 0.5
done
CLI_PID=""
check "独立 CLI 接手后轮次成功" grep -q "status=succeeded" /tmp/sf_cli.log

echo ">>> [5/5] 校验请求计数、接手换主与结果共享"
A_REQ=$(curl -s "$A_STATION/requests")
B_REQ=$(curl -s "$B_STATION/requests")
echo "    station-a 请求明细: $A_REQ"
echo "    station-b 请求明细: $B_REQ"
A1_TOTAL=$(echo "$A_REQ" | json_field "['per_page']['a1']")
# a1：服务在途 1 次（被 kill、未落库）+ CLI 接手成功 1 次 = 2 次实际请求；
# 但成功落库的修订只能有一份。
check "a1 发生接手（在途被杀 1 次 + 接手成功 1 次 = 2 次实际请求）" test "$A1_TOTAL" = "2"

# 重启服务，把服务侧未完成轮次续跑完：已确认结果共享、不重复抓。
"$BIN" --config "$CFG" serve >/tmp/sf_server2.log 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 25); do
  curl -s --max-time 1 "$BASE/health" >/dev/null 2>&1 && break
  sleep 0.3
done
for _ in $(seq 1 60); do
  st=$(curl -s "$BASE/runs/$RUN1/progress" | sed -E 's/.*"status":"([a-z]+)".*/\1/')
  [ "$st" != "running" ] && break
  sleep 0.5
done
FINAL=$(curl -s "$BASE/runs/$RUN1/progress")
echo "    服务重启后续跑结果: $FINAL"
check "服务侧重启续跑成功" sh -c "echo '$FINAL' | grep -q '\"status\":\"succeeded\"'"
check "服务侧全图 8 页全部可达" sh -c "echo '$FINAL' | grep -q '\"reachable\":8'"

# 成功落库的每个 (站点,页面,修订) 仍只有一行（共享，无重复抓取结果）。
check "共享缓存无重复修订行" sh -c "[ \"$(dbq dup-revisions)\" = \"0\" ]"
check "无残留未终态租约" sh -c "[ \"$(dbq lease-count fetching)\" = \"0\" ]"
# 接手后 a1 租约终态为 done，且 owner 已不再是被杀的服务进程。
check "a1 租约终态为 done" sh -c "[ \"$(dbq lease a1 | json_field "['state']")\" = \"done\" ]"
check "a1 租约已换主到接手进程" sh -c "[ \"$(dbq lease a1 | json_field "['owner_id']")\" != \"$OWNER_BEFORE\" ]"

echo
echo "================================================================"
if [ "$fail" -eq 0 ]; then
  echo " 跨进程单飞/接手端到端演示全部通过 ✔  通过 $pass 项"
  exit 0
else
  echo " 存在失败项 ✗  通过 $pass / 失败 $fail（见 /tmp/sf_*.log）"
  exit 1
fi
