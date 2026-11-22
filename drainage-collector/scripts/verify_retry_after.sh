#!/usr/bin/env bash
# 一键验证 Retry-After 统一等待期：
#   起本地 HTTP 模拟站（真实 axum 监听，HTML/JSON 各一站），
#   采集器走完整 HTTP 链路，核对请求时间线与每轮结果。
#
# 验证点：
#   1. 页面/状态/清单任一请求返回 Retry-After，该站三类请求进入同一等待期，
#      等待结束前不再新发；恢复后自动完成且 reachable=8。
#   2. 状态请求持续失败时轮次中止，截止点绝不被填成 0。
#   3. 故障站等待期间其他站同阶段任务继续推进。
#   4. 每站频率/在途预算与每轮最多 8 个页面槽保持（由集成测试 retry_after 覆盖）。
#
# 用法：
#   ./scripts/verify_retry_after.sh [retry-after 等待秒数，默认 1]
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -x /usr/bin/gcc ]; then
  export CC=/usr/bin/gcc
fi
export PATH="${HOME}/.cargo/bin:${PATH}"

WAIT_SECS="${1:-1}"

echo "================================================"
echo " Retry-After 统一等待期 · 本地 HTTP 一键验证"
echo " 等待秒数: ${WAIT_SECS}"
echo " 开始    : $(date '+%H:%M:%S')"
echo "================================================"

# 1) 完整自动化验证（内置断言，失败即非零退出）。
cargo run --quiet -- verify --wait-secs "${WAIT_SECS}"

# 2) 再跑集成测试套件，覆盖工作槽上限/预算与边界场景。
echo
echo "================================================"
echo " 集成测试（含每轮 8 槽、每站频率/在途预算断言）"
echo "================================================"
cargo test --quiet --test retry_after

echo
echo "全部验证通过 ✔"
