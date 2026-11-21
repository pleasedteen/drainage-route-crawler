#!/usr/bin/env bash
# 增量“只核查受影响路线”验收：一条命令跑完，依赖就绪后 80 秒内结束。
#
# 验证内容（tests/incremental_delta.rs）：
#   1. 跨站回流环失去最后一条入口路线时，增量结果与同截止点全量逐页一致；
#   2. ≥2000 页基线上改动一条 ≤10 页的独立支线，增量累计读取 / 写入的
#      设施与边记录各 ≤100 条（基线准备与结果查询另计）；
#   3. 增量中途退出后重启接着原轮次继续，旧轮次照常查询。
#
# 用法：
#   ./scripts/demo_incremental.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -x /usr/bin/gcc ]; then
  export CC=/usr/bin/gcc
fi
export PATH="${HOME}/.cargo/bin:${PATH}"

echo "================================================"
echo " 增量受影响路线核查 · 验收"
echo " Rust    : $(rustc --version 2>/dev/null || echo '未找到 rustc')"
echo " 开始    : $(date '+%H:%M:%S')"
echo "================================================"

# 依赖预编译（首次运行）；不计入 80 秒口径。
echo "[1/2] 预编译测试二进制 ..."
cargo test --no-run >/dev/null 2>&1 || cargo test --no-run

echo "[2/2] 运行增量验收测试（含基线准备与同截止点全量对照）..."
START=$(date +%s)
cargo test --test incremental_delta -- --nocapture
END=$(date +%s)
ELAPSED=$((END - START))

echo "================================================"
echo " 验收通过 ✔  测试阶段耗时 ${ELAPSED}s（要求 ≤ 80s）"
echo " 说明：预算只统计增量的设施/边读写；基线准备与结果查询另计。"
echo " 结束: $(date '+%H:%M:%S')"
echo "================================================"
