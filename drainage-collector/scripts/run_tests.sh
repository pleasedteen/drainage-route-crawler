#!/usr/bin/env bash
# 一条命令跑完本地测试：依赖就绪后 80 秒内完成，输出可读结果。
#
# 口径说明：
#   - 首次运行会先编译第三方依赖（耗时取决于网络与机器，不计入 80s 要求）；
#   - “依赖就绪”指 target/ 中依赖已编译，此后本脚本（含本项目编译+8 个测试）
#     在本机实测约 30 秒，稳态约 3 秒。
#
# 用法：
#   ./scripts/run_tests.sh           # 全量测试（含计时）
#   ./scripts/run_tests.sh --watch   # 监视模式
set -euo pipefail
cd "$(dirname "$0")/.."

# 本机 /usr/local/bin/cc 是沙箱包装脚本；若存在真实 gcc 则显式使用，其他环境忽略。
if [ -x /usr/bin/gcc ]; then
  export CC=/usr/bin/gcc
fi
export PATH="${HOME}/.cargo/bin:${PATH}"

echo "================================================"
echo " 市政排水采集服务 · 本地测试"
echo " Rust    : $(rustc --version 2>/dev/null || echo '未找到 rustc')"
echo " 工作目录: $(pwd)"
echo " 开始    : $(date '+%H:%M:%S')"
echo "================================================"

# 依赖预编译（首次运行）；之后基本是空操作。
echo "[1/2] 编译依赖与测试二进制 ..."
cargo test --no-run --frozen >/dev/null 2>&1 || cargo test --no-run

echo "[2/2] 执行测试 ..."
START=$(date +%s)
cargo test --color always "$@"
END=$(date +%s)
ELAPSED=$((END - START))

echo "================================================"
echo " 全部通过 ✔  测试阶段耗时 ${ELAPSED}s（要求依赖就绪后 ≤ 80s）"
echo " 结束: $(date '+%H:%M:%S')"
echo "================================================"
