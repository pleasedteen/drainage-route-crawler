#!/usr/bin/env bash
# 变更清单游标／断点续采验收：本地分页站 + SQLite，中断后核对请求游标与轮次结果。
#
# 一条命令跑完，依赖就绪后 80 秒内完成：
#   ./scripts/verify_change_cursor.sh
#
# 覆盖：
#   - 清单请求携带轮次截止点，逐页续读，游标单调前进；
#   - 每站最多保留一页未落库清单（不整段攒内存）；
#   - 重启后从已确认游标续读，不重读已收到的页；
#   - 登记序号有空档也取齐本轮变更；
#   - 新轮次卡在后续清单页时，已齐清单的较早轮次先完成。
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -x /usr/bin/gcc ]; then
  export CC=/usr/bin/gcc
fi
export PATH="${HOME}/.cargo/bin:${PATH}"

echo "================================================"
echo " 变更清单游标 / 断点续采验收"
echo " Rust : $(rustc --version 2>/dev/null || echo '未找到 rustc')"
echo " 开始 : $(date '+%H:%M:%S')"
echo "================================================"

echo "[1/2] 编译（依赖已就绪时基本为空操作）..."
cargo test --no-run --frozen >/dev/null 2>&1 || cargo test --no-run

echo "[2/2] 运行端到端验收（本地分页站 + 文件型 SQLite）..."
START=$(date +%s)
cargo test --color always --test change_cursor "$@"
END=$(date +%s)
ELAPSED=$((END - START))

echo "================================================"
echo " 验收通过 ✔ 耗时 ${ELAPSED}s（要求 ≤ 80s）"
echo " 结束: $(date '+%H:%M:%S')"
echo "================================================"
