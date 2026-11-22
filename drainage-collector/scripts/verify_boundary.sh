#!/usr/bin/env bash
# 站点访问边界 + 缓存来源证明：用本地模拟站与旧库夹具一条命令验证。
#
# 覆盖：
#   tests/boundary.rs    越界页键拦截（路径逃逸/多段路径/未配置站点）、
#                        合法站内跳转可采、跨站自动跳转拒绝、显式跨站链接可采
#   tests/provenance.rs  旧库夹具（schema v1，无来源证明）自动迁移、
#                        旧缓存首次用到重新核实、核实后跨入口/跨重启复用、
#                        历史轮次原样可查
#
# 用法：
#   ./scripts/verify_boundary.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# 本机 /usr/local/bin/cc 是沙箱包装脚本；若存在真实 gcc 则显式使用，其他环境忽略。
if [ -x /usr/bin/gcc ]; then
  export CC=/usr/bin/gcc
fi
export PATH="${HOME}/.cargo/bin:${PATH}"

echo "================================================"
echo " 站点访问边界 + 缓存来源证明 · 一条命令验证"
echo " 本地模拟站（进程内 + HTTP 监听）与旧库夹具（schema v1）"
echo "================================================"

echo "[1/2] 站点访问边界（本地模拟站） ..."
cargo test --test boundary "$@"

echo "[2/2] 缓存来源证明（旧库夹具） ..."
cargo test --test provenance "$@"

echo "================================================"
echo " 全部通过 ✔  站点边界与来源证明验证完成"
echo "================================================"
