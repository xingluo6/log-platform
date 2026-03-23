#!/bin/bash
# =============================================================
# 智能运维监控平台 - 压测脚本
# 使用：bash benchmark.sh
# 依赖：brew install wrk
# =============================================================

# 脚本所在目录（用于找 lua 文件）
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LUA_FILE="$SCRIPT_DIR/post_log.lua"

# 如果没有 lua 文件就创建
cat > "$LUA_FILE" << 'LUA'
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"
wrk.body = '{"service":"bench-svc","level":"INFO","message":"benchmark test"}'
LUA

echo "======================================================"
echo " 智能运维监控平台 压测报告"
echo " 环境：Apple M1 8G / Java 17 / Docker"
echo " 时间：$(date '+%Y-%m-%d %H:%M:%S')"
echo "======================================================"

# ── 测试1：直连采集层（核心链路）────────────────────────────
echo ""
echo "▶ 测试1：直连采集层 /api/log（Netty + Kafka，核心链路）"
echo "  wrk -t12 -c1000 -d30s --script post_log.lua"
echo "------------------------------------------------------"
wrk -t12 -c1000 -d30s --script "$LUA_FILE" http://localhost:8082/api/log

# ── 测试2：经 Gateway 路由────────────────────────────────────
echo ""
echo "▶ 测试2：经 Gateway 路由 /ingest/api/log"
echo "  wrk -t8 -c500 -d20s --script post_log.lua"
echo "------------------------------------------------------"
wrk -t8 -c500 -d20s --script "$LUA_FILE" http://localhost:8888/ingest/api/log

# ── 测试3：DuckDB 聚合查询────────────────────────────────────
echo ""
echo "▶ 测试3：查询接口 /api/stats（DuckDB 列存聚合）"
echo "  wrk -t4 -c100 -d20s"
echo "------------------------------------------------------"
wrk -t4 -c100 -d20s http://localhost:8084/api/stats

echo ""
echo "======================================================"
echo " 压测完成，指标说明："
echo "   Requests/sec  → QPS（核心指标）"
echo "   Latency avg   → 平均延迟"
echo "   99%           → P99延迟"
echo "   Non-2xx       → 应为 0，不为 0 说明服务报错"
echo "======================================================"