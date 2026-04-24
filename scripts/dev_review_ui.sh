#!/bin/bash
# 启动 Review UI 本地开发环境：后端 FastAPI (8001) + 前端 Vite (5173)
set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# 清理旧进程
pkill -f "uvicorn backend.app" 2>/dev/null || true
pkill -f "vite" 2>/dev/null || true

echo "→ 启动后端 (FastAPI, port 8001)..."
python3 -m uvicorn backend.app:app --reload --host 127.0.0.1 --port 8001 > /tmp/tso_backend.log 2>&1 &
BACKEND_PID=$!
echo "  后端 PID: $BACKEND_PID"

sleep 2

# 健康检查
if ! curl -sf http://127.0.0.1:8001/api/health > /dev/null; then
  echo "❌ 后端启动失败，查看 /tmp/tso_backend.log"
  cat /tmp/tso_backend.log
  exit 1
fi
echo "  后端就绪"

echo "→ 启动前端 (Vite, port 5173)..."
cd frontend
exec npm run dev -- --host 127.0.0.1
