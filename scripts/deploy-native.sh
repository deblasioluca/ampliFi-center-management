#!/usr/bin/env bash
# ============================================================
# ampliFi Center Management — Native deployment (no Docker)
# Designed for RPI / bare-metal Linux.
# All ports configurable via .env.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Load .env
if [ -f "$ROOT_DIR/.env" ]; then
    set -a
    source "$ROOT_DIR/.env"
    set +a
fi

BACKEND_PORT="${BACKEND_PORT:-8180}"
FRONTEND_PORT="${FRONTEND_PORT:-4321}"

echo "=== ampliFi Native Deployment ==="
echo "Backend port:  $BACKEND_PORT"
echo "Frontend port: $FRONTEND_PORT"

# --- Check prerequisites ---
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not found"; exit 1; }
command -v node >/dev/null 2>&1 || { echo "ERROR: node not found"; exit 1; }

# --- Setup backend ---
echo ""
echo "--- Backend setup ---"
cd "$ROOT_DIR/backend"

if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi
source .venv/bin/activate
pip install --upgrade pip > /dev/null 2>&1
pip install -e . 2>&1 | tail -3

# Run migrations if DB is accessible
if python3 -c "
from app.config import settings
from sqlalchemy import create_engine, text
e = create_engine(settings.database_url)
with e.connect() as c:
    c.execute(text('SELECT 1'))
    print('DB OK')
" 2>/dev/null; then
    echo "[ok] Database reachable — running migrations"
    alembic upgrade head || echo "[warn] Migration failed — may need manual intervention"
    python3 -m app.cli seed || echo "[warn] Seed failed"
else
    echo "[warn] Database not reachable — skipping migrations"
fi

# --- Setup frontend ---
echo ""
echo "--- Frontend setup ---"
cd "$ROOT_DIR/frontend"
npm install 2>&1 | tail -3
npm run build 2>&1 | tail -3 || echo "[warn] Frontend build had issues"

echo ""
echo "=== Deployment ready ==="
echo ""
echo "Start backend:  cd $ROOT_DIR/backend && source .venv/bin/activate && uvicorn app.main:app --host 0.0.0.0 --port $BACKEND_PORT"
echo "Start frontend: cd $ROOT_DIR/frontend && FRONTEND_PORT=$FRONTEND_PORT npm run dev"
echo ""
echo "Or use the systemd service files in systemd/"
