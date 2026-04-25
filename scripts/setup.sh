#!/usr/bin/env bash
# ============================================================
# ampliFi Center Management — Native setup script
# Sets up the app without Docker (e.g., for RPI or bare-metal).
# All ports are read from .env.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== ampliFi Center Management Setup ==="

# Load .env
if [ -f "$ROOT_DIR/.env" ]; then
    set -a
    source "$ROOT_DIR/.env"
    set +a
    echo "[ok] Loaded .env"
else
    echo "[warn] No .env found, using defaults. Copy .env.example to .env first."
fi

# --- Python backend setup ---
echo ""
echo "--- Setting up Python backend ---"
cd "$ROOT_DIR/backend"

if [ ! -d ".venv" ]; then
    python3 -m venv .venv
    echo "[ok] Created virtual environment at backend/.venv"
fi

source .venv/bin/activate
pip install --upgrade pip setuptools wheel > /dev/null 2>&1
pip install -e ".[dev]" 2>&1 | tail -5
echo "[ok] Backend dependencies installed"

# --- Node frontend setup ---
echo ""
echo "--- Setting up Node frontend ---"
cd "$ROOT_DIR/frontend"
npm install 2>&1 | tail -5
echo "[ok] Frontend dependencies installed"

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Ensure PostgreSQL is running on port ${POSTGRES_PORT:-5433}"
echo "  2. Ensure Redis is running on port ${REDIS_PORT:-6380}"
echo "  3. Run: cd backend && source .venv/bin/activate && alembic upgrade head"
echo "  4. Run: cd backend && python -m app.cli seed"
echo "  5. Run: cd backend && uvicorn app.main:app --host 0.0.0.0 --port ${BACKEND_PORT:-8180}"
echo "  6. Run: cd frontend && npm run dev"
