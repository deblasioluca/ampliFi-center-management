# ============================================================
# ampliFi Center Management — Makefile
# ============================================================
# Run from the project root:  cd /path/to/ampliFi-center-management && make <target>
#
# Proxy handling:
#   HTTPS_PROXY / HTTP_PROXY in .env are ONLY used for pip and npm installs.
#   git pull does NOT use the proxy.
#   (Same pattern as sap-ai-consultant Makefile.)
# ============================================================

.PHONY: help start stop restart status setup update verify load-sample delete-sample \
        seed logs git-setup

SHELL := /bin/bash
ROOT_DIR := $(shell pwd)
BACKEND_DIR := $(ROOT_DIR)/backend
FRONTEND_DIR := $(ROOT_DIR)/frontend
VENV := $(BACKEND_DIR)/.venv
BACKEND_PID := $(ROOT_DIR)/.amplifi-backend.pid
FRONTEND_PID := $(ROOT_DIR)/.amplifi-frontend.pid
PIP_TRUST := --trusted-host pypi.org --trusted-host files.pythonhosted.org

# Read ports from .env (without exporting everything)
BACKEND_PORT := $(shell grep -E '^BACKEND_PORT=' $(ROOT_DIR)/.env 2>/dev/null | head -1 | cut -d= -f2-)
FRONTEND_PORT := $(shell grep -E '^FRONTEND_PORT=' $(ROOT_DIR)/.env 2>/dev/null | head -1 | cut -d= -f2-)
BACKEND_PORT := $(if $(BACKEND_PORT),$(BACKEND_PORT),8180)
FRONTEND_PORT := $(if $(FRONTEND_PORT),$(FRONTEND_PORT),4321)

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Core lifecycle
# ---------------------------------------------------------------------------

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

start: ## Start backend + frontend
	@echo "=== Starting ampliFi ==="
	@if [ -d $(FRONTEND_DIR) ] && [ -f $(FRONTEND_DIR)/package.json ]; then \
		if [ -d $(FRONTEND_DIR)/dist ]; then \
			echo "[ok] Frontend already built (serving from backend)"; \
		else \
			echo "Building frontend..."; \
			cd $(FRONTEND_DIR) && npm run build && \
			echo "[ok] Frontend built — served by backend on port $(BACKEND_PORT)"; \
		fi; \
	else \
		echo "[skip] Frontend not found (no frontend/package.json)"; \
	fi
	@if [ -f $(BACKEND_PID) ] && kill -0 $$(cat $(BACKEND_PID)) 2>/dev/null; then \
		echo "Backend already running (PID $$(cat $(BACKEND_PID)))"; \
	else \
		cd $(BACKEND_DIR) && \
		source $(VENV)/bin/activate && \
		export $$(grep -E '^(TLS_MODE|TLS_CERT_FILE|TLS_KEY_FILE)=' $(ROOT_DIR)/.env 2>/dev/null | xargs) 2>/dev/null; \
		TLS_ARGS=""; \
		if [ "$${TLS_MODE:-off}" = "direct" ]; then \
			_CERT="$${TLS_CERT_FILE}"; \
			_KEY="$${TLS_KEY_FILE}"; \
			case "$$_CERT" in /*) ;; *) _CERT="$(ROOT_DIR)/$$_CERT" ;; esac; \
			case "$$_KEY" in /*) ;; *) _KEY="$(ROOT_DIR)/$$_KEY" ;; esac; \
			TLS_ARGS="--ssl-certfile $$_CERT --ssl-keyfile $$_KEY"; \
			echo "[tls] Direct mode: uvicorn will serve HTTPS"; \
		fi; \
		export LOG_FILE="$(ROOT_DIR)/amplifi-backend.log"; \
		nohup uvicorn app.main:app --host 0.0.0.0 --port $(BACKEND_PORT) $$TLS_ARGS \
			>> $(ROOT_DIR)/amplifi-backend.log 2>&1 & \
		echo $$! > $(BACKEND_PID) && \
		echo "[ok] Backend started on port $(BACKEND_PORT) (PID $$!)"; \
	fi

stop: ## Stop backend + frontend
	@echo "=== Stopping ampliFi ==="
	@if [ -f $(BACKEND_PID) ]; then \
		PID=$$(cat $(BACKEND_PID)); \
		if kill -0 $$PID 2>/dev/null; then \
			kill $$PID && echo "[ok] Backend stopped (PID $$PID)"; \
		else \
			echo "[ok] Backend was not running (stale PID)"; \
		fi; \
		rm -f $(BACKEND_PID); \
	else \
		echo "[ok] Backend PID file not found"; \
	fi
	@ps aux 2>/dev/null | grep "[u]vicorn app.main:app.*--port $(BACKEND_PORT)" | awk '{print $$2}' | \
		xargs -r kill 2>/dev/null && echo "[ok] Killed orphan backend on port $(BACKEND_PORT)" || true
	@if [ -f $(FRONTEND_PID) ]; then \
		PID=$$(cat $(FRONTEND_PID)); \
		if kill -0 $$PID 2>/dev/null; then \
			kill $$PID && echo "[ok] Frontend stopped (PID $$PID)"; \
		else \
			echo "[ok] Frontend was not running (stale PID)"; \
		fi; \
		rm -f $(FRONTEND_PID); \
	else \
		echo "[ok] Frontend not running"; \
	fi

restart: stop ## Restart backend + frontend
	@sleep 1
	@$(MAKE) start

status: ## Show whether backend + frontend are running
	@echo "=== ampliFi Status ==="
	@if [ -f $(BACKEND_PID) ] && kill -0 $$(cat $(BACKEND_PID)) 2>/dev/null; then \
		echo "Backend:  running (PID $$(cat $(BACKEND_PID)), port $(BACKEND_PORT))"; \
		curl -sk https://127.0.0.1:$(BACKEND_PORT)/api/healthz 2>/dev/null || \
		curl -s http://127.0.0.1:$(BACKEND_PORT)/api/healthz 2>/dev/null || true; \
		echo ""; \
	else \
		echo "Backend:  not running"; \
	fi
	@if [ -f $(FRONTEND_PID) ] && kill -0 $$(cat $(FRONTEND_PID)) 2>/dev/null; then \
		echo "Frontend: running (PID $$(cat $(FRONTEND_PID)), port $(FRONTEND_PORT))"; \
	else \
		echo "Frontend: not running"; \
	fi

# ---------------------------------------------------------------------------
# Setup & update
# ---------------------------------------------------------------------------

setup: ## Initial setup: venv, deps, build frontend, DB init, seed, start
	@echo "=== ampliFi Setup ==="
	@echo "==> Creating virtual environment..."
	@cd $(BACKEND_DIR) && python3 -m venv $(VENV)
	@echo "==> Installing Python dependencies..."
	@export $$(grep -E '^(HTTPS?_PROXY|NO_PROXY)=' $(ROOT_DIR)/.env 2>/dev/null | xargs) 2>/dev/null; \
	 cd $(BACKEND_DIR) && source $(VENV)/bin/activate && \
	 pip install $(PIP_TRUST) --upgrade pip > /dev/null 2>&1 && \
	 pip install $(PIP_TRUST) -e ".[dev]" 2>&1 | tail -5
	@unset HTTP_PROXY HTTPS_PROXY NO_PROXY 2>/dev/null || true
	@echo "[ok] Backend dependencies installed"
	@echo "==> Building frontend..."
	@if [ -d $(FRONTEND_DIR) ] && [ -f $(FRONTEND_DIR)/package.json ]; then \
		export $$(grep -E '^(HTTPS?_PROXY|NO_PROXY)=' $(ROOT_DIR)/.env 2>/dev/null | xargs) 2>/dev/null; \
		cd $(FRONTEND_DIR) && npm install 2>&1 | tail -3 && npm run build 2>&1 | tail -3 && \
		echo "[ok] Frontend built"; \
	else \
		echo "[skip] No frontend directory"; \
	fi
	@unset HTTP_PROXY HTTPS_PROXY NO_PROXY 2>/dev/null || true
	@echo "==> Initializing database..."
	@cd $(BACKEND_DIR) && \
		source $(VENV)/bin/activate && \
		python -c "\
from app.models.base import Base; \
import app.models; \
from app.infra.db.session import engine; \
from sqlalchemy import text; \
conn = engine.connect(); \
conn.execute(text('CREATE SCHEMA IF NOT EXISTS cleanup')); \
conn.commit(); \
Base.metadata.create_all(engine); \
print('[ok] Database tables created')" && \
		python -m alembic stamp head && \
		echo "[ok] Alembic stamped at head" && \
		python -m app.cli seed && \
		echo "[ok] Sample data loaded"
	@echo "==> Starting application..."
	@$(MAKE) start
	@echo ""
	@echo "=== Setup complete! ==="
	@echo "  Backend:  http://0.0.0.0:$(BACKEND_PORT)"
	@echo "  Frontend: http://0.0.0.0:$(FRONTEND_PORT)"
	@echo "  Tip: run 'make git-setup' to store GitHub credentials for git pull."

update: ## Pull latest code, rebuild frontend, reinstall backend, restart  [CLEAN=1 to wipe dist/]
	@LOG=$(ROOT_DIR)/.amplifi-update.log; \
	: > "$$LOG"; \
	echo "=== ampliFi Update ===" | tee -a "$$LOG"; \
	echo "Started:  $$(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$$LOG"; \
	echo "Log file: $$LOG" | tee -a "$$LOG"; \
	echo ""; \
	set -eo pipefail; \
	OLD_SHA=$$(git rev-parse --short HEAD 2>/dev/null || echo "?"); \
	if [ "$(CLEAN)" = "1" ]; then \
		echo "==> Clean mode: removing build artifacts..." | tee -a "$$LOG"; \
		rm -rf $(FRONTEND_DIR)/dist $(FRONTEND_DIR)/.astro 2>/dev/null || true; \
		find $(BACKEND_DIR) -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true; \
		echo "[ok] Cleaned dist/, .astro/, __pycache__/" | tee -a "$$LOG"; \
	fi; \
	git config --global --add safe.directory "$$(pwd)" 2>/dev/null || true; \
	git config http.sslVerify false 2>/dev/null || true; \
	echo "==> Pulling latest code..." | tee -a "$$LOG"; \
	git pull 2>&1 | tee -a "$$LOG"; \
	NEW_SHA=$$(git rev-parse --short HEAD); \
	if [ "$$OLD_SHA" = "$$NEW_SHA" ]; then \
		echo "[ok] Already at $$NEW_SHA — no new commits" | tee -a "$$LOG"; \
	else \
		echo "[ok] $$OLD_SHA → $$NEW_SHA" | tee -a "$$LOG"; \
	fi; \
	echo "==> Building frontend..." | tee -a "$$LOG"; \
	if [ -d $(FRONTEND_DIR) ] && [ -f $(FRONTEND_DIR)/package.json ]; then \
		export $$(grep -E '^(HTTPS?_PROXY|NO_PROXY)=' $(ROOT_DIR)/.env 2>/dev/null | xargs) 2>/dev/null; \
		cd $(FRONTEND_DIR) && \
			npm install 2>&1 | tee -a "$$LOG" | tail -3 && \
			npm run build 2>&1 | tee -a "$$LOG" | tail -3; \
		echo "[ok] Frontend rebuilt" | tee -a "$$LOG"; \
	else \
		echo "[skip] No frontend directory" | tee -a "$$LOG"; \
	fi; \
	unset HTTP_PROXY HTTPS_PROXY NO_PROXY 2>/dev/null || true; \
	echo "==> Installing Python dependencies..." | tee -a "$$LOG"; \
	export $$(grep -E '^(HTTPS?_PROXY|NO_PROXY)=' $(ROOT_DIR)/.env 2>/dev/null | xargs) 2>/dev/null; \
	cd $(BACKEND_DIR) && source $(VENV)/bin/activate && \
		pip install $(PIP_TRUST) -e ".[dev]" 2>&1 | tee -a "$$LOG" | tail -3; \
	unset HTTP_PROXY HTTPS_PROXY NO_PROXY 2>/dev/null || true; \
	echo "[ok] Backend dependencies updated" | tee -a "$$LOG"; \
	echo "==> Stopping backend before migrations..." | tee -a "$$LOG"
	@$(MAKE) -s stop 2>&1 | tee -a $(ROOT_DIR)/.amplifi-update.log || true
	@set -eo pipefail; LOG=$(ROOT_DIR)/.amplifi-update.log; \
	echo "==> Terminating stale DB sessions before migration..." | tee -a "$$LOG"; \
	( sudo -u postgres psql -d acm -c \
	  "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND state IN ('idle in transaction','active') AND NOW() - state_change > interval '30 seconds';" \
	  2>&1 || echo "(skip — could not terminate sessions)" ) | tee -a "$$LOG" | tail -3; \
	sleep 1; \
	echo "==> Applying database migrations..." | tee -a "$$LOG"; \
	cd $(BACKEND_DIR) && source $(VENV)/bin/activate && \
		python -m alembic upgrade head 2>&1 | tee -a "$$LOG" | tail -5; \
	echo "[ok] Database migrations applied" | tee -a "$$LOG"
	@$(MAKE) -s start 2>&1 | tee -a $(ROOT_DIR)/.amplifi-update.log
	@LOG=$(ROOT_DIR)/.amplifi-update.log; \
	echo "" | tee -a "$$LOG"; \
	echo "==> Verifying deployment..." | tee -a "$$LOG"; \
	NEW_SHA=$$(git rev-parse --short HEAD); \
	HEALTHY=0; \
	for i in $$(seq 1 30); do \
		if curl -fskS -o /dev/null https://127.0.0.1:$(BACKEND_PORT)/api/healthz 2>/dev/null || \
		   curl -fsS -o /dev/null http://127.0.0.1:$(BACKEND_PORT)/api/healthz 2>/dev/null; then \
			HEALTHY=1; break; \
		fi; \
		sleep 2; \
	done; \
	echo ""; \
	echo "=== Update complete ===" | tee -a "$$LOG"; \
	echo "Code:     $$NEW_SHA ($(shell git log -1 --format=%s HEAD 2>/dev/null | head -c 60))" | tee -a "$$LOG"; \
	if [ -d $(FRONTEND_DIR)/dist ]; then \
		FILES=$$(find $(FRONTEND_DIR)/dist -type f 2>/dev/null | wc -l); \
		SIZE=$$(du -sh $(FRONTEND_DIR)/dist 2>/dev/null | cut -f1); \
		echo "Frontend: $$FILES files, $$SIZE in $(FRONTEND_DIR)/dist" | tee -a "$$LOG"; \
	fi; \
	if [ $$HEALTHY = 1 ]; then \
		echo "Backend:  ✓ healthy on port $(BACKEND_PORT)" | tee -a "$$LOG"; \
	else \
		echo "Backend:  ⚠ NOT responding to /api/healthz on port $(BACKEND_PORT) (may still be starting)" | tee -a "$$LOG"; \
		echo "          Check: tail -50 $(ROOT_DIR)/amplifi-backend.log" | tee -a "$$LOG"; \
	fi; \
	echo "Finished: $$(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$$LOG"; \
	echo "Full log: $$LOG" | tee -a "$$LOG"

verify: ## Sanity-check that running services match the latest committed code
	@echo "=== ampliFi Verify ==="
	@echo ""
	@HEAD_SHA=$$(git rev-parse --short HEAD 2>/dev/null || echo "?"); \
	echo "Git HEAD:   $$HEAD_SHA"
	@echo ""
	@echo "==> Frontend"
	@if [ -d $(FRONTEND_DIR)/dist ]; then \
		FILES=$$(find $(FRONTEND_DIR)/dist -type f | wc -l); \
		BUILT=$$(stat -c '%Y' $(FRONTEND_DIR)/dist 2>/dev/null || stat -f '%m' $(FRONTEND_DIR)/dist 2>/dev/null); \
		HEAD_TS=$$(git log -1 --format=%ct HEAD 2>/dev/null || echo 0); \
		BUILT_HUMAN=$$(date -d @$$BUILT '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -r $$BUILT '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo "?"); \
		HEAD_HUMAN=$$(date -d @$$HEAD_TS '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -r $$HEAD_TS '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo "?"); \
		echo "  dist/ exists ($$FILES files)"; \
		echo "  Built at:   $$BUILT_HUMAN"; \
		echo "  HEAD at:    $$HEAD_HUMAN"; \
		if [ $$BUILT -lt $$HEAD_TS ]; then \
			echo "  ✗ STALE: dist/ is older than HEAD — run 'make update' to rebuild"; \
		else \
			echo "  ✓ dist/ is newer than HEAD"; \
		fi; \
	else \
		echo "  ✗ $(FRONTEND_DIR)/dist does not exist — run 'make update'"; \
	fi
	@echo ""
	@echo "==> Backend"
	@if [ -f $(BACKEND_PID) ] && kill -0 $$(cat $(BACKEND_PID)) 2>/dev/null; then \
		PID=$$(cat $(BACKEND_PID)); \
		PROC_START=$$(ps -p $$PID -o lstart= 2>/dev/null | xargs); \
		echo "  Running:    PID $$PID"; \
		echo "  Started at: $$PROC_START"; \
		HEAD_HUMAN=$$(git log -1 --format='%cd' --date=format:'%Y-%m-%d %H:%M:%S' HEAD 2>/dev/null || echo "?"); \
		echo "  HEAD time:  $$HEAD_HUMAN"; \
		if curl -fskS -o /dev/null https://127.0.0.1:$(BACKEND_PORT)/api/healthz 2>/dev/null || \
		   curl -fsS -o /dev/null http://127.0.0.1:$(BACKEND_PORT)/api/healthz 2>/dev/null; then \
			echo "  ✓ /api/healthz responds OK"; \
		else \
			echo "  ✗ /api/healthz NOT responding"; \
		fi; \
	else \
		echo "  ✗ Backend not running — run 'make start' or 'make update'"; \
	fi
	@echo ""

# ---------------------------------------------------------------------------
# Data management
# ---------------------------------------------------------------------------

load-sample: ## Generate sample data (entities, cost centers, balances, etc.)
	@cd $(BACKEND_DIR) && \
		source $(VENV)/bin/activate && \
		python -c "from app.services.seed import generate_sample_data; generate_sample_data()" && \
		echo "[ok] Sample data generated"

delete-sample: ## Delete all sample data (keeps admin user and routines)
	@cd $(BACKEND_DIR) && \
		source $(VENV)/bin/activate && \
		python -c "from app.services.seed import delete_sample_data; delete_sample_data()" && \
		echo "[ok] Sample data deleted"

seed: ## Run full seed (admin user + sample data + routines)
	@cd $(BACKEND_DIR) && \
		source $(VENV)/bin/activate && \
		python -m app.cli seed

logs: ## Tail the backend log
	@tail -f $(ROOT_DIR)/amplifi-backend.log

# ---------------------------------------------------------------------------
# Git credentials
# ---------------------------------------------------------------------------

git-setup: ## Store GitHub credentials so git pull works without prompting
	@git config --global --add safe.directory "$$(pwd)" 2>/dev/null || true
	@git config http.sslVerify false 2>/dev/null || true
	@echo "This will store your GitHub credentials on disk so 'git pull' and"
	@echo "'make update' work without prompting for username/password each time."
	@echo ""
	@echo "You need a GitHub Personal Access Token (PAT)."
	@echo "Create one at: https://github.com/settings/tokens"
	@echo "  -> Fine-grained token -> Repository access -> select this repo"
	@echo "  -> Permissions: Contents (read)"
	@echo ""
	git config --global credential.helper store
	@echo "Credential helper set to 'store'. Next time you run 'git pull',"
	@echo "enter your GitHub username and PAT as password — it will be saved"
	@echo "and reused automatically for all future pulls."
	@echo ""
	@echo "Running 'git pull' now to trigger credential prompt..."
	git pull
