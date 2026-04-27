# ============================================================
# ampliFi Center Management — Makefile
# Run from the project root directory.
# All ports and config are read from .env.
# ============================================================

SHELL := /bin/bash
ROOT_DIR := $(shell pwd)
BACKEND_DIR := $(ROOT_DIR)/backend
FRONTEND_DIR := $(ROOT_DIR)/frontend
VENV := $(BACKEND_DIR)/.venv
BACKEND_PID := $(ROOT_DIR)/.amplifi-backend.pid
FRONTEND_PID := $(ROOT_DIR)/.amplifi-frontend.pid

# Load .env if present
ifneq (,$(wildcard $(ROOT_DIR)/.env))
  include $(ROOT_DIR)/.env
  export
endif

BACKEND_PORT ?= 8180
FRONTEND_PORT ?= 4321

.DEFAULT_GOAL := help
.PHONY: help start stop restart status setup update load-sample delete-sample seed logs git-setup

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

start: ## Start backend + frontend
	@echo "=== Starting ampliFi ==="
	@if [ -f $(BACKEND_PID) ] && kill -0 $$(cat $(BACKEND_PID)) 2>/dev/null; then \
		echo "Backend already running (PID $$(cat $(BACKEND_PID)))"; \
	else \
		cd $(BACKEND_DIR) && \
		source $(VENV)/bin/activate && \
		nohup uvicorn app.main:app --host 0.0.0.0 --port $(BACKEND_PORT) \
			> $(ROOT_DIR)/amplifi-backend.log 2>&1 & \
		echo $$! > $(BACKEND_PID) && \
		echo "[ok] Backend started on port $(BACKEND_PORT) (PID $$!)"; \
	fi
	@if [ -d $(FRONTEND_DIR) ] && [ -f $(FRONTEND_DIR)/package.json ]; then \
		if [ -f $(FRONTEND_PID) ] && kill -0 $$(cat $(FRONTEND_PID)) 2>/dev/null; then \
			echo "Frontend already running (PID $$(cat $(FRONTEND_PID)))"; \
		else \
			cd $(FRONTEND_DIR) && \
			nohup npm run dev > $(ROOT_DIR)/amplifi-frontend.log 2>&1 & \
			echo $$! > $(FRONTEND_PID) && \
			echo "[ok] Frontend started on port $(FRONTEND_PORT) (PID $$!)"; \
		fi; \
	else \
		echo "[skip] Frontend not found (no frontend/package.json)"; \
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
		echo "[ok] Backend not running"; \
	fi
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

setup: ## Initial setup: create venv, install deps, create DB tables, seed data
	@echo "=== ampliFi Setup ==="
	@cd $(BACKEND_DIR) && \
		python3 -m venv $(VENV) && \
		source $(VENV)/bin/activate && \
		pip install --upgrade pip > /dev/null 2>&1 && \
		pip install -e ".[dev]" 2>&1 | tail -5 && \
		echo "[ok] Backend dependencies installed"
	@if [ -d $(FRONTEND_DIR) ] && [ -f $(FRONTEND_DIR)/package.json ]; then \
		cd $(FRONTEND_DIR) && \
		npm install 2>&1 | tail -3 && \
		npm run build 2>&1 | tail -3 && \
		echo "[ok] Frontend built"; \
	fi
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
		python -m app.cli seed && \
		echo "[ok] Sample data loaded"
	@echo "=== Setup complete ==="

update: ## Pull latest code, rebuild frontend, reinstall backend, restart
	@echo "=== ampliFi Update ==="
	git pull
	@if [ -d $(FRONTEND_DIR) ] && [ -f $(FRONTEND_DIR)/package.json ]; then \
		cd $(FRONTEND_DIR) && \
		npm install 2>&1 | tail -3 && \
		npm run build 2>&1 | tail -3 && \
		echo "[ok] Frontend rebuilt"; \
	fi
	@cd $(BACKEND_DIR) && \
		source $(VENV)/bin/activate && \
		pip install -e ".[dev]" 2>&1 | tail -3 && \
		echo "[ok] Backend dependencies updated"
	@cd $(BACKEND_DIR) && \
		source $(VENV)/bin/activate && \
		python -m alembic upgrade head 2>&1 | tail -5 && \
		echo "[ok] Database migrations applied"
	@cd $(BACKEND_DIR) && \
		source $(VENV)/bin/activate && \
		python -m app.cli seed 2>&1 | tail -5 && \
		echo "[ok] Admin user + sample data seeded"
	@$(MAKE) restart
	@echo "=== Update complete ==="

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

git-setup: ## Configure Git credentials (run once — prompts for GitHub username + PAT)
	@echo "=== Git Credential Setup ==="
	@echo "This stores your GitHub credentials so git pull works without prompting."
	@echo "Create a PAT at: https://github.com/settings/tokens/new (select 'repo' scope)"
	@echo ""
	@read -p "GitHub username: " GH_USER && \
		read -sp "GitHub PAT: " GH_PAT && echo "" && \
		git config --global credential.helper store && \
		echo "https://$$GH_USER:$$GH_PAT@github.com" > ~/.git-credentials && \
		chmod 600 ~/.git-credentials && \
		echo "[ok] Git credentials stored. git pull will now work without prompting."
