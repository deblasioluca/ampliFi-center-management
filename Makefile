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
PID_FILE := $(ROOT_DIR)/.amplifi.pid

# Load .env if present
ifneq (,$(wildcard $(ROOT_DIR)/.env))
  include $(ROOT_DIR)/.env
  export
endif

BACKEND_PORT ?= 8180

.PHONY: help start stop restart status setup update load-sample delete-sample seed logs

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

start: ## Start the backend server
	@if [ -f $(PID_FILE) ] && kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "ampliFi is already running (PID $$(cat $(PID_FILE)))"; \
	else \
		cd $(BACKEND_DIR) && \
		source $(VENV)/bin/activate && \
		nohup uvicorn app.main:app --host 0.0.0.0 --port $(BACKEND_PORT) \
			> $(ROOT_DIR)/amplifi-backend.log 2>&1 & \
		echo $$! > $(PID_FILE) && \
		echo "ampliFi started on port $(BACKEND_PORT) (PID $$!)"; \
	fi

stop: ## Stop the backend server
	@if [ -f $(PID_FILE) ]; then \
		PID=$$(cat $(PID_FILE)); \
		if kill -0 $$PID 2>/dev/null; then \
			kill $$PID && echo "ampliFi stopped (PID $$PID)"; \
		else \
			echo "ampliFi was not running (stale PID file)"; \
		fi; \
		rm -f $(PID_FILE); \
	else \
		echo "ampliFi is not running (no PID file)"; \
	fi

restart: stop start ## Restart the backend server

status: ## Show whether the backend is running
	@if [ -f $(PID_FILE) ] && kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "ampliFi is running (PID $$(cat $(PID_FILE)), port $(BACKEND_PORT))"; \
		curl -s http://127.0.0.1:$(BACKEND_PORT)/api/healthz 2>/dev/null || true; \
		echo ""; \
	else \
		echo "ampliFi is not running"; \
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
