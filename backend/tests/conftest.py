"""Shared pytest fixtures for the backend test suite."""

from __future__ import annotations

import os

# Force test settings before any app import
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("DATABASE_ASYNC_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/15")
os.environ.setdefault("APP_SECRET_KEY", "test-secret-key-for-ci")
