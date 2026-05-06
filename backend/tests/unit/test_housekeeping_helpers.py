"""Tests for housekeeping service helpers — pure functions only.

These tests verify the date arithmetic and owner-email resolution logic in
isolation. End-to-end cycle tests require a real DB and are covered by
integration tests (not in this file).
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from app.services.housekeeping import (
    _months_ago_period,
    _resolve_owner_email,
)


# ── _months_ago_period ──────────────────────────────────────────────────


def test_months_ago_simple_within_year() -> None:
    now = datetime(2026, 8, 15, tzinfo=UTC)
    assert _months_ago_period(now, 3) == 202605  # May 2026


def test_months_ago_crosses_year_boundary() -> None:
    now = datetime(2026, 3, 15, tzinfo=UTC)
    assert _months_ago_period(now, 6) == 202509  # Sep 2025


def test_months_ago_full_year() -> None:
    now = datetime(2026, 4, 1, tzinfo=UTC)
    assert _months_ago_period(now, 12) == 202504


def test_months_ago_multiple_years() -> None:
    now = datetime(2026, 4, 15, tzinfo=UTC)
    assert _months_ago_period(now, 36) == 202304  # Apr 2023


def test_months_ago_24_months() -> None:
    now = datetime(2026, 1, 15, tzinfo=UTC)
    assert _months_ago_period(now, 24) == 202401  # Jan 2024


def test_months_ago_zero() -> None:
    now = datetime(2026, 4, 15, tzinfo=UTC)
    assert _months_ago_period(now, 0) == 202604


def test_months_ago_january_minus_one() -> None:
    """Edge case: 1 month before January is December previous year."""
    now = datetime(2026, 1, 15, tzinfo=UTC)
    assert _months_ago_period(now, 1) == 202512


# ── _resolve_owner_email ────────────────────────────────────────────────


def _make_db_with_employee(emp_email: str | None) -> MagicMock:
    """Build a mock DB session that returns a fake Employee on lookup."""
    db = MagicMock()
    if emp_email is None:
        # No matching employee
        db.execute.return_value.scalars.return_value.first.return_value = None
    else:
        emp = MagicMock()
        emp.email_address = emp_email
        db.execute.return_value.scalars.return_value.first.return_value = emp
    return db


def test_resolve_owner_email_returns_none_for_empty_responsible() -> None:
    db = _make_db_with_employee("doesnt-matter@example.com")
    assert _resolve_owner_email(None, db) is None
    assert _resolve_owner_email("", db) is None
    assert _resolve_owner_email("   ", db) is None


def test_resolve_owner_email_returns_none_when_not_found() -> None:
    db = _make_db_with_employee(None)
    assert _resolve_owner_email("UNKNOWN_GPN", db) is None


def test_resolve_owner_email_returns_email_when_found() -> None:
    db = _make_db_with_employee("alice@example.com")
    assert _resolve_owner_email("GPN001", db) == "alice@example.com"


def test_resolve_owner_email_lowercases_and_strips() -> None:
    db = _make_db_with_employee("  Alice@Example.COM  ")
    assert _resolve_owner_email("GPN001", db) == "alice@example.com"


def test_resolve_owner_email_returns_none_when_employee_has_no_email() -> None:
    db = MagicMock()
    emp = MagicMock()
    emp.email_address = None
    db.execute.return_value.scalars.return_value.first.return_value = emp
    assert _resolve_owner_email("GPN001", db) is None


def test_resolve_owner_email_strips_whitespace_in_responsible() -> None:
    """Trailing whitespace on a GPN/PID should not break resolution."""
    db = _make_db_with_employee("alice@example.com")
    # We don't assert the SQL query; just that it returns the resolved email
    # when the responsible string has leading/trailing whitespace.
    assert _resolve_owner_email("  GPN001  ", db) == "alice@example.com"
