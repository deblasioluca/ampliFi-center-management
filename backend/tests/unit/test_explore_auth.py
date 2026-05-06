"""Tests for explorer auth gating and compare semantics.

These tests don't spin up a DB — they verify the auth helper logic in
isolation and the structure of the compare endpoint.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from app.api.explore import _SENSITIVE_OBJECT_TYPES, _check_sensitive_access

# ── Sensitive types catalogue ───────────────────────────────────────────


def test_sensitive_set_contains_employees_and_balances() -> None:
    assert "employees" in _SENSITIVE_OBJECT_TYPES
    assert "balances" in _SENSITIVE_OBJECT_TYPES


def test_non_sensitive_types_pass_through() -> None:
    assert "cost-centers" not in _SENSITIVE_OBJECT_TYPES
    assert "target-cost-centers" not in _SENSITIVE_OBJECT_TYPES
    assert "target-profit-centers" not in _SENSITIVE_OBJECT_TYPES


# ── Auth gating ─────────────────────────────────────────────────────────


def test_no_check_when_feature_off() -> None:
    """When the feature flag is off, sensitive endpoints stay public."""
    with patch("app.api.explore.settings") as mock_settings:
        mock_settings.explorer_require_auth = False
        # Should not raise even with no user and a sensitive type
        _check_sensitive_access("employees", None)
        _check_sensitive_access("balances", None)


def test_unauth_blocked_for_sensitive_when_feature_on() -> None:
    with patch("app.api.explore.settings") as mock_settings:
        mock_settings.explorer_require_auth = True
        with pytest.raises(HTTPException) as exc_info:
            _check_sensitive_access("employees", None)
        assert exc_info.value.status_code == 401


def test_unauth_allowed_for_non_sensitive_when_feature_on() -> None:
    with patch("app.api.explore.settings") as mock_settings:
        mock_settings.explorer_require_auth = True
        # Cost centers are not sensitive — must remain accessible
        _check_sensitive_access("cost-centers", None)
        _check_sensitive_access("target-cost-centers", None)


def test_analyst_role_allowed() -> None:
    with patch("app.api.explore.settings") as mock_settings:
        mock_settings.explorer_require_auth = True
        user = MagicMock()
        user.role = "analyst"
        _check_sensitive_access("employees", user)


def test_admin_role_allowed() -> None:
    with patch("app.api.explore.settings") as mock_settings:
        mock_settings.explorer_require_auth = True
        user = MagicMock()
        user.role = "admin"
        _check_sensitive_access("balances", user)


def test_data_manager_role_allowed() -> None:
    with patch("app.api.explore.settings") as mock_settings:
        mock_settings.explorer_require_auth = True
        user = MagicMock()
        user.role = "data_manager"
        _check_sensitive_access("balances", user)


def test_reviewer_role_blocked() -> None:
    """Reviewers should not see employee PII or balances by default."""
    with patch("app.api.explore.settings") as mock_settings:
        mock_settings.explorer_require_auth = True
        user = MagicMock()
        user.role = "reviewer"
        with pytest.raises(HTTPException) as exc_info:
            _check_sensitive_access("employees", user)
        assert exc_info.value.status_code == 403


# ── Object map exposes both legacy and target ───────────────────────────


def test_target_models_in_object_map() -> None:
    from app.api.explore import _OBJECT_MODELS
    from app.models.core import TargetCostCenter, TargetProfitCenter

    assert _OBJECT_MODELS.get("target-cost-centers") is TargetCostCenter
    assert _OBJECT_MODELS.get("target-profit-centers") is TargetProfitCenter


def test_target_models_have_default_columns() -> None:
    from app.api.explore import _DEFAULT_TABLE_COLUMNS

    for key in ("target-cost-centers", "target-profit-centers"):
        cols = _DEFAULT_TABLE_COLUMNS.get(key)
        assert cols, f"{key} must have default columns defined"
        assert "approved_in_wave" in cols, f"{key} default columns must surface approved_in_wave"
