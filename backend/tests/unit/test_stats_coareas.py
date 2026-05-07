"""Tests for ``GET /api/stats/coareas``.

The endpoint surfaces the distinct CO Area values from active legacy
cost centers so pickers (e.g. the Cluster Explorer) can populate a
dropdown instead of asking users to type an arbitrary code. NULL
coareas are excluded since they're not selectable; counts let the UI
show "1000 (130024 CCs)".
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.api.stats import list_coareas


def _mock_db_with_rows(rows: list[tuple[str, int]]) -> MagicMock:
    """Mock a Session whose single ``execute(...)`` returns the given rows."""
    db = MagicMock()
    result = MagicMock()
    result.all.return_value = rows
    db.execute.return_value = result
    return db


def test_coareas_returns_one_entry_per_distinct_value() -> None:
    """Two coareas with different CC counts come back as two items."""
    db = _mock_db_with_rows([("1000", 130024), ("2000", 432)])

    result = list_coareas(db=db)

    assert result["items"] == [
        {"coarea": "1000", "cc_count": 130024},
        {"coarea": "2000", "cc_count": 432},
    ]


def test_coareas_returns_empty_list_when_no_active_ccs() -> None:
    """Fresh-install scenario: no CCs imported yet."""
    db = _mock_db_with_rows([])

    result = list_coareas(db=db)

    assert result["items"] == []


def test_coareas_single_value_supports_auto_select() -> None:
    """Common case: one CO Area for the whole org. The frontend uses
    this single-item response to auto-select the coarea so the user
    doesn't have to click."""
    db = _mock_db_with_rows([("1000", 130024)])

    result = list_coareas(db=db)

    assert len(result["items"]) == 1
    assert result["items"][0]["coarea"] == "1000"
    assert result["items"][0]["cc_count"] == 130024


def test_coareas_cc_count_is_int_not_decimal() -> None:
    """SQLAlchemy's func.count() can return a Decimal on some backends.
    The endpoint normalises it to int so JSON serialisation stays clean
    and the frontend's toLocaleString() formats nicely."""
    # Simulate a Decimal-like return from the DB
    from decimal import Decimal

    db = _mock_db_with_rows([("1000", Decimal("130024"))])

    result = list_coareas(db=db)

    assert result["items"][0]["cc_count"] == 130024
    assert isinstance(result["items"][0]["cc_count"], int)
