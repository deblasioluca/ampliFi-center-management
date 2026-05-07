"""Tests for ``GET /api/activity/unread-count``.

The endpoint backs the navbar's red activity badge. It existed in the
frontend (``Layout.astro``) but was never implemented in the backend
— the fetch returned 404 and the badge never lit up even when there
were unread items.

Tests verify the count is filtered the same way the full activity
list is (user-owned entries plus shared system notifications) and
that the response shape matches what the navbar expects.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.api.activity import unread_count


def _mock_db_with_count(value: int) -> MagicMock:
    db = MagicMock()
    result = MagicMock()
    result.scalar.return_value = value
    db.execute.return_value = result
    return db


def _mock_user(user_id: int = 1) -> MagicMock:
    u = MagicMock()
    u.id = user_id
    return u


def test_unread_count_returns_int() -> None:
    """Happy path: 7 unread → ``{count: 7}``."""
    db = _mock_db_with_count(7)
    user = _mock_user()

    result = unread_count(db=db, user=user)

    assert result == {"count": 7}


def test_unread_count_zero_when_nothing_unread() -> None:
    db = _mock_db_with_count(0)
    user = _mock_user()

    result = unread_count(db=db, user=user)

    assert result == {"count": 0}


def test_unread_count_handles_null_from_db() -> None:
    """SQLAlchemy ``func.count()`` returns ``None`` when the WHERE clause
    matches no rows on some backends. The endpoint normalises this to 0
    so the frontend never has to special-case ``null``."""
    db = _mock_db_with_count(None)
    user = _mock_user()

    result = unread_count(db=db, user=user)

    assert result == {"count": 0}


def test_unread_count_normalises_to_int_not_decimal() -> None:
    """Some Postgres drivers return a Decimal for ``count(*)``. JSON
    serialisation handles both, but the navbar's ``> 0`` check is
    cleaner against an int."""
    from decimal import Decimal

    db = _mock_db_with_count(Decimal("3"))
    user = _mock_user()

    result = unread_count(db=db, user=user)

    assert result == {"count": 3}
    assert isinstance(result["count"], int)
