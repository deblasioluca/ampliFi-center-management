"""Tests for ``DELETE /api/configs/{code}``.

PR #83 adds the ability to delete Decision Tree variants. Default
behaviour is conservative: returns 409 with a count of referencing
analysis runs instead of orphaning rows. ``?force=true`` cascades
the run deletion through.

This is admin-only because removing the config that produced an
analysis run destroys audit history. The endpoint is the only path —
there's no soft-delete or "deactivate" flag, so admins need to be
deliberate.

Tests cover the four interesting code paths:
* 404 when no config with that code exists
* clean delete when no runs reference it
* 409 with structured body when runs DO reference it (no force)
* force=true cascades run deletion + the config rows
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from app.api.configs import delete_config


def _mock_db_for_delete(versions: list, ref_count: int) -> MagicMock:
    """Mock the SELECT-versions, SELECT-count, and DELETE calls."""
    db = MagicMock()

    versions_result = MagicMock()
    versions_result.scalars.return_value.all.return_value = versions

    count_result = MagicMock()
    count_result.scalar.return_value = ref_count

    # The endpoint executes calls in this order:
    # 1. SELECT versions
    # 2. SELECT count of refs
    # 3. (if force + refs) DELETE runs
    # 4. DELETE configs
    db.execute.side_effect = [versions_result, count_result, MagicMock(), MagicMock()]
    return db


def test_delete_404_when_code_not_found() -> None:
    db = MagicMock()
    versions_result = MagicMock()
    versions_result.scalars.return_value.all.return_value = []
    db.execute.return_value = versions_result

    with pytest.raises(HTTPException) as exc:
        delete_config(code="nonexistent", force=False, db=db, _user=MagicMock())

    assert exc.value.status_code == 404
    assert "nonexistent" in str(exc.value.detail)


def test_delete_clean_when_no_runs_reference_config() -> None:
    """Happy path: variant has versions but no runs use it.
    The config rows are deleted; no analysis_run table touched."""
    v1 = MagicMock(id=1)
    v2 = MagicMock(id=2)
    db = _mock_db_for_delete(versions=[v1, v2], ref_count=0)

    result = delete_config(code="my_variant", force=False, db=db, _user=MagicMock())

    assert result == {
        "deleted": True,
        "code": "my_variant",
        "versions_deleted": 2,
        "runs_deleted": 0,
    }
    db.commit.assert_called_once()


def test_delete_409_when_runs_reference_config_without_force() -> None:
    """Without force, an existing run reference protects the config.
    The 409 detail must include the count so the UI can show it in
    the force-confirmation dialog."""
    v1 = MagicMock(id=1)
    db = _mock_db_for_delete(versions=[v1], ref_count=5)

    with pytest.raises(HTTPException) as exc:
        delete_config(code="my_variant", force=False, db=db, _user=MagicMock())

    assert exc.value.status_code == 409
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["referencing_runs"] == 5
    assert detail["code"] == "my_variant"
    assert detail["version_count"] == 1
    # Make sure we DIDN'T commit — the config must still exist
    db.commit.assert_not_called()


def test_delete_force_cascades_runs() -> None:
    """With force=true and references, we delete the runs first
    (which cascades to their proposals etc.) and then the config."""
    v1 = MagicMock(id=1)
    v2 = MagicMock(id=2)
    db = _mock_db_for_delete(versions=[v1, v2], ref_count=3)

    result = delete_config(code="my_variant", force=True, db=db, _user=MagicMock())

    assert result == {
        "deleted": True,
        "code": "my_variant",
        "versions_deleted": 2,
        "runs_deleted": 3,
    }
    # 4 execute calls expected: SELECT versions, SELECT count, DELETE runs, DELETE configs
    assert db.execute.call_count == 4
    db.commit.assert_called_once()


def test_delete_force_with_no_runs_skips_run_deletion() -> None:
    """force=true with zero references behaves like the clean path —
    no DELETE on analysis_run gets executed."""
    v1 = MagicMock(id=1)
    db = MagicMock()
    versions_result = MagicMock()
    versions_result.scalars.return_value.all.return_value = [v1]
    count_result = MagicMock()
    count_result.scalar.return_value = 0
    # Only 3 execute calls: SELECT versions, SELECT count, DELETE configs
    db.execute.side_effect = [versions_result, count_result, MagicMock()]

    result = delete_config(code="my_variant", force=True, db=db, _user=MagicMock())

    assert result["runs_deleted"] == 0
    assert db.execute.call_count == 3  # No DELETE-runs call when count is 0


def test_delete_handles_null_count_from_db() -> None:
    """``func.count(...)`` returns ``None`` when the SELECT matches no
    rows. The endpoint normalises that to 0 so the comparison
    ``if ref_count and not force`` doesn't crash."""
    v1 = MagicMock(id=1)
    db = MagicMock()
    versions_result = MagicMock()
    versions_result.scalars.return_value.all.return_value = [v1]
    count_result = MagicMock()
    count_result.scalar.return_value = None
    db.execute.side_effect = [versions_result, count_result, MagicMock()]

    result = delete_config(code="my_variant", force=False, db=db, _user=MagicMock())

    assert result["deleted"] is True
    assert result["runs_deleted"] == 0
