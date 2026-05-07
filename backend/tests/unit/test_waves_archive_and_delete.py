"""Tests for the wave archive + delete behaviour added in PR #84.

The endpoint changes split into three separate behaviours that need
their own tests:

* ``DELETE /api/waves/{id}`` — newly allows deletion of in_review
  waves (operator request: aborting waves stuck in review). Still
  blocks terminal waves (signed_off, closed, cancelled) UNLESS the
  wave has been archived first.
* ``POST /api/waves/{id}/archive`` — only valid on terminal waves.
  Active waves get a 409 telling the user to delete instead. Already-
  archived waves get a 409 too (no-op protection).
* ``POST /api/waves/{id}/unarchive`` — undoes archival. The wave's
  status is preserved so the archive flag is genuinely orthogonal.

These tests use ``MagicMock`` rather than a real DB session to isolate
the endpoint logic from migration / model setup. The audit-write call
happens inside both endpoints; we mock it out via patch so the unit
tests don't need to construct a full audit row.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from app.api.waves import archive_wave, delete_wave, unarchive_wave


def _mock_wave(
    wave_id: int = 1,
    status: str = "draft",
    is_archived: bool = False,
    code: str = "W-001",
) -> MagicMock:
    w = MagicMock()
    w.id = wave_id
    w.code = code
    w.status = status
    w.is_archived = is_archived
    return w


def _mock_user() -> MagicMock:
    u = MagicMock()
    u.id = 99
    u.email = "admin@example.com"
    u.username = "admin"
    return u


# ---------- delete_wave -----------------------------------------------------


@patch("app.api.waves.write_audit", create=True)
@patch("app.domain.proposal.service.release_proposal_ids")
def test_delete_wave_allows_in_review(_release: MagicMock, _audit: MagicMock) -> None:
    """Operator request — in_review waves used to be blocked from
    deletion. PR #84 lifts that block."""
    wave = _mock_wave(status="in_review")
    db = MagicMock()
    db.get.return_value = wave
    proposals_result = MagicMock()
    proposals_result.scalars.return_value.all.return_value = []
    db.execute.return_value = proposals_result

    result = delete_wave(wave_id=1, db=db, user=_mock_user())

    assert result == {"status": "deleted"}
    db.delete.assert_called_once_with(wave)
    db.commit.assert_called_once()


def test_delete_wave_blocks_terminal_when_not_archived() -> None:
    """A signed_off wave that hasn't been archived must NOT be
    deletable directly. The archive-then-delete two-step is
    deliberate friction against accidental destruction of completed
    work."""
    wave = _mock_wave(status="signed_off", is_archived=False)
    db = MagicMock()
    db.get.return_value = wave

    with pytest.raises(HTTPException) as exc:
        delete_wave(wave_id=1, db=db, user=_mock_user())

    assert exc.value.status_code == 409
    assert "Archive it first" in str(exc.value.detail)
    db.delete.assert_not_called()


@patch("app.api.waves.write_audit", create=True)
@patch("app.domain.proposal.service.release_proposal_ids")
def test_delete_wave_allows_terminal_when_archived(_release: MagicMock, _audit: MagicMock) -> None:
    """An archived signed_off wave CAN be deleted — that's the whole
    point of the archive view."""
    wave = _mock_wave(status="signed_off", is_archived=True)
    db = MagicMock()
    db.get.return_value = wave
    proposals_result = MagicMock()
    proposals_result.scalars.return_value.all.return_value = []
    db.execute.return_value = proposals_result

    result = delete_wave(wave_id=1, db=db, user=_mock_user())

    assert result == {"status": "deleted"}
    db.delete.assert_called_once_with(wave)


def test_delete_wave_404_when_not_found() -> None:
    db = MagicMock()
    db.get.return_value = None

    with pytest.raises(HTTPException) as exc:
        delete_wave(wave_id=999, db=db, user=_mock_user())

    assert exc.value.status_code == 404


# ---------- archive_wave ----------------------------------------------------


@patch("app.api.waves.write_audit", create=True)
def test_archive_wave_succeeds_for_signed_off(_audit: MagicMock) -> None:
    wave = _mock_wave(status="signed_off", is_archived=False)
    db = MagicMock()
    db.get.return_value = wave

    result = archive_wave(wave_id=1, db=db, user=_mock_user())

    assert result == {"status": "archived", "wave_id": 1}
    assert wave.is_archived is True
    db.commit.assert_called_once()


@patch("app.api.waves.write_audit", create=True)
def test_archive_wave_succeeds_for_closed(_audit: MagicMock) -> None:
    wave = _mock_wave(status="closed", is_archived=False)
    db = MagicMock()
    db.get.return_value = wave

    archive_wave(wave_id=1, db=db, user=_mock_user())

    assert wave.is_archived is True


@patch("app.api.waves.write_audit", create=True)
def test_archive_wave_succeeds_for_cancelled(_audit: MagicMock) -> None:
    wave = _mock_wave(status="cancelled", is_archived=False)
    db = MagicMock()
    db.get.return_value = wave

    archive_wave(wave_id=1, db=db, user=_mock_user())

    assert wave.is_archived is True


def test_archive_wave_blocks_active_status() -> None:
    """Archive only makes sense on terminal waves. Trying to archive
    an in-progress wave is almost certainly a UI bug — we 409 with a
    clear message instead of silently doing it."""
    wave = _mock_wave(status="analysing", is_archived=False)
    db = MagicMock()
    db.get.return_value = wave

    with pytest.raises(HTTPException) as exc:
        archive_wave(wave_id=1, db=db, user=_mock_user())

    assert exc.value.status_code == 409
    # Wave should NOT have been modified
    assert wave.is_archived is False


def test_archive_wave_blocks_already_archived() -> None:
    wave = _mock_wave(status="signed_off", is_archived=True)
    db = MagicMock()
    db.get.return_value = wave

    with pytest.raises(HTTPException) as exc:
        archive_wave(wave_id=1, db=db, user=_mock_user())

    assert exc.value.status_code == 409
    assert "already archived" in str(exc.value.detail)


# ---------- unarchive_wave --------------------------------------------------


@patch("app.api.waves.write_audit", create=True)
def test_unarchive_wave_succeeds(_audit: MagicMock) -> None:
    """Unarchive preserves the underlying status — the flag is genuinely
    orthogonal to the analysis lifecycle."""
    wave = _mock_wave(status="signed_off", is_archived=True)
    db = MagicMock()
    db.get.return_value = wave

    result = unarchive_wave(wave_id=1, db=db, user=_mock_user())

    assert result == {"status": "unarchived", "wave_id": 1}
    assert wave.is_archived is False
    assert wave.status == "signed_off"  # status untouched


def test_unarchive_wave_blocks_non_archived() -> None:
    wave = _mock_wave(status="signed_off", is_archived=False)
    db = MagicMock()
    db.get.return_value = wave

    with pytest.raises(HTTPException) as exc:
        unarchive_wave(wave_id=1, db=db, user=_mock_user())

    assert exc.value.status_code == 409
    assert "not archived" in str(exc.value.detail)
