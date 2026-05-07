"""Tests for the wave scope and entity-resolution fixes in PR #85.

Three independent bugs are fixed here, and the tests stay focused on
the behaviour that operators reported:

1. ``_resolve_entity_by_ccode`` — the previous ``.scalar_one_or_none()``
   lookup either returned None (when the ccode existed in a non-cleanup
   scope) or raised ``MultipleResultsFound`` (when both legacy and
   target rows were present). Both failure modes silently dropped
   the entity from wave creation.

2. The hierarchy-scopes CRUD endpoints replace the old broken pattern
   of ``PATCH /api/waves/{id}`` with ``{config: {hierarchy_scope: …}}``
   — Pydantic's ``WaveUpdate`` model didn't list ``config`` so the
   field was silently dropped. The new endpoints write to
   ``WaveHierarchyScope`` directly, which the analysis pipeline
   actually reads.

We use ``MagicMock`` for the DB session because Base.metadata.create_all
fails on SQLite (JSONB columns) — the unit-test pattern established in
earlier PRs in this codebase.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from app.api.waves import (
    WaveHierarchyScopeIn,
    _resolve_entity_by_ccode,
    add_wave_hierarchy_scope,
    delete_wave_hierarchy_scope,
    list_wave_hierarchy_scopes,
)


def _user_admin() -> MagicMock:
    u = MagicMock()
    u.id = 99
    u.email = "admin@example.com"
    u.username = "admin"
    return u


# ---------- _resolve_entity_by_ccode ---------------------------------------


def test_resolve_entity_prefers_cleanup_scope() -> None:
    """When ccode exists in both 'cleanup' and other scopes, the cleanup
    row is the right answer — that's the analysis source. Picking any
    other row would put the wave on a target/explorer entity which the
    analysis pipeline can't act on."""
    cleanup_row = MagicMock()
    cleanup_row.scope = "cleanup"
    cleanup_row.ccode = "1000"
    cleanup_row.id = 5
    target_row = MagicMock()
    target_row.scope = "target"
    target_row.ccode = "1000"
    target_row.id = 9

    db = MagicMock()
    result = MagicMock()
    # Order is non-deterministic on insertion order — helper sorts by
    # scope and prefers cleanup, so simulate the DB returning rows in
    # the "wrong" order (target first) and verify the helper picks
    # cleanup anyway.
    result.scalars.return_value.all.return_value = [target_row, cleanup_row]
    db.execute.return_value = result

    out = _resolve_entity_by_ccode(db, "1000")
    assert out is cleanup_row


def test_resolve_entity_falls_back_when_no_cleanup_scope() -> None:
    """If the only entity row for this ccode lives in target scope (e.g.
    test fixtures with only target data), accept it rather than
    silently failing — the operator will see in the wave detail that
    the linkage exists. Better than the old behaviour of zero entities
    on the wave."""
    target_row = MagicMock()
    target_row.scope = "target"
    target_row.ccode = "1000"
    target_row.id = 9

    db = MagicMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = [target_row]
    db.execute.return_value = result

    out = _resolve_entity_by_ccode(db, "1000")
    assert out is target_row


def test_resolve_entity_returns_none_when_ccode_unknown() -> None:
    db = MagicMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = []
    db.execute.return_value = result

    assert _resolve_entity_by_ccode(db, "9999") is None


# ---------- list_wave_hierarchy_scopes -------------------------------------


def test_list_hierarchy_scopes_returns_items() -> None:
    """GET endpoint surfaces existing rows. Critical for the wave-detail
    page: until PR #85 there was no way to *read* the hierarchy scope,
    so the UI couldn't display chips with × buttons or even know what
    was already saved."""
    wave = MagicMock()
    scope_row = MagicMock()
    scope_row.id = 7
    scope_row.wave_id = 1
    scope_row.hierarchy_id = 3
    scope_row.node_setname = "EUROPE"

    db = MagicMock()
    db.get.return_value = wave
    result = MagicMock()
    result.scalars.return_value.all.return_value = [scope_row]
    db.execute.return_value = result

    out = list_wave_hierarchy_scopes(wave_id=1, db=db)
    assert out["wave_id"] == 1
    assert len(out["items"]) == 1
    assert out["items"][0] == {
        "id": 7,
        "wave_id": 1,
        "hierarchy_id": 3,
        "node_setname": "EUROPE",
    }


def test_list_hierarchy_scopes_404_when_wave_missing() -> None:
    db = MagicMock()
    db.get.return_value = None
    with pytest.raises(HTTPException) as exc:
        list_wave_hierarchy_scopes(wave_id=999, db=db)
    assert exc.value.status_code == 404


# ---------- add_wave_hierarchy_scope ---------------------------------------


def test_add_hierarchy_scope_creates_new_row() -> None:
    wave = MagicMock()
    wave.status = "draft"

    db = MagicMock()
    db.get.return_value = wave
    # First execute (the existing-row check) returns None
    existing_check = MagicMock()
    existing_check.scalar_one_or_none.return_value = None
    db.execute.return_value = existing_check

    body = WaveHierarchyScopeIn(hierarchy_id=3, node_setname="EUROPE")
    out = add_wave_hierarchy_scope(wave_id=1, body=body, db=db, user=_user_admin())

    db.add.assert_called_once()
    db.commit.assert_called_once()
    assert out["created"] is True
    assert out["hierarchy_id"] == 3
    assert out["node_setname"] == "EUROPE"


def test_add_hierarchy_scope_idempotent_on_duplicate() -> None:
    """Re-adding the same (hierarchy_id, node_setname) returns the
    existing row with created=False. Operators selecting a node twice
    in the modal shouldn't get two DB rows."""
    wave = MagicMock()
    wave.status = "draft"

    existing_row = MagicMock()
    existing_row.id = 7
    existing_row.wave_id = 1
    existing_row.hierarchy_id = 3
    existing_row.node_setname = "EUROPE"

    db = MagicMock()
    db.get.return_value = wave
    existing_check = MagicMock()
    existing_check.scalar_one_or_none.return_value = existing_row
    db.execute.return_value = existing_check

    body = WaveHierarchyScopeIn(hierarchy_id=3, node_setname="EUROPE")
    out = add_wave_hierarchy_scope(wave_id=1, body=body, db=db, user=_user_admin())

    db.add.assert_not_called()
    db.commit.assert_not_called()
    assert out["id"] == 7
    assert out["created"] is False


def test_add_hierarchy_scope_blocks_if_wave_locked() -> None:
    """Status guard: scope changes are only legal in draft / analysing.
    Adding a node to a locked wave would invalidate the analysis run
    that's already grounded on the previous scope."""
    wave = MagicMock()
    wave.status = "locked"

    db = MagicMock()
    db.get.return_value = wave

    body = WaveHierarchyScopeIn(hierarchy_id=3, node_setname="EUROPE")
    with pytest.raises(HTTPException) as exc:
        add_wave_hierarchy_scope(wave_id=1, body=body, db=db, user=_user_admin())
    assert exc.value.status_code == 409


def test_add_hierarchy_scope_404_when_wave_missing() -> None:
    db = MagicMock()
    db.get.return_value = None
    body = WaveHierarchyScopeIn(hierarchy_id=3, node_setname="EUROPE")
    with pytest.raises(HTTPException) as exc:
        add_wave_hierarchy_scope(wave_id=999, body=body, db=db, user=_user_admin())
    assert exc.value.status_code == 404


# ---------- delete_wave_hierarchy_scope ------------------------------------


def test_delete_hierarchy_scope_removes_row() -> None:
    wave = MagicMock()
    wave.status = "draft"

    scope_row = MagicMock()
    scope_row.wave_id = 1

    db = MagicMock()
    # First db.get(Wave, ...) returns wave; second db.get(WaveHierarchyScope, ...)
    # returns scope_row.
    db.get.side_effect = [wave, scope_row]

    out = delete_wave_hierarchy_scope(wave_id=1, scope_id=7, db=db, user=_user_admin())

    db.delete.assert_called_once_with(scope_row)
    db.commit.assert_called_once()
    assert out == {"deleted": True, "id": 7}


def test_delete_hierarchy_scope_rejects_cross_wave() -> None:
    """Defence-in-depth: scope_id from URL plus wave_id from URL must
    match. Otherwise an operator with access to wave A could delete
    scope rows belonging to wave B by guessing the scope_id."""
    wave = MagicMock()
    wave.status = "draft"

    other_wave_scope = MagicMock()
    other_wave_scope.wave_id = 2  # belongs to a different wave

    db = MagicMock()
    db.get.side_effect = [wave, other_wave_scope]

    with pytest.raises(HTTPException) as exc:
        delete_wave_hierarchy_scope(wave_id=1, scope_id=7, db=db, user=_user_admin())
    assert exc.value.status_code == 404
    db.delete.assert_not_called()
