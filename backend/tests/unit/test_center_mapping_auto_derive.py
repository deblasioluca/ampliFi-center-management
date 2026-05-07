"""Tests for ``POST /api/center-mappings/auto-derive`` (PR #89, A13).

Operator question: 'mapping is leer, wie kann diese befüllen?'

The endpoint reads MERGE_MAP proposals from completed analysis runs
and creates CenterMapping rows from them. Behaviour we lock in here:

* No completed runs → empty result, no exception
* Specific run_id that doesn't exist → 404
* MERGE_MAP proposals → one CenterMapping row per (legacy → target)
* Already-existing mapping → skipped by default, refreshed when
  overwrite=True
* Proposals without merge_into_cctr → skipped (counted)
* The result counts (created/updated/skipped) match what actually
  happened
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def _make_run(run_id: int = 21, status: str = "completed") -> MagicMock:
    r = MagicMock()
    r.id = run_id
    r.status = status
    return r


def _proposal_row(legacy_cc_id, merge_into_cctr, cctr, coarea, txtsh):
    """Build a tuple matching the SELECT in auto_derive_center_mappings."""
    return (legacy_cc_id, merge_into_cctr, cctr, coarea, txtsh)


def test_no_runs_returns_empty_result() -> None:
    """No completed runs in the DB → returns zeroes, no crash, no
    HTTPException. The dashboard frontend renders this as "no runs
    to derive from yet".
    """
    from app.api.data_management import auto_derive_center_mappings

    db = MagicMock()
    db.execute.return_value.scalars.return_value.all.return_value = []

    out = auto_derive_center_mappings(run_id=None, overwrite=False, db=db, user=MagicMock())

    assert out.created == 0
    assert out.updated == 0
    assert out.skipped == 0
    assert out.runs_consulted == 0
    assert out.source_runs == []


def test_specific_unknown_run_id_404s() -> None:
    """run_id given but not found among completed runs → 404, not a
    silent zero result. Otherwise operators would never know they
    typo'd the id."""
    from fastapi import HTTPException

    from app.api.data_management import auto_derive_center_mappings

    db = MagicMock()
    db.execute.return_value.scalars.return_value.all.return_value = []

    with pytest.raises(HTTPException) as exc:
        auto_derive_center_mappings(run_id=999, overwrite=False, db=db, user=MagicMock())
    assert exc.value.status_code == 404


def test_creates_mappings_from_merge_map_proposals() -> None:
    """The happy path: a completed run with two MERGE_MAP proposals
    creates two new CenterMapping rows."""
    from app.api.data_management import auto_derive_center_mappings

    runs = [_make_run(21)]
    proposal_rows = [
        _proposal_row(100, "TARGET01", "00010001", "X100", "Cost Center 1"),
        _proposal_row(200, "TARGET02", "00010002", "X100", "Cost Center 2"),
    ]
    target_name_rows = [
        ("X100", "TARGET01", "Target One"),
        ("X100", "TARGET02", "Target Two"),
    ]

    db = MagicMock()
    # The endpoint issues these queries in order:
    #   1. select runs
    #   2. select proposals (joined to legacy CC)
    #   3. select target names (one IN-bounded query)
    #   4..n. one "find existing mapping" select per proposal
    runs_result = MagicMock()
    runs_result.scalars.return_value.all.return_value = runs
    proposals_result = MagicMock()
    proposals_result.all.return_value = proposal_rows
    target_names_result = MagicMock()
    target_names_result.all.return_value = target_name_rows
    # No existing mapping for either pair
    no_existing = MagicMock()
    no_existing.scalar_one_or_none.return_value = None

    db.execute.side_effect = [
        runs_result,
        proposals_result,
        target_names_result,
        no_existing,
        no_existing,
    ]

    out = auto_derive_center_mappings(run_id=None, overwrite=False, db=db, user=MagicMock())

    assert out.created == 2
    assert out.updated == 0
    assert out.skipped == 0
    assert out.runs_consulted == 1
    assert out.source_runs == [21]
    assert db.add.call_count == 2
    db.commit.assert_called_once()


def test_existing_mapping_is_skipped_without_overwrite() -> None:
    """Existing rows on the unique-constraint key → skipped (idempotent
    re-run is safe and doesn't double up rows). The skipped count
    bumps so the operator sees "already exists" feedback in the alert.
    """
    from app.api.data_management import auto_derive_center_mappings

    runs = [_make_run(21)]
    proposal_rows = [
        _proposal_row(100, "TARGET01", "00010001", "X100", "Cost Center 1"),
    ]

    db = MagicMock()
    runs_result = MagicMock()
    runs_result.scalars.return_value.all.return_value = runs
    proposals_result = MagicMock()
    proposals_result.all.return_value = proposal_rows
    target_names_result = MagicMock()
    target_names_result.all.return_value = [("X100", "TARGET01", "Target One")]
    existing = MagicMock()
    existing.scalar_one_or_none.return_value = MagicMock()  # truthy → already exists

    db.execute.side_effect = [
        runs_result,
        proposals_result,
        target_names_result,
        existing,
    ]

    out = auto_derive_center_mappings(run_id=None, overwrite=False, db=db, user=MagicMock())

    assert out.created == 0
    assert out.updated == 0
    assert out.skipped == 1
    db.add.assert_not_called()


def test_existing_mapping_is_updated_when_overwrite_true() -> None:
    """overwrite=True refreshes legacy_name / target_name / mapping_type /
    notes on the existing row. We don't delete and re-create — that
    would lose any FK references and is unnecessary."""
    from app.api.data_management import auto_derive_center_mappings

    runs = [_make_run(21)]
    proposal_rows = [
        _proposal_row(100, "TARGET01", "00010001", "X100", "New CC Name"),
    ]

    db = MagicMock()
    runs_result = MagicMock()
    runs_result.scalars.return_value.all.return_value = runs
    proposals_result = MagicMock()
    proposals_result.all.return_value = proposal_rows
    target_names_result = MagicMock()
    target_names_result.all.return_value = [("X100", "TARGET01", "Refreshed Target")]
    existing_obj = MagicMock()
    existing_result = MagicMock()
    existing_result.scalar_one_or_none.return_value = existing_obj

    db.execute.side_effect = [
        runs_result,
        proposals_result,
        target_names_result,
        existing_result,
    ]

    out = auto_derive_center_mappings(run_id=None, overwrite=True, db=db, user=MagicMock())

    assert out.created == 0
    assert out.updated == 1
    assert out.skipped == 0
    # Existing row's mutable fields are refreshed in place
    assert existing_obj.legacy_name == "New CC Name"
    assert existing_obj.target_name == "Refreshed Target"
    assert existing_obj.mapping_type == "merge"


def test_proposal_without_merge_into_cctr_is_skipped() -> None:
    """A proposal where merge_into_cctr is None shouldn't even reach
    the existing-mapping query — it has nowhere to point. Counted as
    skipped so the operator sees the dropoff in the result.
    """
    from app.api.data_management import auto_derive_center_mappings

    runs = [_make_run(21)]
    proposal_rows = [
        _proposal_row(100, None, "00010001", "X100", "Cost Center 1"),
        _proposal_row(200, "TARGET02", "00010002", "X100", "Cost Center 2"),
    ]

    db = MagicMock()
    runs_result = MagicMock()
    runs_result.scalars.return_value.all.return_value = runs
    proposals_result = MagicMock()
    proposals_result.all.return_value = proposal_rows
    target_names_result = MagicMock()
    target_names_result.all.return_value = [("X100", "TARGET02", "Target Two")]
    no_existing = MagicMock()
    no_existing.scalar_one_or_none.return_value = None

    db.execute.side_effect = [
        runs_result,
        proposals_result,
        target_names_result,
        # Note: only ONE existing-check query — the None proposal is
        # rejected before reaching the find-existing query.
        no_existing,
    ]

    out = auto_derive_center_mappings(run_id=None, overwrite=False, db=db, user=MagicMock())

    assert out.created == 1
    assert out.skipped == 1
    assert db.add.call_count == 1
