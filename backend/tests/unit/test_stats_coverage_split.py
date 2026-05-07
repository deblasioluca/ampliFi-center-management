"""Tests for the refactored ``GET /api/stats/coverage`` endpoint.

The endpoint now splits coverage into two buckets — **scoped** (real wave
work) and **global** (full-scope reference waves + wave_id IS NULL runs)
— so the dashboard's progress numbers reflect actual cleansing progress
instead of being inflated to 100% by a single full-scope reference wave.

Mocking strategy: the endpoint runs a long, conditional sequence of
``db.execute(...)`` calls. We feed return values via a queue so each call
pops the next pre-canned result. The order of the queue tracks the
control flow inside ``scope_coverage``, so the tests double as a
documentation of which queries fire under which conditions.
"""

from __future__ import annotations

from collections import deque
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.api.stats import scope_coverage

# ── helpers ──────────────────────────────────────────────────────────────


def _scalar_result(value: int) -> MagicMock:
    """Mock a result whose ``.scalar()`` returns ``value``."""
    res = MagicMock()
    res.scalar.return_value = value
    return res


def _ids_result(ids: list[int]) -> MagicMock:
    """Mock a result whose ``.scalars().all()`` returns ``ids``."""
    res = MagicMock()
    res.scalars.return_value.all.return_value = ids
    return res


def _waves_result(waves: list[SimpleNamespace]) -> MagicMock:
    """Same shape as ``_ids_result`` but for Wave objects."""
    return _ids_result(waves)


def _make_wave(
    wave_id: int,
    code: str,
    *,
    is_full_scope: bool = False,
    status: str = "analysing",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=wave_id,
        code=code,
        status=status,
        is_full_scope=is_full_scope,
        created_at=None,
    )


def _mock_db(results: list[MagicMock]) -> MagicMock:
    """Wire a sequence of pre-canned results onto db.execute via a queue."""
    queue: deque[MagicMock] = deque(results)
    db = MagicMock()
    db.execute.side_effect = lambda *_a, **_kw: queue.popleft()
    db._queue = queue  # exposed so tests can assert it's drained
    return db


# ── scenario A: only a full-scope "Global" wave (matches user's screenshot)


def test_only_global_wave_yields_zero_scoped_progress() -> None:
    """The exact scenario the user reported: one full-scope wave with
    608 entities, no scoped waves. Coverage bars should show 0% for
    scoped progress (the meaningful metric) — NOT 100% inflated by the
    Global wave's full-coverage entity assignment."""
    global_wave = _make_wave(1, "Global", is_full_scope=True)
    results = [
        _scalar_result(608),  # 1. total_entities
        _scalar_result(130024),  # 2. total_cc
        _ids_result([]),  # 3. scoped_wave_ids — none
        _ids_result([1]),  # 4. global_wave_ids — [1]
        # No scoped_covered_entities query (scoped_wave_ids empty)
        _scalar_result(608),  # 5. global_covered_entities
        # No scoped_covered_cc query (scoped_wave_ids empty)
        _scalar_result(0),  # 6. global_covered_cc
        _waves_result([global_wave]),  # 7. waves list
        _scalar_result(608),  # 8. global wave's WaveEntity count
        _scalar_result(0),  # 9. global wave's CC count
        _scalar_result(0),  # 10. unassigned_cc
    ]
    db = _mock_db(results)

    result = scope_coverage(db=db)

    assert result["total_entities"] == 608
    assert result["total_cc"] == 130024

    # Scoped progress is 0/0/0% — that's the truth the user wants visible
    s = result["scoped"]
    assert s["covered_entities"] == 0
    assert s["covered_cc"] == 0
    assert s["entity_pct"] == 0
    assert s["cc_pct"] == 0
    assert s["wave_count"] == 0

    # Global block surfaces the full-scope wave separately
    g = result["global"]
    assert g["covered_entities"] == 608
    assert g["covered_cc"] == 0
    assert g["wave_count"] == 1
    assert g["unassigned_run_cc"] == 0

    # Per-wave arrays — Global wave goes in global_waves only
    assert len(result["scoped_waves"]) == 0
    assert len(result["global_waves"]) == 1
    assert result["global_waves"][0]["code"] == "Global"
    assert result["global_waves"][0]["is_full_scope"] is True

    # Backwards-compat top-level fields mirror the scoped block —
    # this is the deliberate behaviour change so the legacy frontend
    # would also stop showing "100%" for the user's situation
    assert result["covered_entities"] == 0
    assert result["covered_cc"] == 0
    assert result["entity_pct"] == 0
    assert result["cc_pct"] == 0


# ── scenario B: one scoped wave + one global wave ────────────────────────


def test_scoped_and_global_waves_populate_independently() -> None:
    """A real scoped wave (50 entities, 30 CCs analysed) alongside the
    full-scope Global wave (608 entities, 100 CCs analysed). Each
    bucket should reflect its own slice."""
    global_wave = _make_wave(1, "Global", is_full_scope=True)
    scoped_wave = _make_wave(2, "WAVE-2025-Q1", is_full_scope=False)
    results = [
        _scalar_result(608),  # total_entities
        _scalar_result(130024),  # total_cc
        _ids_result([2]),  # scoped_wave_ids
        _ids_result([1]),  # global_wave_ids
        _scalar_result(50),  # scoped_covered_entities
        _scalar_result(608),  # global_covered_entities
        _scalar_result(30),  # scoped_covered_cc
        _scalar_result(127),  # global_covered_cc (incl. unassigned)
        _waves_result([scoped_wave, global_wave]),  # waves list (newest first)
        _scalar_result(50),  # scoped wave's WaveEntity count
        _scalar_result(30),  # scoped wave's CC count
        _scalar_result(608),  # global wave's WaveEntity count
        _scalar_result(100),  # global wave's CC count
        _scalar_result(27),  # unassigned_cc (e.g. global runs from PR #77)
    ]
    db = _mock_db(results)

    result = scope_coverage(db=db)

    s = result["scoped"]
    assert s["covered_entities"] == 50
    assert s["covered_cc"] == 30
    assert s["entity_pct"] == round(50 / 608 * 100, 1)
    assert s["wave_count"] == 1

    g = result["global"]
    assert g["covered_entities"] == 608
    assert g["covered_cc"] == 127
    # wave_count counts wave rows, NOT the synthetic "Unassigned" entry
    assert g["wave_count"] == 1
    assert g["unassigned_run_cc"] == 27

    # Both arrays populated correctly
    assert len(result["scoped_waves"]) == 1
    assert result["scoped_waves"][0]["code"] == "WAVE-2025-Q1"
    # global_waves has the wave + the synthetic "Unassigned global runs" row
    assert len(result["global_waves"]) == 2
    codes = [w["code"] for w in result["global_waves"]]
    assert "Global" in codes
    assert "Unassigned global runs" in codes
    # The synthetic row has id=None so the UI knows not to link it
    unassigned = next(w for w in result["global_waves"] if w["code"] == "Unassigned global runs")
    assert unassigned["id"] is None
    assert unassigned["cc_covered"] == 27


# ── scenario C: nothing yet — no waves, no runs ─────────────────────────


def test_empty_universe_returns_zeros_without_crashing() -> None:
    """Fresh-install scenario: 0 entities, 0 CCs, no waves, no runs.
    The endpoint must not divide by zero — pcts should be 0."""
    results = [
        _scalar_result(0),  # total_entities
        _scalar_result(0),  # total_cc
        _ids_result([]),  # scoped_wave_ids
        _ids_result([]),  # global_wave_ids
        # No scoped_covered_entities (empty list)
        # No global_covered_entities (empty list)
        # No scoped_covered_cc (empty list)
        _scalar_result(0),  # global_covered_cc
        _waves_result([]),  # waves list (empty)
        _scalar_result(0),  # unassigned_cc
    ]
    db = _mock_db(results)

    result = scope_coverage(db=db)

    assert result["total_entities"] == 0
    assert result["total_cc"] == 0
    assert result["scoped"]["entity_pct"] == 0
    assert result["scoped"]["cc_pct"] == 0
    assert result["global"]["entity_pct"] == 0
    assert result["global"]["cc_pct"] == 0
    assert result["scoped_waves"] == []
    assert result["global_waves"] == []


# ── scenario D: only wave-less analysis runs (e.g. fresh global runs) ────


def test_only_unassigned_runs_show_under_global() -> None:
    """If the operator has only ever clicked "Run Global Analysis" (PR #77)
    without ever creating a wave row, the resulting analyses should
    show up under the Global block as 'Unassigned global runs' — not
    inflate scoped progress."""
    results = [
        _scalar_result(608),  # total_entities
        _scalar_result(130024),  # total_cc
        _ids_result([]),  # scoped_wave_ids
        _ids_result([]),  # global_wave_ids
        _scalar_result(127),  # global_covered_cc (= unassigned)
        _waves_result([]),  # waves list
        _scalar_result(127),  # unassigned_cc
    ]
    db = _mock_db(results)

    result = scope_coverage(db=db)

    s = result["scoped"]
    assert s["covered_entities"] == 0
    assert s["covered_cc"] == 0
    g = result["global"]
    assert g["covered_cc"] == 127
    assert g["unassigned_run_cc"] == 127
    assert g["wave_count"] == 0  # No wave rows, just runs

    # The synthetic Unassigned row is the only entry in global_waves
    assert len(result["global_waves"]) == 1
    assert result["global_waves"][0]["code"] == "Unassigned global runs"
    assert result["global_waves"][0]["id"] is None


# ── scenario E: backwards-compat — legacy `waves` array still present ──


def test_legacy_waves_array_concatenates_scoped_then_global() -> None:
    """Keep returning the legacy ``waves`` field for any caller that
    hasn't migrated to ``scoped_waves`` / ``global_waves`` yet. Order
    is scoped first then global so old per-wave rendering still groups
    real work above reference rows."""
    scoped_wave = _make_wave(2, "WAVE-A", is_full_scope=False)
    global_wave = _make_wave(1, "Global", is_full_scope=True)
    results = [
        _scalar_result(100),  # total_entities
        _scalar_result(1000),  # total_cc
        _ids_result([2]),  # scoped_wave_ids
        _ids_result([1]),  # global_wave_ids
        _scalar_result(20),  # scoped_covered_entities
        _scalar_result(100),  # global_covered_entities
        _scalar_result(10),  # scoped_covered_cc
        _scalar_result(50),  # global_covered_cc
        _waves_result([scoped_wave, global_wave]),
        _scalar_result(20),
        _scalar_result(10),  # scoped wave entity + cc counts
        _scalar_result(100),
        _scalar_result(50),  # global wave entity + cc counts
        _scalar_result(0),  # unassigned_cc
    ]
    db = _mock_db(results)

    result = scope_coverage(db=db)

    legacy = result["waves"]
    assert len(legacy) == 2
    # Scoped comes first
    assert legacy[0]["code"] == "WAVE-A"
    assert legacy[0]["is_full_scope"] is False
    assert legacy[1]["code"] == "Global"
    assert legacy[1]["is_full_scope"] is True
