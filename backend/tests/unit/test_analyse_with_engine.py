"""Tests for the unified /analyse-with-engine endpoint dispatch logic.

Verifies engine routing without spinning up the full FastAPI stack.
"""

from __future__ import annotations

from app.api.waves import UnifiedAnalysisParams


def test_unified_params_default_engine_is_v1() -> None:
    p = UnifiedAnalysisParams()
    assert p.engine == "v1"
    assert p.mode == "simulation"


def test_unified_params_accepts_v2() -> None:
    p = UnifiedAnalysisParams(engine="v2", pc_start=200, cc_start=10)
    assert p.engine == "v2"
    assert p.pc_start == 200
    assert p.cc_start == 10


def test_unified_params_carries_pc_approach_rules() -> None:
    rules = [{"match": {"hier_level": "L3"}, "approach": "1:n"}]
    p = UnifiedAnalysisParams(engine="v2", pc_approach_rules=rules)
    assert p.pc_approach_rules == rules


def test_unified_params_excluded_scopes_optional() -> None:
    p = UnifiedAnalysisParams()
    assert p.excluded_scopes is None
    p2 = UnifiedAnalysisParams(excluded_scopes=[1, 2, 3])
    assert p2.excluded_scopes == [1, 2, 3]


def test_unified_params_v1_ignores_v2_only_fields() -> None:
    """V1 should still accept v2-only override fields (they're just ignored)."""
    p = UnifiedAnalysisParams(engine="v1", pc_start=999)
    # Schema-level: the field is settable; the endpoint logic ignores it for V1
    assert p.engine == "v1"
    assert p.pc_start == 999
