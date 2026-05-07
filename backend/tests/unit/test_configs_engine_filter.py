"""Tests for ``_detect_engine_from_config`` and ``?engine=`` filter
on the ``GET /api/configs`` endpoint (PR #90).

Operator bug: dashboard's Engine selector + Config dropdown didn't
agree. Selecting Engine=V2 still showed V1 configs in the dropdown,
because the API didn't filter by engine.
"""

from __future__ import annotations


def test_detect_v2_when_pipeline_has_v2_routine():
    """Any v2.* routine code marks the config as V2."""
    from app.api.configs import _detect_engine_from_config

    cfg = {
        "pipeline": [
            {"routine": "v2.retire_flag", "enabled": True},
            {"routine": "v2.balance_migrate", "enabled": True},
        ]
    }
    assert _detect_engine_from_config(cfg) == "v2"


def test_detect_v1_when_pipeline_has_only_rule_routines():
    """Only rule.* / ml.* / llm.* routines → V1."""
    from app.api.configs import _detect_engine_from_config

    cfg = {
        "pipeline": [
            {"routine": "rule.posting_activity", "enabled": True},
            {"routine": "rule.ownership", "enabled": True},
            {"routine": "ml.classify_outcome", "enabled": True},
        ]
    }
    assert _detect_engine_from_config(cfg) == "v1"


def test_detect_v2_when_one_v2_routine_among_many():
    """Mixed pipeline — even one v2.* makes it V2 (the engine actually
    runs is V2, since v2.* routines aren't valid V1 routines)."""
    from app.api.configs import _detect_engine_from_config

    cfg = {
        "pipeline": [
            {"routine": "rule.posting_activity"},
            {"routine": "v2.pc_approach"},  # v2 — should win
        ]
    }
    assert _detect_engine_from_config(cfg) == "v2"


def test_detect_v1_for_empty_or_missing_blob():
    """Empty / missing config falls back to V1 (the historic default)."""
    from app.api.configs import _detect_engine_from_config

    assert _detect_engine_from_config(None) == "v1"
    assert _detect_engine_from_config({}) == "v1"
    assert _detect_engine_from_config({"pipeline": []}) == "v1"


def test_detect_handles_legacy_routines_key():
    """Older configs use the ``routines`` key instead of ``pipeline``.
    The detector handles both."""
    from app.api.configs import _detect_engine_from_config

    cfg = {"routines": [{"routine": "v2.combine_migration"}]}
    assert _detect_engine_from_config(cfg) == "v2"


def test_detect_handles_string_entry_format():
    """Some configs ship the routines list as plain strings rather than
    dicts. Detector should still recognise prefix."""
    from app.api.configs import _detect_engine_from_config

    cfg = {"pipeline": ["v2.balance_migrate", "v2.pc_approach"]}
    assert _detect_engine_from_config(cfg) == "v2"


def test_list_configs_filters_by_engine_v2():
    """``?engine=v2`` returns only V2 configs."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from app.api.configs import list_configs

    # Build two configs as plain objects so Pydantic's from_attributes
    # validation gets real strings, not MagicMock proxies.
    cfg_v1 = SimpleNamespace(
        id=1,
        code="STD-V1",
        version=1,
        name="Standard Pipeline",
        description=None,
        status="active",
        config={"pipeline": [{"routine": "rule.posting_activity"}]},
    )
    cfg_v2 = SimpleNamespace(
        id=2,
        code="CEMA",
        version=1,
        name="V2 CEMA Migration",
        description=None,
        status="active",
        config={"pipeline": [{"routine": "v2.retire_flag"}]},
    )

    db = MagicMock()
    db.execute.return_value.scalars.return_value.all.return_value = [cfg_v1, cfg_v2]
    user = MagicMock()

    out_v2 = list_configs(db=db, user=user, engine="v2")
    assert [c.id for c in out_v2] == [2]
    assert out_v2[0].engine_version == "v2"

    out_v1 = list_configs(db=db, user=user, engine="v1")
    assert [c.id for c in out_v1] == [1]
    assert out_v1[0].engine_version == "v1"


def test_list_configs_no_filter_returns_all_with_engine_version():
    """No ``?engine=`` — every config comes back, each tagged with its
    detected engine_version. The frontend can show all and the operator
    can pick. (Used when the dashboard hasn't picked an engine yet.)"""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from app.api.configs import list_configs

    cfg_v1 = SimpleNamespace(
        id=1,
        code="A",
        version=1,
        name="A",
        description=None,
        status="active",
        config={"pipeline": [{"routine": "rule.x"}]},
    )
    cfg_v2 = SimpleNamespace(
        id=2,
        code="B",
        version=1,
        name="B",
        description=None,
        status="active",
        config={"pipeline": [{"routine": "v2.x"}]},
    )

    db = MagicMock()
    db.execute.return_value.scalars.return_value.all.return_value = [cfg_v1, cfg_v2]
    user = MagicMock()

    out = list_configs(db=db, user=user, engine=None)
    assert len(out) == 2
    by_id = {c.id: c for c in out}
    assert by_id[1].engine_version == "v1"
    assert by_id[2].engine_version == "v2"
