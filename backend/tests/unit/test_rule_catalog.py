"""Tests for the business-friendly rule catalog and preset templates."""

from __future__ import annotations

import pytest

from app.domain.decision_tree.rule_catalog import (
    CATALOG,
    PRESETS_V1,
    PRESETS_V2,
    build_v1_config_from_preset,
    build_v2_config_from_preset,
    get_preset,
    get_rule_metadata,
    list_presets,
)


# ── Catalog completeness ────────────────────────────────────────────────


def test_catalog_covers_all_v1_cleansing_rules() -> None:
    expected = {
        "rule.posting_activity",
        "rule.ownership",
        "rule.redundancy",
        "rule.hierarchy_compliance",
        "rule.cross_system_dependency",
    }
    assert expected.issubset(CATALOG.keys())


def test_catalog_covers_all_v1_mapping_rules() -> None:
    expected = {
        "rule.bs_relevance",
        "rule.has_direct_revenue",
        "rule.collects_project_costs",
        "rule.has_operational_costs",
        "rule.revenue_allocation_vehicle",
        "rule.cost_allocation_vehicle",
        "rule.info_only",
    }
    assert expected.issubset(CATALOG.keys())


def test_catalog_covers_all_v2_routines() -> None:
    expected = {
        "v2.retire_flag",
        "v2.balance_migrate",
        "v2.pc_approach",
        "v2.combine_migration",
    }
    assert expected.issubset(CATALOG.keys())


def test_each_catalog_entry_has_required_fields() -> None:
    """Every catalog entry must have the fields the frontend needs."""
    for code, meta in CATALOG.items():
        assert meta.get("business_label"), f"{code} missing business_label"
        assert meta.get("description"), f"{code} missing description"
        assert "decides" in meta, f"{code} missing decides list"
        assert "verdict_meanings" in meta, f"{code} missing verdict_meanings"
        # Every decided verdict should have a meaning
        for verdict in meta["decides"]:
            assert verdict in meta["verdict_meanings"], (
                f"{code}: verdict '{verdict}' has no meaning"
            )


def test_param_metadata_has_help_text() -> None:
    """Every param must have help_text — that's the whole point for business users."""
    for code, meta in CATALOG.items():
        for pname, pmeta in meta.get("params", {}).items():
            assert pmeta.get("help_text"), (
                f"{code}.{pname}: missing help_text — business user needs this"
            )
            assert pmeta.get("friendly_label"), (
                f"{code}.{pname}: missing friendly_label"
            )


def test_numeric_params_have_min_max() -> None:
    """Numeric params should declare min/max to drive UI sliders."""
    for code, meta in CATALOG.items():
        for pname, pmeta in meta.get("params", {}).items():
            if pmeta.get("type") in ("integer", "number"):
                assert "min" in pmeta and "max" in pmeta, (
                    f"{code}.{pname}: numeric param needs min/max for slider UI"
                )


# ── get_rule_metadata ──────────────────────────────────────────────────


def test_get_rule_metadata_known_code() -> None:
    meta = get_rule_metadata("rule.posting_activity")
    assert meta is not None
    assert meta["business_label"] == "Inaktivität erkennen"


def test_get_rule_metadata_unknown_code_returns_none() -> None:
    assert get_rule_metadata("rule.does_not_exist") is None


# ── Presets ────────────────────────────────────────────────────────────


def test_v1_presets_have_three_levels() -> None:
    assert "strict" in PRESETS_V1
    assert "standard" in PRESETS_V1
    assert "lenient" in PRESETS_V1


def test_v2_presets_have_three_options() -> None:
    assert "all_one_to_one" in PRESETS_V2
    assert "by_level3" in PRESETS_V2
    assert "by_country" in PRESETS_V2


def test_strict_preset_more_aggressive_than_standard() -> None:
    """Strict should have lower inactivity threshold (more aggressive RETIRE)."""
    strict_t = PRESETS_V1["strict"]["params"]["posting_inactivity_threshold"]
    std_t = PRESETS_V1["standard"]["params"]["posting_inactivity_threshold"]
    assert strict_t < std_t


def test_lenient_preset_more_conservative_than_standard() -> None:
    lenient_t = PRESETS_V1["lenient"]["params"]["posting_inactivity_threshold"]
    std_t = PRESETS_V1["standard"]["params"]["posting_inactivity_threshold"]
    assert lenient_t > std_t


def test_list_presets_v1() -> None:
    presets = list_presets("v1")
    assert "strict" in presets


def test_list_presets_v2() -> None:
    presets = list_presets("v2")
    assert "by_level3" in presets


def test_list_presets_case_insensitive() -> None:
    assert list_presets("V1") == PRESETS_V1
    assert list_presets("V2") == PRESETS_V2


def test_get_preset_returns_correct_dict() -> None:
    p = get_preset("v1", "standard")
    assert p is not None
    assert p["params"]["posting_inactivity_threshold"] == 12


def test_get_preset_unknown_returns_none() -> None:
    assert get_preset("v1", "nonexistent") is None


# ── Config builders ────────────────────────────────────────────────────


def test_build_v1_config_from_strict_preset() -> None:
    cfg = build_v1_config_from_preset("strict")
    assert "pipeline" in cfg
    pa = next(s for s in cfg["pipeline"] if s["routine"] == "rule.posting_activity")
    assert pa["params"]["posting_inactivity_threshold"] == 6


def test_build_v1_config_from_lenient_preset() -> None:
    cfg = build_v1_config_from_preset("lenient")
    pa = next(s for s in cfg["pipeline"] if s["routine"] == "rule.posting_activity")
    assert pa["params"]["posting_inactivity_threshold"] == 24
    redund = next(s for s in cfg["pipeline"] if s["routine"] == "rule.redundancy")
    assert redund["params"]["similarity_threshold"] == 0.97


def test_build_v1_config_unknown_preset_raises() -> None:
    with pytest.raises(ValueError, match="Unknown V1 preset"):
        build_v1_config_from_preset("totally_made_up")


def test_build_v2_config_from_one_to_one_preset() -> None:
    cfg = build_v2_config_from_preset("all_one_to_one")
    pc_step = next(s for s in cfg["pipeline"] if s["routine"] == "v2.pc_approach")
    assert pc_step["params"]["default_approach"] == "1:1"
    assert pc_step["params"]["approach_rules"] == []


def test_build_v2_config_from_level3_preset() -> None:
    cfg = build_v2_config_from_preset("by_level3")
    pc_step = next(s for s in cfg["pipeline"] if s["routine"] == "v2.pc_approach")
    assert len(pc_step["params"]["approach_rules"]) == 1
    rule = pc_step["params"]["approach_rules"][0]
    assert rule["match"]["hier_level"] == "L3"
    assert rule["approach"] == "1:n"


def test_built_config_carries_preset_origin_marker() -> None:
    """Configs built from presets should be tagged so the UI can show origin."""
    cfg = build_v1_config_from_preset("standard")
    assert cfg.get("_preset_origin") == "standard"
    cfg2 = build_v2_config_from_preset("by_level3")
    assert cfg2.get("_preset_origin") == "by_level3"
