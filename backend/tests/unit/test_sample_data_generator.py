"""Tests for the sample-data generator's planning logic.

These are pure-Python tests — no database required. They lock down the
shape of the output so future changes don't accidentally break the
distribution invariants the user relies on (~30% retire, ~40% in shared
PC groups, realistic geographic spread, etc.).
"""

# ruff: noqa: S311
# random.Random is fine here — sample data, not crypto.

from __future__ import annotations

import importlib.util
import random
import sys
from pathlib import Path

# The script lives outside the normal `app/` import tree because it's a CLI,
# not part of the package. Load it via importlib so the tests can address
# its internals without polluting sys.modules with side-effect imports.
SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "generate_sample_data.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("generate_sample_data", SCRIPT_PATH)
    assert spec and spec.loader, "spec/loader missing"
    mod = importlib.util.module_from_spec(spec)
    sys.modules["generate_sample_data"] = mod
    spec.loader.exec_module(mod)
    return mod


GEN = _load_module()


# ── Entity generation ──────────────────────────────────────────────────────


def test_build_entities_hits_target_count() -> None:
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 600)
    assert len(ents) == 600


def test_build_entities_includes_real_ubs_legal_names() -> None:
    """The catalogue should always seed with the real UBS entities first."""
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 600)
    names = {e.name for e in ents}
    # A handful of well-known names that MUST be present
    expected = {
        "UBS AG",
        "UBS Switzerland AG",
        "UBS Europe SE",
        "UBS Americas Inc.",
        "UBS Securities LLC",
        "UBS Financial Services Inc.",
        "UBS AG London Branch",
        "UBS AG Hong Kong Branch",
    }
    missing = expected - names
    assert not missing, f"Expected real UBS entities missing: {missing}"


def test_build_entities_marks_credit_suisse_as_legacy() -> None:
    """Former CS entities should be flagged so the CC generator routes them
    into Non-core and Legacy."""
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 600)
    cs_ents = [e for e in ents if "Credit Suisse" in e.name or "DLJ" in e.name]
    assert len(cs_ents) >= 4
    assert all(e.is_legacy_cs for e in cs_ents)


def test_build_entities_assigns_unique_ccodes() -> None:
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 600)
    ccodes = [e.ccode for e in ents]
    assert len(ccodes) == len(set(ccodes)), "duplicate company codes"


def test_build_entities_geographic_concentration() -> None:
    """Switzerland and US should be the two largest country footprints —
    that matches UBS's real concentration after the Credit Suisse merger."""
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 600)
    by_country: dict[str, int] = {}
    for e in ents:
        by_country[e.country.code] = by_country.get(e.country.code, 0) + 1
    top3 = sorted(by_country.items(), key=lambda kv: -kv[1])[:3]
    top3_codes = [c for c, _ in top3]
    assert "CH" in top3_codes, f"CH not in top 3: {top3_codes}"
    assert "US" in top3_codes, f"US not in top 3: {top3_codes}"


# ── Cost center generation ─────────────────────────────────────────────────


def test_build_cost_centers_hits_target_count() -> None:
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 100)
    ccs = GEN.build_cost_centers(rng, ents, 5000, retire_pct=0.30, sharing_pct=0.40)
    assert len(ccs) == 5000


def test_retire_rate_is_within_tolerance() -> None:
    """User asked for ~30% retire; allow ±5pp because of NCL bias."""
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 100)
    ccs = GEN.build_cost_centers(rng, ents, 10_000, retire_pct=0.30, sharing_pct=0.40)
    n_retire = sum(1 for c in ccs if c.will_retire)
    rate = n_retire / len(ccs)
    assert 0.25 <= rate <= 0.40, f"retire rate {rate:.1%} outside 25-40% band"


def test_sharing_rate_at_scale_is_realistic() -> None:
    """At 130k+ scale the user wants ~40% in sharing groups. At 10k we
    accept a wider band because singleton buckets fall through. The
    important shape: more than a quarter share."""
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 100)
    ccs = GEN.build_cost_centers(rng, ents, 10_000, retire_pct=0.30, sharing_pct=0.40)
    n_share = sum(1 for c in ccs if c.pc_group_key)
    rate = n_share / len(ccs)
    # We don't need to hit 40% at this scale, but it must be substantial.
    assert 0.20 <= rate <= 0.50, f"sharing rate {rate:.1%} outside 20-50% band"


def test_pc_groups_have_2_to_6_members() -> None:
    """The whole point of the sharing logic is m:1 migration — verify
    every actual shared group has 2–6 members. Singletons MUST become
    non-shared (their own PC)."""
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 100)
    ccs = GEN.build_cost_centers(rng, ents, 10_000, retire_pct=0.30, sharing_pct=0.40)
    by_group: dict[str, int] = {}
    for cc in ccs:
        if cc.pc_group_key:
            by_group[cc.pc_group_key] = by_group.get(cc.pc_group_key, 0) + 1
    assert by_group, "no sharing groups generated"
    sizes = list(by_group.values())
    assert min(sizes) >= 2, f"group with size <2: min={min(sizes)}"
    assert max(sizes) <= 6, f"group too large: max={max(sizes)}"


def test_pc_group_has_exactly_one_leader() -> None:
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 100)
    ccs = GEN.build_cost_centers(rng, ents, 5000, retire_pct=0.30, sharing_pct=0.40)
    leaders_by_group: dict[str, int] = {}
    for cc in ccs:
        if cc.pc_group_key and cc.is_pc_group_leader:
            leaders_by_group[cc.pc_group_key] = leaders_by_group.get(cc.pc_group_key, 0) + 1
    for grp, n in leaders_by_group.items():
        assert n == 1, f"group {grp} has {n} leaders, expected 1"


def test_legacy_cs_entities_produce_mostly_ncl_centers() -> None:
    """Credit Suisse / DLJ entities should be heavily Non-core and Legacy
    (matching the Q1 2026 report's Non-core wind-down portfolio)."""
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 200)
    ccs = GEN.build_cost_centers(rng, ents, 10_000, retire_pct=0.30, sharing_pct=0.40)
    cs_ccs = [c for c in ccs if c.entity.is_legacy_cs]
    if not cs_ccs:
        return  # statistically possible but unlikely with this seed
    ncl = sum(1 for c in cs_ccs if c.division.code == "NCL")
    rate = ncl / len(cs_ccs)
    assert rate >= 0.70, f"only {rate:.1%} of CS-entity CCs are NCL"


def test_distinct_pc_count_reflects_grouping() -> None:
    """If sharing is working, distinct PCs < total CCs by a meaningful margin."""
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 100)
    ccs = GEN.build_cost_centers(rng, ents, 5000, retire_pct=0.30, sharing_pct=0.40)
    distinct_pcs = len({c.pctr for c in ccs})
    # With ~30% retire and groups of 2-6 averaging 3, distinct PCs should be
    # at least 5% below total CCs at this scale (more at 130k scale).
    assert distinct_pcs <= 0.95 * len(ccs), (
        f"PCs ({distinct_pcs}) too close to CCs ({len(ccs)}) — sharing not working"
    )


def test_every_cc_has_a_profit_center() -> None:
    """No CC should be left without a pctr — that would crash the V2 routines."""
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 100)
    ccs = GEN.build_cost_centers(rng, ents, 5000, retire_pct=0.30, sharing_pct=0.40)
    missing = [c for c in ccs if not c.pctr]
    assert not missing, f"{len(missing)} CCs missing pctr"


def test_activity_levels_are_in_unit_interval() -> None:
    rng = random.Random(42)
    ents = GEN.build_entities(rng, 100)
    ccs = GEN.build_cost_centers(rng, ents, 2000, retire_pct=0.30, sharing_pct=0.40)
    for cc in ccs:
        assert 0.0 <= cc.activity_level <= 1.0, f"activity {cc.activity_level} OOB on {cc.cctr}"


# ── Reproducibility ────────────────────────────────────────────────────────


def test_same_seed_produces_same_output() -> None:
    """The script's whole value for demos is reproducibility."""
    rng1 = random.Random(7)
    ents1 = GEN.build_entities(rng1, 50)
    ccs1 = GEN.build_cost_centers(rng1, ents1, 1000, retire_pct=0.30, sharing_pct=0.40)

    rng2 = random.Random(7)
    ents2 = GEN.build_entities(rng2, 50)
    ccs2 = GEN.build_cost_centers(rng2, ents2, 1000, retire_pct=0.30, sharing_pct=0.40)

    assert [e.ccode for e in ents1] == [e.ccode for e in ents2]
    assert [c.cctr for c in ccs1] == [c.cctr for c in ccs2]
    assert [c.pctr for c in ccs1] == [c.pctr for c in ccs2]
