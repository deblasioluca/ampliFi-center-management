"""Tests for V2 CEMA migration routines."""

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.routines.v2_cema_migration import (
    BalanceMigrateRoutine,
    CombineMigrationRoutine,
    PCApproachRoutine,
    RetireFlagRoutine,
)


def _ctx(**kwargs) -> CenterContext:
    defaults = {
        "center_id": 1,
        "coarea": "1000",
        "cctr": "0001234567",
        "ccode": "CH01",
        "txtsh": "Test Center",
    }
    defaults.update(kwargs)
    return CenterContext(**defaults)


class TestRetireFlag:
    def test_retire_found_in_txtsh(self):
        ctx = _ctx(txtsh="Old Center_RETIRE")
        r = RetireFlagRoutine()
        result = r.run(ctx, {})
        assert result.verdict == "RETIRE"
        assert result.short_circuit is True

    def test_retire_found_in_txtmi(self):
        ctx = _ctx(txtmi="Some Unit _RETIRE here")
        r = RetireFlagRoutine()
        result = r.run(ctx, {})
        assert result.verdict == "RETIRE"

    def test_no_retire(self):
        ctx = _ctx(txtsh="Active Center", txtmi="Normal description")
        r = RetireFlagRoutine()
        result = r.run(ctx, {})
        assert result.verdict == "PASS"

    def test_custom_pattern(self):
        ctx = _ctx(txtsh="Center CLOSED_2024")
        r = RetireFlagRoutine()
        result = r.run(ctx, {"retire_pattern": "CLOSED_"})
        assert result.verdict == "RETIRE"

    def test_case_insensitive(self):
        ctx = _ctx(txtsh="Center_retire_2024")
        r = RetireFlagRoutine()
        result = r.run(ctx, {})
        assert result.verdict == "RETIRE"


class TestBalanceMigrate:
    def test_has_balance_categories(self):
        ctx = _ctx(balance_by_category={"ASSET": 1000.0, "REV": 500.0})
        r = BalanceMigrateRoutine()
        result = r.run(ctx, {})
        assert result.verdict == "MIGRATE_YES"
        assert "ASSET" in result.payload["active_categories"]

    def test_no_balance(self):
        ctx = _ctx(balance_by_category={})
        r = BalanceMigrateRoutine()
        result = r.run(ctx, {})
        assert result.verdict == "MIGRATE_NO"

    def test_fallback_to_total(self):
        ctx = _ctx(total_balance=5000.0)
        r = BalanceMigrateRoutine()
        result = r.run(ctx, {})
        assert result.verdict == "MIGRATE_YES"

    def test_zero_balance_no_migrate(self):
        ctx = _ctx(
            balance_by_category={},
            total_balance=0.0,
            bs_amt=0.0,
            rev_amt=0.0,
            opex_amt=0.0,
        )
        r = BalanceMigrateRoutine()
        result = r.run(ctx, {})
        assert result.verdict == "MIGRATE_NO"

    def test_no_fallback(self):
        ctx = _ctx(total_balance=5000.0)
        r = BalanceMigrateRoutine()
        result = r.run(ctx, {"use_total_fallback": False})
        assert result.verdict == "MIGRATE_NO"


class TestPCApproach:
    def test_default_1_1(self):
        ctx = _ctx(
            ext_levels={"ext_l0": "ROOT", "ext_l1": "TRADING", "ext_l2": "EMEA"},
            ext_descs={"ext_l2_desc": "EMEA Trading"},
        )
        r = PCApproachRoutine()
        result = r.run(ctx, {"approach_rules": []})
        assert result.verdict == "1_1"
        assert result.payload["approach"] == "1:1"

    def test_1_n_match(self):
        ctx = _ctx(
            ext_levels={"ext_l0": "ROOT", "ext_l1": "GRP_FUNC", "ext_l2": "FINANCE"},
            ext_descs={"ext_l3_desc": "Group Functions Finance"},
        )
        rules = [
            {
                "name": "Group Functions",
                "match_field": "ext_l1",
                "match_op": "==",
                "match_value": "GRP_FUNC",
                "approach": "1:n",
                "pc_level": 3,
            }
        ]
        r = PCApproachRoutine()
        result = r.run(ctx, {"approach_rules": rules})
        assert result.payload["approach"] == "1:n"

    def test_1_n_with_exclusion(self):
        ctx = _ctx(
            ext_levels={"ext_l0": "ROOT", "ext_l1": "GRP_FUNC", "ext_l2": "TREASURY"},
            ext_descs={"ext_l3_desc": "Treasury"},
        )
        rules = [
            {
                "name": "Group Functions (excl Treasury)",
                "match_field": "ext_l1",
                "match_op": "==",
                "match_value": "GRP_FUNC",
                "exclude_field": "ext_l2",
                "exclude_op": "==",
                "exclude_value": "TREASURY",
                "approach": "1:n",
                "pc_level": 3,
            }
        ]
        r = PCApproachRoutine()
        result = r.run(ctx, {"approach_rules": rules})
        # Should NOT match because of exclusion
        assert result.payload["approach"] == "1:1"

    def test_in_operator(self):
        ctx = _ctx(
            ext_levels={"ext_l1": "ASSET_MGMT"},
            ext_descs={"ext_l3_desc": "Asset Management"},
        )
        rules = [
            {
                "name": "Asset Management",
                "match_field": "ext_l1",
                "match_op": "in",
                "match_values": ["ASSET_MGMT", "GRP_FUNC"],
                "approach": "1:n",
                "pc_level": 3,
            }
        ]
        r = PCApproachRoutine()
        result = r.run(ctx, {"approach_rules": rules})
        assert result.payload["approach"] == "1:n"

    def test_pc_name_from_level(self):
        ctx = _ctx(
            ext_levels={"ext_l0": "ROOT", "ext_l1": "DIV1", "ext_l2": "UNIT1"},
            ext_descs={"ext_l2_desc": "Business Unit One"},
        )
        r = PCApproachRoutine()
        result = r.run(ctx, {"approach_rules": []})
        assert result.payload["pc_name"] == "Business Unit One"


class TestCombineMigration:
    def test_retire_stops_migration(self):
        ctx = _ctx()
        ctx.flags["_prior_results"] = [
            {
                "code": "v2.retire_flag",
                "verdict": "RETIRE",
                "reason": "v2.retire_flag_found",
                "payload": {},
            },
            {
                "code": "v2.balance_migrate",
                "verdict": "MIGRATE_YES",
                "reason": "v2.balance_active",
                "payload": {},
            },
            {
                "code": "v2.pc_approach",
                "verdict": "1_1",
                "reason": "v2.approach.1:1",
                "payload": {"approach": "1:1", "pc_name": "Test", "group_key": "K1"},
            },
        ]
        r = CombineMigrationRoutine()
        result = r.run(ctx, {})
        assert result.payload["migrate"] == "N"
        assert result.verdict == "RETIRE"

    def test_migrate_yes_with_1_1(self):
        ctx = _ctx()
        ctx.flags["_prior_results"] = [
            {
                "code": "v2.retire_flag",
                "verdict": "PASS",
                "reason": "v2.no_retire_flag",
                "payload": {},
            },
            {
                "code": "v2.balance_migrate",
                "verdict": "MIGRATE_YES",
                "reason": "v2.balance_active",
                "payload": {"active_categories": ["ASSET"]},
            },
            {
                "code": "v2.pc_approach",
                "verdict": "1_1",
                "reason": "v2.approach.1:1",
                "payload": {"approach": "1:1", "pc_name": "My Center", "group_key": "0001234567"},
            },
        ]
        r = CombineMigrationRoutine()
        result = r.run(ctx, {})
        assert result.payload["migrate"] == "Y"
        assert result.payload["approach"] == "1:1"
        assert result.payload["pc_name"] == "My Center"
        assert result.verdict == "KEEP"

    def test_no_balance_no_migration(self):
        ctx = _ctx()
        ctx.flags["_prior_results"] = [
            {
                "code": "v2.retire_flag",
                "verdict": "PASS",
                "reason": "v2.no_retire_flag",
                "payload": {},
            },
            {
                "code": "v2.balance_migrate",
                "verdict": "MIGRATE_NO",
                "reason": "v2.no_balance",
                "payload": {},
            },
            {
                "code": "v2.pc_approach",
                "verdict": "1_1",
                "reason": "v2.approach.1:1",
                "payload": {"approach": "1:1", "pc_name": "Test", "group_key": "K1"},
            },
        ]
        r = CombineMigrationRoutine()
        result = r.run(ctx, {})
        assert result.payload["migrate"] == "N"
        assert result.verdict == "RETIRE"
