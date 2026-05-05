"""V2 CEMA Migration routines — decision tree for center migration.

Implements the CEMA-based migration logic:
  1. retire_flag: skip centers whose description contains _RETIRE
  2. balance_migrate: check balance categories for migration eligibility
  3. pc_approach: determine 1:1 vs 1:n PC-CC relationship from hierarchy
  4. combine_migration: aggregate into final migrate/approach/pc_name
"""

from __future__ import annotations

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.registry import register_routine


@register_routine
class RetireFlagRoutine:
    """V2 step 1: check for _RETIRE flag in center description."""

    @property
    def code(self) -> str:
        return "v2.retire_flag"

    @property
    def name(self) -> str:
        return "RETIRE Flag Check"

    @property
    def kind(self) -> str:
        return "rule"

    @property
    def tree(self) -> str:
        return "cleansing"

    @property
    def params_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "retire_pattern": {
                    "type": "string",
                    "default": "_RETIRE",
                    "description": "Substring in description that flags center for retirement",
                },
                "check_fields": {
                    "type": "array",
                    "default": ["txtsh", "txtmi"],
                    "description": "Fields to check for the retire pattern",
                },
            },
        }

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        pattern = params.get("retire_pattern", "_RETIRE")
        fields = params.get("check_fields", ["txtsh", "txtmi"])

        for field_name in fields:
            val = getattr(ctx, field_name, "") or ""
            if pattern.upper() in val.upper():
                return RoutineResult(
                    code=self.code,
                    verdict="RETIRE",
                    reason="v2.retire_flag_found",
                    short_circuit=True,
                    payload={"field": field_name, "value": val, "pattern": pattern},
                )

        return RoutineResult(
            code=self.code,
            verdict="PASS",
            reason="v2.no_retire_flag",
        )


# Balance categories aligned with Patrick's specification
_DEFAULT_BALANCE_CATEGORIES = [
    "ASSET",
    "LIABILITY",
    "EQUITY",
    "STAT",
    "REV",
    "DIRECT_COST",
    "HARD_ALLOC",
    "ALLOC_COST",
]


@register_routine
class BalanceMigrateRoutine:
    """V2 step 2: check if center has any balance activity → migrate Y/N."""

    @property
    def code(self) -> str:
        return "v2.balance_migrate"

    @property
    def name(self) -> str:
        return "Balance Migration Check"

    @property
    def kind(self) -> str:
        return "rule"

    @property
    def tree(self) -> str:
        return "cleansing"

    @property
    def params_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "balance_categories": {
                    "type": "array",
                    "default": _DEFAULT_BALANCE_CATEGORIES,
                    "description": "GL account categories to check for non-zero balances",
                },
                "use_total_fallback": {
                    "type": "boolean",
                    "default": True,
                    "description": "If no category breakdown, fall back to total_balance",
                },
            },
        }

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        categories = params.get("balance_categories", _DEFAULT_BALANCE_CATEGORIES)
        use_total = params.get("use_total_fallback", True)

        # Check detailed balance categories if available
        active_categories: list[str] = []
        if ctx.balance_by_category:
            for cat in categories:
                amt = ctx.balance_by_category.get(cat, 0.0)
                if amt != 0.0:
                    active_categories.append(cat)

        if active_categories:
            return RoutineResult(
                code=self.code,
                verdict="MIGRATE_YES",
                reason="v2.balance_active",
                payload={
                    "active_categories": active_categories,
                    "category_count": len(active_categories),
                },
            )

        # Fallback: check aggregate amounts
        if use_total:
            total = abs(ctx.bs_amt) + abs(ctx.rev_amt) + abs(ctx.opex_amt)
            if ctx.total_balance != 0.0 or total != 0.0:
                return RoutineResult(
                    code=self.code,
                    verdict="MIGRATE_YES",
                    reason="v2.balance_total_nonzero",
                    payload={"total_balance": ctx.total_balance, "aggregate": total},
                )

        return RoutineResult(
            code=self.code,
            verdict="MIGRATE_NO",
            reason="v2.no_balance",
            payload={"balance_by_category": dict(ctx.balance_by_category)},
        )


@register_routine
class PCApproachRoutine:
    """V2 step 3: determine 1:1 vs 1:n PC-CC relationship from hierarchy."""

    @property
    def code(self) -> str:
        return "v2.pc_approach"

    @property
    def name(self) -> str:
        return "PC-CC Approach Determination"

    @property
    def kind(self) -> str:
        return "rule"

    @property
    def tree(self) -> str:
        return "mapping"

    @property
    def params_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "approach_rules": {
                    "type": "array",
                    "description": "Rules matching hierarchy levels to 1:n approach",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "match_field": {"type": "string"},
                            "match_op": {
                                "type": "string",
                                "enum": ["in", "==", "contains", "starts_with"],
                            },
                            "match_values": {"type": "array", "items": {"type": "string"}},
                            "match_value": {"type": "string"},
                            "exclude_field": {"type": "string"},
                            "exclude_op": {"type": "string"},
                            "exclude_value": {"type": "string"},
                            "approach": {"type": "string", "enum": ["1:1", "1:n"]},
                            "pc_level": {
                                "type": "integer",
                                "description": "Hierarchy level to use for PC name (1:n)",
                            },
                        },
                    },
                },
                "default_approach": {
                    "type": "string",
                    "default": "1:1",
                },
            },
        }

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        rules = params.get("approach_rules", [])
        default = params.get("default_approach", "1:1")

        for rule in rules:
            if self._match_rule(ctx, rule):
                approach = rule.get("approach", "1:n")
                pc_level = rule.get("pc_level", 3)
                group_key = self._get_group_key(ctx, pc_level)
                pc_name = self._get_pc_name(ctx, pc_level)
                return RoutineResult(
                    code=self.code,
                    verdict=approach.upper().replace(":", "_"),
                    reason=f"v2.approach.{approach}",
                    payload={
                        "approach": approach,
                        "rule_name": rule.get("name", ""),
                        "pc_level": pc_level,
                        "group_key": group_key,
                        "pc_name": pc_name,
                    },
                )

        # Default: 1:1
        pc_name = self._get_lowest_level_name(ctx)
        return RoutineResult(
            code=self.code,
            verdict="1_1",
            reason="v2.approach.1:1.default",
            payload={
                "approach": default,
                "pc_name": pc_name,
                "group_key": ctx.cctr,
            },
        )

    def _match_rule(self, ctx: CenterContext, rule: dict) -> bool:
        field_name = rule.get("match_field", "")
        op = rule.get("match_op", "==")
        val = ctx.ext_levels.get(field_name, "") or ctx.attrs.get(field_name, "")

        if not val:
            return False

        if op == "in":
            values = rule.get("match_values", [])
            if val not in values:
                return False
        elif op == "==":
            if val != rule.get("match_value", ""):
                return False
        elif op == "contains":
            if rule.get("match_value", "") not in val:
                return False
        elif op == "starts_with":
            if not val.startswith(rule.get("match_value", "")):
                return False
        else:
            return False

        # Check exclusion
        excl_field = rule.get("exclude_field")
        if excl_field:
            excl_val = ctx.ext_levels.get(excl_field, "") or ctx.attrs.get(excl_field, "")
            excl_op = rule.get("exclude_op", "==")
            excl_target = rule.get("exclude_value", "")
            if excl_op == "==" and excl_val == excl_target:
                return False
            if excl_op == "contains" and excl_target in (excl_val or ""):
                return False

        return True

    def _get_group_key(self, ctx: CenterContext, level: int) -> str:
        key = ctx.ext_levels.get(f"ext_l{level}", "")
        if not key:
            for i in range(level, -1, -1):
                key = ctx.ext_levels.get(f"ext_l{i}", "")
                if key:
                    break
        return key or ctx.cctr

    def _get_pc_name(self, ctx: CenterContext, level: int) -> str:
        desc = ctx.ext_descs.get(f"ext_l{level}_desc", "")
        if desc:
            return desc
        return ctx.ext_levels.get(f"ext_l{level}", "") or ctx.txtsh or ctx.cctr

    def _get_lowest_level_name(self, ctx: CenterContext) -> str:
        for i in range(13, -1, -1):
            desc = ctx.ext_descs.get(f"ext_l{i}_desc", "")
            if desc:
                return desc
        return ctx.txtsh or ctx.cctr


@register_routine
class CombineMigrationRoutine:
    """V2 aggregate: combine retire + balance + approach into final decision."""

    @property
    def code(self) -> str:
        return "v2.combine_migration"

    @property
    def name(self) -> str:
        return "Combine Migration Decision"

    @property
    def kind(self) -> str:
        return "aggregate"

    @property
    def tree(self) -> str | None:
        return None

    @property
    def params_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "default_approach": {"type": "string", "default": "1:1"},
            },
        }

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        prior: list[dict] = ctx.flags.get("_prior_results", [])

        migrate = "N"
        approach = params.get("default_approach", "1:1")
        pc_name = ctx.txtsh or ctx.cctr
        group_key = ctx.cctr
        retire_reason = ""
        balance_info: dict = {}
        rule_path: list[str] = []

        for r in prior:
            code = r.get("code", "")
            verdict = r.get("verdict", "PASS")
            payload = r.get("payload", {})

            if code == "v2.retire_flag" and verdict == "RETIRE":
                retire_reason = r.get("reason", "retired")
                rule_path.append(f"{code}:RETIRE")
                return RoutineResult(
                    code=self.code,
                    verdict="RETIRE",
                    reason=f"v2.combined.retire.{retire_reason}",
                    payload={
                        "migrate": "N",
                        "approach": approach,
                        "pc_name": "",
                        "cc_name": "",
                        "group_key": "",
                        "rule_path": rule_path,
                    },
                )

            if code == "v2.balance_migrate":
                if verdict == "MIGRATE_YES":
                    migrate = "Y"
                    balance_info = payload
                    rule_path.append(f"{code}:MIGRATE_YES")
                else:
                    rule_path.append(f"{code}:MIGRATE_NO")

            if code == "v2.pc_approach":
                approach = payload.get("approach", approach)
                pc_name = payload.get("pc_name", pc_name)
                group_key = payload.get("group_key", group_key)
                rule_path.append(f"{code}:{approach}")

        # Final PC name: for 1:1, use lowest hierarchy desc; for 1:n, use level-based name
        if migrate == "N":
            pc_name = ""
            group_key = ""

        cleansing = "KEEP" if migrate == "Y" else "RETIRE"
        target = "CC_AND_PC" if migrate == "Y" else "NONE"

        return RoutineResult(
            code=self.code,
            verdict=cleansing,
            reason=f"v2.combined.{cleansing.lower()}.{approach}",
            payload={
                "migrate": migrate,
                "cleansing_outcome": cleansing,
                "target_object": target,
                "approach": approach,
                "pc_name": pc_name,
                "cc_name": ctx.txtsh or ctx.cctr,
                "group_key": group_key,
                "balance_info": balance_info,
                "rule_path": rule_path,
            },
        )
