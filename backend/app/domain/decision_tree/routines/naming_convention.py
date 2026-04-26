"""Rule: naming convention compliance (§04.6 example plugin).

Checks whether cost center names follow the organization's naming pattern.
Centers with non-compliant names are flagged for REDESIGN.
"""

from __future__ import annotations

import re

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.registry import register_routine


@register_routine
class NamingConventionRoutine:
    """Checks cost center naming compliance against configurable patterns."""

    @property
    def code(self) -> str:
        return "rule.naming_convention"

    @property
    def name(self) -> str:
        return "Naming Convention Check"

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
                "pattern": {
                    "type": "string",
                    "default": "^[A-Z]{2}\\d{2}-[A-Z]{3,}(-\\d+)?$",
                    "description": "Regex pattern for compliant names (e.g. 'DE01-SALES-001')",
                },
                "check_field": {
                    "type": "string",
                    "default": "txtsh",
                    "description": "Which name field to check (txtsh or txtmi)",
                },
                "non_compliant_verdict": {
                    "type": "string",
                    "default": "REDESIGN",
                    "description": "Verdict for non-compliant names",
                },
            },
        }

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        pattern = params.get("pattern", r"^[A-Z]{2}\d{2}-[A-Z]{3,}(-\d+)?$")
        check_field = params.get("check_field", "txtsh")
        verdict_on_fail = params.get("non_compliant_verdict", "REDESIGN")

        name_value = getattr(ctx, check_field, "") or ""
        if not name_value.strip():
            return RoutineResult(
                code=self.code,
                verdict="PASS",
                reason="naming.empty_name",
                payload={"name": name_value, "pattern": pattern},
            )

        try:
            if re.match(pattern, name_value.strip()):
                return RoutineResult(
                    code=self.code,
                    verdict="PASS",
                    reason="naming.compliant",
                    payload={"name": name_value, "pattern": pattern},
                )
        except re.error:
            return RoutineResult(
                code=self.code,
                verdict="PASS",
                reason="naming.invalid_pattern",
                payload={"pattern": pattern, "error": "Invalid regex"},
            )

        return RoutineResult(
            code=self.code,
            verdict=verdict_on_fail,
            reason="naming.non_compliant",
            payload={"name": name_value, "pattern": pattern},
        )
