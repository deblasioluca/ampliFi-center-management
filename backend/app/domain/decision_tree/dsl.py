"""DSL rule engine for no-code rule authoring (§04.5).

Evaluates JSON-serializable rule definitions against CenterContext.

Rule format:
{
    "conditions": [
        {"field": "months_since_last_posting", "op": ">=", "value": 12},
        {"field": "posting_count_window", "op": "<=", "value": 0}
    ],
    "logic": "AND",  // AND | OR
    "verdict": "RETIRE",
    "reason": "custom.inactive_and_no_postings"
}
"""

from __future__ import annotations

import operator
from typing import Any

from app.domain.decision_tree.context import CenterContext, RoutineResult

OPS: dict[str, Any] = {
    "==": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "in": lambda a, b: a in b,
    "not_in": lambda a, b: a not in b,
    "contains": lambda a, b: b in str(a) if a else False,
    "starts_with": lambda a, b: str(a).startswith(str(b)) if a else False,
    "ends_with": lambda a, b: str(a).endswith(str(b)) if a else False,
    "is_null": lambda a, _: a is None,
    "is_not_null": lambda a, _: a is not None,
}


def evaluate_condition(ctx: CenterContext, cond: dict) -> bool:
    """Evaluate a single condition against a CenterContext."""
    field = cond["field"]
    op_name = cond.get("op", "==")
    expected = cond.get("value")

    actual = getattr(ctx, field, None)
    op_fn = OPS.get(op_name)
    if not op_fn:
        return False

    try:
        return bool(op_fn(actual, expected))
    except (TypeError, ValueError):
        return False


def evaluate_rule(ctx: CenterContext, rule: dict) -> RoutineResult:
    """Evaluate a DSL rule against a CenterContext.

    Returns a RoutineResult with the configured verdict if all conditions match,
    otherwise returns PASS.
    """
    conditions = rule.get("conditions", [])
    logic = rule.get("logic", "AND").upper()
    verdict = rule.get("verdict", "PASS")
    reason = rule.get("reason", "dsl.custom_rule")
    code = rule.get("code", "dsl.custom")
    short_circuit = rule.get("short_circuit", False)

    if not conditions:
        return RoutineResult(code=code, verdict="PASS", reason="dsl.no_conditions")

    results = [evaluate_condition(ctx, c) for c in conditions]

    matched = any(results) if logic == "OR" else all(results)

    if matched:
        return RoutineResult(
            code=code,
            verdict=verdict,
            reason=reason,
            short_circuit=short_circuit,
            payload={"conditions_matched": sum(results), "total": len(results)},
        )

    return RoutineResult(
        code=code,
        verdict="PASS",
        reason="dsl.not_matched",
        payload={"conditions_matched": sum(results), "total": len(results)},
    )


def validate_rule(rule: dict) -> list[str]:
    """Validate a DSL rule definition. Returns a list of errors."""
    errors: list[str] = []
    if not isinstance(rule, dict):
        return ["Rule must be a dictionary"]

    conditions = rule.get("conditions")
    if not conditions:
        errors.append("At least one condition is required")
    elif not isinstance(conditions, list):
        errors.append("conditions must be a list")
    else:
        for i, cond in enumerate(conditions):
            if not isinstance(cond, dict):
                errors.append(f"Condition {i} must be a dictionary")
                continue
            if "field" not in cond:
                errors.append(f"Condition {i}: 'field' is required")
            if "op" in cond and cond["op"] not in OPS:
                errors.append(f"Condition {i}: unknown operator '{cond['op']}'")

    verdict = rule.get("verdict")
    if not verdict:
        errors.append("verdict is required")
    elif verdict not in ("KEEP", "RETIRE", "MERGE_MAP", "REDESIGN", "PASS", "UNKNOWN"):
        errors.append(f"Unknown verdict: {verdict}")

    logic = rule.get("logic", "AND")
    if logic.upper() not in ("AND", "OR"):
        errors.append(f"Unknown logic: {logic}")

    return errors


# Alias for backward compatibility with tests
evaluate_dsl_rule = evaluate_rule
