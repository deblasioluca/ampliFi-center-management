"""DSL rule engine for no-code rule authoring (§04.5).

Supports two interfaces:
1. Expression-based (used by pipeline/tests): evaluate_dsl_rule(expression, ctx, ...)
2. Rule-based (used by API/UI): evaluate_rule(ctx, rule)

Expression format (combinators):
  {"feature": "has_owner", "op": "==", "value": True}
  {"all": [expr1, expr2]}
  {"any": [expr1, expr2]}
  {"not": expr}

Rule format (flat conditions):
  {"conditions": [...], "logic": "AND", "verdict": "RETIRE", "reason": "..."}
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


# ── Expression-based evaluation (combinators) ──


def _eval_expr(expr: dict, ctx: CenterContext) -> bool:
    """Recursively evaluate a DSL expression tree against a CenterContext."""
    if "all" in expr:
        return all(_eval_expr(sub, ctx) for sub in expr["all"])
    if "any" in expr:
        return any(_eval_expr(sub, ctx) for sub in expr["any"])
    if "not" in expr:
        return not _eval_expr(expr["not"], ctx)

    # Leaf condition: {"feature": ..., "op": ..., "value": ...}
    feature = expr.get("feature", expr.get("field"))
    op_name = expr.get("op", "==")
    expected = expr.get("value")

    actual = getattr(ctx, feature, None) if feature else None
    op_fn = OPS.get(op_name)
    if not op_fn:
        return False
    try:
        return bool(op_fn(actual, expected))
    except (TypeError, ValueError):
        return False


def evaluate_dsl_rule(
    *,
    expression: dict,
    ctx: CenterContext,
    verdict_when_true: dict | str,
    verdict_when_false: str = "PASS",
    routine_code: str = "dsl.custom",
) -> RoutineResult:
    """Evaluate a DSL expression and return a RoutineResult.

    This is the primary interface used by the pipeline engine and tests.
    """
    matched = _eval_expr(expression, ctx)

    if matched:
        if isinstance(verdict_when_true, dict):
            outcome = verdict_when_true.get("outcome", "PASS")
            reason = verdict_when_true.get("reason", "dsl.matched")
        else:
            outcome = str(verdict_when_true)
            reason = "dsl.matched"
        return RoutineResult(code=routine_code, verdict=outcome, reason=reason)

    return RoutineResult(code=routine_code, verdict=verdict_when_false, reason="dsl.not_matched")


# ── Flat-rule evaluation (API/UI) ──


def evaluate_condition(ctx: CenterContext, cond: dict) -> bool:
    """Evaluate a single flat condition against a CenterContext."""
    return _eval_expr(cond, ctx)


def evaluate_rule(ctx: CenterContext, rule: dict) -> RoutineResult:
    """Evaluate a flat DSL rule (conditions list + logic).

    Used by the API endpoints and rule builder UI.
    """
    conditions = rule.get("conditions", [])
    logic = rule.get("logic", "AND").upper()
    verdict = rule.get("verdict", "PASS")
    reason = rule.get("reason", "dsl.custom_rule")
    code = rule.get("code", "dsl.custom")
    short_circuit = rule.get("short_circuit", False)

    if not conditions:
        return RoutineResult(code=code, verdict="PASS", reason="dsl.no_conditions")

    results = [_eval_expr(c, ctx) for c in conditions]
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


# ── Validation ──


def validate_rule(rule: dict) -> list[str]:
    """Validate a flat DSL rule definition. Returns a list of errors."""
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
            if "field" not in cond and "feature" not in cond:
                errors.append(f"Condition {i}: 'field' or 'feature' is required")
            op_name = cond.get("op")
            if op_name and op_name not in OPS:
                errors.append(f"Condition {i}: unknown operator '{op_name}'")

    verdict = rule.get("verdict")
    if not verdict:
        errors.append("verdict is required")
    elif verdict not in ("KEEP", "RETIRE", "MERGE_MAP", "REDESIGN", "PASS", "UNKNOWN", "FLAG"):
        errors.append(f"Unknown verdict: {verdict}")

    logic = rule.get("logic", "AND")
    if logic.upper() not in ("AND", "OR"):
        errors.append(f"Unknown logic: {logic}")

    return errors
