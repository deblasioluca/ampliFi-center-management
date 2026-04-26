"""No-code DSL rule evaluator (§04.6C).

Evaluates JSON rule expressions against center features. Only whitelisted
operators are allowed — no eval(), no code execution.

Expression shape:
    {"all": [<condition>, ...]}       # AND
    {"any": [<condition>, ...]}       # OR
    {"not": <condition>}              # NOT
    {"feature": "name", "op": ">", "value": 12}  # comparison

Supported ops: ==, !=, <, <=, >, >=, in, not_in, is_null, is_not_null
"""

from __future__ import annotations

import operator
from typing import Any

from app.domain.decision_tree.context import CenterContext, RoutineResult

_OPS: dict[str, Any] = {
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}

_MAX_DEPTH = 10


class DSLEvalError(Exception):
    pass


def _get_feature_value(ctx: CenterContext, name: str) -> Any:
    """Safely extract a feature value from the center context."""
    if hasattr(ctx, name):
        return getattr(ctx, name)
    if name in ctx.attrs:
        return ctx.attrs[name]
    if name in ctx.flags:
        return ctx.flags[name]
    return None


def _eval_condition(expr: dict, ctx: CenterContext, depth: int = 0) -> bool:
    """Evaluate a single condition expression recursively."""
    if depth > _MAX_DEPTH:
        raise DSLEvalError(f"Expression nesting exceeds max depth {_MAX_DEPTH}")

    if "all" in expr:
        conditions = expr["all"]
        if not isinstance(conditions, list):
            raise DSLEvalError("'all' must contain a list")
        return all(_eval_condition(c, ctx, depth + 1) for c in conditions)

    if "any" in expr:
        conditions = expr["any"]
        if not isinstance(conditions, list):
            raise DSLEvalError("'any' must contain a list")
        return any(_eval_condition(c, ctx, depth + 1) for c in conditions)

    if "not" in expr:
        return not _eval_condition(expr["not"], ctx, depth + 1)

    if "feature" not in expr or "op" not in expr:
        raise DSLEvalError(
            f"Invalid condition: must have 'feature' and 'op', got {list(expr.keys())}"
        )

    feature_name = expr["feature"]
    op_name = expr["op"]
    actual = _get_feature_value(ctx, feature_name)

    if op_name == "is_null":
        return actual is None
    if op_name == "is_not_null":
        return actual is not None

    if "value" not in expr:
        raise DSLEvalError(f"Operator '{op_name}' requires 'value'")
    expected = expr["value"]

    if op_name == "in":
        if not isinstance(expected, list):
            raise DSLEvalError("'in' operator requires a list value")
        return actual in expected

    if op_name == "not_in":
        if not isinstance(expected, list):
            raise DSLEvalError("'not_in' operator requires a list value")
        return actual not in expected

    if op_name not in _OPS:
        raise DSLEvalError(f"Unknown operator: {op_name}")

    if actual is None:
        return False

    try:
        return _OPS[op_name](actual, expected)
    except TypeError:
        return False


def evaluate_dsl_rule(
    expression: dict,
    ctx: CenterContext,
    verdict_when_true: dict,
    verdict_when_false: str,
    routine_code: str,
) -> RoutineResult:
    """Evaluate a DSL rule expression and return a RoutineResult.

    Args:
        expression: The JSON rule expression
        ctx: Center context with features
        verdict_when_true: dict with 'outcome' and 'reason' keys
        verdict_when_false: "passthrough" or a verdict string
        routine_code: code of the DSL routine
    """
    try:
        matched = _eval_condition(expression, ctx)
    except DSLEvalError as e:
        return RoutineResult(
            code=routine_code,
            verdict="ERROR",
            reason=f"dsl.eval_error: {e}",
            payload={"error": str(e)},
        )

    if matched:
        outcome = verdict_when_true.get("outcome", "RETIRE")
        reason = verdict_when_true.get("reason", f"{routine_code}.matched")
        return RoutineResult(
            code=routine_code,
            verdict=outcome,
            reason=reason,
            short_circuit=outcome in ("RETIRE", "MERGE_MAP", "REDESIGN"),
            payload={"dsl_matched": True, "expression": expression},
        )

    if verdict_when_false == "passthrough":
        return RoutineResult(
            code=routine_code,
            verdict="PASS",
            reason=f"{routine_code}.no_match",
        )

    return RoutineResult(
        code=routine_code,
        verdict=verdict_when_false,
        reason=f"{routine_code}.false_branch",
    )
