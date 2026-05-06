"""Business-friendly translation of decision reasoning data.

Takes the raw rule_path / outcome / target stored in CenterProposal and
translates it into plain language using the rule catalog metadata. Used
by the why-panel endpoint so the frontend can render reviewer-friendly
explanations instead of raw routine codes.
"""

from __future__ import annotations

from typing import Any

# ── Outcome and target translations (keyed by the values stored on
# CenterProposal). These are the user-facing English equivalents for the
# enum-like string codes in the database.

OUTCOME_TRANSLATIONS: dict[str, dict[str, str]] = {
    "KEEP": {
        "label": "Keep",
        "sentence": "Keep this center as-is — no changes needed.",
    },
    "RETIRE": {
        "label": "Retire",
        "sentence": "Retire this center — it is no longer needed in the new model.",
    },
    "MERGE_MAP": {
        "label": "Merge",
        "sentence": (
            "Merge this center into another canonical center — its activity will be redirected."
        ),
    },
    "REDESIGN": {
        "label": "Redesign",
        "sentence": "Redesign this center — it must be reconceptualised before migration.",
    },
    "MIGRATE": {
        "label": "Migrate",
        "sentence": "Migrate this center into the new model.",
    },
    "UNKNOWN": {
        "label": "Undetermined",
        "sentence": "No final outcome could be determined from the rule pipeline.",
    },
}


TARGET_TRANSLATIONS: dict[str, str] = {
    "CC": "Cost Center only",
    "PC": "Profit Center only",
    "PC_ONLY": "Profit Center only (no Cost Center counterpart)",
    "CC_AND_PC": "Cost Center AND Profit Center (both objects)",
    "WBS_REAL": "WBS element with real costs",
    "WBS_STAT": "Statistical WBS element (reporting only)",
    "NONE": "Nothing — center is removed",
}


def _split_step(step: str) -> tuple[str, str]:
    """Split 'code:verdict' string into (code, verdict).

    Verdicts may themselves contain colons (e.g. ``v2.pc_approach:1:1``),
    so split on the FIRST colon only — everything after is the verdict.
    """
    if not isinstance(step, str):
        return ("", "")
    if ":" not in step:
        return (step, "")
    code, _, verdict = step.partition(":")
    return (code.strip(), verdict.strip())


def translate_step(
    code: str, verdict: str, catalog: dict[str, dict] | None = None
) -> dict[str, Any]:
    """Translate a single (routine_code, verdict) into a friendly dict.

    Returns the structured form used by the frontend:
        {
          "code": "v2.balance_migrate",
          "verdict": "MIGRATE_YES",
          "label": "Balance sheet migration check",
          "verdict_meaning": "Has balance sheet activity → must be migrated",
          "description": "Looks at the center's balance sheet…",
        }

    Falls back gracefully when the catalog has no entry for the routine
    or no meaning for the verdict — returns the raw values so the user
    still sees something.
    """
    if catalog is None:
        from app.domain.decision_tree.rule_catalog import CATALOG

        catalog = CATALOG

    entry = catalog.get(code, {}) if catalog else {}
    label = entry.get("business_label") or code or "(unknown step)"
    description = entry.get("description") or ""
    meanings = entry.get("verdict_meanings") or {}
    verdict_meaning = meanings.get(verdict) or _humanize_verdict(verdict)
    return {
        "code": code,
        "verdict": verdict,
        "label": label,
        "verdict_meaning": verdict_meaning,
        "description": description,
    }


def _humanize_verdict(verdict: str) -> str:
    """Last-resort fallback when the catalog has no meaning for a verdict.

    Turns ``MIGRATE_YES`` into ``Migrate yes``; leaves friendly verdicts
    like ``1:1`` alone.
    """
    if not verdict:
        return ""
    if verdict.isupper() and "_" in verdict:
        return verdict.replace("_", " ").capitalize()
    return verdict


def translate_rule_path(
    rule_path: Any, catalog: dict[str, dict] | None = None
) -> list[dict[str, Any]]:
    """Translate a stored rule_path into a list of structured friendly dicts.

    Handles the two storage formats currently in use:

    1. ``{"steps": [{"routine": ..., "verdict": ..., "confidence": ...}, ...]}``
       (V1 format — produced by ``analysis.py``)
    2. ``["code:verdict", "code:verdict", ...]``
       (V2 format — produced by ``analysis_v2.py``)

    Returns an empty list when the path is missing or empty.
    """
    if rule_path is None:
        return []

    out: list[dict[str, Any]] = []

    # V1: {"steps": [...]}
    if isinstance(rule_path, dict) and isinstance(rule_path.get("steps"), list):
        for step in rule_path["steps"]:
            if isinstance(step, dict):
                code = step.get("routine") or step.get("code") or ""
                verdict = step.get("verdict") or step.get("result") or ""
                translated = translate_step(code, verdict, catalog)
                if step.get("confidence") is not None:
                    translated["confidence"] = step.get("confidence")
                out.append(translated)
            elif isinstance(step, str):
                code, verdict = _split_step(step)
                out.append(translate_step(code, verdict, catalog))
        return out

    # V2: ["code:verdict", ...]
    if isinstance(rule_path, list):
        for step in rule_path:
            if isinstance(step, dict):
                code = step.get("routine") or step.get("code") or ""
                verdict = step.get("verdict") or step.get("result") or ""
                out.append(translate_step(code, verdict, catalog))
            elif isinstance(step, str):
                code, verdict = _split_step(step)
                out.append(translate_step(code, verdict, catalog))
        return out

    # Unknown format — return as-is wrapped in a single fallback entry.
    return [
        {
            "code": "",
            "verdict": "",
            "label": "Decision steps (technical detail)",
            "verdict_meaning": "",
            "description": str(rule_path)[:500],
        }
    ]


def translate_outcome(outcome: str | None) -> dict[str, str]:
    """Map an outcome enum value to label + sentence."""
    if not outcome:
        return OUTCOME_TRANSLATIONS["UNKNOWN"]
    return OUTCOME_TRANSLATIONS.get(
        outcome.upper(),
        {
            "label": outcome,
            "sentence": f"Outcome: {outcome}.",
        },
    )


def translate_target(target: str | None) -> str:
    """Map a target_object enum value to a friendly label."""
    if not target:
        return ""
    return TARGET_TRANSLATIONS.get(target.upper(), target)
