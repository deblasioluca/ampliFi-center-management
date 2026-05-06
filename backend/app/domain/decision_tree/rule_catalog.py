"""User-friendly rule catalog metadata.

The decision tree pipeline is built from "routines" (rules, ML models, LLM
passes, aggregates). Each routine has a technical ``code`` and a Python
implementation in ``app.domain.decision_tree.routines``.

This module enriches that with metadata aimed at *users who configure
variants of the decision tree without reading Python*:

- ``business_label``: short, non-technical name displayed in the UI
- ``description``: one paragraph explaining what the rule checks and why
- ``verdict_meanings``: maps each verdict the rule emits to a plain-language
  explanation (so "RETIRE" appears as "Center will be retired" in the UI)
- ``params``: enriched param schema with ``min``, ``max``, ``unit``,
  ``help_text``, ``friendly_label`` per parameter
- ``decides``: the cleansing or mapping outcomes this rule can drive — used
  to color-code the rule on the simulation results page

This is the single source of truth: the frontend ``rule-catalog`` endpoint
returns this dict, and config editors render forms from it.

Adding a new rule? Add an entry here at the same time you add the routine
class. Missing entries fall back to the routine's own ``params_schema``,
but users will see the technical code instead of a friendly name.
"""

from __future__ import annotations

from typing import Any

# ── Catalog entries ──────────────────────────────────────────────────────


CATALOG: dict[str, dict[str, Any]] = {
    # ── Cleansing tree (V1) ──────────────────────────────────────────────
    "rule.posting_activity": {
        "business_label": "Detect inactivity",
        "description": (
            "Flags cost centers with no or very few postings over the last "
            "few months. These are candidates for retirement (RETIRE) "
            "because they no longer represent an active business purpose."
        ),
        "decides": ["RETIRE", "KEEP"],
        "verdict_meanings": {
            "RETIRE": "Inactive — proposed for retirement",
            "KEEP": "Active enough — stays",
            "PASS": "No determination possible (e.g. missing data)",
        },
        "params": {
            "posting_inactivity_threshold": {
                "friendly_label": "Inactivity threshold",
                "type": "integer",
                "default": 12,
                "min": 1,
                "max": 60,
                "unit": "months",
                "help_text": (
                    "A cost center with no postings for more than this many "
                    "months is considered inactive. Recommended: 12–24 months. "
                    "Lower values = stricter (more RETIRE proposals)."
                ),
            },
            "posting_minimal_threshold": {
                "friendly_label": "Minimum postings",
                "type": "integer",
                "default": 0,
                "min": 0,
                "max": 100,
                "unit": "postings",
                "help_text": (
                    "Posting count in the observation window above which the "
                    "center still counts as active. 0 = every posting counts; "
                    "5 = sporadic postings are ignored (center still inactive)."
                ),
            },
        },
    },
    "rule.ownership": {
        "business_label": "Check ownership",
        "description": (
            "Flags cost centers without a valid owner ('responsible' field "
            "empty or pointing to someone no longer at the company). These "
            "need clarification before migration — either a new owner or "
            "retirement."
        ),
        "decides": ["RETIRE", "REDESIGN", "KEEP"],
        "verdict_meanings": {
            "RETIRE": "No valid owner — candidate for retirement",
            "REDESIGN": "Owner unclear — must be reassigned",
            "KEEP": "Valid owner present",
        },
        "params": {
            "require_active_employee": {
                "friendly_label": "Require active employee",
                "type": "boolean",
                "default": True,
                "help_text": (
                    "When on, the owner must be a currently active employee. "
                    "When off, a historical employee record is enough."
                ),
            },
        },
    },
    "rule.redundancy": {
        "business_label": "Detect duplicates / redundancies",
        "description": (
            "Identifies cost centers that are functional duplicates of others "
            "(similar name, same area, similar posting patterns). These can "
            "be merged into a single canonical center (MERGE_MAP)."
        ),
        "decides": ["MERGE_MAP", "KEEP"],
        "verdict_meanings": {
            "MERGE_MAP": "Duplicate — will be merged into another center",
            "KEEP": "Unique — stays",
        },
        "params": {
            "similarity_threshold": {
                "friendly_label": "Similarity threshold",
                "type": "number",
                "default": 0.92,
                "min": 0.5,
                "max": 1.0,
                "step": 0.01,
                "unit": "0..1",
                "help_text": (
                    "How similar must name + attributes be for two centers "
                    "to count as duplicates? 0.92 = very strict (near-"
                    "identical only); 0.75 = loose (more matches but also "
                    "more false positives)."
                ),
            },
        },
    },
    "rule.hierarchy_compliance": {
        "business_label": "Hierarchy compliance",
        "description": (
            "Checks whether a cost center sits in the right place within the "
            "standard hierarchy (e.g. CEMA hierarchy). Centers that hang "
            "loose somewhere or aren't in any standard hierarchy need "
            "redesign before migration."
        ),
        "decides": ["REDESIGN", "KEEP"],
        "verdict_meanings": {
            "REDESIGN": "Not in standard hierarchy — must be re-anchored",
            "KEEP": "Properly anchored",
        },
        "params": {
            "strict_hierarchy_mode": {
                "friendly_label": "Strict hierarchy",
                "type": "boolean",
                "default": False,
                "help_text": (
                    "When on, centers must be exactly anchored in the "
                    "standard hierarchy. Loose (default): tolerant placement "
                    "is allowed."
                ),
            },
        },
    },
    "rule.cross_system_dependency": {
        "business_label": "System dependencies",
        "description": (
            "Identifies cost centers that are referenced by other SAP systems "
            "(e.g. WBS elements, orders, purchase requisitions). These can't "
            "simply be changed — they must at minimum live on as a mapping."
        ),
        "decides": ["MERGE_MAP", "KEEP"],
        "verdict_meanings": {
            "MERGE_MAP": "Has dependencies — must be preserved as a mapping",
            "KEEP": "No external dependencies",
        },
        "params": {},
    },
    # ── Mapping tree (V1) ────────────────────────────────────────────────
    "rule.bs_relevance": {
        "business_label": "Balance sheet relevance",
        "description": (
            "Checks whether the center has balance sheet postings (B/S "
            "accounts). Such centers must be carried forward as cost "
            "centers (CC) in the new model — profit centers alone do not "
            "satisfy B/S reporting requirements."
        ),
        "decides": ["CC", "PC_ONLY"],
        "verdict_meanings": {
            "CC": "B/S relevant — becomes both CC and PC in the new model",
            "PC_ONLY": "P&L only — profit center is sufficient",
        },
        "params": {},
    },
    "rule.has_direct_revenue": {
        "business_label": "Direct revenue",
        "description": (
            "Centers with direct revenue postings must be modeled as profit "
            "centers (PC_ONLY or CC_AND_PC, depending on B/S relevance)."
        ),
        "decides": ["PC_ONLY", "CC_AND_PC"],
        "verdict_meanings": {
            "PC_ONLY": "Direct revenue, no B/S — pure PC",
            "CC_AND_PC": "Direct revenue + B/S — both CC and PC",
        },
        "params": {},
    },
    "rule.collects_project_costs": {
        "business_label": "Project cost collector",
        "description": (
            "Centers acting as project cost collectors are converted to WBS "
            "elements (real or statistical, depending on usage)."
        ),
        "decides": ["WBS_REAL", "WBS_STAT"],
        "verdict_meanings": {
            "WBS_REAL": "Project with real costs — real WBS element",
            "WBS_STAT": "Project for reporting only — statistical WBS",
        },
        "params": {},
    },
    "rule.has_operational_costs": {
        "business_label": "Operational costs",
        "description": (
            "Centers with operational costs (salaries, rent, supplies) are "
            "classic cost centers and remain so in the new model."
        ),
        "decides": ["CC"],
        "verdict_meanings": {"CC": "Operational costs — classic cost center"},
        "params": {},
    },
    "rule.revenue_allocation_vehicle": {
        "business_label": "Revenue allocation vehicle",
        "description": (
            "Centers used to allocate revenue to other centers (allocation "
            "vehicles) can be modeled as pure PCs or replaced entirely."
        ),
        "decides": ["PC_ONLY", "RETIRE"],
        "verdict_meanings": {
            "PC_ONLY": "Allocation vehicle — stays as a PC",
            "RETIRE": "Legacy allocation vehicle — replaceable",
        },
        "params": {},
    },
    "rule.cost_allocation_vehicle": {
        "business_label": "Cost allocation vehicle",
        "description": "Analogous to revenue allocation, but for cost allocation.",
        "decides": ["CC", "RETIRE"],
        "verdict_meanings": {
            "CC": "Cost allocation vehicle — stays as a CC",
            "RETIRE": "Legacy — replaceable",
        },
        "params": {},
    },
    "rule.info_only": {
        "business_label": "Reporting-only",
        "description": (
            "Centers used purely for reporting/statistics (no real postings) "
            "become statistical WBS elements or are retired."
        ),
        "decides": ["WBS_STAT", "RETIRE"],
        "verdict_meanings": {
            "WBS_STAT": "Reporting-only — statistical WBS",
            "RETIRE": "Reporting no longer needed — replaceable",
        },
        "params": {},
    },
    "aggregate.combine_outcomes": {
        "business_label": "Combine outcomes",
        "description": (
            "This step aggregates the verdicts from all preceding rules and "
            "determines the final outcome (KEEP / RETIRE / MERGE_MAP / "
            "REDESIGN). Should sit at the end of the pipeline."
        ),
        "decides": ["KEEP", "RETIRE", "MERGE_MAP", "REDESIGN"],
        "verdict_meanings": {
            "KEEP": "Carried forward unchanged into the new model",
            "RETIRE": "Will be retired",
            "MERGE_MAP": "Will be merged with another center",
            "REDESIGN": "Must be reconceptualised",
        },
        "params": {},
    },
    # ── V2 (CEMA Migration) ──────────────────────────────────────────────
    "v2.retire_flag": {
        "business_label": "RETIRE flag (V2)",
        "description": (
            "V2: Centers whose name matches a configured pattern (e.g. "
            "'_RETIRE') are immediately marked for retirement, bypassing "
            "further analysis steps."
        ),
        "decides": ["RETIRE", "PASS"],
        "verdict_meanings": {
            "RETIRE": "Pattern matched — retire",
            "PASS": "Pattern not matched — continue analysis",
        },
        "params": {
            "retire_pattern": {
                "friendly_label": "Retirement pattern",
                "type": "string",
                "default": "_RETIRE",
                "help_text": (
                    "Substring that must appear in the name for the center "
                    "to be flagged for retirement. Note: case-sensitive."
                ),
            },
        },
    },
    "v2.balance_migrate": {
        "business_label": "Balance sheet migration (V2)",
        "description": (
            "V2: Determines whether a center must be migrated due to its "
            "balance sheet character (vs. PC-only migration)."
        ),
        "decides": ["MIGRATE", "PASS"],
        "verdict_meanings": {
            "MIGRATE": "B/S relevant — must be migrated",
            "PASS": "No mandatory B/S migration",
        },
        "params": {},
    },
    "v2.pc_approach": {
        "business_label": "PC grouping strategy (V2)",
        "description": (
            "V2 — CORE LOGIC: For each center, determines whether it migrates "
            "1:1 (its own PC) or 1:n (multiple CCs share one PC). Grouping "
            "rules access hierarchy levels (e.g. 'all centers under L3 "
            "ROOT/EUROPE/DACH share a common PC')."
        ),
        "decides": ["1:1", "1:n"],
        "verdict_meanings": {
            "1:1": "Own PC per center (classic)",
            "1:n": "Multiple CCs share one PC (canonical SAP m:1)",
        },
        "params": {
            "approach_rules": {
                "friendly_label": "Grouping rules",
                "type": "array",
                "default": [],
                "help_text": (
                    "List of rules that determine which centers are grouped "
                    "together. Each rule has 'match' (which centers are hit?) "
                    "and 'approach' (1:1 or 1:n). Edited via a dedicated UI."
                ),
            },
            "default_approach": {
                "friendly_label": "Default strategy",
                "type": "string",
                "default": "1:1",
                "enum": ["1:1", "1:n"],
                "help_text": (
                    "What happens to centers that no rule matches? "
                    "1:1 is conservative (everyone gets their own PC)."
                ),
            },
        },
    },
    "v2.combine_migration": {
        "business_label": "Build migration result (V2)",
        "description": (
            "V2: Final pipeline step — combines the outputs of the preceding "
            "V2 steps into a final migration plan per center."
        ),
        "decides": ["KEEP", "RETIRE", "MIGRATE"],
        "verdict_meanings": {
            "KEEP": "Stays unchanged",
            "RETIRE": "Will be retired",
            "MIGRATE": "Will be migrated to the new model",
        },
        "params": {},
    },
    # ── ML routines ──────────────────────────────────────────────────────
    "ml.outcome_predictor": {
        "business_label": "ML outcome predictor",
        "description": (
            "Probabilistic alternative to the rule tree. Each numeric/boolean "
            "feature of a center contributes a weighted signal toward each "
            "possible outcome (KEEP / RETIRE / MERGE_MAP / REDESIGN). Returns "
            "a continuous confidence score per outcome instead of a hard "
            "verdict, which makes it useful as a 'second opinion' alongside "
            "the deterministic rules. Also computes a per-center anomaly "
            "score so unusual cases bubble up."
        ),
        "decides": ["KEEP", "RETIRE", "MERGE_MAP", "REDESIGN"],
        "verdict_meanings": {
            "KEEP": "Most likely outcome: stay",
            "RETIRE": "Most likely outcome: retire",
            "MERGE_MAP": "Most likely outcome: merge into another center",
            "REDESIGN": "Most likely outcome: reconceptualise",
        },
        "params": {
            "inactivity_threshold_months": {
                "friendly_label": "Inactivity midpoint",
                "type": "number",
                "default": 12,
                "min": 1,
                "max": 60,
                "unit": "months",
                "help_text": (
                    "Months past which inactivity strongly suggests RETIRE. "
                    "Used as the steepness midpoint of the activity sigmoid."
                ),
            },
            "balance_significance_threshold": {
                "friendly_label": "Material balance threshold",
                "type": "number",
                "default": 10000.0,
                "min": 0,
                "max": 1_000_000,
                "unit": "EUR",
                "help_text": (
                    "Above this value a balance is considered 'material' for "
                    "the model and pushes confidence toward KEEP."
                ),
            },
            "compute_anomaly": {
                "friendly_label": "Compute anomaly score",
                "type": "boolean",
                "default": True,
                "help_text": (
                    "Also produce a per-center anomaly score in [0,1]. Useful "
                    "for sorting reviewer queues by 'most unusual first'."
                ),
            },
        },
    },
    # ── LLM routines ─────────────────────────────────────────────────────
    "llm.advisor": {
        "business_label": "LLM advisor",
        "description": (
            "Asks a configured LLM (e.g. Azure OpenAI) for an independent "
            "opinion on a single center. The LLM sees the same facts as the "
            "rule tree — it does NOT see what the rule tree decided — so its "
            "vote is genuinely independent. Used in comparison mode: where "
            "the rules, the ML model, and the LLM all agree, confidence is "
            "high; where they disagree, the case deserves human review. "
            "Falls back to PASS when no LLM is configured."
        ),
        "decides": ["KEEP", "RETIRE", "MERGE_MAP", "REDESIGN"],
        "verdict_meanings": {
            "KEEP": "LLM advises: keep",
            "RETIRE": "LLM advises: retire",
            "MERGE_MAP": "LLM advises: merge",
            "REDESIGN": "LLM advises: redesign",
            "PASS": "LLM not available or could not parse response",
        },
        "params": {
            "model": {
                "friendly_label": "Model",
                "type": "string",
                "default": "gpt-4o-mini",
                "help_text": "Provider-specific model identifier.",
            },
            "temperature": {
                "friendly_label": "Temperature",
                "type": "number",
                "default": 0.0,
                "min": 0.0,
                "max": 1.0,
                "step": 0.1,
                "help_text": (
                    "0.0 = deterministic. Raise to inject creativity; "
                    "leave at 0 for reproducible verdicts."
                ),
            },
            "max_tokens": {
                "friendly_label": "Max tokens",
                "type": "integer",
                "default": 250,
                "min": 50,
                "max": 2000,
                "unit": "tokens",
                "help_text": "Cap response length — verdicts are short.",
            },
            "skip_if_high_confidence": {
                "friendly_label": "Skip if ML confident ≥",
                "type": "number",
                "default": 0.0,
                "min": 0.0,
                "max": 1.0,
                "step": 0.05,
                "help_text": (
                    "Cost-saver: skip the LLM call entirely if the ML routine "
                    "is already this confident. 0.0 = always call the LLM."
                ),
            },
        },
    },
}


# ── Preset variant templates ─────────────────────────────────────────────
# Pre-built decision tree configs for common scenarios. A user can pick one
# as a starting point and then tweak parameters.

PRESETS_V1: dict[str, dict[str, Any]] = {
    "strict": {
        "label": "Strict — many RETIRE",
        "description": (
            "Aggressive cleanup: low inactivity threshold, strict hierarchy "
            "check. Use when the portfolio should shrink significantly. "
            "Expect many RETIRE proposals that need manual review."
        ),
        "params": {
            "posting_inactivity_threshold": 6,
            "posting_minimal_threshold": 0,
            "similarity_threshold": 0.85,
            "strict_hierarchy_mode": True,
        },
    },
    "standard": {
        "label": "Standard — recommended",
        "description": (
            "Balanced cleanup vs. stability. Default values from the "
            "Implementation Plan §04 spec. Recommended for the first run."
        ),
        "params": {
            "posting_inactivity_threshold": 12,
            "posting_minimal_threshold": 0,
            "similarity_threshold": 0.92,
            "strict_hierarchy_mode": False,
        },
    },
    "lenient": {
        "label": "Lenient — few RETIRE",
        "description": (
            "Conservative: high inactivity threshold, loose duplicate "
            "detection. Use when migration must be maximally cautious and "
            "manual RETIRE review is expensive."
        ),
        "params": {
            "posting_inactivity_threshold": 24,
            "posting_minimal_threshold": 5,
            "similarity_threshold": 0.97,
            "strict_hierarchy_mode": False,
        },
    },
}


PRESETS_V2: dict[str, dict[str, Any]] = {
    "all_one_to_one": {
        "label": "Conservative — 1:1 for all",
        "description": (
            "Every cost center gets its own profit center (1:1). Safe and "
            "simple, but produces many PCs. Use when cost-granularity must "
            "be preserved exactly."
        ),
        "approach_rules": [],
        "default_approach": "1:1",
    },
    "by_level3": {
        "label": "Group by L3",
        "description": (
            "All cost centers under the same level-3 hierarchy node share a "
            "profit center (1:n). Drastically reduces PC count while "
            "preserving reporting granularity at the L3 level."
        ),
        "approach_rules": [
            {"match": {"hier_level": "L3"}, "approach": "1:n"},
        ],
        "default_approach": "1:1",
    },
    "by_country": {
        "label": "Group by country",
        "description": (
            "Cost centers are grouped by country code (`ccode`). Useful for "
            "regional reporting structures."
        ),
        "approach_rules": [
            {"match": {"by_field": "ccode"}, "approach": "1:n"},
        ],
        "default_approach": "1:1",
    },
}


# ── Public API ───────────────────────────────────────────────────────────


def get_rule_metadata(code: str) -> dict[str, Any] | None:
    """Return user-friendly metadata for a routine code, or None."""
    return CATALOG.get(code)


def list_rule_catalog(tree: str | None = None) -> list[dict[str, Any]]:
    """Return the full catalog as a list, optionally filtered by tree.

    Each entry includes:
    - code (technical identifier)
    - business_label, description, decides, verdict_meanings, params
    - tree (cleansing | mapping | None for v2/aggregate)
    - kind (rule | aggregate | ml | llm)
    """
    from app.domain.decision_tree.registry import boot_registry, get_registry

    reg = get_registry()
    if not reg.codes():
        boot_registry()

    out: list[dict[str, Any]] = []
    for routine in reg.list():
        meta = CATALOG.get(routine.code, {})
        if tree is not None and routine.tree != tree and meta.get("tree") != tree:
            continue
        entry = {
            "code": routine.code,
            "name": routine.name,
            "kind": routine.kind,
            "tree": routine.tree,
            # Catalog metadata (with sensible fallbacks)
            "business_label": meta.get("business_label", routine.name),
            "description": meta.get("description", ""),
            "decides": meta.get("decides", []),
            "verdict_meanings": meta.get("verdict_meanings", {}),
            "params": meta.get("params", {}),
            "params_schema": getattr(routine, "params_schema", None),
        }
        out.append(entry)
    return out


def list_presets(engine: str = "v1") -> dict[str, dict[str, Any]]:
    """Return preset templates for the given engine ('v1' or 'v2')."""
    if engine.lower() == "v2":
        return PRESETS_V2
    return PRESETS_V1


def get_preset(engine: str, name: str) -> dict[str, Any] | None:
    return list_presets(engine).get(name)


def build_v1_config_from_preset(preset_name: str) -> dict[str, Any]:
    """Build a full V1 AnalysisConfig.config dict from a preset name."""
    preset = PRESETS_V1.get(preset_name)
    if not preset:
        raise ValueError(f"Unknown V1 preset: {preset_name}")
    p = preset["params"]
    return {
        "pipeline": [
            {
                "routine": "rule.posting_activity",
                "enabled": True,
                "params": {
                    "posting_inactivity_threshold": p["posting_inactivity_threshold"],
                    "posting_minimal_threshold": p["posting_minimal_threshold"],
                },
            },
            {"routine": "rule.ownership", "enabled": True, "params": {}},
            {
                "routine": "rule.redundancy",
                "enabled": True,
                "params": {"similarity_threshold": p["similarity_threshold"]},
            },
            {
                "routine": "rule.hierarchy_compliance",
                "enabled": True,
                "params": {"strict_hierarchy_mode": p["strict_hierarchy_mode"]},
            },
            {"routine": "rule.cross_system_dependency", "enabled": True, "params": {}},
            {"routine": "rule.has_direct_revenue", "enabled": True, "params": {}},
            {"routine": "rule.collects_project_costs", "enabled": True, "params": {}},
            {"routine": "rule.has_operational_costs", "enabled": True, "params": {}},
            {"routine": "rule.revenue_allocation_vehicle", "enabled": True, "params": {}},
            {"routine": "rule.cost_allocation_vehicle", "enabled": True, "params": {}},
            {"routine": "rule.info_only", "enabled": True, "params": {}},
            {"routine": "aggregate.combine_outcomes", "enabled": True, "params": {}},
        ],
        "params": {
            "inactivity_threshold_months": p["posting_inactivity_threshold"],
            "posting_threshold": p["posting_minimal_threshold"],
            "strict_hierarchy_compliance": p["strict_hierarchy_mode"],
        },
        "_preset_origin": preset_name,
    }


def build_v2_config_from_preset(preset_name: str) -> dict[str, Any]:
    """Build a full V2 AnalysisConfig.config dict from a preset name."""
    preset = PRESETS_V2.get(preset_name)
    if not preset:
        raise ValueError(f"Unknown V2 preset: {preset_name}")
    return {
        "version": 2,
        "pipeline": [
            {"routine": "v2.retire_flag", "enabled": True, "params": {"retire_pattern": "_RETIRE"}},
            {"routine": "v2.balance_migrate", "enabled": True, "params": {}},
            {
                "routine": "v2.pc_approach",
                "enabled": True,
                "params": {
                    "approach_rules": preset["approach_rules"],
                    "default_approach": preset["default_approach"],
                },
            },
            {"routine": "v2.combine_migration", "enabled": True, "params": {}},
        ],
        "id_assignment": {
            "pc_prefix": "P",
            "cc_prefix": "C",
            "pc_start": 137,
            "cc_start": 1,
            "id_width": 5,
        },
        "_preset_origin": preset_name,
    }
