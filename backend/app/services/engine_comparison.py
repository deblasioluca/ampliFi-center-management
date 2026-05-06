"""Multi-engine comparison service.

Runs three independent analysis engines against the same wave and produces
a side-by-side comparison report:

1. **Decision tree** — the existing rule-based pipeline (V1 cleansing tree).
2. **ML predictor** — the ``ml.outcome_predictor`` routine used standalone.
3. **LLM advisor** — the ``llm.advisor`` routine used standalone.

The output highlights agreement (all three pick the same outcome → high
confidence) and disagreement (the cases worth reviewer time).

Population-level anomaly scores (from ``compute_population_anomalies``) are
also attached so reviewers can sort by "most unusual first".

This service is read-only: it does NOT persist proposals. Persistence is
the job of the existing analysis runners. The comparison is a one-shot
diagnostic tool for analysts to understand where the engines diverge before
committing to one as the production analyser.
"""

from __future__ import annotations

import time
from collections import Counter
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.engine import PipelineEngine
from app.domain.decision_tree.registry import boot_registry, get_registry
from app.domain.decision_tree.routines.ml_outcome_predictor import (
    compute_population_anomalies,
)
from app.models.core import LegacyCostCenter, Wave, WaveEntity
from app.services.analysis import _build_context

logger = structlog.get_logger()


# Default V1 cleansing tree pipeline used as the deterministic baseline.
DEFAULT_TREE_PIPELINE = {
    "pipeline": [
        {"routine": "rule.posting_activity", "enabled": True, "params": {}},
        {"routine": "rule.ownership", "enabled": True, "params": {}},
        {"routine": "rule.redundancy", "enabled": True, "params": {}},
        {"routine": "rule.hierarchy_compliance", "enabled": True, "params": {}},
        {"routine": "rule.cross_system_dependency", "enabled": True, "params": {}},
        {"routine": "rule.has_direct_revenue", "enabled": True, "params": {}},
        {"routine": "rule.has_operational_costs", "enabled": True, "params": {}},
        {"routine": "aggregate.combine_outcomes", "enabled": True, "params": {}},
    ],
}


def _get_wave_centers(
    wave_id: int, db: Session, limit: int | None = None
) -> list[LegacyCostCenter]:
    """Return cost centers in a wave's scope. Reuses the wave-entity link."""
    entity_ids_q = select(WaveEntity.entity_id).where(WaveEntity.wave_id == wave_id)
    query = (
        select(LegacyCostCenter)
        .where(LegacyCostCenter.entity_id.in_(entity_ids_q))
        .order_by(LegacyCostCenter.cctr)
    )
    if limit:
        query = query.limit(limit)
    return list(db.execute(query).scalars().all())


def _final_outcome_from_results(results: list[RoutineResult]) -> tuple[str, float, str]:
    """Extract a single outcome + confidence + reason from a routine result list.

    The aggregate routine (when present) sets the final verdict. Without it
    we take the last non-PASS result.
    """
    # Prefer the aggregate's verdict if it ran.
    for r in reversed(results):
        if r.code.startswith("aggregate.") and r.verdict and r.verdict != "PASS":
            return r.verdict, float(r.score or 1.0), r.reason or r.code
    # Fall back to the last decisive verdict.
    for r in reversed(results):
        if r.verdict and r.verdict not in ("PASS", "ERROR", "UNKNOWN"):
            return r.verdict, float(r.score or 1.0), r.reason or r.code
    return "UNKNOWN", 0.0, "no_decisive_verdict"


def _run_engine_for_center(
    engine_name: str, ctx: CenterContext, pipeline_engine: PipelineEngine
) -> dict[str, Any]:
    """Run a single engine for a single center and summarise its verdict."""
    if engine_name == "tree":
        results = pipeline_engine.execute(DEFAULT_TREE_PIPELINE, ctx)
        verdict, confidence, reason = _final_outcome_from_results(results)
        return {
            "engine": "tree",
            "verdict": verdict,
            "confidence": confidence,
            "reason": reason,
            "rule_path": [f"{r.code}:{r.verdict}" for r in results],
            "comment": None,
        }
    if engine_name == "ml":
        registry = get_registry()
        routine = registry.get("ml.outcome_predictor")
        if routine is None:
            return _missing_engine("ml.outcome_predictor")
        result = routine.run(ctx, {})
        return {
            "engine": "ml",
            "verdict": result.verdict,
            "confidence": float(result.score or 0.0),
            "reason": result.reason,
            "probs": result.payload.get("probs", {}),
            "anomaly": result.payload.get("anomaly"),
            "contributors": result.payload.get("contributors", []),
            "comment": None,
        }
    if engine_name == "llm":
        registry = get_registry()
        routine = registry.get("llm.advisor")
        if routine is None:
            return _missing_engine("llm.advisor")
        result = routine.run(ctx, {})
        return {
            "engine": "llm",
            "verdict": result.verdict,
            "confidence": float(result.score or 0.0),
            "reason": result.reason,
            "comment": result.comment,
            "available": result.payload.get("available", False),
            "model": result.payload.get("model"),
            "tokens": (result.payload.get("tokens_in", 0), result.payload.get("tokens_out", 0)),
        }
    return {
        "engine": engine_name,
        "verdict": "UNKNOWN",
        "confidence": 0.0,
        "reason": "unknown_engine",
    }


def _missing_engine(routine_code: str) -> dict[str, Any]:
    return {
        "engine": routine_code,
        "verdict": "UNKNOWN",
        "confidence": 0.0,
        "reason": f"routine_not_registered:{routine_code}",
        "comment": None,
    }


def compare_engines_on_wave(
    wave_id: int,
    db: Session,
    engines: list[str] | None = None,
    sample_size: int | None = 100,
) -> dict[str, Any]:
    """Run requested engines on the wave's centers and summarise.

    By default this samples ``sample_size`` centers (deterministic order by
    cctr) so the comparison is fast and bounded — full-population runs are
    the job of the persistent analyser. Set ``sample_size=None`` to run
    against every center.

    Engines: subset of {"tree", "ml", "llm"}. Defaults to all three.
    """
    if engines is None:
        engines = ["tree", "ml", "llm"]
    engines = [e for e in engines if e in {"tree", "ml", "llm"}]

    # Make sure built-in routines are loaded.
    registry = get_registry()
    if not registry.codes():
        boot_registry()
    pipeline_engine = PipelineEngine(registry)

    wave = db.get(Wave, wave_id)
    if wave is None:
        raise ValueError(f"Wave {wave_id} not found")

    centers = _get_wave_centers(wave_id, db, limit=sample_size)
    if not centers:
        return {
            "wave_id": wave_id,
            "wave_code": wave.code,
            "engines": engines,
            "sample_size": 0,
            "centers": [],
            "summary": {"per_engine": {}, "agreement": {}, "anomaly_top": []},
            "duration_ms": 0,
        }

    started = time.monotonic()

    # 1. Build all contexts once. Re-used across engines.
    contexts = [_build_context(cc, db) for cc in centers]

    # 2. Population-level anomaly scores (only if ML engine is requested
    #    and the population is large enough for IsolationForest).
    anomaly_scores: dict[int, float] = {}
    if "ml" in engines:
        anomaly_scores = compute_population_anomalies(contexts)

    # 3. Run each engine for each center.
    rows: list[dict[str, Any]] = []
    for ctx, cc in zip(contexts, centers, strict=True):
        row: dict[str, Any] = {
            "center_id": cc.id,
            "cctr": cc.cctr,
            "ccode": cc.ccode,
            "txtsh": cc.txtsh,
            "engines": {},
            "population_anomaly": anomaly_scores.get(cc.id),
        }
        for engine_name in engines:
            row["engines"][engine_name] = _run_engine_for_center(engine_name, ctx, pipeline_engine)

        # Agreement: do all engines pick the same verdict?
        verdicts = {
            r["verdict"] for r in row["engines"].values() if r["verdict"] not in ("PASS", "UNKNOWN")
        }
        if len(verdicts) == 0:
            row["agreement"] = "no_data"
        elif len(verdicts) == 1:
            row["agreement"] = "unanimous"
        elif len(verdicts) <= len(engines) - 1:
            # Two engines agree, one differs (or LLM was unavailable).
            row["agreement"] = "majority"
        else:
            row["agreement"] = "split"
        rows.append(row)

    # 4. Aggregate summary.
    per_engine_counts: dict[str, dict[str, int]] = {}
    for engine_name in engines:
        c = Counter(
            r["engines"][engine_name]["verdict"] for r in rows if engine_name in r["engines"]
        )
        per_engine_counts[engine_name] = dict(c)

    agreement_counts = Counter(r["agreement"] for r in rows)

    anomaly_top = sorted(
        (r for r in rows if r.get("population_anomaly") is not None),
        key=lambda r: r["population_anomaly"],
        reverse=True,
    )[:10]
    anomaly_summary = [
        {
            "center_id": r["center_id"],
            "cctr": r["cctr"],
            "ccode": r["ccode"],
            "txtsh": r["txtsh"],
            "anomaly": r["population_anomaly"],
            "verdicts": {e: r["engines"][e]["verdict"] for e in engines if e in r["engines"]},
        }
        for r in anomaly_top
    ]

    duration_ms = int((time.monotonic() - started) * 1000)

    return {
        "wave_id": wave_id,
        "wave_code": wave.code,
        "wave_name": wave.name,
        "engines": engines,
        "sample_size": len(rows),
        "centers": rows,
        "summary": {
            "per_engine": per_engine_counts,
            "agreement": dict(agreement_counts),
            "anomaly_top": anomaly_summary,
        },
        "duration_ms": duration_ms,
    }
