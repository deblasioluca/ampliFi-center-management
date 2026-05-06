"""ML routine: outcome predictor with confidence + anomaly detection.

This routine produces a probabilistic verdict per cost center using two
techniques in parallel:

1. **Feature-based scoring** — each numeric feature contributes a weighted
   signal toward each possible outcome (KEEP / RETIRE / MERGE_MAP / REDESIGN).
   Weights are calibrated against the rule-based logic so the ML output
   correlates with what the deterministic tree produces, but with a
   continuous confidence score instead of a hard verdict.

2. **Anomaly detection** — IsolationForest fit on-the-fly on the population
   of centers being analysed. Centers with high anomaly scores often
   correspond to unusual cases worth manual review.

Why this design vs. a trained supervised model? Two reasons:
- No labelled training data exists yet (we're producing the labels here).
- It runs deterministically on a single center context, which fits the
  routine protocol cleanly. Once labelled history accumulates, this can
  be replaced with a real trained classifier loaded from a model file.

The routine populates ``ctx.ml_outcome_probs`` and ``ctx.ml_anomaly_score``
on the context so that downstream routines (especially aggregators) can
read it. It also writes a ``score`` and rich ``payload`` on its own
RoutineResult so the comparison page can show a per-class probability bar.
"""

from __future__ import annotations

import math
from typing import Any

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.registry import register_routine


def _sigmoid(x: float) -> float:
    """Squash any real number into (0, 1)."""
    if x > 50:
        return 1.0
    if x < -50:
        return 0.0
    return 1.0 / (1.0 + math.exp(-x))


def _softmax(scores: dict[str, float]) -> dict[str, float]:
    """Normalise a dict of logits into a probability distribution."""
    if not scores:
        return {}
    max_score = max(scores.values())
    exps = {k: math.exp(v - max_score) for k, v in scores.items()}
    total = sum(exps.values()) or 1.0
    return {k: v / total for k, v in exps.items()}


@register_routine
class MLOutcomePredictor:
    """Probabilistic outcome predictor using feature-based scoring.

    Returns a result whose ``verdict`` is the most likely outcome and whose
    ``score`` is the confidence (= max probability). The full distribution
    is stored in ``payload['probs']`` for use by the comparison UI.
    """

    @property
    def code(self) -> str:
        return "ml.outcome_predictor"

    @property
    def name(self) -> str:
        return "ML Outcome Predictor"

    @property
    def kind(self) -> str:
        return "ml"

    @property
    def tree(self) -> str | None:
        return None  # Cross-tree — works on cleansing AND mapping

    @property
    def params_schema(self) -> dict | None:
        return {
            "type": "object",
            "properties": {
                "inactivity_threshold_months": {
                    "type": "number",
                    "default": 12,
                    "description": (
                        "Months past which inactivity strongly suggests RETIRE. "
                        "Used as the steepness midpoint of the activity sigmoid."
                    ),
                },
                "balance_significance_threshold": {
                    "type": "number",
                    "default": 10000.0,
                    "description": (
                        "EUR threshold above which a balance is 'material'. "
                        "Used to log-normalise the balance feature."
                    ),
                },
                "compute_anomaly": {
                    "type": "boolean",
                    "default": True,
                    "description": "Also compute an anomaly score (slower for large populations).",
                },
            },
        }

    # ── Feature scoring ──────────────────────────────────────────────────

    def _score_outcomes(self, ctx: CenterContext, params: dict) -> dict[str, float]:
        """Compute logits for KEEP / RETIRE / MERGE_MAP / REDESIGN.

        Higher logit = stronger evidence for that outcome. The actual
        probabilities come from softmax over these.
        """
        inactivity_pivot = float(params.get("inactivity_threshold_months", 12))
        balance_pivot = float(params.get("balance_significance_threshold", 10000.0))

        months_inactive = ctx.months_since_last_posting or 0
        posting_count = ctx.posting_count_window or 0

        # Activity signal: positive when inactive, negative when active.
        # Centred at the configured threshold.
        activity_signal = (months_inactive - inactivity_pivot) / max(inactivity_pivot, 1.0)

        # Material balance signal: log-scaled magnitude of total balance,
        # signed by sign of bs_amt+rev_amt+opex_amt.
        total_abs = abs(ctx.bs_amt) + abs(ctx.rev_amt) + abs(ctx.opex_amt)
        balance_signal = math.log1p(total_abs / balance_pivot)

        # ── Logits per outcome ───────────────────────────────────────────
        # RETIRE: rewarded by inactivity, lack of dependencies, no owner,
        # no postings. Penalised by material balance / external dependencies.
        retire_logit = (
            +1.4 * activity_signal
            + (1.2 if not ctx.is_active else 0.0)
            + (0.9 if not ctx.has_owner else -0.4)
            + (-0.6 if posting_count > 5 else 0.3)
            + (-1.5 if ctx.in_bw_extractors or ctx.in_grc or ctx.in_intercompany else 0.0)
            + (-1.0 * balance_signal if total_abs > balance_pivot else 0.0)
        )

        # MERGE_MAP: rewarded by being part of a duplicate cluster, by
        # external dependencies (must persist as a mapping), small balance.
        merge_logit = (
            +2.5 * (ctx.duplicate_cluster_size > 1)
            + (0.8 if ctx.in_bw_extractors or ctx.in_grc or ctx.in_intercompany else 0.0)
            + (-0.4 * activity_signal)
            + (0.3 if ctx.is_allocation_vehicle else 0.0)
        )

        # REDESIGN: rewarded by hierarchy non-compliance (no anchoring),
        # ownership issues despite activity, project-related but no owner.
        redesign_logit = (
            +1.2 * (ctx.hierarchy_membership_count == 0)
            + (1.0 if ctx.is_project_related and not ctx.has_owner else 0.0)
            + (0.5 if not ctx.has_owner and ctx.is_active else 0.0)
            + (-0.6 * activity_signal)  # very inactive → prefer RETIRE
        )

        # KEEP: the residual — rewarded by activity, owner, anchoring,
        # material activity. This is the default for "healthy" centers.
        keep_logit = (
            -1.4 * activity_signal  # inverse of retire
            + (0.8 if ctx.has_owner else -0.5)
            + (0.6 if ctx.hierarchy_membership_count > 0 else -0.5)
            + (0.4 if posting_count > 5 else 0.0)
            + (0.5 * balance_signal if total_abs > balance_pivot else 0.0)
            + (-1.5 if ctx.duplicate_cluster_size > 1 else 0.0)  # duplicates → MERGE
        )

        return {
            "KEEP": keep_logit,
            "RETIRE": retire_logit,
            "MERGE_MAP": merge_logit,
            "REDESIGN": redesign_logit,
        }

    # ── Anomaly scoring ──────────────────────────────────────────────────

    def _anomaly_score(self, ctx: CenterContext) -> float:
        """Cheap per-center anomaly proxy without needing the population.

        For a true IsolationForest we'd need the population at scoring time.
        That requires a population-aware aggregator, which complicates the
        single-context routine protocol. As a reasonable proxy we use:

        - Distance from "typical center" (active, has owner, in hierarchy,
          recent postings, small balance, 0 dependencies)

        A real IsolationForest hook is wired in
        :func:`compute_population_anomalies` below for use by the
        comparison endpoint where we have all centers in memory.
        """
        z = 0.0
        if ctx.months_since_last_posting and ctx.months_since_last_posting > 36:
            z += min((ctx.months_since_last_posting - 36) / 12, 3.0)
        if not ctx.has_owner:
            z += 0.5
        if ctx.hierarchy_membership_count == 0:
            z += 0.5
        total_abs = abs(ctx.bs_amt) + abs(ctx.rev_amt) + abs(ctx.opex_amt)
        if total_abs > 1_000_000:
            z += min(math.log10(total_abs / 1_000_000), 2.0)
        if ctx.in_bw_extractors and ctx.in_grc and ctx.in_intercompany:
            z += 1.0
        return _sigmoid(z - 1.5)  # roughly 0.0–0.95

    # ── Run ──────────────────────────────────────────────────────────────

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        logits = self._score_outcomes(ctx, params)
        probs = _softmax(logits)
        verdict = max(probs, key=probs.get)
        confidence = probs[verdict]

        anomaly = None
        if params.get("compute_anomaly", True):
            anomaly = self._anomaly_score(ctx)
            ctx.ml_anomaly_score = anomaly

        # Persist back to context for downstream routines.
        ctx.ml_outcome_probs = probs

        # Build a human-readable reason from the top contributing features.
        contributors: list[tuple[str, str]] = []
        if (ctx.months_since_last_posting or 0) > params.get("inactivity_threshold_months", 12):
            contributors.append(("inactive", f"{ctx.months_since_last_posting}mo no postings"))
        if not ctx.has_owner:
            contributors.append(("no_owner", "no responsible owner"))
        if ctx.hierarchy_membership_count == 0:
            contributors.append(("hier_orphan", "no hierarchy anchor"))
        if ctx.duplicate_cluster_size > 1:
            contributors.append(("duplicate", f"in cluster of {ctx.duplicate_cluster_size}"))
        if ctx.in_bw_extractors or ctx.in_grc or ctx.in_intercompany:
            deps = [
                k
                for k, v in [
                    ("BW", ctx.in_bw_extractors),
                    ("GRC", ctx.in_grc),
                    ("IC", ctx.in_intercompany),
                ]
                if v
            ]
            contributors.append(("external_deps", "+".join(deps)))
        reason_text = (
            "ml.predicted:"
            + verdict.lower()
            + (" (" + ",".join(c[0] for c in contributors) + ")" if contributors else "")
        )

        payload: dict[str, Any] = {
            "probs": probs,
            "logits": logits,
            "anomaly": anomaly,
            "contributors": [{"code": c[0], "explain": c[1]} for c in contributors],
            "engine": "ml",
        }

        return RoutineResult(
            code=self.code,
            verdict=verdict,
            score=confidence,
            payload=payload,
            reason=reason_text,
        )


# ── Population-aware anomaly helper (called from the comparison endpoint) ──


def compute_population_anomalies(contexts: list[CenterContext]) -> dict[int, float]:
    """Run IsolationForest over the entire population of contexts.

    Returns a dict keyed by ``center_id`` → anomaly score in [0, 1].
    Returns an empty dict if scikit-learn is not available or the population
    is too small (< 10 centers).
    """
    if len(contexts) < 10:
        return {}

    try:
        import numpy as np
        from sklearn.ensemble import IsolationForest
    except ImportError:
        return {}

    feature_dicts = [c.as_feature_dict() for c in contexts]
    keys = sorted({k for d in feature_dicts for k in d})
    matrix = np.array([[float(d.get(k, 0)) for k in keys] for d in feature_dicts])

    iso = IsolationForest(
        n_estimators=100,
        contamination="auto",
        random_state=42,
        n_jobs=1,
    )
    iso.fit(matrix)
    raw = -iso.score_samples(matrix)
    if raw.size == 0:
        return {}
    lo, hi = float(raw.min()), float(raw.max())
    span = hi - lo if hi > lo else 1.0
    normalised = (raw - lo) / span
    return {ctx.center_id: float(normalised[i]) for i, ctx in enumerate(contexts)}
