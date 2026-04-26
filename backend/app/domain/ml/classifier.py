"""ML classifier for center outcome prediction (§05).

Uses scikit-learn Random Forest with feature engineering from CenterContext.
Falls back to a deterministic heuristic if sklearn is not available.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

# Feature engineering from CenterContext fields
FEATURE_NAMES = [
    "is_active",
    "months_since_last_posting",
    "posting_count_window",
    "bs_amt",
    "opex_amt",
    "rev_amt",
    "hierarchy_depth",
    "name_length",
    "has_responsible",
]


def extract_features(ctx: dict) -> list[float]:
    """Extract numeric features from a center context dict."""
    name = ctx.get("txtsh", "") or ""
    return [
        1.0 if ctx.get("is_active") else 0.0,
        float(ctx.get("months_since_last_posting") or 0),
        float(ctx.get("posting_count_window") or 0),
        float(ctx.get("bs_amt") or 0),
        float(ctx.get("opex_amt") or 0),
        float(ctx.get("rev_amt") or 0),
        float(ctx.get("hierarchy_depth") or 0),
        float(len(name)),
        1.0 if ctx.get("responsible") else 0.0,
    ]


def train_classifier(
    contexts: list[dict],
    labels: list[str],
    model_path: str | None = None,
) -> dict[str, Any]:
    """Train a Random Forest classifier on labeled center data."""
    try:
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import cross_val_score
    except ImportError:
        return {"error": "scikit-learn not installed", "trained": False}

    x_arr = np.array([extract_features(ctx) for ctx in contexts])
    y = np.array(labels)

    clf = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        class_weight="balanced",
    )
    scores = cross_val_score(clf, x_arr, y, cv=min(5, len(set(y))), scoring="accuracy")
    clf.fit(x_arr, y)

    result: dict[str, Any] = {
        "trained": True,
        "n_samples": len(contexts),
        "n_features": len(FEATURE_NAMES),
        "classes": list(clf.classes_),
        "cv_accuracy_mean": round(float(scores.mean()), 4),
        "cv_accuracy_std": round(float(scores.std()), 4),
        "feature_importance": {
            name: round(float(imp), 4)
            for name, imp in zip(FEATURE_NAMES, clf.feature_importances_, strict=False)
        },
    }

    if model_path:
        import pickle

        Path(model_path).parent.mkdir(parents=True, exist_ok=True)
        with open(model_path, "wb") as f:
            pickle.dump(clf, f)  # noqa: S301
        result["model_path"] = model_path

    return result


def predict(
    contexts: list[dict],
    model_path: str | None = None,
) -> list[dict]:
    """Predict outcomes for center contexts."""
    try:
        import importlib

        if importlib.util.find_spec("sklearn") is None:
            raise ImportError("sklearn")
    except ImportError:
        return _heuristic_predict(contexts)

    if model_path and Path(model_path).exists():
        import pickle

        with open(model_path, "rb") as f:
            clf = pickle.load(f)  # noqa: S301
    else:
        return _heuristic_predict(contexts)

    x_arr = np.array([extract_features(ctx) for ctx in contexts])
    predictions = clf.predict(x_arr)
    probabilities = clf.predict_proba(x_arr)

    results = []
    for i, (_ctx, pred) in enumerate(zip(contexts, predictions, strict=False)):
        prob_dict = {
            cls: round(float(p), 4) for cls, p in zip(clf.classes_, probabilities[i], strict=False)
        }
        results.append(
            {
                "prediction": pred,
                "confidence": max(prob_dict.values()),
                "probabilities": prob_dict,
            }
        )

    return results


def explain_prediction(
    ctx: dict,
    model_path: str,
) -> dict:
    """Explain a prediction using feature importances (simplified SHAP)."""
    try:
        import pickle

        with open(model_path, "rb") as f:
            clf = pickle.load(f)  # noqa: S301
    except (ImportError, FileNotFoundError):
        return {"error": "Model not available"}

    features = extract_features(ctx)
    x_arr = np.array([features])
    pred = clf.predict(x_arr)[0]
    proba = clf.predict_proba(x_arr)[0]

    # Simplified feature contribution: importance * feature_value / max
    contributions = {}
    for name, imp, val in zip(FEATURE_NAMES, clf.feature_importances_, features, strict=False):
        contributions[name] = {
            "importance": round(float(imp), 4),
            "value": val,
            "contribution": round(float(imp * abs(val)), 4),
        }

    return {
        "prediction": pred,
        "confidence": round(float(max(proba)), 4),
        "contributions": contributions,
    }


def _heuristic_predict(contexts: list[dict]) -> list[dict]:
    """Fallback heuristic prediction when sklearn is unavailable."""
    results = []
    for ctx in contexts:
        months = ctx.get("months_since_last_posting") or 0
        postings = ctx.get("posting_count_window") or 0
        is_active = ctx.get("is_active", True)

        if not is_active or months >= 24:
            pred, conf = "RETIRE", 0.85
        elif months >= 12 and postings <= 5:
            pred, conf = "RETIRE", 0.7
        elif postings > 100:
            pred, conf = "KEEP", 0.8
        else:
            pred, conf = "KEEP", 0.6

        results.append(
            {
                "prediction": pred,
                "confidence": conf,
                "probabilities": {pred: conf, "other": round(1 - conf, 2)},
            }
        )
    return results
