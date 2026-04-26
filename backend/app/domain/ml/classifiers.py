"""ML classifiers for outcome and target prediction (§05.3).

LightGBM-based classifiers with SHAP explainability. Falls back to
heuristic predictions when no trained model is available.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger()

MODELS_DIR = Path(__file__).parent.parent.parent.parent / "models"

OUTCOME_CLASSES = ["KEEP", "RETIRE", "MERGE_MAP", "REDESIGN"]
TARGET_CLASSES = ["CC", "PC", "CC_AND_PC", "PC_ONLY", "WBS_REAL", "WBS_STAT", "NONE"]
NAMING_CLASSES = [
    "operational",
    "technical",
    "project",
    "statistical",
    "allocation_vehicle",
    "unknown",
]


class BaseClassifier:
    """Base ML classifier with lazy model loading and SHAP support."""

    def __init__(self, model_name: str, classes: list[str]) -> None:
        self.model_name = model_name
        self.classes = classes
        self._model = None
        self._explainer = None

    def _model_path(self, version: str = "latest") -> Path:
        return MODELS_DIR / self.model_name / version / "model.txt"

    def load(self, version: str = "latest") -> bool:
        """Load a trained LightGBM model. Returns False if not available."""
        path = self._model_path(version)
        if not path.exists():
            logger.info("ml.model_not_found", model=self.model_name, version=version)
            return False
        try:
            import lightgbm as lgb

            self._model = lgb.Booster(model_file=str(path))
            logger.info("ml.model_loaded", model=self.model_name, version=version)
            return True
        except ImportError:
            logger.warning("ml.lightgbm_not_installed")
            return False
        except Exception as e:
            logger.error("ml.load_error", model=self.model_name, error=str(e))
            return False

    @property
    def is_loaded(self) -> bool:
        return self._model is not None

    def predict_proba(self, features: dict) -> dict[str, float]:
        """Predict class probabilities from feature dict."""
        if self._model is not None:
            return self._predict_with_model(features)
        return self._predict_heuristic(features)

    def _predict_with_model(self, features: dict) -> dict[str, float]:
        """Predict using trained LightGBM model."""
        feature_values = [features.get(f, 0.0) for f in self._expected_features()]
        arr = np.array([feature_values])
        proba = self._model.predict(arr)[0]
        if len(proba.shape) == 1 and proba.shape[0] == len(self.classes):
            return dict(zip(self.classes, [float(p) for p in proba], strict=False))
        return self._predict_heuristic(features)

    def _predict_heuristic(self, features: dict) -> dict[str, float]:
        """Heuristic fallback when no model is loaded."""
        raise NotImplementedError

    def _expected_features(self) -> list[str]:
        return [
            "bs_amt",
            "rev_amt",
            "opex_amt",
            "other_amt",
            "posting_count_window",
            "months_active_in_window",
            "months_since_last_posting",
            "period_count_with_postings",
            "balance_volatility",
            "has_owner",
            "hierarchy_membership_count",
        ]

    def explain(self, features: dict, top_k: int = 5) -> list[dict]:
        """Return top-K SHAP feature contributions."""
        if self._model is not None:
            return self._explain_with_shap(features, top_k)
        return self._explain_heuristic(features, top_k)

    def _explain_with_shap(self, features: dict, top_k: int) -> list[dict]:
        """SHAP explanation using TreeExplainer."""
        try:
            import shap

            if self._explainer is None:
                self._explainer = shap.TreeExplainer(self._model)
            feature_names = self._expected_features()
            feature_values = np.array([[features.get(f, 0.0) for f in feature_names]])
            shap_values = self._explainer.shap_values(feature_values)

            # For multiclass, take the predicted class's SHAP values
            if isinstance(shap_values, list):
                proba = self.predict_proba(features)
                best_class = max(proba, key=proba.get)
                class_idx = self.classes.index(best_class)
                sv = shap_values[class_idx][0]
            else:
                sv = shap_values[0]

            # Sort by absolute magnitude
            pairs = list(zip(feature_names, sv, strict=False))
            pairs.sort(key=lambda x: abs(x[1]), reverse=True)
            return [{"feature": f, "shap_value": float(v)} for f, v in pairs[:top_k]]
        except ImportError:
            return self._explain_heuristic(features, top_k)
        except Exception as e:
            logger.warning("ml.shap_error", error=str(e))
            return self._explain_heuristic(features, top_k)

    def _explain_heuristic(self, features: dict, top_k: int) -> list[dict]:
        """Simple heuristic explanation based on feature magnitudes."""
        items = [(k, abs(v)) for k, v in features.items() if isinstance(v, (int, float))]
        items.sort(key=lambda x: x[1], reverse=True)
        return [{"feature": f, "shap_value": v} for f, v in items[:top_k]]

    def save(self, version: str = "latest") -> None:
        """Save the trained model."""
        if self._model is None:
            return
        path = self._model_path(version)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._model.save_model(str(path))
        logger.info("ml.model_saved", model=self.model_name, version=version)


class OutcomeClassifier(BaseClassifier):
    """LightGBM classifier for cleansing outcome (KEEP/RETIRE/MERGE_MAP/REDESIGN)."""

    def __init__(self) -> None:
        super().__init__("outcome_classifier", OUTCOME_CLASSES)

    def _predict_heuristic(self, features: dict) -> dict[str, float]:
        """Heuristic: use posting activity and balance to estimate outcome."""
        posting = features.get("posting_count_window", 0)
        balance = abs(features.get("bs_amt", 0)) + abs(features.get("opex_amt", 0))
        months_inactive = features.get("months_since_last_posting", 18)
        hier_count = features.get("hierarchy_membership_count", 1)

        # Simple heuristic scoring
        keep_score = 0.0
        retire_score = 0.0
        merge_score = 0.0
        redesign_score = 0.0

        if posting > 0 and months_inactive < 6:
            keep_score += 0.5
        elif months_inactive >= 12:
            retire_score += 0.5
        else:
            keep_score += 0.2
            retire_score += 0.2

        if balance > 100000:
            keep_score += 0.2
        elif balance < 100:
            retire_score += 0.2

        if hier_count > 1:
            merge_score += 0.3

        if not features.get("has_owner", 0):
            retire_score += 0.1
            redesign_score += 0.1

        # Normalize
        total = keep_score + retire_score + merge_score + redesign_score
        if total == 0:
            return {"KEEP": 0.25, "RETIRE": 0.25, "MERGE_MAP": 0.25, "REDESIGN": 0.25}
        return {
            "KEEP": keep_score / total,
            "RETIRE": retire_score / total,
            "MERGE_MAP": merge_score / total,
            "REDESIGN": redesign_score / total,
        }

    def train(self, x_train: Any, y: Any, params: dict | None = None) -> None:  # noqa: N803
        """Train the outcome classifier."""
        try:
            import lightgbm as lgb

            default_params = {
                "objective": "multiclass",
                "num_class": len(self.classes),
                "metric": "multi_logloss",
                "verbosity": -1,
                "num_threads": 4,
                "learning_rate": 0.05,
                "num_leaves": 63,
                "max_depth": 8,
                "min_data_in_leaf": 20,
            }
            if params:
                default_params.update(params)

            dtrain = lgb.Dataset(x_train, label=y)
            self._model = lgb.train(default_params, dtrain, num_boost_round=200)
            logger.info("ml.trained", model=self.model_name, samples=len(y))
        except ImportError:
            logger.warning("ml.lightgbm_not_installed")


class TargetObjectClassifier(BaseClassifier):
    """LightGBM classifier for target object type."""

    def __init__(self) -> None:
        super().__init__("target_object_classifier", TARGET_CLASSES)

    def _predict_heuristic(self, features: dict) -> dict[str, float]:
        """Heuristic: estimate target object based on balance composition."""
        bs = abs(features.get("bs_amt", 0))
        rev = abs(features.get("rev_amt", 0))
        opex = abs(features.get("opex_amt", 0))
        total = bs + rev + opex + 1e-9

        cc_score = opex / total * 0.5 + 0.2
        pc_score = rev / total * 0.5 + 0.1
        cc_and_pc_score = 0.1
        none_score = 0.05

        total_score = cc_score + pc_score + cc_and_pc_score + none_score + 0.05 * 3
        return {
            "CC": cc_score / total_score,
            "PC": pc_score / total_score,
            "CC_AND_PC": cc_and_pc_score / total_score,
            "PC_ONLY": 0.05 / total_score,
            "WBS_REAL": 0.05 / total_score,
            "WBS_STAT": 0.05 / total_score,
            "NONE": none_score / total_score,
        }


class AnomalyDetector:
    """IsolationForest-based anomaly detector for housekeeping (§08)."""

    def __init__(self) -> None:
        self.model_name = "anomaly_detector"
        self._model = None

    def score(self, features: dict) -> float:
        """Return anomaly score 0..1 (higher = more anomalous)."""
        if self._model is not None:
            return self._score_with_model(features)
        return self._score_heuristic(features)

    def _score_with_model(self, features: dict) -> float:
        feature_values = np.array(
            [
                [
                    features.get(f, 0.0)
                    for f in [
                        "bs_amt",
                        "opex_amt",
                        "posting_count_window",
                        "months_since_last_posting",
                        "balance_volatility",
                    ]
                ]
            ]
        )
        raw = self._model.decision_function(feature_values)[0]
        # IsolationForest: more negative = more anomalous
        return float(max(0.0, min(1.0, 0.5 - raw)))

    def _score_heuristic(self, features: dict) -> float:
        """Simple anomaly heuristic based on extreme values."""
        score = 0.0
        if features.get("months_since_last_posting", 0) > 12:
            score += 0.3
        if features.get("posting_count_window", 0) == 0:
            score += 0.3
        if not features.get("has_owner", 0):
            score += 0.2
        if abs(features.get("balance_volatility", 0)) > 100000:
            score += 0.2
        return min(1.0, score)

    def train(self, x_train: Any) -> None:  # noqa: N803
        """Train the anomaly detector."""
        try:
            from sklearn.ensemble import IsolationForest

            self._model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
            self._model.fit(x_train)
            logger.info("ml.trained", model=self.model_name, samples=len(x_train))
        except ImportError:
            logger.warning("ml.sklearn_not_installed")
