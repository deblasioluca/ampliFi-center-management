"""LLM cost guardrails (§05.9 — cost tracking and limits).

Enforces per-call and daily spending caps. Tracks all LLM usage
for auditing and cost reporting.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = structlog.get_logger()


class CostGuardrail:
    """Enforces LLM spending limits."""

    def __init__(
        self,
        max_cost_per_call: float = 1.0,
        daily_cap_usd: float = 50.0,
        monthly_cap_usd: float = 500.0,
    ) -> None:
        self.max_cost_per_call = max_cost_per_call
        self.daily_cap_usd = daily_cap_usd
        self.monthly_cap_usd = monthly_cap_usd

    def check_pre_call(self, db: Session, estimated_cost: float = 0.0) -> tuple[bool, str]:
        """Check whether we're within budget before making an LLM call."""
        if estimated_cost > self.max_cost_per_call:
            return (
                False,
                f"Estimated cost ${estimated_cost:.4f} exceeds "
                f"per-call limit ${self.max_cost_per_call:.2f}",
            )

        daily_spend = self._get_daily_spend(db)
        if daily_spend + estimated_cost > self.daily_cap_usd:
            return (
                False,
                f"Daily spend ${daily_spend:.2f} + ${estimated_cost:.4f} "
                f"would exceed daily cap ${self.daily_cap_usd:.2f}",
            )

        monthly_spend = self._get_monthly_spend(db)
        if monthly_spend + estimated_cost > self.monthly_cap_usd:
            return (
                False,
                f"Monthly spend ${monthly_spend:.2f} would exceed "
                f"monthly cap ${self.monthly_cap_usd:.2f}",
            )

        return True, "OK"

    def record_usage(
        self,
        db: Session,
        *,
        model: str,
        provider: str,
        tokens_in: int,
        tokens_out: int,
        cost_usd: float,
        mode: str,
        run_id: int | None = None,
        center_cctr: str | None = None,
        prompt_hash: str | None = None,
    ) -> None:
        """Record LLM usage for cost tracking and auditing."""
        db.execute(
            text(
                """
                INSERT INTO cleanup.llm_usage_log
                    (model, provider, tokens_in, tokens_out, cost_usd, mode,
                     run_id, center_cctr, prompt_hash, created_at)
                VALUES
                    (:model, :provider, :tokens_in, :tokens_out, :cost_usd, :mode,
                     :run_id, :center_cctr, :prompt_hash, :created_at)
                """
            ),
            {
                "model": model,
                "provider": provider,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "cost_usd": cost_usd,
                "mode": mode,
                "run_id": run_id,
                "center_cctr": center_cctr,
                "prompt_hash": prompt_hash,
                "created_at": datetime.now(UTC),
            },
        )

    def _get_daily_spend(self, db: Session) -> float:
        today_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        try:
            result = db.execute(
                text(
                    "SELECT COALESCE(SUM(cost_usd), 0) "
                    "FROM cleanup.llm_usage_log "
                    "WHERE created_at >= :since"
                ),
                {"since": today_start},
            ).scalar()
            return float(result or 0)
        except Exception:
            return 0.0

    def _get_monthly_spend(self, db: Session) -> float:
        month_start = datetime.now(UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        try:
            result = db.execute(
                text(
                    "SELECT COALESCE(SUM(cost_usd), 0) "
                    "FROM cleanup.llm_usage_log "
                    "WHERE created_at >= :since"
                ),
                {"since": month_start},
            ).scalar()
            return float(result or 0)
        except Exception:
            return 0.0

    def get_usage_summary(self, db: Session) -> dict:
        """Get usage summary for dashboard display."""
        now = datetime.now(UTC)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        try:
            daily = db.execute(
                text(
                    "SELECT COALESCE(SUM(cost_usd), 0), "
                    "COALESCE(SUM(tokens_in+tokens_out), 0), "
                    "COUNT(*) FROM cleanup.llm_usage_log "
                    "WHERE created_at >= :since"
                ),
                {"since": today_start},
            ).one()
            monthly = db.execute(
                text(
                    "SELECT COALESCE(SUM(cost_usd), 0), "
                    "COALESCE(SUM(tokens_in+tokens_out), 0), "
                    "COUNT(*) FROM cleanup.llm_usage_log "
                    "WHERE created_at >= :since"
                ),
                {"since": month_start},
            ).one()
            return {
                "daily": {
                    "cost_usd": float(daily[0]),
                    "tokens": int(daily[1]),
                    "calls": int(daily[2]),
                    "cap_usd": self.daily_cap_usd,
                },
                "monthly": {
                    "cost_usd": float(monthly[0]),
                    "tokens": int(monthly[1]),
                    "calls": int(monthly[2]),
                    "cap_usd": self.monthly_cap_usd,
                },
                "per_call_limit_usd": self.max_cost_per_call,
            }
        except Exception:
            return {
                "daily": {"cost_usd": 0, "tokens": 0, "calls": 0, "cap_usd": self.daily_cap_usd},
                "monthly": {
                    "cost_usd": 0,
                    "tokens": 0,
                    "calls": 0,
                    "cap_usd": self.monthly_cap_usd,
                },
                "per_call_limit_usd": self.max_cost_per_call,
            }
