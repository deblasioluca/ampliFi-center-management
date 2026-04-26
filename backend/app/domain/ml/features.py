"""Feature engineering for ML models (§05.2).

Builds the feature matrix from balance aggregates, posting cadence,
hierarchy attributes, and text embeddings.
"""

from __future__ import annotations

from decimal import Decimal

import structlog
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.models.core import Balance, LegacyCostCenter

logger = structlog.get_logger()


# Standard numeric features expected by the outcome/target classifiers
FEATURE_COLUMNS = [
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


def compute_center_features(
    db: Session, center: LegacyCostCenter, data_window_months: int = 18
) -> dict:
    """Compute all numeric features for a single cost center."""
    # Balance aggregates
    balances = (
        db.execute(
            select(Balance).where(
                Balance.cctr == center.cctr,
                Balance.coarea == center.coarea,
            )
        )
        .scalars()
        .all()
    )

    bs_amt = Decimal(0)
    rev_amt = Decimal(0)
    opex_amt = Decimal(0)
    other_amt = Decimal(0)
    posting_count = 0
    periods_with_postings: set[str] = set()
    monthly_amounts: list[float] = []

    for b in balances:
        tc = b.tc_amt or Decimal(0)
        gc2 = b.gc2_amt or Decimal(0)
        total = tc + gc2
        acls = (b.account_class or "").upper()

        if acls in ("BS", "BALANCE_SHEET"):
            bs_amt += total
        elif acls in ("REV", "REVENUE"):
            rev_amt += total
        elif acls in ("OPEX", "OPERATIONAL"):
            opex_amt += total
        else:
            other_amt += total

        if tc != 0 or gc2 != 0:
            posting_count += 1
            if b.period:
                periods_with_postings.add(str(b.period))

        monthly_amounts.append(float(total))

    # Volatility: std dev of monthly amounts
    volatility = 0.0
    if len(monthly_amounts) > 1:
        mean = sum(monthly_amounts) / len(monthly_amounts)
        variance = sum((x - mean) ** 2 for x in monthly_amounts) / len(monthly_amounts)
        volatility = variance**0.5

    # Months since last posting (approximate)
    months_since_last = data_window_months  # default: no activity

    # Ownership
    has_owner = 1 if center.responsible else 0

    # Hierarchy membership count
    hierarchy_count = 0
    try:
        result = db.execute(
            text("SELECT COUNT(*) FROM cleanup.set_leaf WHERE cctr = :cctr AND coarea = :coarea"),
            {"cctr": center.cctr, "coarea": center.coarea},
        ).scalar()
        hierarchy_count = result or 0
    except Exception:
        logger.debug("ml.hierarchy_count_failed", cctr=center.cctr)

    return {
        "bs_amt": float(bs_amt),
        "rev_amt": float(rev_amt),
        "opex_amt": float(opex_amt),
        "other_amt": float(other_amt),
        "posting_count_window": posting_count,
        "months_active_in_window": len(periods_with_postings),
        "months_since_last_posting": months_since_last,
        "period_count_with_postings": len(periods_with_postings),
        "balance_volatility": volatility,
        "has_owner": has_owner,
        "hierarchy_membership_count": hierarchy_count,
    }


def compute_batch_features(
    db: Session, centers: list[LegacyCostCenter], data_window_months: int = 18
) -> dict[str, dict]:
    """Compute features for all centers in one batch (efficient SQL).

    Returns a dict keyed by cctr with feature dicts as values.
    """
    if not centers:
        return {}

    cctrs = [c.cctr for c in centers]
    coareas = list({c.coarea for c in centers})

    # Batch balance aggregation
    balance_agg = db.execute(
        select(
            Balance.cctr,
            func.sum(Balance.tc_amt).label("total_tc"),
            func.sum(Balance.gc2_amt).label("total_gc2"),
            func.count().label("posting_count"),
            func.count(func.distinct(Balance.period)).label("period_count"),
        )
        .where(Balance.cctr.in_(cctrs), Balance.coarea.in_(coareas))
        .group_by(Balance.cctr)
    ).all()

    balance_map: dict[str, dict] = {}
    for row in balance_agg:
        balance_map[row.cctr] = {
            "total_tc": float(row.total_tc or 0),
            "total_gc2": float(row.total_gc2 or 0),
            "posting_count": row.posting_count or 0,
            "period_count": row.period_count or 0,
        }

    # Batch hierarchy counts
    hier_map: dict[str, int] = {}
    try:
        hier_rows = db.execute(
            text(
                "SELECT cctr, COUNT(*) as cnt FROM cleanup.set_leaf "
                "WHERE cctr = ANY(:cctrs) GROUP BY cctr"
            ),
            {"cctrs": cctrs},
        ).all()
        for row in hier_rows:
            hier_map[row.cctr] = row.cnt
    except Exception:
        logger.debug("ml.batch_hierarchy_failed")

    results: dict[str, dict] = {}
    for c in centers:
        bdata = balance_map.get(c.cctr, {})
        results[c.cctr] = {
            "bs_amt": bdata.get("total_tc", 0.0),
            "rev_amt": 0.0,
            "opex_amt": 0.0,
            "other_amt": bdata.get("total_gc2", 0.0),
            "posting_count_window": bdata.get("posting_count", 0),
            "months_active_in_window": bdata.get("period_count", 0),
            "months_since_last_posting": data_window_months
            if bdata.get("posting_count", 0) == 0
            else 0,
            "period_count_with_postings": bdata.get("period_count", 0),
            "balance_volatility": 0.0,
            "has_owner": 1 if c.responsible else 0,
            "hierarchy_membership_count": hier_map.get(c.cctr, 0),
        }

    return results
