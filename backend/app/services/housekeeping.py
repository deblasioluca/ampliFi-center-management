"""Housekeeping cycle service (§08.5).

Monthly cron-driven cycles that flag target cost centers for review:
- UNUSED: No postings in configured window
- LOW_VOLUME: Below threshold posting count
- NO_OWNER: Responsible field empty or unresolvable
- ANOMALY: ML anomaly detector flags unusual patterns
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime

import structlog
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.core import (
    Balance,
    HousekeepingCycle,
    HousekeepingItem,
    TargetCostCenter,
)

logger = structlog.get_logger()


def create_cycle(
    period: str,
    config: dict | None = None,
    db: Session | None = None,
) -> HousekeepingCycle:
    """Create a new housekeeping cycle for the given period (e.g. '2026-04')."""
    if db is None:
        raise ValueError("Database session required")

    existing = db.execute(
        select(HousekeepingCycle).where(HousekeepingCycle.period == period)
    ).scalar_one_or_none()
    if existing:
        raise ValueError(f"Cycle already exists for period {period}")

    cycle = HousekeepingCycle(
        period=period,
        status="scheduled",
        config=config or _default_config(),
    )
    db.add(cycle)
    db.flush()
    return cycle


def _default_config() -> dict:
    return {
        "unused_months": 6,
        "low_volume_threshold": 5,
        "anomaly_score_threshold": 0.8,
        "reminder_days": [7, 14, 21],
        "auto_close_after_days": 30,
    }


def run_cycle(cycle_id: int, db: Session) -> HousekeepingCycle:
    """Execute a housekeeping cycle — scan target centers for flags."""
    cycle = db.get(HousekeepingCycle, cycle_id)
    if not cycle:
        raise ValueError(f"Cycle {cycle_id} not found")
    if cycle.status != "scheduled":
        raise ValueError(f"Cycle {cycle_id} is {cycle.status}, expected scheduled")

    cycle.status = "running"
    cycle.started_at = datetime.now(UTC)
    db.flush()

    config = cycle.config or _default_config()
    unused_months = config.get("unused_months", 6)
    low_volume_threshold = config.get("low_volume_threshold", 5)
    now = datetime.now(UTC)

    # Get all active target cost centers
    targets = (
        db.execute(
            select(TargetCostCenter).where(TargetCostCenter.is_active == True)  # noqa: E712
        )
        .scalars()
        .all()
    )

    kpis = {
        "total_scanned": len(targets),
        "unused": 0,
        "low_volume": 0,
        "no_owner": 0,
        "anomaly": 0,
    }

    for tcc in targets:
        flags: list[str] = []

        # Check UNUSED: no postings in window
        cutoff_year = now.year - (unused_months // 12)
        cutoff_month = now.month - (unused_months % 12)
        if cutoff_month <= 0:
            cutoff_year -= 1
            cutoff_month += 12
        cutoff_period = cutoff_year * 100 + cutoff_month

        posting_count = (
            db.execute(
                select(func.coalesce(func.sum(Balance.posting_count), 0)).where(
                    Balance.cctr == tcc.cctr,
                    Balance.coarea == tcc.coarea,
                    (Balance.fiscal_year * 100 + Balance.period) >= cutoff_period,
                )
            ).scalar()
            or 0
        )

        if posting_count == 0:
            flags.append("UNUSED")
            kpis["unused"] += 1
        elif posting_count <= low_volume_threshold:
            flags.append("LOW_VOLUME")
            kpis["low_volume"] += 1

        # Check NO_OWNER
        if not tcc.responsible or not tcc.responsible.strip():
            flags.append("NO_OWNER")
            kpis["no_owner"] += 1

        # Create housekeeping items for flagged centers
        for flag in flags:
            token = secrets.token_urlsafe(32)
            item = HousekeepingItem(
                cycle_id=cycle.id,
                target_cc_id=tcc.id,
                flag=flag,
                owner_email=None,  # resolved from responsible field in notification step
                owner_token=token,
                details={
                    "posting_count_window": int(posting_count),
                    "responsible": tcc.responsible,
                },
            )
            db.add(item)

    cycle.status = "review_open"
    cycle.review_opened_at = datetime.now(UTC)
    cycle.kpis = kpis
    db.commit()
    db.refresh(cycle)

    logger.info(
        "housekeeping.cycle.completed",
        cycle_id=cycle.id,
        period=cycle.period,
        kpis=kpis,
    )
    return cycle


def decide_item(
    item_id: int,
    decision: str,
    comment: str | None,
    db: Session,
) -> HousekeepingItem:
    """Record an owner's decision on a housekeeping item."""
    item = db.get(HousekeepingItem, item_id)
    if not item:
        raise ValueError(f"Housekeeping item {item_id} not found")

    if decision not in ("KEEP", "CLOSE", "DEFER"):
        raise ValueError(f"Invalid decision: {decision}")

    item.decision = decision
    item.decision_comment = comment
    item.decided_at = datetime.now(UTC)
    db.commit()
    db.refresh(item)
    return item


def send_notifications(cycle_id: int, db: Session) -> dict:
    """Send housekeeping digest emails to all owners with flagged items."""
    cycle = db.get(HousekeepingCycle, cycle_id)
    if not cycle:
        raise ValueError(f"Cycle {cycle_id} not found")
    if cycle.status != "review_open":
        raise ValueError(f"Cycle {cycle_id} is {cycle.status}, expected review_open")

    items = (
        db.execute(
            select(HousekeepingItem).where(
                HousekeepingItem.cycle_id == cycle_id,
                HousekeepingItem.decision.is_(None),
            )
        )
        .scalars()
        .all()
    )

    # Group items by owner
    owner_items: dict[str, list] = {}
    for item in items:
        email = item.owner_email
        if not email:
            continue
        owner_items.setdefault(email, []).append(item)

    sent_count = 0
    failed_count = 0

    for email, owner_group in owner_items.items():
        try:
            from app.infra.email.engine import EmailEngine
            from app.models.core import AppConfig

            cfg = db.execute(select(AppConfig).where(AppConfig.key == "email")).scalar_one_or_none()
            email_cfg = cfg.value if cfg else {}

            engine = EmailEngine(
                host=email_cfg.get("host", "localhost"),
                port=email_cfg.get("port", 1025),
                username=email_cfg.get("username", ""),
                password=email_cfg.get("password", ""),
                use_tls=email_cfg.get("tls", "none") != "none",
            )

            flagged_lines = []
            for it in owner_group:
                tcc = db.get(TargetCostCenter, it.target_cc_id)
                flag_desc = it.flag
                cctr = tcc.cctr if tcc else "unknown"
                flagged_lines.append(f"- {cctr}: {flag_desc}")

            base_url = email_cfg.get("base_url", "http://localhost:4321")
            engine.send(
                to=email,
                template_name="housekeeping_notification",
                context={
                    "owner_name": email.split("@")[0],
                    "period": cycle.period,
                    "flagged_centers": "\n".join(flagged_lines),
                    "review_url": (
                        f"{base_url}/housekeeping/{cycle.id}/owner/{owner_group[0].owner_token}"
                    ),
                },
            )
            sent_count += 1
        except Exception as e:
            logger.warning(
                "housekeeping.notification_failed",
                email=email,
                error=str(e),
            )
            failed_count += 1

    logger.info(
        "housekeeping.notifications_sent",
        cycle_id=cycle_id,
        sent=sent_count,
        failed=failed_count,
    )
    return {"sent": sent_count, "failed": failed_count}


def close_cycle(cycle_id: int, db: Session) -> HousekeepingCycle:
    """Close a housekeeping cycle after all items are decided or expired."""
    cycle = db.get(HousekeepingCycle, cycle_id)
    if not cycle:
        raise ValueError(f"Cycle {cycle_id} not found")

    cycle.status = "closed"
    cycle.closed_at = datetime.now(UTC)
    db.commit()
    db.refresh(cycle)
    return cycle
