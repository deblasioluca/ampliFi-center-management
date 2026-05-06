"""Housekeeping cycle service (§08.5).

Monthly cron-driven cycles that flag target cost centers AND profit centers
for review:
- UNUSED: No postings in the configured window (CC and PC both)
- LOW_VOLUME: Below threshold posting count (CC only — PCs aggregate)
- NO_OWNER: Responsible field empty or unresolvable (both CC and PC)
- ANOMALY: Statistical anomaly flag (placeholder — populated by ML pipeline)

Datasphere note: ``Balance`` may be routed to SAP Datasphere via the
``infra/datasphere/storage`` layer. This module currently queries Balance
directly via SQLAlchemy; when balance moves to Datasphere, the posting-count
aggregation must be replaced with a call through the storage abstraction.
The current code remains compatible with both backends because it uses
standard SQL only (no PostgreSQL-specific aggregations or window functions).
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.core import (
    Balance,
    Employee,
    HousekeepingCycle,
    HousekeepingItem,
    TargetCostCenter,
    TargetProfitCenter,
)

logger = structlog.get_logger()


# ── Cycle lifecycle ──────────────────────────────────────────────────────


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
        "suppress_recurring_months": 3,
        "scan_profit_centers": True,
    }


# ── Owner email resolution ───────────────────────────────────────────────


def _resolve_owner_email(responsible: str | None, db: Session) -> str | None:
    """Resolve the cost-center 'responsible' string to an email address.

    The ``responsible`` field on a TargetCostCenter / TargetProfitCenter
    typically holds a GPN (Global Personnel Number) or a personnel ID.
    Looks up the Employee table by GPN or user_id_pid and returns the
    employee's email_address. Returns None if not resolvable — caller
    should skip notification rather than crash.
    """
    if not responsible or not responsible.strip():
        return None

    key = responsible.strip()
    emp = (
        db.execute(
            select(Employee)
            .where((Employee.gpn == key) | (Employee.user_id_pid == key))
            .order_by(Employee.id.desc())
        )
        .scalars()
        .first()
    )
    if emp and emp.email_address:
        return emp.email_address.strip().lower()
    return None


# ── Scan logic ───────────────────────────────────────────────────────────


def _months_ago_period(now: datetime, months: int) -> int:
    """Return YYYYMM as int representing ``months`` ago.

    Pure date arithmetic — no DB-side date functions, so works on both
    PostgreSQL and Datasphere/HANA.
    """
    year = now.year - (months // 12)
    month = now.month - (months % 12)
    if month <= 0:
        year -= 1
        month += 12
    return year * 100 + month


def _build_suppression_set(
    cycle_id: int, suppress_months: int, db: Session
) -> set[tuple[str, int | None, int | None, str]]:
    """Return set of (entity_type, cc_id, pc_id, flag) tuples to suppress.

    Centers/PCs that an owner already decided KEEP for in a recent cycle
    should not be re-flagged with the same flag this cycle.
    """
    if suppress_months <= 0:
        return set()
    cutoff_days = suppress_months * 30
    now = datetime.now(UTC)
    recent = (
        db.execute(
            select(HousekeepingItem)
            .join(HousekeepingCycle)
            .where(
                HousekeepingItem.decision == "KEEP",
                HousekeepingCycle.id != cycle_id,
            )
            .order_by(HousekeepingItem.decided_at.desc())
        )
        .scalars()
        .all()
    )
    suppressed: set[tuple[str, int | None, int | None, str]] = set()
    for ri in recent:
        if ri.decided_at and (now - ri.decided_at).days <= cutoff_days:
            suppressed.add(
                (ri.entity_type or "CC", ri.target_cc_id, ri.target_pc_id, ri.flag)
            )
    return suppressed


def _scan_cost_centers(
    cycle: HousekeepingCycle,
    config: dict,
    suppressed: set[tuple[str, int | None, int | None, str]],
    kpis: dict[str, int],
    db: Session,
) -> None:
    """Scan TargetCostCenters and create HousekeepingItem rows for flagged ones."""
    unused_months = int(config.get("unused_months", 6))
    low_volume_threshold = int(config.get("low_volume_threshold", 5))
    now = datetime.now(UTC)
    cutoff_period = _months_ago_period(now, unused_months)

    targets = (
        db.execute(
            select(TargetCostCenter).where(TargetCostCenter.is_active.is_(True))
        )
        .scalars()
        .all()
    )
    kpis["total_cc_scanned"] = len(targets)

    for tcc in targets:
        # Posting count over window. Uses standard SQL aggregation that works
        # equally on PostgreSQL and HANA/Datasphere.
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
        posting_count = int(posting_count)

        flags: list[str] = []
        if posting_count == 0:
            flags.append("UNUSED")
        elif posting_count <= low_volume_threshold:
            flags.append("LOW_VOLUME")

        owner_email = _resolve_owner_email(tcc.responsible, db)
        if owner_email is None:
            flags.append("NO_OWNER")

        for flag in flags:
            if ("CC", tcc.id, None, flag) in suppressed:
                kpis["suppressed"] = kpis.get("suppressed", 0) + 1
                continue
            kpis[flag.lower()] = kpis.get(flag.lower(), 0) + 1
            db.add(
                HousekeepingItem(
                    cycle_id=cycle.id,
                    entity_type="CC",
                    target_cc_id=tcc.id,
                    target_pc_id=None,
                    flag=flag,
                    owner_email=owner_email,
                    owner_token=secrets.token_urlsafe(32),
                    details={
                        "posting_count_window": posting_count,
                        "responsible": tcc.responsible,
                        "cctr": tcc.cctr,
                        "coarea": tcc.coarea,
                    },
                )
            )


def _scan_profit_centers(
    cycle: HousekeepingCycle,
    config: dict,
    suppressed: set[tuple[str, int | None, int | None, str]],
    kpis: dict[str, int],
    db: Session,
) -> None:
    """Scan TargetProfitCenters for UNUSED and NO_OWNER flags.

    A Profit Center is flagged UNUSED only if *all* its child cost centers
    aggregate to zero postings. We don't flag LOW_VOLUME on PCs because they
    aggregate — the meaningful unit is the underlying CC.
    """
    if not config.get("scan_profit_centers", True):
        return

    unused_months = int(config.get("unused_months", 6))
    now = datetime.now(UTC)
    cutoff_period = _months_ago_period(now, unused_months)

    pcs = (
        db.execute(
            select(TargetProfitCenter).where(TargetProfitCenter.is_active.is_(True))
        )
        .scalars()
        .all()
    )
    kpis["total_pc_scanned"] = len(pcs)

    for tpc in pcs:
        flags: list[str] = []

        # Aggregate posting count across all CCs assigned to this PC.
        # One join — no per-CC roundtrip — so it scales.
        posting_count = (
            db.execute(
                select(func.coalesce(func.sum(Balance.posting_count), 0))
                .select_from(Balance)
                .join(
                    TargetCostCenter,
                    (Balance.cctr == TargetCostCenter.cctr)
                    & (Balance.coarea == TargetCostCenter.coarea),
                )
                .where(
                    TargetCostCenter.pctr == tpc.pctr,
                    TargetCostCenter.coarea == tpc.coarea,
                    (Balance.fiscal_year * 100 + Balance.period) >= cutoff_period,
                )
            ).scalar()
            or 0
        )
        posting_count = int(posting_count)

        if posting_count == 0:
            flags.append("UNUSED")

        owner_email = _resolve_owner_email(tpc.responsible, db)
        if owner_email is None:
            flags.append("NO_OWNER")

        for flag in flags:
            if ("PC", None, tpc.id, flag) in suppressed:
                kpis["suppressed"] = kpis.get("suppressed", 0) + 1
                continue
            counter_key = f"pc_{flag.lower()}"
            kpis[counter_key] = kpis.get(counter_key, 0) + 1
            db.add(
                HousekeepingItem(
                    cycle_id=cycle.id,
                    entity_type="PC",
                    target_cc_id=None,
                    target_pc_id=tpc.id,
                    flag=flag,
                    owner_email=owner_email,
                    owner_token=secrets.token_urlsafe(32),
                    details={
                        "posting_count_window": posting_count,
                        "responsible": tpc.responsible,
                        "pctr": tpc.pctr,
                        "coarea": tpc.coarea,
                    },
                )
            )


def run_cycle(cycle_id: int, db: Session) -> HousekeepingCycle:
    """Execute a housekeeping cycle — scan target CCs and PCs for flags."""
    cycle = db.get(HousekeepingCycle, cycle_id)
    if not cycle:
        raise ValueError(f"Cycle {cycle_id} not found")
    if cycle.status != "scheduled":
        raise ValueError(f"Cycle {cycle_id} is {cycle.status}, expected scheduled")

    cycle.status = "running"
    cycle.started_at = datetime.now(UTC)
    db.flush()

    config = cycle.config or _default_config()

    kpis: dict[str, int] = {
        "total_cc_scanned": 0,
        "total_pc_scanned": 0,
        "unused": 0,
        "low_volume": 0,
        "no_owner": 0,
        "anomaly": 0,
        "pc_unused": 0,
        "pc_no_owner": 0,
        "suppressed": 0,
    }

    suppressed = _build_suppression_set(
        cycle.id,
        int(config.get("suppress_recurring_months", 3)),
        db,
    )

    _scan_cost_centers(cycle, config, suppressed, kpis, db)
    _scan_profit_centers(cycle, config, suppressed, kpis, db)

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


# ── Item-level decisions ─────────────────────────────────────────────────


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


# ── Notifications ────────────────────────────────────────────────────────


def _load_email_engine(db: Session) -> tuple[Any | None, dict]:
    """Build an EmailEngine from AppConfig once per cycle. Returns (engine, cfg)."""
    from app.infra.email.engine import EmailEngine
    from app.models.core import AppConfig

    cfg_row = (
        db.execute(select(AppConfig).where(AppConfig.key == "email")).scalar_one_or_none()
    )
    cfg = cfg_row.value if cfg_row else {}
    if not cfg:
        logger.warning("housekeeping.no_email_config")
        return None, {}

    engine = EmailEngine(
        host=cfg.get("host", "localhost"),
        port=int(cfg.get("port", 1025)),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        use_tls=cfg.get("tls", "none") != "none",
        from_address=cfg.get("from_address", "noreply@amplifi.dev"),
    )
    return engine, cfg


def _format_flagged_lines(items: list[HousekeepingItem], db: Session) -> str:
    """Format flagged items as one line per (entity, flag) pair."""
    lines = []
    for it in items:
        if it.entity_type == "PC" and it.target_pc_id:
            tpc = db.get(TargetProfitCenter, it.target_pc_id)
            label = f"PC {tpc.pctr if tpc else '?'}"
        else:
            tcc = db.get(TargetCostCenter, it.target_cc_id) if it.target_cc_id else None
            label = f"CC {tcc.cctr if tcc else '?'}"
        lines.append(f"- {label}: {it.flag}")
    return "\n".join(lines)


def send_notifications(cycle_id: int, db: Session) -> dict:
    """Send housekeeping digest emails to all owners with flagged items.

    Owner email is resolved at scan time and stored on the item, so this
    function just groups by email and sends one digest per owner.
    """
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

    # Group by owner email
    owner_items: dict[str, list[HousekeepingItem]] = {}
    no_email_count = 0
    for item in items:
        email = item.owner_email
        if not email:
            no_email_count += 1
            continue
        owner_items.setdefault(email, []).append(item)

    engine, cfg = _load_email_engine(db)
    if engine is None:
        return {"sent": 0, "failed": 0, "no_email": no_email_count}

    base_url = cfg.get("base_url", "http://localhost:4321")
    sent_count = 0
    failed_count = 0
    now = datetime.now(UTC)

    for email, owner_group in owner_items.items():
        try:
            engine.send(
                to=email,
                template_name="housekeeping_notification",
                context={
                    "owner_name": email.split("@")[0],
                    "period": cycle.period,
                    "flagged_centers": _format_flagged_lines(owner_group, db),
                    "review_url": (
                        f"{base_url}/housekeeping/{cycle.id}/owner/{owner_group[0].owner_token}"
                    ),
                },
            )
            for it in owner_group:
                it.notified_at = now
            sent_count += 1
        except Exception as e:
            logger.warning(
                "housekeeping.notification_failed",
                email=email,
                error=str(e),
            )
            failed_count += 1

    db.commit()
    logger.info(
        "housekeeping.notifications_sent",
        cycle_id=cycle_id,
        sent=sent_count,
        failed=failed_count,
        no_email=no_email_count,
    )
    return {"sent": sent_count, "failed": failed_count, "no_email": no_email_count}


def send_reminders(cycle_id: int, db: Session) -> dict:
    """Re-send notification to owners who haven't responded yet.

    Increments the ``reminders_sent`` counter and updates ``notified_at``.
    Skips items decided more than ``auto_close_after_days`` days ago — those
    should be auto-closed by ``auto_close_overdue`` instead.
    """
    cycle = db.get(HousekeepingCycle, cycle_id)
    if not cycle:
        raise ValueError(f"Cycle {cycle_id} not found")

    items = (
        db.execute(
            select(HousekeepingItem).where(
                HousekeepingItem.cycle_id == cycle_id,
                HousekeepingItem.decision.is_(None),
                HousekeepingItem.owner_email.isnot(None),
            )
        )
        .scalars()
        .all()
    )

    if not items:
        return {"sent": 0, "items": 0}

    owner_items: dict[str, list[HousekeepingItem]] = {}
    for item in items:
        owner_items.setdefault(item.owner_email or "", []).append(item)

    engine, cfg = _load_email_engine(db)
    if engine is None:
        return {"sent": 0, "items": len(items)}

    base_url = cfg.get("base_url", "http://localhost:4321")
    sent = 0
    now = datetime.now(UTC)

    for email, owner_group in owner_items.items():
        try:
            engine.send(
                to=email,
                template_name="housekeeping_notification",
                context={
                    "owner_name": email.split("@")[0],
                    "period": cycle.period,
                    "flagged_centers": _format_flagged_lines(owner_group, db),
                    "review_url": (
                        f"{base_url}/housekeeping/{cycle.id}/owner/{owner_group[0].owner_token}"
                    ),
                    "is_reminder": True,
                },
            )
            for it in owner_group:
                it.reminders_sent = (it.reminders_sent or 0) + 1
                it.notified_at = now
            sent += 1
        except Exception:
            logger.warning("housekeeping.reminder_failed", email=email)

    db.commit()
    logger.info("housekeeping.reminders_sent", cycle_id=cycle_id, sent=sent, items=len(items))
    return {"sent": sent, "items": len(items)}


def auto_close_overdue(cycle_id: int, db: Session) -> dict:
    """Default-decide overdue items to DEFER (safer than CLOSE).

    Items whose ``notified_at`` is older than ``auto_close_after_days`` and
    that have no owner decision yet are marked DEFER with a system comment.
    """
    cycle = db.get(HousekeepingCycle, cycle_id)
    if not cycle:
        raise ValueError(f"Cycle {cycle_id} not found")

    config = cycle.config or _default_config()
    days = int(config.get("auto_close_after_days", 30))
    if days <= 0:
        return {"auto_closed": 0}

    cutoff_days = days
    now = datetime.now(UTC)
    items = (
        db.execute(
            select(HousekeepingItem).where(
                HousekeepingItem.cycle_id == cycle_id,
                HousekeepingItem.decision.is_(None),
                HousekeepingItem.notified_at.isnot(None),
            )
        )
        .scalars()
        .all()
    )
    closed = 0
    for it in items:
        if it.notified_at and (now - it.notified_at).days >= cutoff_days:
            it.decision = "DEFER"
            it.decision_comment = (
                f"Auto-deferred after {days} days without owner response"
            )
            it.decided_at = now
            closed += 1
    if closed:
        db.commit()
    logger.info("housekeeping.auto_closed", cycle_id=cycle_id, closed=closed)
    return {"auto_closed": closed}


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
