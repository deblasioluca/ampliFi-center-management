"""Seed service — generate and delete sample data.

Used by both the CLI (``python -m app.cli seed``) and the admin API
(``POST /api/admin/sample-data``, ``DELETE /api/admin/sample-data``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import structlog
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.models.core import (
    Balance,
    Entity,
    Hierarchy,
    HierarchyLeaf,
    HierarchyNode,
    LegacyCostCenter,
    LegacyProfitCenter,
)

logger = structlog.get_logger()

# ── Static data definitions ──────────────────────────────────────────────

SAMPLE_ENTITIES = [
    ("1000", "Global Corp", "DE", "EMEA", "EUR"),
    ("1100", "US Operations", "US", "AMER", "USD"),
    ("1200", "UK Division", "GB", "EMEA", "GBP"),
    ("1300", "Japan Branch", "JP", "APAC", "JPY"),
    ("1400", "Brazil Unit", "BR", "LATAM", "BRL"),
    ("2000", "France Subsidiary", "FR", "EMEA", "EUR"),
    ("2100", "India Tech Center", "IN", "APAC", "INR"),
    ("2200", "Canada Office", "CA", "AMER", "CAD"),
]

SAMPLE_COST_CENTERS = [
    ("1000", "CC0100", "Administration", "Administration General", "1000", "H", True),
    ("1000", "CC0200", "IT Services", "IT Infrastructure", "1000", "H", True),
    ("1000", "CC0300", "Finance", "Finance & Controlling", "1000", "H", True),
    ("1000", "CC0400", "HR", "Human Resources", "1000", "H", True),
    ("1000", "CC0500", "Procurement", "Strategic Procurement", "1000", "H", True),
    ("1000", "CC0600", "Marketing", "Marketing & Brand", "1000", "H", True),
    ("1000", "CC0700", "R&D Center", "Research & Development", "1000", "F", True),
    ("1000", "CC0800", "Production", "Production Line 1", "1000", "F", True),
    ("1000", "CC0900", "Logistics", "Warehousing & Distribution", "1000", "F", True),
    ("1000", "CC1000", "Quality", "Quality Assurance", "1000", "H", True),
    ("1000", "CC9000", "Old Admin", "Legacy Admin (inactive)", "1000", "H", False),
    ("1000", "CC9100", "Obsolete IT", "Decommissioned IT", "1000", "H", False),
    ("1100", "CC2100", "US Sales", "US Sales Team", "1100", "H", True),
    ("1100", "CC2200", "US Support", "US Customer Support", "1100", "H", True),
    ("1100", "CC2300", "US Warehouse", "US Distribution Center", "1100", "F", True),
    ("1200", "CC3100", "UK Finance", "UK Finance Operations", "1200", "H", True),
    ("1200", "CC3200", "UK Sales", "UK Sales & Marketing", "1200", "H", True),
    ("1300", "CC4100", "JP Engineering", "Japan Engineering Center", "1300", "F", True),
    ("1300", "CC4200", "JP Admin", "Japan Administration", "1300", "H", True),
    ("1400", "CC5100", "BR Operations", "Brazil Operations", "1400", "F", True),
]

SAMPLE_PROFIT_CENTERS = [
    ("1000", "PC0100", "Admin PC", "Administration Profit Center", "1000"),
    ("1000", "PC0200", "IT PC", "IT Profit Center", "1000"),
    ("1000", "PC0300", "Finance PC", "Finance Profit Center", "1000"),
    ("1000", "PC0700", "R&D PC", "Research Profit Center", "1000"),
    ("1000", "PC0800", "Production PC", "Production Profit Center", "1000"),
    ("1100", "PC2100", "US Sales PC", "US Sales Profit Center", "1100"),
    ("1200", "PC3100", "UK Finance PC", "UK Finance Profit Center", "1200"),
    ("1300", "PC4100", "JP Eng PC", "Japan Engineering PC", "1300"),
    ("1400", "PC5100", "BR Ops PC", "Brazil Operations PC", "1400"),
]

ACTIVE_CCS_WITH_POSTINGS = [
    "CC0100",
    "CC0200",
    "CC0300",
    "CC0500",
    "CC0700",
    "CC0800",
    "CC2100",
    "CC2200",
    "CC3100",
    "CC4100",
    "CC5100",
]

HIERARCHY_NODES = [
    ("STDH_1000", "OVERHEAD", 1),
    ("STDH_1000", "PRODUCTION", 2),
    ("OVERHEAD", "ADMIN", 1),
    ("OVERHEAD", "SUPPORT", 2),
    ("PRODUCTION", "MANUFACTURING", 1),
]

HIERARCHY_LEAVES = [
    ("ADMIN", "CC0100", 1),
    ("ADMIN", "CC0400", 2),
    ("SUPPORT", "CC0200", 1),
    ("SUPPORT", "CC0300", 2),
    ("SUPPORT", "CC0500", 3),
    ("MANUFACTURING", "CC0800", 1),
    ("MANUFACTURING", "CC0900", 2),
    ("MANUFACTURING", "CC1000", 3),
]

# Known sample entity ccodes — used to identify sample data for deletion
SAMPLE_ENTITY_CCODES = [e[0] for e in SAMPLE_ENTITIES]
SAMPLE_CC_CCTRS = [cc[1] for cc in SAMPLE_COST_CENTERS]
SAMPLE_PC_PCTRS = [pc[1] for pc in SAMPLE_PROFIT_CENTERS]


def _coarea_for_cctr(cctr: str) -> str:
    if cctr.startswith(("CC0", "CC1", "CC9")):
        return "1000"
    if cctr.startswith("CC2"):
        return "1100"
    if cctr.startswith("CC3"):
        return "1200"
    if cctr.startswith("CC4"):
        return "1300"
    return "1400"


# ── Generate ─────────────────────────────────────────────────────────────


def generate_sample_data(db: Session | None = None) -> dict[str, int]:
    """Insert sample entities, cost centers, profit centers, balances, hierarchy.

    Returns a dict with counts of created records per table.
    Idempotent — skips records that already exist.
    """
    close_db = False
    if db is None:
        from app.infra.db.session import SessionLocal

        db = SessionLocal()
        close_db = True

    counts: dict[str, int] = {
        "entities": 0,
        "cost_centers": 0,
        "profit_centers": 0,
        "balances": 0,
        "hierarchy_nodes": 0,
        "hierarchy_leaves": 0,
    }

    # Entities
    for ccode, name, country, region, currency in SAMPLE_ENTITIES:
        if not db.execute(select(Entity).where(Entity.ccode == ccode)).scalar_one_or_none():
            db.add(
                Entity(
                    ccode=ccode,
                    name=name,
                    country=country,
                    region=region,
                    currency=currency,
                )
            )
            counts["entities"] += 1

    # Cost centers
    for coarea, cctr, txtsh, txtmi, ccode, cctrcgy, is_active in SAMPLE_COST_CENTERS:
        if not db.execute(
            select(LegacyCostCenter).where(
                LegacyCostCenter.coarea == coarea,
                LegacyCostCenter.cctr == cctr,
            )
        ).scalar_one_or_none():
            db.add(
                LegacyCostCenter(
                    coarea=coarea,
                    cctr=cctr,
                    txtsh=txtsh,
                    txtmi=txtmi,
                    ccode=ccode,
                    cctrcgy=cctrcgy,
                    currency="EUR" if ccode == "1000" else "USD",
                    is_active=is_active,
                    pctr=cctr.replace("CC", "PC"),
                )
            )
            counts["cost_centers"] += 1

    # Profit centers
    for coarea, pctr, txtsh, txtmi, ccode in SAMPLE_PROFIT_CENTERS:
        if not db.execute(
            select(LegacyProfitCenter).where(
                LegacyProfitCenter.coarea == coarea,
                LegacyProfitCenter.pctr == pctr,
            )
        ).scalar_one_or_none():
            db.add(
                LegacyProfitCenter(
                    coarea=coarea,
                    pctr=pctr,
                    txtsh=txtsh,
                    txtmi=txtmi,
                    ccode=ccode,
                    currency="EUR" if ccode == "1000" else "USD",
                    is_active=True,
                )
            )
            counts["profit_centers"] += 1

    # Balances
    now = datetime.now(UTC)
    current_year = now.year
    for cctr in ACTIVE_CCS_WITH_POSTINGS:
        coarea = _coarea_for_cctr(cctr)
        for period in range(1, 13):
            if not db.execute(
                select(Balance).where(
                    Balance.coarea == coarea,
                    Balance.cctr == cctr,
                    Balance.fiscal_year == current_year,
                    Balance.period == period,
                )
            ).scalar_one_or_none():
                db.add(
                    Balance(
                        coarea=coarea,
                        cctr=cctr,
                        ccode=coarea,
                        fiscal_year=current_year,
                        period=period,
                        account="600000",
                        account_class="expense",
                        tc_amt=Decimal("12500"),
                        gc_amt=Decimal("12500"),
                        currency_tc="EUR",
                        currency_gc="EUR",
                        posting_count=8,
                    )
                )
                counts["balances"] += 1

    for cctr in ["CC9000", "CC9100"]:
        if not db.execute(
            select(Balance).where(
                Balance.coarea == "1000",
                Balance.cctr == cctr,
                Balance.fiscal_year == current_year - 3,
                Balance.period == 6,
            )
        ).scalar_one_or_none():
            db.add(
                Balance(
                    coarea="1000",
                    cctr=cctr,
                    ccode="1000",
                    fiscal_year=current_year - 3,
                    period=6,
                    account="600000",
                    account_class="expense",
                    tc_amt=Decimal("500"),
                    gc_amt=Decimal("500"),
                    currency_tc="EUR",
                    currency_gc="EUR",
                    posting_count=1,
                )
            )
            counts["balances"] += 1

    # Hierarchy
    if not db.execute(
        select(Hierarchy).where(Hierarchy.setname == "STDH_1000")
    ).scalar_one_or_none():
        hier = Hierarchy(
            setclass="0101",
            setname="STDH_1000",
            description="Standard Hierarchy Co.Code 1000",
            coarea="1000",
            is_active=True,
        )
        db.add(hier)
        db.flush()
        for parent, child, seq in HIERARCHY_NODES:
            db.add(
                HierarchyNode(
                    hierarchy_id=hier.id,
                    parent_setname=parent,
                    child_setname=child,
                    seq=seq,
                )
            )
            counts["hierarchy_nodes"] += 1
        for setname, value, seq in HIERARCHY_LEAVES:
            db.add(
                HierarchyLeaf(
                    hierarchy_id=hier.id,
                    setname=setname,
                    value=value,
                    seq=seq,
                )
            )
            counts["hierarchy_leaves"] += 1

    db.commit()
    logger.info("seed.sample_data.generated", **counts)
    if close_db:
        db.close()
    return counts


# ── Delete ───────────────────────────────────────────────────────────────


def delete_sample_data(db: Session | None = None) -> dict[str, int]:
    """Delete sample data created by ``generate_sample_data``.

    Keeps the admin user and built-in routines intact.
    Returns a dict with counts of deleted records per table.
    """
    close_db = False
    if db is None:
        from app.infra.db.session import SessionLocal

        db = SessionLocal()
        close_db = True

    counts: dict[str, int] = {}

    # Hierarchy leaves + nodes (cascade from known hierarchy)
    hier = db.execute(
        select(Hierarchy).where(Hierarchy.setname == "STDH_1000")
    ).scalar_one_or_none()
    if hier:
        r = db.execute(delete(HierarchyLeaf).where(HierarchyLeaf.hierarchy_id == hier.id))
        counts["hierarchy_leaves"] = r.rowcount
        r = db.execute(delete(HierarchyNode).where(HierarchyNode.hierarchy_id == hier.id))
        counts["hierarchy_nodes"] = r.rowcount
        db.delete(hier)
        counts["hierarchies"] = 1
    else:
        counts["hierarchy_leaves"] = 0
        counts["hierarchy_nodes"] = 0
        counts["hierarchies"] = 0

    # Balances for known sample CCs
    all_sample_cctrs = SAMPLE_CC_CCTRS
    r = db.execute(delete(Balance).where(Balance.cctr.in_(all_sample_cctrs)))
    counts["balances"] = r.rowcount

    # Profit centers
    r = db.execute(delete(LegacyProfitCenter).where(LegacyProfitCenter.pctr.in_(SAMPLE_PC_PCTRS)))
    counts["profit_centers"] = r.rowcount

    # Cost centers
    r = db.execute(delete(LegacyCostCenter).where(LegacyCostCenter.cctr.in_(SAMPLE_CC_CCTRS)))
    counts["cost_centers"] = r.rowcount

    # Entities
    r = db.execute(delete(Entity).where(Entity.ccode.in_(SAMPLE_ENTITY_CCODES)))
    counts["entities"] = r.rowcount

    db.commit()
    total = sum(counts.values())
    logger.info("seed.sample_data.deleted", total=total, **counts)
    if close_db:
        db.close()
    return counts


# ── Counts ───────────────────────────────────────────────────────────────


def sample_data_counts(db: Session) -> dict[str, int]:
    """Return counts of sample data records currently in the database."""
    return {
        "entities": db.execute(
            select(func.count(Entity.id)).where(Entity.ccode.in_(SAMPLE_ENTITY_CCODES))
        ).scalar()
        or 0,
        "cost_centers": db.execute(
            select(func.count(LegacyCostCenter.id)).where(
                LegacyCostCenter.cctr.in_(SAMPLE_CC_CCTRS)
            )
        ).scalar()
        or 0,
        "profit_centers": db.execute(
            select(func.count(LegacyProfitCenter.id)).where(
                LegacyProfitCenter.pctr.in_(SAMPLE_PC_PCTRS)
            )
        ).scalar()
        or 0,
        "balances": db.execute(
            select(func.count(Balance.id)).where(Balance.cctr.in_(SAMPLE_CC_CCTRS))
        ).scalar()
        or 0,
        "hierarchies": db.execute(
            select(func.count(Hierarchy.id)).where(Hierarchy.setname == "STDH_1000")
        ).scalar()
        or 0,
    }
