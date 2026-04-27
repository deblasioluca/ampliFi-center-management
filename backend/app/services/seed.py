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
    Employee,
    Entity,
    Hierarchy,
    HierarchyLeaf,
    HierarchyNode,
    LegacyCostCenter,
    LegacyProfitCenter,
)

logger = structlog.get_logger()

# ── Static data definitions ──────────────────────────────────────────────
# Single controlling area: 1000
COAREA = "1000"

SAMPLE_ENTITIES = [
    ("1000", "Global Corp HQ", "DE", "EMEA", "EUR"),
    ("1100", "US Operations", "US", "AMER", "USD"),
    ("1200", "UK Division", "GB", "EMEA", "GBP"),
    ("1300", "Japan Branch", "JP", "APAC", "JPY"),
    ("1400", "Brazil Unit", "BR", "LATAM", "BRL"),
    ("2000", "France Subsidiary", "FR", "EMEA", "EUR"),
    ("2100", "India Tech Center", "IN", "APAC", "INR"),
    ("2200", "Canada Office", "CA", "AMER", "CAD"),
]

# 1:1 CC↔PC mapping — same name for both, all under CO area 1000
# (coarea, cctr/pctr-suffix, name, long_name, ccode, cctrcgy, is_active, responsible_gpn)
SAMPLE_CENTERS = [
    # Entity 1000 — HQ
    ("0100", "Administration", "Administration General", "1000", "H", True, "10001"),
    ("0200", "IT Services", "IT Infrastructure & Operations", "1000", "H", True, "10002"),
    ("0300", "Finance", "Finance & Controlling", "1000", "H", True, "10003"),
    ("0400", "Human Resources", "Human Resources Management", "1000", "H", True, "10004"),
    ("0500", "Procurement", "Strategic Procurement", "1000", "H", True, "10005"),
    ("0600", "Marketing", "Marketing & Brand Management", "1000", "H", True, "10006"),
    ("0700", "Research", "Research & Development", "1000", "F", True, "10007"),
    ("0800", "Production Line 1", "Production Manufacturing L1", "1000", "F", True, "10008"),
    ("0810", "Production Line 2", "Production Manufacturing L2", "1000", "F", True, "10008"),
    ("0900", "Logistics", "Warehousing & Distribution", "1000", "F", True, "10009"),
    ("1000", "Quality Assurance", "Quality Assurance & Testing", "1000", "H", True, "10010"),
    ("1100", "Legal", "Legal & Compliance", "1000", "H", True, "10011"),
    ("1200", "Corporate Strategy", "Corporate Strategy Office", "1000", "H", True, "10012"),
    # Entity 1000 — inactive / legacy
    ("9000", "Old Admin", "Legacy Administration (inactive)", "1000", "H", False, "10001"),
    ("9100", "Obsolete IT", "Decommissioned IT Center", "1000", "H", False, "10002"),
    ("9200", "Old Marketing", "Closed Marketing Unit", "1000", "H", False, "10006"),
    # Entity 1100 — US
    ("2100", "US Sales", "US Sales Operations", "1100", "H", True, "20001"),
    ("2200", "US Support", "US Customer Support", "1100", "H", True, "20002"),
    ("2300", "US Warehouse", "US Distribution Center", "1100", "F", True, "20003"),
    ("2400", "US Engineering", "US Engineering Lab", "1100", "F", True, "20004"),
    # Entity 1200 — UK
    ("3100", "UK Finance", "UK Finance Operations", "1200", "H", True, "30001"),
    ("3200", "UK Sales", "UK Sales & Marketing", "1200", "H", True, "30002"),
    ("3300", "UK Operations", "UK Operations Center", "1200", "F", True, "30003"),
    # Entity 1300 — Japan
    ("4100", "JP Engineering", "Japan Engineering Center", "1300", "F", True, "40001"),
    ("4200", "JP Admin", "Japan Administration", "1300", "H", True, "40002"),
    # Entity 1400 — Brazil
    ("5100", "BR Operations", "Brazil Operations", "1400", "F", True, "50001"),
    ("5200", "BR Sales", "Brazil Sales Office", "1400", "H", True, "50002"),
]

# Hierarchy structure: root → nodes → leaves (CCs)
# All under single hierarchy STDH_1000
HIERARCHY_NODES = [
    # Root children
    ("STDH_1000", "OVERHEAD", 1),
    ("STDH_1000", "PRODUCTION", 2),
    ("STDH_1000", "REGIONAL", 3),
    # OVERHEAD children
    ("OVERHEAD", "ADMIN_SUPPORT", 1),
    ("OVERHEAD", "COMMERCIAL", 2),
    ("OVERHEAD", "GOVERNANCE", 3),
    # ADMIN_SUPPORT children (leaf-level nodes)
    ("ADMIN_SUPPORT", "ADMIN", 1),
    ("ADMIN_SUPPORT", "HR_PROC", 2),
    # COMMERCIAL children
    ("COMMERCIAL", "SALES_MKT", 1),
    ("COMMERCIAL", "IT_DIGITAL", 2),
    # PRODUCTION children
    ("PRODUCTION", "MANUFACTURING", 1),
    ("PRODUCTION", "SUPPLY_CHAIN", 2),
    ("PRODUCTION", "QUALITY_RD", 3),
    # REGIONAL children
    ("REGIONAL", "REG_US", 1),
    ("REGIONAL", "REG_UK", 2),
    ("REGIONAL", "REG_JP", 3),
    ("REGIONAL", "REG_BR", 4),
]

HIERARCHY_LEAVES = [
    # ADMIN node → cost centers
    ("ADMIN", "CC0100", 1),
    ("ADMIN", "CC1100", 2),
    ("ADMIN", "CC1200", 3),
    # HR_PROC node → cost centers
    ("HR_PROC", "CC0400", 1),
    ("HR_PROC", "CC0500", 2),
    # SALES_MKT node → cost centers
    ("SALES_MKT", "CC0600", 1),
    # IT_DIGITAL node → cost centers
    ("IT_DIGITAL", "CC0200", 1),
    # MANUFACTURING node → cost centers
    ("MANUFACTURING", "CC0800", 1),
    ("MANUFACTURING", "CC0810", 2),
    # SUPPLY_CHAIN node → cost centers
    ("SUPPLY_CHAIN", "CC0900", 1),
    # QUALITY_RD node → cost centers
    ("QUALITY_RD", "CC1000", 1),
    ("QUALITY_RD", "CC0700", 2),
    # GOVERNANCE node → cost centers
    ("GOVERNANCE", "CC0300", 1),
    # REG_US → US cost centers
    ("REG_US", "CC2100", 1),
    ("REG_US", "CC2200", 2),
    ("REG_US", "CC2300", 3),
    ("REG_US", "CC2400", 4),
    # REG_UK → UK cost centers
    ("REG_UK", "CC3100", 1),
    ("REG_UK", "CC3200", 2),
    ("REG_UK", "CC3300", 3),
    # REG_JP → Japan cost centers
    ("REG_JP", "CC4100", 1),
    ("REG_JP", "CC4200", 2),
    # REG_BR → Brazil cost centers
    ("REG_BR", "CC5100", 1),
    ("REG_BR", "CC5200", 2),
]

# Employee records (responsible persons referenced from CCs)
# Fields: gpn, firstname, lastname, email, job, ou_cd, cc_cd, ccode, city, country
_E = [
    ("10001", "Hans", "Mueller", "Head of Admin", "OU_ADMIN", "CC0100", "1000", "Zurich", "CH"),
    ("10002", "Sarah", "Johnson", "CIO", "OU_IT", "CC0200", "1000", "Zurich", "CH"),
    ("10003", "Michael", "Weber", "CFO", "OU_FIN", "CC0300", "1000", "Zurich", "CH"),
    ("10004", "Anna", "Schmidt", "CHRO", "OU_HR", "CC0400", "1000", "Zurich", "CH"),
    (
        "10005",
        "Thomas",
        "Fischer",
        "Head of Procurement",
        "OU_PROC",
        "CC0500",
        "1000",
        "Zurich",
        "CH",
    ),
    ("10006", "Lisa", "Brunner", "CMO", "OU_MKT", "CC0600", "1000", "Zurich", "CH"),
    ("10007", "Peter", "Keller", "Head of R&D", "OU_RD", "CC0700", "1000", "Zurich", "CH"),
    ("10008", "Martin", "Huber", "Head of Production", "OU_PROD", "CC0800", "1000", "Basel", "CH"),
    ("10009", "Julia", "Meier", "Head of Logistics", "OU_LOG", "CC0900", "1000", "Basel", "CH"),
    ("10010", "David", "Steiner", "Head of QA", "OU_QA", "CC1000", "1000", "Zurich", "CH"),
    ("10011", "Claudia", "Roth", "General Counsel", "OU_LEGAL", "CC1100", "1000", "Zurich", "CH"),
    (
        "10012",
        "Stefan",
        "Baumann",
        "Chief Strategy Officer",
        "OU_STRAT",
        "CC1200",
        "1000",
        "Zurich",
        "CH",
    ),
    ("20001", "James", "Smith", "VP US Sales", "OU_US_SALES", "CC2100", "1100", "New York", "US"),
    (
        "20002",
        "Emily",
        "Davis",
        "Director US Support",
        "OU_US_SUP",
        "CC2200",
        "1100",
        "Chicago",
        "US",
    ),
    (
        "20003",
        "Robert",
        "Wilson",
        "Warehouse Manager",
        "OU_US_WH",
        "CC2300",
        "1100",
        "Houston",
        "US",
    ),
    (
        "20004",
        "Jennifer",
        "Taylor",
        "US Engineering Lead",
        "OU_US_ENG",
        "CC2400",
        "1100",
        "San Jose",
        "US",
    ),
    (
        "30001",
        "William",
        "Brown",
        "UK Finance Director",
        "OU_UK_FIN",
        "CC3100",
        "1200",
        "London",
        "GB",
    ),
    (
        "30002",
        "Charlotte",
        "Jones",
        "UK Sales Manager",
        "OU_UK_SALES",
        "CC3200",
        "1200",
        "London",
        "GB",
    ),
    (
        "30003",
        "Oliver",
        "Clark",
        "UK Ops Manager",
        "OU_UK_OPS",
        "CC3300",
        "1200",
        "Manchester",
        "GB",
    ),
    (
        "40001",
        "Yuki",
        "Tanaka",
        "JP Engineering Lead",
        "OU_JP_ENG",
        "CC4100",
        "1300",
        "Tokyo",
        "JP",
    ),
    ("40002", "Kenji", "Sato", "JP Admin Manager", "OU_JP_ADM", "CC4200", "1300", "Tokyo", "JP"),
    (
        "50001",
        "Carlos",
        "Silva",
        "BR Operations Lead",
        "OU_BR_OPS",
        "CC5100",
        "1400",
        "Sao Paulo",
        "BR",
    ),
    (
        "50002",
        "Maria",
        "Santos",
        "BR Sales Manager",
        "OU_BR_SALES",
        "CC5200",
        "1400",
        "Sao Paulo",
        "BR",
    ),
]
# Expand to full tuples with email derived from name
SAMPLE_EMPLOYEES = [
    (
        gpn,
        fn,
        ln,
        f"{fn.lower()}.{ln.lower()}@globalcorp.com",
        job,
        ou,
        cc,
        ccode,
        city,
        country,
    )
    for gpn, fn, ln, job, ou, cc, ccode, city, country in _E
]

# Balance posting patterns: (cctr, has_recent_activity, avg_monthly_amount)
# Active CCs get current-year balances; inactive get old balances only
BALANCE_PATTERNS = [
    ("CC0100", True, Decimal("45000")),
    ("CC0200", True, Decimal("120000")),
    ("CC0300", True, Decimal("85000")),
    ("CC0400", True, Decimal("55000")),
    ("CC0500", True, Decimal("32000")),
    ("CC0600", True, Decimal("78000")),
    ("CC0700", True, Decimal("210000")),
    ("CC0800", True, Decimal("350000")),
    ("CC0810", True, Decimal("280000")),
    ("CC0900", True, Decimal("95000")),
    ("CC1000", True, Decimal("42000")),
    ("CC1100", True, Decimal("38000")),
    ("CC1200", True, Decimal("25000")),
    ("CC2100", True, Decimal("150000")),
    ("CC2200", True, Decimal("65000")),
    ("CC2300", True, Decimal("88000")),
    ("CC2400", True, Decimal("175000")),
    ("CC3100", True, Decimal("72000")),
    ("CC3200", True, Decimal("95000")),
    ("CC3300", True, Decimal("58000")),
    ("CC4100", True, Decimal("190000")),
    ("CC4200", True, Decimal("28000")),
    ("CC5100", True, Decimal("110000")),
    ("CC5200", True, Decimal("68000")),
    # Inactive — only old balances
    ("CC9000", False, Decimal("500")),
    ("CC9100", False, Decimal("1200")),
    ("CC9200", False, Decimal("800")),
]

# Lookup: cost center → entity company code (for balance generation)
_CCTR_TO_CCODE = {f"CC{c[0]}": c[3] for c in SAMPLE_CENTERS}

# Known sample identifiers — used for deletion
SAMPLE_ENTITY_CCODES = [e[0] for e in SAMPLE_ENTITIES]
SAMPLE_CC_CCTRS = [f"CC{c[0]}" for c in SAMPLE_CENTERS]
SAMPLE_PC_PCTRS = [f"PC{c[0]}" for c in SAMPLE_CENTERS]
SAMPLE_EMPLOYEE_GPNS = [e[0] for e in SAMPLE_EMPLOYEES]


# ── Generate ─────────────────────────────────────────────────────────────


def generate_sample_data(db: Session | None = None) -> dict[str, int]:
    """Insert sample entities, cost centers, profit centers, balances,
    hierarchy, and employees.

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
        "employees": 0,
    }

    # Deduplicate sample cost centers only (scoped to known sample IDs)
    from sqlalchemy import text

    sample_cctrs = list(SAMPLE_CC_CCTRS)
    if sample_cctrs:
        db.execute(
            text("""
            DELETE FROM cleanup.legacy_cost_center
            WHERE coarea = :coarea AND cctr = ANY(:cctrs) AND id NOT IN (
                SELECT MAX(id) FROM cleanup.legacy_cost_center
                WHERE coarea = :coarea AND cctr = ANY(:cctrs)
                GROUP BY coarea, cctr
            )
            """),
            {"coarea": COAREA, "cctrs": sample_cctrs},
        )
        db.flush()

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

    # Cost centers + profit centers (1:1, same name)
    for suffix, name, long_name, ccode, cctrcgy, is_active, resp_gpn in SAMPLE_CENTERS:
        cctr = f"CC{suffix}"
        pctr = f"PC{suffix}"
        currency_map = {
            "1000": "EUR",
            "1100": "USD",
            "1200": "GBP",
            "1300": "JPY",
            "1400": "BRL",
            "2000": "EUR",
            "2100": "INR",
            "2200": "CAD",
        }
        currency = currency_map.get(ccode, "EUR")

        # Cost center — insert or update existing
        existing_cc = (
            db.execute(
                select(LegacyCostCenter)
                .where(LegacyCostCenter.coarea == COAREA, LegacyCostCenter.cctr == cctr)
                .limit(1)
            )
            .scalars()
            .first()
        )
        if existing_cc:
            existing_cc.txtsh = name
            existing_cc.txtmi = long_name
            existing_cc.ccode = ccode
            existing_cc.cctrcgy = cctrcgy
            existing_cc.currency = currency
            existing_cc.is_active = is_active
            existing_cc.pctr = pctr
            existing_cc.responsible = resp_gpn
        else:
            db.add(
                LegacyCostCenter(
                    coarea=COAREA,
                    cctr=cctr,
                    txtsh=name,
                    txtmi=long_name,
                    ccode=ccode,
                    cctrcgy=cctrcgy,
                    currency=currency,
                    is_active=is_active,
                    pctr=pctr,
                    responsible=resp_gpn,
                )
            )
            counts["cost_centers"] += 1

        # Profit center (same name as CC — 1:1 mapping)
        if not db.execute(
            select(LegacyProfitCenter).where(
                LegacyProfitCenter.coarea == COAREA,
                LegacyProfitCenter.pctr == pctr,
            )
        ).scalar_one_or_none():
            db.add(
                LegacyProfitCenter(
                    coarea=COAREA,
                    pctr=pctr,
                    txtsh=name,
                    txtmi=long_name,
                    ccode=ccode,
                    currency=currency,
                    is_active=is_active,
                )
            )
            counts["profit_centers"] += 1

    # Balances
    now = datetime.now(UTC)
    current_year = now.year
    for cctr, has_recent, avg_amt in BALANCE_PATTERNS:
        cc_ccode = _CCTR_TO_CCODE.get(cctr, COAREA)
        if has_recent:
            # Current year: 12 monthly balances with varied amounts
            for period in range(1, 13):
                if not db.execute(
                    select(Balance).where(
                        Balance.coarea == COAREA,
                        Balance.cctr == cctr,
                        Balance.fiscal_year == current_year,
                        Balance.period == period,
                    )
                ).scalar_one_or_none():
                    # Vary amounts slightly per period
                    variation = Decimal(str(1.0 + (period % 4 - 2) * 0.05))
                    amt = (avg_amt * variation).quantize(Decimal("0.01"))
                    db.add(
                        Balance(
                            coarea=COAREA,
                            cctr=cctr,
                            ccode=cc_ccode,
                            fiscal_year=current_year,
                            period=period,
                            account="600000",
                            account_class="expense",
                            tc_amt=amt,
                            gc_amt=amt,
                            currency_tc="EUR",
                            currency_gc="EUR",
                            posting_count=max(3, period * 2),
                        )
                    )
                    counts["balances"] += 1
            # Previous year too
            for period in range(1, 13):
                if not db.execute(
                    select(Balance).where(
                        Balance.coarea == COAREA,
                        Balance.cctr == cctr,
                        Balance.fiscal_year == current_year - 1,
                        Balance.period == period,
                    )
                ).scalar_one_or_none():
                    amt = (avg_amt * Decimal("0.95")).quantize(Decimal("0.01"))
                    db.add(
                        Balance(
                            coarea=COAREA,
                            cctr=cctr,
                            ccode=cc_ccode,
                            fiscal_year=current_year - 1,
                            period=period,
                            account="600000",
                            account_class="expense",
                            tc_amt=amt,
                            gc_amt=amt,
                            currency_tc="EUR",
                            currency_gc="EUR",
                            posting_count=max(2, period),
                        )
                    )
                    counts["balances"] += 1
        else:
            # Inactive: only old balances (3 years ago, 1 period)
            if not db.execute(
                select(Balance).where(
                    Balance.coarea == COAREA,
                    Balance.cctr == cctr,
                    Balance.fiscal_year == current_year - 3,
                    Balance.period == 6,
                )
            ).scalar_one_or_none():
                db.add(
                    Balance(
                        coarea=COAREA,
                        cctr=cctr,
                        ccode=cc_ccode,
                        fiscal_year=current_year - 3,
                        period=6,
                        account="600000",
                        account_class="expense",
                        tc_amt=avg_amt,
                        gc_amt=avg_amt,
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
            coarea=COAREA,
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

    # Employees
    for gpn, fn, ln, email, job, ou_cd, cc_cd, ccode, city, country in SAMPLE_EMPLOYEES:
        if not db.execute(select(Employee).where(Employee.gpn == gpn)).scalars().first():
            db.add(
                Employee(
                    gpn=gpn,
                    bs_name=f"{fn} {ln}",
                    bs_firstname=fn,
                    bs_lastname=ln,
                    email_address=email,
                    emp_status="A",
                    job_desc=job,
                    ou_cd=ou_cd,
                    ou_desc=ou_cd.replace("OU_", "").replace("_", " ").title(),
                    local_cc_cd=cc_cd,
                    local_cc_desc=cc_cd,
                    gcrs_comp_cd=ccode,
                    locn_city_name_1=city,
                    locn_ctry_cd_1=country,
                    full_time_eq="1.0",
                    head_of_own_ou="X",
                )
            )
            counts["employees"] += 1

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

    # Employees
    r = db.execute(delete(Employee).where(Employee.gpn.in_(SAMPLE_EMPLOYEE_GPNS)))
    counts["employees"] = r.rowcount

    # Balances for known sample CCs
    r = db.execute(delete(Balance).where(Balance.cctr.in_(SAMPLE_CC_CCTRS)))
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
        "employees": db.execute(
            select(func.count(Employee.id)).where(Employee.gpn.in_(SAMPLE_EMPLOYEE_GPNS))
        ).scalar()
        or 0,
        "hierarchies": db.execute(
            select(func.count(Hierarchy.id)).where(Hierarchy.setname == "STDH_1000")
        ).scalar()
        or 0,
    }
