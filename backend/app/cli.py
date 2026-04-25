"""CLI tool for ampliFi cleanup — seed, migrations, ML training."""

from __future__ import annotations

import argparse
import sys

import structlog

from app.infra.logging import setup_logging

logger = structlog.get_logger()


def cmd_seed(args: argparse.Namespace) -> None:
    from datetime import UTC, datetime

    from sqlalchemy import select

    from app.auth.service import hash_password
    from app.infra.db.session import SessionLocal
    from app.models.core import (
        AppUser,
        Balance,
        Entity,
        Hierarchy,
        HierarchyLeaf,
        HierarchyNode,
        LegacyCostCenter,
        LegacyProfitCenter,
        Routine,
    )

    setup_logging()
    db = SessionLocal()

    # ── Admin user ──────────────────────────────────────────────────────
    existing = db.execute(
        select(AppUser).where(AppUser.email == "admin@amplifi.local")
    ).scalar_one_or_none()
    if not existing:
        admin = AppUser(
            email="admin@amplifi.local",
            display_name="Admin",
            password_hash=hash_password("admin"),
            role="admin",
        )
        db.add(admin)
        logger.info("seed.user.created", email="admin@amplifi.local")
    else:
        logger.info("seed.user.exists", email="admin@amplifi.local")

    # ── Sample entities ─────────────────────────────────────────────────
    sample_entities = [
        ("1000", "Global Corp", "DE", "EMEA", "EUR"),
        ("1100", "US Operations", "US", "AMER", "USD"),
        ("1200", "UK Division", "GB", "EMEA", "GBP"),
        ("1300", "Japan Branch", "JP", "APAC", "JPY"),
        ("1400", "Brazil Unit", "BR", "LATAM", "BRL"),
        ("2000", "France Subsidiary", "FR", "EMEA", "EUR"),
        ("2100", "India Tech Center", "IN", "APAC", "INR"),
        ("2200", "Canada Office", "CA", "AMER", "CAD"),
    ]
    entity_count = 0
    for ccode, name, country, region, currency in sample_entities:
        if not db.execute(select(Entity).where(Entity.ccode == ccode)).scalar_one_or_none():
            db.add(
                Entity(ccode=ccode, name=name, country=country, region=region, currency=currency)
            )
            entity_count += 1
    if entity_count:
        logger.info("seed.entities.created", count=entity_count)

    # ── Sample legacy cost centers ──────────────────────────────────────
    sample_ccs = [
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
    cc_count = 0
    for coarea, cctr, txtsh, txtmi, ccode, cctrcgy, is_active in sample_ccs:
        exists = db.execute(
            select(LegacyCostCenter).where(
                LegacyCostCenter.coarea == coarea, LegacyCostCenter.cctr == cctr
            )
        ).scalar_one_or_none()
        if not exists:
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
            cc_count += 1
    if cc_count:
        logger.info("seed.cost_centers.created", count=cc_count)

    # ── Sample legacy profit centers ────────────────────────────────────
    sample_pcs = [
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
    pc_count = 0
    for coarea, pctr, txtsh, txtmi, ccode in sample_pcs:
        exists = db.execute(
            select(LegacyProfitCenter).where(
                LegacyProfitCenter.coarea == coarea, LegacyProfitCenter.pctr == pctr
            )
        ).scalar_one_or_none()
        if not exists:
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
            pc_count += 1
    if pc_count:
        logger.info("seed.profit_centers.created", count=pc_count)

    # ── Sample balances (for decision tree testing) ─────────────────────
    now = datetime.now(UTC)
    current_year = now.year
    sample_balances = []
    active_ccs_with_postings = [
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
    for cctr in active_ccs_with_postings:
        coarea = (
            "1000"
            if cctr.startswith("CC0")
            else (
                "1100"
                if cctr.startswith("CC2")
                else (
                    "1200"
                    if cctr.startswith("CC3")
                    else ("1300" if cctr.startswith("CC4") else "1400")
                )
            )
        )
        for period in range(1, 13):
            sample_balances.append(
                (coarea, cctr, current_year, period, "600000", "expense", 12500, 8, "EUR")
            )
    # Inactive CCs get no recent postings
    for cctr in ["CC9000", "CC9100"]:
        sample_balances.append(
            ("1000", cctr, current_year - 3, 6, "600000", "expense", 500, 1, "EUR")
        )

    bal_count = 0
    for coarea, cctr, fy, period, account, acc_class, amt, pcount, curr in sample_balances:
        from decimal import Decimal

        exists = db.execute(
            select(Balance).where(
                Balance.coarea == coarea,
                Balance.cctr == cctr,
                Balance.fiscal_year == fy,
                Balance.period == period,
            )
        ).scalar_one_or_none()
        if not exists:
            db.add(
                Balance(
                    coarea=coarea,
                    cctr=cctr,
                    ccode=coarea,
                    fiscal_year=fy,
                    period=period,
                    account=account,
                    account_class=acc_class,
                    tc_amt=Decimal(str(amt)),
                    gc_amt=Decimal(str(amt)),
                    currency_tc=curr,
                    currency_gc=curr,
                    posting_count=pcount,
                )
            )
            bal_count += 1
    if bal_count:
        logger.info("seed.balances.created", count=bal_count)

    # ── Sample hierarchy ────────────────────────────────────────────────
    exists_h = db.execute(
        select(Hierarchy).where(Hierarchy.setname == "STDH_1000")
    ).scalar_one_or_none()
    if not exists_h:
        hier = Hierarchy(
            setclass="0101",
            setname="STDH_1000",
            description="Standard Hierarchy Co.Code 1000",
            coarea="1000",
            is_active=True,
        )
        db.add(hier)
        db.flush()
        nodes = [
            ("STDH_1000", "OVERHEAD", 1),
            ("STDH_1000", "PRODUCTION", 2),
            ("OVERHEAD", "ADMIN", 1),
            ("OVERHEAD", "SUPPORT", 2),
            ("PRODUCTION", "MANUFACTURING", 1),
        ]
        for parent, child, seq in nodes:
            db.add(
                HierarchyNode(
                    hierarchy_id=hier.id, parent_setname=parent, child_setname=child, seq=seq
                )
            )
        leaves = [
            ("ADMIN", "CC0100", 1),
            ("ADMIN", "CC0400", 2),
            ("SUPPORT", "CC0200", 1),
            ("SUPPORT", "CC0300", 2),
            ("SUPPORT", "CC0500", 3),
            ("MANUFACTURING", "CC0800", 1),
            ("MANUFACTURING", "CC0900", 2),
            ("MANUFACTURING", "CC1000", 3),
        ]
        for setname, value, seq in leaves:
            db.add(HierarchyLeaf(hierarchy_id=hier.id, setname=setname, value=value, seq=seq))
        logger.info("seed.hierarchy.created", setname="STDH_1000")

    # ── Register built-in routines ──────────────────────────────────────
    builtin_routines = [
        ("rule.inactive_check", "Inactive center check", "rule", "cleansing", 10),
        ("rule.posting_activity", "Posting activity check", "rule", "cleansing", 20),
        ("rule.duplicate_cluster", "Duplicate cluster detection", "rule", "cleansing", 30),
        ("rule.hierarchy_compliance", "Hierarchy compliance", "rule", "cleansing", 40),
        ("rule.bs_relevance", "Balance sheet relevance", "rule", "mapping", 50),
        ("rule.project_check", "Project-related check", "rule", "mapping", 60),
        ("rule.feeder_allocation", "Feeder / allocation vehicle", "rule", "mapping", 70),
        ("rule.revenue_check", "Revenue presence check", "rule", "mapping", 80),
        ("ml.outcome_classifier", "Outcome classifier (LightGBM)", "ml", "cleansing", 100),
        ("ml.target_object_classifier", "Target object classifier", "ml", "mapping", 110),
        ("ml.duplicate_cluster", "Duplicate clustering", "ml", None, 120),
        ("ml.naming_purpose", "Naming purpose head", "ml", None, 130),
        ("ml.anomaly_detector", "Anomaly detector", "ml", None, 140),
        ("llm.single_review", "LLM single review", "llm", None, 200),
    ]
    routine_count = 0
    for code, name, kind, tree, order in builtin_routines:
        if not db.execute(select(Routine).where(Routine.code == code)).scalar_one_or_none():
            db.add(
                Routine(code=code, name=name, kind=kind, tree=tree, source="builtin", order=order)
            )
            routine_count += 1
    if routine_count:
        logger.info("seed.routines.created", count=routine_count)

    db.commit()
    db.close()
    logger.info(
        "seed.complete",
        entities=entity_count,
        cost_centers=cc_count,
        profit_centers=pc_count,
        balances=bal_count,
        routines=routine_count,
    )


def cmd_migrate(args: argparse.Namespace) -> None:
    import shutil
    import subprocess

    alembic_path = shutil.which("alembic") or "alembic"
    subprocess.run([alembic_path, "upgrade", "head"], check=True)  # noqa: S603


def main() -> None:
    parser = argparse.ArgumentParser(prog="amplifi-cli", description="ampliFi Cleanup CLI")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("seed", help="Seed database with admin user, sample data, and routines")

    sub.add_parser("migrate", help="Run database migrations")

    ml_parser = sub.add_parser("ml", help="ML operations")
    ml_sub = ml_parser.add_subparsers(dest="ml_command")
    train_parser = ml_sub.add_parser("train", help="Train a model")
    train_parser.add_argument("model_name", help="Model name to train")

    args = parser.parse_args()

    if args.command == "seed":
        cmd_seed(args)
    elif args.command == "migrate":
        cmd_migrate(args)
    elif args.command == "ml":
        logger.info("ml.command", sub=args.ml_command)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
