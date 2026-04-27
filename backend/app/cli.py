"""CLI tool for ampliFi cleanup — seed, migrations, ML training."""

from __future__ import annotations

import argparse
import sys

import structlog

from app.infra.logging import setup_logging

logger = structlog.get_logger()


def cmd_seed(args: argparse.Namespace) -> None:
    from sqlalchemy import select

    from app.auth.service import hash_password
    from app.infra.db.session import SessionLocal
    from app.models.core import AppUser, Routine
    from app.services.seed import generate_sample_data

    setup_logging()
    db = SessionLocal()

    # ── Admin user ──────────────────────────────────────────────────────
    existing = db.execute(select(AppUser).where(AppUser.username == "admin")).scalar_one_or_none()
    if not existing:
        admin = AppUser(
            username="admin",
            email="admin@amplifi.dev",
            display_name="Admin",
            password_hash=hash_password("admin"),
            role="admin",
        )
        db.add(admin)
        db.commit()
        logger.info("seed.user.created", username="admin")
    else:
        logger.info("seed.user.exists", username="admin")

    # ── Sample data (entities, CCs, PCs, balances, hierarchy) ────────
    counts = generate_sample_data(db)

    # ── Register built-in routines ──────────────────────────────────────
    builtin_routines = [
        # Cleansing tree routines
        ("rule.posting_activity", "Posting activity check", "rule", "cleansing", 10),
        ("rule.ownership", "Ownership / responsible check", "rule", "cleansing", 20),
        ("rule.redundancy", "Duplicate / redundancy detection", "rule", "cleansing", 30),
        ("rule.hierarchy_compliance", "Hierarchy compliance", "rule", "cleansing", 40),
        ("rule.cross_system_dependency", "Cross-system dependency", "rule", "cleansing", 50),
        # Mapping tree routines
        ("rule.bs_relevance", "Balance sheet relevance", "rule", "mapping", 60),
        ("rule.has_direct_revenue", "Has direct revenue", "rule", "mapping", 70),
        ("rule.collects_project_costs", "Collects project costs", "rule", "mapping", 80),
        ("rule.has_operational_costs", "Has operational costs", "rule", "mapping", 90),
        ("rule.revenue_allocation_vehicle", "Revenue allocation vehicle", "rule", "mapping", 100),
        ("rule.cost_allocation_vehicle", "Cost allocation vehicle", "rule", "mapping", 110),
        ("rule.info_only", "Info-only / statistical", "rule", "mapping", 120),
        # Aggregate
        ("aggregate.combine_outcomes", "Combine outcomes", "aggregate", None, 200),
        # ML routines (placeholders)
        ("ml.outcome_classifier", "Outcome classifier (LightGBM)", "ml", "cleansing", 300),
        ("ml.target_object_classifier", "Target object classifier", "ml", "mapping", 310),
        ("ml.duplicate_cluster", "Duplicate clustering", "ml", None, 320),
        ("ml.naming_purpose", "Naming purpose head", "ml", None, 330),
        ("ml.anomaly_detector", "Anomaly detector", "ml", None, 340),
        # LLM routines (placeholders)
        ("llm.single_review", "LLM single review", "llm", None, 400),
        ("llm.sequential_review", "LLM sequential review", "llm", None, 410),
        ("llm.debate_review", "LLM debate review", "llm", None, 420),
    ]
    routine_count = 0
    for code, name, kind, tree, order in builtin_routines:
        if not db.execute(select(Routine).where(Routine.code == code)).scalar_one_or_none():
            db.add(
                Routine(code=code, name=name, kind=kind, tree=tree, source="builtin", order=order)
            )
            routine_count += 1
    if routine_count:
        db.commit()
        logger.info("seed.routines.created", count=routine_count)

    db.close()
    logger.info("seed.complete", routines=routine_count, **counts)


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
