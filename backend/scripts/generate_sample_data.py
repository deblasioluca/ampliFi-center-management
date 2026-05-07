#!/usr/bin/env python3
# ruff: noqa: T201 S311
"""Generate UBS-flavored sample data for ampliFi.

Produces a realistic-looking corpus matching the user-requested defaults:
  * 130k cost centers (configurable)
  * 600 entities across ~20 countries (configurable)
  * 12 months of historical balance data
  * Entity hierarchy, 5 levels (Group → Region → Country → BU/Type → Entity)
  * Cost center hierarchy, 6 levels (Bank → Division → Function → Dept → Team → CC)
  * ~30% retire candidates, ~40% in 1:n PC migration groups, ~30% one-CC-one-PC

The data is shaped after UBS AG's actual structure as documented in public
filings (Q1 2026 report, US Resolution Plans, corporate governance pages):
  - Five business divisions: GWM, P&C, Asset Mgmt, Investment Bank, Non-core/Legacy
  - Real legal-entity names where they exist (UBS AG, UBS Switzerland AG,
    UBS Europe SE, UBS Americas Inc, UBS Securities LLC, etc.) plus
    plausible-but-fictional country branches to fill out the 600.
  - Real city/country mix: Zurich/Basel/Geneva for CH; New York/Weehawken/
    Stamford/Chicago/Nashville/Raleigh for US; London/Frankfurt/Luxembourg
    for EMEA; HK/Singapore/Tokyo/Mumbai for APAC.
  - Cost weights per division match the Q1 2026 operating-expense split:
    GWM 50%, IB 26%, P&C 14%, AM 5%, NCL 3%, Group Items 2%.

This is SAMPLE DATA. It is structurally inspired by UBS public disclosures
but the cost centers, balance amounts, responsible owners and posting
patterns are all generated. No internal UBS data is reproduced here.

Usage
-----
    cd backend
    python scripts/generate_sample_data.py            # 130k CCs default
    python scripts/generate_sample_data.py --centers 5000  # smaller for testing
    python scripts/generate_sample_data.py --reset    # wipe scope first
    python scripts/generate_sample_data.py --seed 42  # reproducible

The script writes everything to scope='cleanup' (the default app scope) using
bulk inserts. On a Pi 5 with Postgres locally, expect ~3-5 minutes for the
default 130k centers; on SQLite, expect ~10 minutes.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

# Make `app.*` imports work when run as a script.
_HERE = Path(__file__).resolve().parent
_BACKEND = _HERE.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

# These come after sys.path adjustment.
from sqlalchemy import delete, select  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from app.infra.db.session import SessionLocal  # noqa: E402
from app.models.core import (  # noqa: E402
    Balance,
    Entity,
    GLAccountSKA1,
    Hierarchy,
    HierarchyLeaf,
    HierarchyNode,
    LegacyCostCenter,
    LegacyProfitCenter,
)

SCOPE = "cleanup"
DATA_CATEGORY = "legacy"
COAREA = "UBS1"  # synthetic controlling area (CO area) for the whole UBS group


# ── Domain knowledge: real UBS structure (from public filings) ────────────


@dataclass(frozen=True)
class Country:
    code: str  # ISO 2-letter — used as SAP LAND1
    name: str
    currency: str
    region: str  # one of: CH, EMEA, AMS, APAC
    cities: tuple[str, ...]
    weight: float  # relative count of entities (Switzerland and US dominate)


COUNTRIES: tuple[Country, ...] = (
    # Switzerland — heaviest concentration (head office + branches + UBS Switzerland AG)
    Country(
        "CH",
        "Switzerland",
        "CHF",
        "CH",
        ("Zurich", "Basel", "Geneva", "Lugano", "Lausanne", "Bern", "St. Gallen"),
        80,
    ),
    # United States — second heaviest (UBS Americas Holding + many state subs)
    Country(
        "US",
        "United States",
        "USD",
        "AMS",
        ("New York", "Weehawken", "Stamford", "Chicago", "Nashville", "Raleigh"),
        120,
    ),
    # United Kingdom
    Country("GB", "United Kingdom", "GBP", "EMEA", ("London",), 40),
    # Germany — UBS Europe SE headquarters
    Country("DE", "Germany", "EUR", "EMEA", ("Frankfurt", "Munich", "Hamburg"), 30),
    # Other EMEA
    Country("LU", "Luxembourg", "EUR", "EMEA", ("Luxembourg",), 18),
    Country("FR", "France", "EUR", "EMEA", ("Paris",), 14),
    Country("IT", "Italy", "EUR", "EMEA", ("Milan", "Rome"), 12),
    Country("ES", "Spain", "EUR", "EMEA", ("Madrid", "Barcelona"), 10),
    Country("NL", "Netherlands", "EUR", "EMEA", ("Amsterdam",), 8),
    Country("AT", "Austria", "EUR", "EMEA", ("Vienna",), 6),
    Country("MC", "Monaco", "EUR", "EMEA", ("Monaco",), 4),
    Country("JE", "Jersey", "GBP", "EMEA", ("Saint Helier",), 4),
    Country("IE", "Ireland", "EUR", "EMEA", ("Dublin",), 6),
    # APAC
    Country("HK", "Hong Kong SAR", "HKD", "APAC", ("Hong Kong",), 35),
    Country("SG", "Singapore", "SGD", "APAC", ("Singapore",), 30),
    Country("JP", "Japan", "JPY", "APAC", ("Tokyo",), 22),
    Country("AU", "Australia", "AUD", "APAC", ("Sydney", "Melbourne"), 12),
    Country("IN", "India", "INR", "APAC", ("Mumbai", "Pune", "Hyderabad"), 18),
    Country("CN", "China", "CNY", "APAC", ("Shanghai", "Beijing"), 10),
    # Americas non-US
    Country("BR", "Brazil", "BRL", "AMS", ("Sao Paulo",), 14),
    Country("CA", "Canada", "CAD", "AMS", ("Toronto",), 10),
    Country("MX", "Mexico", "MXN", "AMS", ("Mexico City",), 6),
    Country("BM", "Bermuda", "USD", "AMS", ("Hamilton",), 4),
    Country("KY", "Cayman Islands", "USD", "AMS", ("George Town",), 4),
    # Middle East
    Country("AE", "United Arab Emirates", "AED", "EMEA", ("Dubai", "Abu Dhabi"), 8),
    Country("IL", "Israel", "ILS", "EMEA", ("Tel Aviv",), 5),
)


# Entity-name templates. These produce the real legal-entity name when one
# exists, and a plausible synthetic name otherwise. The label after the
# template is the "type" that drives the L4 of the entity hierarchy.
ENTITY_TYPES: tuple[tuple[str, str], ...] = (
    ("Operating", "{country_name} Operating Bank"),
    ("Wealth", "Wealth Management {country_name}"),
    ("AssetMgmt", "Asset Management ({country_name})"),
    ("InvestmentBank", "Securities ({country_name})"),
    ("Service", "Business Solutions ({country_name})"),
)

# A small registry of REAL UBS legal entities that should appear with their
# official names. Picked from public filings. The rest are filled
# synthetically.
REAL_ENTITIES: tuple[tuple[str, str, str], ...] = (
    # (country_code, type, real_name)
    ("CH", "Operating", "UBS AG"),
    ("CH", "Operating", "UBS Switzerland AG"),
    ("CH", "AssetMgmt", "UBS Asset Management AG"),
    ("CH", "AssetMgmt", "UBS Fund Management (Switzerland) AG"),
    ("CH", "Service", "UBS Business Solutions AG"),
    ("DE", "Operating", "UBS Europe SE"),
    ("LU", "AssetMgmt", "UBS Fund Administration Services Luxembourg S.A."),
    ("LU", "Operating", "UBS Europe SE, Luxembourg branch"),
    ("US", "Operating", "UBS Americas Holding LLC"),
    ("US", "Operating", "UBS Americas Inc."),
    ("US", "InvestmentBank", "UBS Securities LLC"),
    ("US", "Wealth", "UBS Financial Services Inc."),
    ("US", "AssetMgmt", "UBS Asset Management (Americas) LLC"),
    ("GB", "Operating", "UBS AG London Branch"),
    ("GB", "AssetMgmt", "UBS Asset Management (UK) Ltd."),
    ("HK", "Operating", "UBS AG Hong Kong Branch"),
    ("HK", "InvestmentBank", "UBS Securities Hong Kong Ltd."),
    ("HK", "AssetMgmt", "UBS Asset Management (Hong Kong) Ltd."),
    ("SG", "Operating", "UBS AG Singapore Branch"),
    ("SG", "InvestmentBank", "UBS Securities Pte. Ltd."),
    ("SG", "AssetMgmt", "UBS Asset Management (Singapore) Ltd."),
    ("JP", "Operating", "UBS AG Tokyo Branch"),
    ("JP", "InvestmentBank", "UBS Securities Japan Co. Ltd."),
    ("AU", "AssetMgmt", "UBS Asset Management (Australia) Ltd."),
    # Former Credit Suisse entities — these are the natural retire candidates
    # because the Q1 2026 report shows them being run off in Non-core & Legacy.
    ("CH", "Operating", "Credit Suisse Services AG"),
    ("SG", "Wealth", "Credit Suisse Trust Limited"),
    ("BM", "Wealth", "Credit Suisse Life (Bermuda) Ltd."),
    ("US", "InvestmentBank", "DLJ Mortgage Capital Inc."),
)


# Business divisions and their share of the cost-center population, taken
# from the Q1 2026 operating-expense split (USD m): GWM 5,349 / IB 2,817 /
# P&C 1,477 / AM 556 / NCL 319 / Group Items 260.
@dataclass(frozen=True)
class Division:
    code: str
    name: str
    weight: float  # share of total CCs
    retire_bias: float  # extra retire probability (0 = baseline 30%)
    sharing_bias: float  # extra 1:n PC-sharing probability


DIVISIONS: tuple[Division, ...] = (
    Division("GWM", "Global Wealth Management", 50.0, 0.0, 0.0),
    Division("IB", "Investment Bank", 26.0, 0.0, 0.10),  # IB has more shared infra
    Division("PC", "Personal & Corporate Banking", 14.0, 0.0, 0.0),
    Division("AM", "Asset Management", 5.0, 0.0, -0.10),
    Division("NCL", "Non-core and Legacy", 3.0, 0.55, 0.0),  # mostly retired
    Division("CC", "Group Items / Corporate Center", 2.0, 0.10, 0.20),  # support → shared PCs
)


# Function names by division — used as L3 of the CC hierarchy. Real UBS
# functional taxonomy from annual reports.
FUNCTIONS: dict[str, tuple[str, ...]] = {
    "GWM": (
        "Switzerland",
        "EMEA",
        "Americas",
        "APAC",
        "Mandates",
        "Lending",
        "Banking Products",
        "Family Office",
    ),
    "IB": (
        "Equities",
        "FICC",
        "Banking",
        "Research",
        "Capital Markets",
        "Prime Services",
        "Global Markets",
    ),
    "PC": (
        "Retail Banking",
        "Affluent",
        "SME",
        "Large Corporates",
        "Real Estate Financing",
        "Mortgages",
        "Commodity Trade Finance",
    ),
    "AM": (
        "Equities",
        "Fixed Income",
        "Multi-Asset",
        "Hedge Funds",
        "Real Estate",
        "Sustainable Investing",
        "Indexed Strategies",
    ),
    "NCL": ("Legacy IB", "Legacy WM", "Legacy P&C", "Run-off Portfolios", "Litigation Provisions"),
    "CC": (
        "Group Treasury",
        "Group Risk Control",
        "Group Compliance",
        "Group Finance",
        "Group Operations",
        "Group Technology",
        "Group HR",
        "Group Internal Audit",
        "Group Legal",
    ),
}

# Sub-functions / department types. Used as L4 of the CC hierarchy.
DEPARTMENTS: tuple[str, ...] = (
    "Front Office",
    "Middle Office",
    "Back Office",
    "Operations",
    "Technology",
    "Product Management",
    "Sales",
    "Trading",
    "Advisory",
    "Coverage",
    "Structuring",
    "Origination",
    "Risk",
    "Compliance",
    "Finance",
    "HR",
    "Legal",
    "Client Service",
    "Reporting",
    "Analytics",
    "Project Office",
)

# Plausible team / desk names. L5 in the CC hierarchy.
TEAM_SUFFIXES: tuple[str, ...] = (
    "Team Alpha",
    "Team Beta",
    "Team Gamma",
    "Team Delta",
    "Team Epsilon",
    "Desk 1",
    "Desk 2",
    "Desk 3",
    "Desk 4",
    "Desk 5",
    "North",
    "South",
    "East",
    "West",
    "Central",
    "Strategic",
    "Tactical",
    "Specialist",
    "Generalist",
    "Coverage",
)


# Sample person names for "responsible" owners. Mix of common Swiss-German,
# Anglo, French, Italian and Asian names — UBS is multinational.
FIRST_NAMES: tuple[str, ...] = (
    "Anna",
    "Marco",
    "Sophie",
    "Lukas",
    "Elena",
    "Jan",
    "Mia",
    "Felix",
    "Léa",
    "Antoine",
    "Clara",
    "Luca",
    "Giulia",
    "Matteo",
    "Sofia",
    "James",
    "Sarah",
    "Michael",
    "Emma",
    "David",
    "Olivia",
    "Wei",
    "Mei",
    "Hiroshi",
    "Aiko",
    "Priya",
    "Arjun",
    "Rohan",
    "Ahmed",
    "Fatima",
    "Rafael",
    "Camila",
    "Lucas",
    "Isabela",
)
LAST_NAMES: tuple[str, ...] = (
    "Müller",
    "Meier",
    "Schmid",
    "Weber",
    "Fischer",
    "Keller",
    "Brunner",
    "Steiner",
    "Huber",
    "Bachmann",
    "Furrer",
    "Stocker",
    "Smith",
    "Jones",
    "Brown",
    "Wilson",
    "Taylor",
    "Anderson",
    "Dubois",
    "Laurent",
    "Martin",
    "Bernard",
    "Petit",
    "Rossi",
    "Bianchi",
    "Conti",
    "Ricci",
    "Marino",
    "Tanaka",
    "Suzuki",
    "Sato",
    "Watanabe",
    "Yamamoto",
    "Wang",
    "Li",
    "Zhang",
    "Liu",
    "Chen",
    "Patel",
    "Sharma",
    "Kumar",
    "Singh",
)


# ── Helpers ───────────────────────────────────────────────────────────────


def make_full_name(rng: random.Random) -> str:
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def make_user_id(name: str, rng: random.Random) -> str:
    """Plausible 8-char SAP user ID (e.g. 'amueller', 'jsmith42')."""
    parts = name.lower().split()
    initial = parts[0][0] if parts else "x"
    last = parts[-1] if len(parts) > 1 else "user"
    last_clean = "".join(c for c in last if c.isalpha())[:6]
    suffix = "" if rng.random() < 0.4 else str(rng.randint(1, 99))
    return f"{initial}{last_clean}{suffix}"[:12]


def chunked(seq: list, size: int):
    """Yield successive chunks of ``seq``."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def progress(label: str, n: int, total: int, started: float) -> None:
    if total <= 0:
        return
    pct = n / total
    elapsed = time.monotonic() - started
    eta = (elapsed / pct - elapsed) if pct > 0.01 else 0
    bar_w = 32
    filled = int(bar_w * pct)
    bar = "█" * filled + "░" * (bar_w - filled)
    sys.stdout.write(
        f"\r  {label:24s} [{bar}] {n:>9,}/{total:<9,} ({pct * 100:5.1f}%)  "
        f"elapsed {elapsed:5.1f}s  eta {eta:5.1f}s"
    )
    sys.stdout.flush()
    if n >= total:
        sys.stdout.write("\n")


# ── Generation ────────────────────────────────────────────────────────────


@dataclass
class GeneratedEntity:
    ccode: str
    name: str
    country: Country
    city: str
    type: str  # one of ENTITY_TYPES keys
    division: Division  # which division dominates this entity (for CC distribution)
    is_legacy_cs: bool


@dataclass
class GeneratedEmployee:
    """A synthetic employee, anchored to one entity.

    Mirrors the ``cleanup.employee`` table's most-used columns. The full
    SAP HR schema has many more fields, but the rest are left null in
    sample data — the loader will fill them when a real HR feed is wired.
    """

    gpn: str  # Global Personnel Number — primary external key
    first_name: str
    last_name: str
    user_id: str  # 6-char SAP user id
    email: str
    entity: GeneratedEntity
    division: Division
    rank_code: str  # e.g. ED, MD, AD, VP, AVP, ASSOC, ANALYST, INTERN
    rank_text: str
    is_manager: bool
    manager_gpn: str | None  # set after generation when org-tree is wired
    db_id: int = 0  # filled after DB insert so CC/PC FKs can reference it


@dataclass
class GeneratedCC:
    cctr: str
    pctr: str
    name: str
    description: str
    entity: GeneratedEntity
    division: Division
    function: str
    department: str
    team: str
    responsible_employee: GeneratedEmployee  # FK target
    will_retire: bool
    pc_group_key: str  # cost centers sharing the same key share a PC (1:n migration)
    is_pc_group_leader: bool
    activity_level: float  # 0 (dead) … 1 (very active)


def _division_for_type(type_code: str, rng: random.Random) -> Division:
    """Map an entity type to a likely dominant division."""
    lookup: dict[str, str] = {
        "Wealth": "GWM",
        "InvestmentBank": "IB",
        "AssetMgmt": "AM",
        "Service": "CC",
        "Operating": "PC",
    }
    target_code = lookup.get(type_code, "PC")
    return next(d for d in DIVISIONS if d.code == target_code)


# UBS-style rank ladder. Used to weight cost-center responsibility — most
# cost centers are owned by a Director or VP, fewer by an MD or higher.
RANK_LADDER: tuple[tuple[str, str, float, float], ...] = (
    # (code, text, share, manager_probability)
    ("ANALYST", "Analyst", 0.30, 0.0),
    ("ASSOC", "Associate", 0.22, 0.0),
    ("AVP", "Associate Vice President", 0.16, 0.10),
    ("VP", "Vice President", 0.18, 0.50),
    ("ED", "Executive Director", 0.08, 0.85),
    ("MD", "Managing Director", 0.05, 0.95),
    ("GMD", "Group Managing Director", 0.01, 1.00),
)


def build_employees(
    rng: random.Random,
    entities: list[GeneratedEntity],
    target: int,
) -> list[GeneratedEmployee]:
    """Generate ``target`` employees distributed across the entities.

    Distribution:
      * Each employee belongs to exactly one entity (= one company code).
      * Entity headcount weights match real UBS concentration — UBS AG +
        UBS Switzerland AG + UBS Americas Inc dominate; small branches
        get a handful each.
      * Within each entity, rank distribution follows ``RANK_LADDER``:
        bottom-heavy (lots of analysts/associates, very few MDs).
      * Roughly 18% of employees end up flagged as managers (rank
        determines manager probability — every MD is, no analyst is).

    Manager linkage (``manager_gpn``) is wired in a second pass, after
    all GPNs are minted, so we can point each non-manager at a real
    manager in the same entity + division.
    """
    # Step 1: entity headcount. The real UBS legal entities (UBS AG, UBS
    # Switzerland AG, UBS Americas Inc, etc.) host the bulk of FTEs. Use
    # the same exponential-with-real-boost shape we use for cost centers.
    weights = []
    for ent in entities:
        is_real = any(ent.name == real_name for _, _, real_name in REAL_ENTITIES)
        # Bigger spread for headcount than for CCs — UBS AG alone has
        # tens of thousands of FTEs while a small foreign branch has 50-200.
        base = 1.0 + rng.expovariate(0.5)
        weights.append(base * (8.0 if is_real else 1.0))

    total_weight = sum(weights)
    # Cap the per-entity floor so the floor times entity count never
    # exceeds the target — otherwise the rebalance loop below would spin
    # forever trying to decrement all-at-floor quotas.
    floor = max(1, min(5, target // max(1, len(entities))))
    quotas = [max(floor, int(target * w / total_weight)) for w in weights]
    while sum(quotas) < target:
        quotas[rng.randrange(len(quotas))] += 1
    while sum(quotas) > target:
        idx = rng.randrange(len(quotas))
        if quotas[idx] > floor:
            quotas[idx] -= 1

    # Step 2: mint employees per entity.
    employees: list[GeneratedEmployee] = []
    gpn_counter = 100_000  # GPNs look like 8-digit numbers in real UBS HR
    for ent, quota in zip(entities, quotas, strict=True):
        for _ in range(quota):
            gpn_counter += 1
            gpn = f"{gpn_counter:08d}"
            first = rng.choice(FIRST_NAMES)
            last = rng.choice(LAST_NAMES)
            user_id = make_user_id(f"{first} {last}", rng)[:6]
            # Rank — sample from RANK_LADDER weighted distribution
            rank = _weighted_choice(rng, [(r, r[2]) for r in RANK_LADDER])
            rank_code, rank_text, _, mgr_prob = rank
            is_manager = rng.random() < mgr_prob
            # Email pattern: firstname.lastname@ubs.com (lowercase, no
            # special chars). Real UBS uses ubs.com; this is sample data
            # so the domain is harmless.
            email = (
                f"{first.lower()}.{last.lower()}@ubs.com".replace("é", "e")
                .replace("ü", "u")
                .replace("ö", "o")
                .replace("ä", "a")
                .replace(" ", "")
            )
            # Pick a division the employee primarily works in. Heavy bias
            # toward the entity's dominant division (employees in UBS
            # Securities LLC are almost all IB).
            if rng.random() < 0.75:
                division = ent.division
            else:
                division = _weighted_choice(rng, [(d, d.weight) for d in DIVISIONS])
            employees.append(
                GeneratedEmployee(
                    gpn=gpn,
                    first_name=first,
                    last_name=last,
                    user_id=user_id,
                    email=email,
                    entity=ent,
                    division=division,
                    rank_code=rank_code,
                    rank_text=rank_text,
                    is_manager=is_manager,
                    manager_gpn=None,
                )
            )

    # Step 3: wire manager_gpn — point each non-manager at a manager in
    # the same (entity, division) bucket. If no manager available, fall
    # back to any manager in the entity. If the entity has no managers,
    # the employee is unmanaged (harmless null FK in real systems too).
    by_bucket: dict[tuple[str, str], list[GeneratedEmployee]] = {}
    for emp in employees:
        if emp.is_manager:
            by_bucket.setdefault((emp.entity.ccode, emp.division.code), []).append(emp)
    by_entity: dict[str, list[GeneratedEmployee]] = {}
    for emp in employees:
        if emp.is_manager:
            by_entity.setdefault(emp.entity.ccode, []).append(emp)

    for emp in employees:
        if emp.is_manager:
            continue
        candidates = by_bucket.get((emp.entity.ccode, emp.division.code), [])
        if not candidates:
            candidates = by_entity.get(emp.entity.ccode, [])
        if candidates:
            emp.manager_gpn = rng.choice(candidates).gpn

    return employees


def build_entities(rng: random.Random, target: int) -> list[GeneratedEntity]:
    """Build the entity master, biased toward UBS's real geographic concentration."""
    ents: list[GeneratedEntity] = []

    # Step 1: seed the catalogue with real UBS legal entities first.
    real_used: set[tuple[str, str, str]] = set()
    for cc, ty, name in REAL_ENTITIES:
        country = next(c for c in COUNTRIES if c.code == cc)
        city = country.cities[0]
        ents.append(
            GeneratedEntity(
                ccode="",  # filled below
                name=name,
                country=country,
                city=city,
                type=ty,
                division=_division_for_type(ty, rng),
                is_legacy_cs=name.startswith(("Credit Suisse", "DLJ")),
            )
        )
        real_used.add((cc, ty, name))

    # Step 2: fill the rest synthetically until we hit `target`.
    pool: list[Country] = []
    for c in COUNTRIES:
        pool.extend([c] * int(c.weight))

    while len(ents) < target:
        country = rng.choice(pool)
        type_code, template = rng.choice(ENTITY_TYPES)
        # Avoid generating duplicate (country, type, generic-name) triples.
        suffix = rng.randint(1, 9999)
        name = template.format(country_name=country.name) + f" #{suffix:04d}"
        city = rng.choice(country.cities)
        ents.append(
            GeneratedEntity(
                ccode="",
                name=name,
                country=country,
                city=city,
                type=type_code,
                division=_division_for_type(type_code, rng),
                is_legacy_cs=False,
            )
        )

    # Step 3: assign company codes (BUKRS) — 4-digit per country, sequential.
    by_country: dict[str, int] = {}
    for ent in ents:
        seq = by_country.get(ent.country.code, 0) + 1
        by_country[ent.country.code] = seq
        # ccode pattern: 2-letter country + 2-digit running number — stable for re-runs given seed
        ent.ccode = f"{ent.country.code}{seq:02d}"

    rng.shuffle(ents)  # break clustering before downstream sampling
    return ents


def build_cost_centers(
    rng: random.Random,
    entities: list[GeneratedEntity],
    employees: list[GeneratedEmployee],
    target: int,
    retire_pct: float,
    sharing_pct: float,
) -> list[GeneratedCC]:
    """Generate `target` cost centers spread across the entities.

    Distribution is weighted by division cost-share AND by entity dominant
    division — entities classified as Wealth/IB/AM see most of their cost
    centers from that division.

    The ``retire_pct`` and ``sharing_pct`` parameters are interpreted as
    TOTAL shares of the cost-center population (not conditional). Internally
    we convert ``sharing_pct`` to a per-non-retire probability so the math
    works out:  share_among_alive = sharing_pct / (1 - retire_pct).

    The shared cost centers end up in PC groups of 2–6 members each — that's
    the canonical SAP m:1 migration shape (a small handful of cost centers
    rolled into a single profit center). Larger groups would be unrealistic;
    a group of 1 isn't actually shared.

    Owner selection: each cost center's responsible owner is picked from
    the ``employees`` pool, biased toward employees in the same (entity,
    division) bucket and weighted toward higher ranks (Director / VP /
    Executive Director). MD-and-up are rare and reserved for the most
    active cost centers.
    """
    # Convert sharing_pct (total share) into the conditional probability we
    # check against rng.random() inside the per-CC loop.
    share_among_alive = min(0.99, sharing_pct / (1 - retire_pct)) if retire_pct < 1 else 0.0

    # Pre-build owner-candidate pools by (entity, division). Lookup is O(1)
    # per cost center, no full-list scans inside the inner loop.
    owner_pool: dict[tuple[str, str], list[GeneratedEmployee]] = {}
    owner_pool_by_entity: dict[str, list[GeneratedEmployee]] = {}
    for emp in employees:
        owner_pool.setdefault((emp.entity.ccode, emp.division.code), []).append(emp)
        owner_pool_by_entity.setdefault(emp.entity.ccode, []).append(emp)

    # Rank-weighted preference inside the pool: higher-rank candidates
    # are more likely to be picked, since cost-center responsibility
    # typically falls to managers, not interns.
    rank_weight: dict[str, float] = {
        "ANALYST": 0.05,
        "ASSOC": 0.15,
        "AVP": 0.40,
        "VP": 0.55,
        "ED": 0.30,
        "MD": 0.15,
        "GMD": 0.05,
    }

    def pick_owner(ent_code: str, div_code: str) -> GeneratedEmployee:
        """Pick a plausible owner for a CC in (entity, division)."""
        candidates = owner_pool.get((ent_code, div_code))
        if not candidates:
            candidates = owner_pool_by_entity.get(ent_code, [])
        if not candidates:
            # No employee at this entity at all — fall back to ANY employee.
            return rng.choice(employees)
        # Rank-weighted pick.
        weighted = [(c, rank_weight.get(c.rank_code, 0.1)) for c in candidates]
        return _weighted_choice(rng, weighted)

    # Step 1: decide how many CCs each entity hosts. Real UBS concentration
    # is heavy on a few large entities (UBS AG, UBS Switzerland AG, UBS
    # Americas Inc, UBS Europe SE) so we use a power-law instead of uniform.
    weights = []
    for ent in entities:
        # Real entities get a 5x boost — UBS AG alone hosts thousands of CCs.
        is_real = any(ent.name == real_name for _, _, real_name in REAL_ENTITIES)
        base = 1.0 + rng.expovariate(1.0)  # exponential tail
        weights.append(base * (5.0 if is_real else 1.0))

    total_weight = sum(weights)
    quotas = [max(1, int(target * w / total_weight)) for w in weights]
    # Adjust to hit target exactly.
    while sum(quotas) < target:
        quotas[rng.randrange(len(quotas))] += 1
    while sum(quotas) > target:
        idx = rng.randrange(len(quotas))
        if quotas[idx] > 1:
            quotas[idx] -= 1

    # Step 2: walk each entity, mint cost centers.
    ccs: list[GeneratedCC] = []
    cctr_counter = 1000

    for ent, quota in zip(entities, quotas, strict=True):
        # Decide division mix per CC. The Q1 2026 expense split gives the
        # global target shares (GWM 50, IB 26, P&C 14, AM 5, NCL 3, CC 2).
        # We respect that as the primary driver, then add a 30% entity-flavour
        # bias — an entity classified as Wealth still hosts mostly GWM CCs,
        # but real UBS shows that Operating-bank entities (UBS AG, UBS
        # Switzerland AG) host every division simultaneously.
        for _ in range(quota):
            if rng.random() < 0.15:
                # Entity-flavour pick — biased toward the entity's "type"
                # but only 15% of the time, so the global mix survives.
                div = ent.division
            else:
                # Global-share pick — sample by Q1 2026 expense share.
                div = _weighted_choice(rng, [(d, d.weight) for d in DIVISIONS])

            # Legacy CS entities should host mostly NCL — they ARE the
            # legacy book per the Q1 2026 report.
            if ent.is_legacy_cs and rng.random() < 0.85:
                div = next(d for d in DIVISIONS if d.code == "NCL")

            function = rng.choice(FUNCTIONS[div.code])
            department = rng.choice(DEPARTMENTS)
            team = rng.choice(TEAM_SUFFIXES)
            owner = pick_owner(ent.ccode, div.code)

            # Retire decision — div bias on top of base rate.
            retire_p = retire_pct + div.retire_bias
            will_retire = rng.random() < retire_p

            # PC sharing decision is made post-hoc in step 3 below,
            # using the same retire flag and a deterministic offshoot RNG
            # so we can later assemble groups bucket by bucket.

            # Activity level shapes posting counts and balance amounts.
            if will_retire:
                activity = rng.uniform(0.0, 0.05)  # near-dead
            elif div.code == "NCL":
                activity = rng.uniform(0.0, 0.2)  # winding down
            else:
                activity = max(0.05, min(1.0, rng.gauss(0.55, 0.22)))

            cctr_counter += 1
            cctr_code = f"{cctr_counter:08d}"

            description = f"{div.name} / {function} / {department} / {team}"
            short_name = f"{div.code}-{function[:8]}-{team[:8]}"[:40]

            ccs.append(
                GeneratedCC(
                    cctr=cctr_code,
                    pctr="",  # filled in step 3 below — depends on group assembly
                    name=short_name,
                    description=description,
                    entity=ent,
                    division=div,
                    function=function,
                    department=department,
                    team=team,
                    responsible_employee=owner,
                    will_retire=will_retire,
                    pc_group_key="",
                    is_pc_group_leader=False,
                    activity_level=activity,
                )
            )

    # Step 3: assemble PC groups for the will_share CCs.
    # Bucket sharing CCs by (entity, division, function), then within each
    # bucket split into chunks of 2–6 (target ~3 per group). CCs in the same
    # chunk share a PC; the chunk's most-active CC is the leader.
    # Re-mark which of those actually want to share (we haven't set the flag
    # because we wanted to assign group keys post-hoc). The earlier loop set
    # `will_share` locally; persist it via a sentinel on `pc_group_key` ==
    # 'SHARE' for now.
    # … but we already lost `will_share` outside the loop. Capture it on the
    # CC object via a temporary attribute set in the loop above instead.
    # See the loop: we set will_share but never stored it on the CC. Fix that
    # by re-deriving from the same RNG state? Cleanest: re-roll quickly
    # using the same parameters but a separate RNG seeded from the original.
    # Simpler: do the bucketing inline in the main loop. Let's restructure.

    # … but the cleanest fix is to keep the will_share flag on the CC. Done
    # via a small additional field — we use a non-empty pc_group_key as the
    # marker. Re-roll deterministically: same `share_p` per CC.
    rng2 = random.Random(rng.random())  # deterministic offshoot
    buckets: dict[tuple[str, str, str], list[GeneratedCC]] = {}
    for cc in ccs:
        if cc.will_retire:
            continue
        share_p_cc = share_among_alive + cc.division.sharing_bias
        if rng2.random() < share_p_cc:
            key = (cc.entity.ccode, cc.division.code, cc.function)
            buckets.setdefault(key, []).append(cc)

    # Split each bucket into chunks of size 2–6. Singletons (only 1 CC in the
    # bucket) become non-shared (their own PC).
    for (ec, dc, fn), bucket_ccs in buckets.items():
        if len(bucket_ccs) < 2:
            continue
        # Random chunk sizes summing to len(bucket_ccs); target chunk = 3.
        rng2.shuffle(bucket_ccs)
        i = 0
        chunk_idx = 0
        # Sanitise the function name into a stable group-key segment. We
        # keep 8 chars — long enough that all CC-division functions stay
        # distinct (GROUP_TR / GROUP_RI / GROUP_FI / etc.) and short enough
        # that the full key fits in the 20-char ``pctr`` column.
        # Final format:  "{ec}{dc}-{fn8}-G{nn}"  → max 4+3+1+8+1+3 = 20 chars.
        fn_safe = fn.replace(" ", "_").replace("&", "and").replace("/", "_").upper()[:8]
        while i < len(bucket_ccs):
            size = min(rng2.randint(2, 6), len(bucket_ccs) - i)
            if size < 2:
                break  # leave the last 1 as non-shared
            chunk = bucket_ccs[i : i + size]
            group_key = f"{ec}{dc}-{fn_safe}-G{chunk_idx:02d}"[:20]
            for cc in chunk:
                cc.pc_group_key = group_key
                cc.pctr = group_key
            # Promote most-active CC as leader (matters for the m:1 detector).
            leader = max(chunk, key=lambda c: c.activity_level)
            leader.is_pc_group_leader = True
            i += size
            chunk_idx += 1

    # Step 4: every remaining CC (not in a sharing group) gets its own PC.
    for cc in ccs:
        if not cc.pctr:
            cc.pctr = f"P{cc.cctr[1:]}"
            cc.is_pc_group_leader = True

    return ccs


def _weighted_choice(rng: random.Random, items: list[tuple]) -> object:
    total = sum(w for _, w in items)
    r = rng.random() * total
    cumulative = 0.0
    for item, w in items:
        cumulative += w
        if r < cumulative:
            return item
    return items[-1][0]


# ── Database write ────────────────────────────────────────────────────────


def reset_scope(session: Session, *, purge_waves: bool = False) -> dict[str, int]:
    """Wipe all data in the ``cleanup`` scope so we can start fresh.

    Returns a dict of {table_name: rows_deleted} so the caller can report.

    Deletion order respects foreign keys. Most downstream tables (
    ``routine_output``, ``center_proposal``, ``wave_entity``, etc.) are
    cleaned automatically because they have ``ON DELETE CASCADE`` against
    the legacy data we're deleting, but a handful of scope-aware tables
    written by the analyser (target_cost_center, target_profit_center,
    center_mapping) and a couple of manual tables (employee) need an
    explicit pass.

    If ``purge_waves`` is True, also deletes all ``wave``, ``analysis_run``
    and ``review_scope`` rows. This is the nuclear option — useful when
    starting a fresh demo, but it nukes any analyser configurations or
    review work the user might want to keep.
    """
    print("Resetting scope='cleanup'...")
    counts: dict[str, int] = {}

    # The model classes with a `scope` field, in dependency order
    # (children first so FKs don't fire). We import inside the function
    # to keep the top-level imports of the script tight.
    from app.models.core import (  # noqa: PLC0415
        AnalysisRun,
        CenterMapping,
        Employee,
        ReviewScope,
        TargetCostCenter,
        TargetProfitCenter,
        Wave,
    )

    # 1. Analyser-generated outputs that share the scope concept.
    counts["target_profit_center"] = _delete_by_scope(session, TargetProfitCenter)
    counts["target_cost_center"] = _delete_by_scope(session, TargetCostCenter)
    counts["center_mapping"] = _delete_by_scope(session, CenterMapping)

    # 2. Master data — order matters because of FKs.
    counts["balance"] = session.execute(delete(Balance).where(Balance.scope == SCOPE)).rowcount or 0
    counts["hierarchy_leaf"] = (
        session.execute(
            delete(HierarchyLeaf).where(
                HierarchyLeaf.hierarchy_id.in_(
                    session.query(Hierarchy.id).filter(Hierarchy.scope == SCOPE).scalar_subquery()
                )
            )
        ).rowcount
        or 0
    )
    counts["hierarchy_node"] = (
        session.execute(
            delete(HierarchyNode).where(
                HierarchyNode.hierarchy_id.in_(
                    session.query(Hierarchy.id).filter(Hierarchy.scope == SCOPE).scalar_subquery()
                )
            )
        ).rowcount
        or 0
    )
    counts["hierarchy"] = (
        session.execute(delete(Hierarchy).where(Hierarchy.scope == SCOPE)).rowcount or 0
    )
    counts["legacy_profit_center"] = (
        session.execute(
            delete(LegacyProfitCenter).where(LegacyProfitCenter.scope == SCOPE)
        ).rowcount
        or 0
    )
    counts["legacy_cost_center"] = (
        session.execute(delete(LegacyCostCenter).where(LegacyCostCenter.scope == SCOPE)).rowcount
        or 0
    )
    counts["employee"] = _delete_by_scope(session, Employee)
    counts["gl_account_ska1"] = (
        session.execute(delete(GLAccountSKA1).where(GLAccountSKA1.scope == SCOPE)).rowcount or 0
    )

    # 3. Entity last — wave_entity rows CASCADE-delete with it.
    counts["entity"] = session.execute(delete(Entity).where(Entity.scope == SCOPE)).rowcount or 0

    # 4. Optionally nuke wave/run/review_scope rows. Even without --purge
    # the wave_entity rows are gone (CASCADE), so any leftover Wave is an
    # empty husk. Fine to leave for users who care about their wave names.
    if purge_waves:
        counts["review_scope"] = session.execute(delete(ReviewScope)).rowcount or 0
        counts["analysis_run"] = session.execute(delete(AnalysisRun)).rowcount or 0
        counts["wave"] = session.execute(delete(Wave)).rowcount or 0

    session.commit()

    # Summary — only print non-zero rows so the output stays scannable.
    width = max((len(k) for k in counts), default=10)
    for table, n in counts.items():
        if n:
            print(f"  {table:<{width}}  {n:>10,} rows")
    if not any(counts.values()):
        print("  (nothing to delete — scope was already empty)")
    return counts


def _delete_by_scope(session: Session, model: type) -> int:
    """Delete all rows of ``model`` belonging to SCOPE. Returns rowcount."""
    return session.execute(delete(model).where(model.scope == SCOPE)).rowcount or 0


def insert_entities(session: Session, entities: list[GeneratedEntity]) -> None:
    started = time.monotonic()
    rows = [
        {
            "scope": SCOPE,
            "data_category": DATA_CATEGORY,
            "ccode": ent.ccode,
            "name": ent.name,
            "city": ent.city,
            "country": ent.country.code,
            "region": ent.country.region,
            "currency": ent.country.currency,
            "language": "EN",
            "chart_of_accounts": "UBS01",
        }
        for ent in entities
    ]
    n = 0
    total = len(rows)
    for chunk in chunked(rows, 1000):
        session.bulk_insert_mappings(Entity, chunk)
        n += len(chunk)
        progress("entities", n, total, started)
    session.commit()


def insert_employees(session: Session, employees: list[GeneratedEmployee]) -> None:
    """Bulk insert employees and stamp the FK target id back onto each
    generated employee object so cost-center inserts can reference it."""
    from app.models.core import Employee  # noqa: PLC0415 — script-local

    started = time.monotonic()
    rows = [
        {
            "scope": SCOPE,
            "data_category": DATA_CATEGORY,
            "gpn": emp.gpn,
            "name": emp.last_name,
            "vorname": emp.first_name,
            "userid": emp.user_id,
            "sap_bukrs": emp.entity.ccode,
            "sap_bukrs_text": emp.entity.name[:25],
            "rang_code": emp.rank_code,
            "rang_text": emp.rank_text,
            "gpn_vg_ma": emp.manager_gpn,
            "oe_text": emp.division.name[:40],
            "ubs_funk": emp.division.code,
            "ubs_funk_text": emp.division.name[:50],
            "eintrittsdatum": "20200101",
            "ersda": "20200101",
            "usnam": "GENSCRIPT",
        }
        for emp in employees
    ]

    n = 0
    total = len(rows)
    for chunk in chunked(rows, 2000):
        session.bulk_insert_mappings(Employee, chunk)
        n += len(chunk)
        progress("employees", n, total, started)
    session.commit()

    # Step 2: hydrate the GeneratedEmployee.db_id field so CC/PC inserts
    # can reference the FK. We can't get RETURNING from bulk_insert_mappings,
    # so we re-query by gpn (which is unique within scope).
    started = time.monotonic()
    gpn_to_id: dict[str, int] = dict(
        session.execute(select(Employee.gpn, Employee.id).where(Employee.scope == SCOPE)).all()
    )
    for emp in employees:
        emp.db_id = gpn_to_id.get(emp.gpn, 0)
    print(
        f"  employee FK lookup: {len(gpn_to_id):,} ids resolved in "
        f"{time.monotonic() - started:.1f}s"
    )


def _full_name(emp: GeneratedEmployee) -> str:
    """Concatenate first + last name; small helper to keep insert dicts readable."""
    return f"{emp.first_name} {emp.last_name}"


def insert_cost_centers(session: Session, ccs: list[GeneratedCC], today: date) -> None:
    """Bulk insert legacy_cost_center."""
    started = time.monotonic()
    far_future = "99991231"

    rows: list[dict] = []
    for cc in ccs:
        # SAP date fields: datab=valid-from, datbi=valid-to. Retire candidates
        # get a past datbi (already expired) to make their status realistic.
        datbi = (
            (today.replace(day=1).toordinal() - 365 * 2)  # >2y expired
            and date.fromordinal(today.toordinal() - 365 * 2).strftime("%Y%m%d")
            if cc.will_retire and cc.activity_level == 0
            else far_future
        )
        rows.append(
            {
                "scope": SCOPE,
                "data_category": DATA_CATEGORY,
                "coarea": COAREA,
                "cctr": cc.cctr,
                "txtsh": cc.name,
                "txtmi": cc.description,
                "datab": "20200101",
                "datbi": datbi,
                "ccode": cc.entity.ccode,
                "responsible": _full_name(cc.responsible_employee),
                "verak_user": cc.responsible_employee.user_id,
                "responsible_employee_id": cc.responsible_employee.db_id or None,
                "currency": cc.entity.country.currency,
                "pctr": cc.pctr,
                "land1": cc.entity.country.code,
                "name1": _full_name(cc.responsible_employee),
                "ort01": cc.entity.city,
                "ersda": "20200101",
                "usnam": "GENSCRIPT",
            }
        )

    n = 0
    total = len(rows)
    for chunk in chunked(rows, 2000):
        session.bulk_insert_mappings(LegacyCostCenter, chunk)
        n += len(chunk)
        progress("cost centers", n, total, started)
    session.commit()


def insert_profit_centers(session: Session, ccs: list[GeneratedCC]) -> None:
    """One profit center per *unique* pctr code.

    Group leaders get a friendly description; group followers don't get their
    own PC row at all (they share the leader's PC). This is the m:1 migration
    case the V2 pipeline is supposed to detect.
    """
    started = time.monotonic()
    seen: set[str] = set()
    rows: list[dict] = []
    for cc in ccs:
        if cc.pctr in seen:
            continue
        seen.add(cc.pctr)
        rows.append(
            {
                "scope": SCOPE,
                "data_category": DATA_CATEGORY,
                "coarea": COAREA,
                "pctr": cc.pctr,
                "txtsh": cc.name,
                "txtmi": cc.description,
                "datab": "20200101",
                "datbi": "99991231",
                "ccode": cc.entity.ccode,
                "responsible": _full_name(cc.responsible_employee),
                "verak_user": cc.responsible_employee.user_id,
                "responsible_employee_id": cc.responsible_employee.db_id or None,
                "currency": cc.entity.country.currency,
                "department": cc.department,
                "land1": cc.entity.country.code,
                "name1": _full_name(cc.responsible_employee),
                "ort01": cc.entity.city,
                "ersda": "20200101",
                "usnam": "GENSCRIPT",
            }
        )

    n = 0
    total = len(rows)
    for chunk in chunked(rows, 2000):
        session.bulk_insert_mappings(LegacyProfitCenter, chunk)
        n += len(chunk)
        progress("profit centers", n, total, started)
    session.commit()


def insert_balances(
    session: Session,
    rng: random.Random,
    ccs: list[GeneratedCC],
    months: int,
    today: date,
    gl_accounts: list[str] | None = None,
) -> None:
    """Generate `months` months of balance data per cost center.

    Realistic features:
      * Active CCs have a noisy monthly cost trend.
      * NCL / retire candidates have zero or near-zero amounts.
      * Posting count correlates with activity level.
      * Per-period currency = entity currency; gc_amt translated to USD.
      * When gl_accounts is provided, each CC gets 1-5 accounts assigned from
        the pool so balance rows reference real GL account numbers.
    """
    started = time.monotonic()
    fx_to_usd = {
        "USD": 1.00,
        "CHF": 1.10,
        "EUR": 1.06,
        "GBP": 1.27,
        "HKD": 0.13,
        "SGD": 0.74,
        "JPY": 0.0064,
        "AUD": 0.66,
        "INR": 0.012,
        "CNY": 0.14,
        "BRL": 0.20,
        "CAD": 0.74,
        "MXN": 0.058,
        "AED": 0.272,
        "ILS": 0.27,
    }

    # Build (year, period) list — last `months` months, period=1..12 of fiscal year.
    periods: list[tuple[int, int]] = []
    y, m = today.year, today.month
    for _ in range(months):
        periods.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    periods.reverse()

    # For each CC, decide a baseline monthly cost (USD-equivalent) based on
    # division and activity. Big IB trading desks: USD millions/month. Small
    # back-office team: USD tens of thousands.
    baselines = []
    for cc in ccs:
        scale = {
            "GWM": 1_500_000,
            "IB": 3_000_000,
            "PC": 600_000,
            "AM": 900_000,
            "NCL": 200_000,
            "CC": 400_000,
        }[cc.division.code]
        baseline_usd = scale * cc.activity_level * rng.uniform(0.4, 1.6)
        baselines.append(baseline_usd)

    # Assign 1-5 GL accounts per cost center for realistic balance splits.
    # Map first 2 digits of account to class code for account_class column.
    prefix_to_class = {p: c for c, p, _, _ in GL_ACCOUNT_CLASSES}
    cc_accounts: list[list[tuple[str, str]]] = []
    for _cc in ccs:
        if gl_accounts:
            n_accts = rng.randint(1, 5)
            chosen = rng.sample(gl_accounts, min(n_accts, len(gl_accounts)))
            cc_accounts.append([(a, prefix_to_class.get(a[:2], "OTHER")) for a in chosen])
        else:
            cc_accounts.append([("9000", "PERS")])

    rows_buffer: list[dict] = []
    n = 0
    n_accts_per_cc = [len(a) for a in cc_accounts]
    total = sum(c * len(periods) for c in n_accts_per_cc)
    batch_size = 5000

    for cc_idx, (cc, baseline_usd) in enumerate(zip(ccs, baselines, strict=True)):
        currency = cc.entity.country.currency
        fx = fx_to_usd.get(currency, 1.0)
        accts = cc_accounts[cc_idx]
        # Split baseline across accounts (personnel gets 60%, rest shared)
        acct_weights = []
        for _acct_nr, acct_cls in accts:
            w = 0.6 if acct_cls == "PERS" else 0.4 / max(1, len(accts) - 1)
            acct_weights.append(w)
        total_w = sum(acct_weights) or 1.0
        acct_weights = [w / total_w for w in acct_weights]

        for fy, period in periods:
            for (acct_nr, acct_cls), acct_w in zip(accts, acct_weights, strict=True):
                noise = rng.gauss(1.0, 0.12)
                trend = 1.0 + (period - 6) * 0.005
                monthly_usd = max(0.0, baseline_usd * acct_w * noise * trend)
                tc_amt = Decimal(str(round(monthly_usd / fx, 2))) if fx else Decimal("0.00")
                gc_amt = Decimal(str(round(monthly_usd, 2)))
                posting_count = (
                    0
                    if cc.will_retire and rng.random() < 0.85
                    else max(0, int(rng.gauss(40 * cc.activity_level * acct_w, 4)))
                )
                rows_buffer.append(
                    {
                        "scope": SCOPE,
                        "data_category": DATA_CATEGORY,
                        "coarea": COAREA,
                        "cctr": cc.cctr,
                        "ccode": cc.entity.ccode,
                        "fiscal_year": fy,
                        "period": period,
                        "account": acct_nr,
                        "account_class": acct_cls,
                        "tc_amt": tc_amt,
                        "gc_amt": gc_amt,
                        "currency_tc": currency,
                        "currency_gc": "USD",
                        "posting_count": posting_count,
                    }
                )
                if len(rows_buffer) >= batch_size:
                    session.bulk_insert_mappings(Balance, rows_buffer)
                    n += len(rows_buffer)
                    rows_buffer.clear()
                    progress("balances", n, total, started)

    if rows_buffer:
        session.bulk_insert_mappings(Balance, rows_buffer)
        n += len(rows_buffer)
        progress("balances", n, total, started)
    session.commit()


# ── GL Accounts ───────────────────────────────────────────────────────────

# SAP-style account class ranges (first 1-2 digits define the class).
GL_ACCOUNT_CLASSES: list[tuple[str, str, str, float]] = [
    # (class_code, prefix_start, description_prefix, weight)
    ("BS-A", "10", "Cash & Bank Accounts", 0.04),
    ("BS-A", "11", "Short-term Investments", 0.03),
    ("BS-A", "12", "Receivables — Trade", 0.04),
    ("BS-A", "13", "Receivables — Intercompany", 0.03),
    ("BS-A", "14", "Provisions & Accruals — Assets", 0.02),
    ("BS-A", "15", "Prepaid Expenses", 0.02),
    ("BS-A", "16", "Fixed Assets — Tangible", 0.03),
    ("BS-A", "17", "Fixed Assets — Intangible", 0.02),
    ("BS-L", "20", "Payables — Trade", 0.04),
    ("BS-L", "21", "Payables — Intercompany", 0.03),
    ("BS-L", "22", "Tax Liabilities", 0.02),
    ("BS-L", "23", "Accrued Liabilities", 0.03),
    ("BS-L", "24", "Long-term Debt", 0.02),
    ("BS-L", "25", "Equity — Capital", 0.01),
    ("BS-L", "26", "Equity — Retained Earnings", 0.01),
    ("PL", "30", "Revenue — Fee Income", 0.06),
    ("PL", "31", "Revenue — Interest Income", 0.04),
    ("PL", "32", "Revenue — Trading Income", 0.03),
    ("PL", "33", "Revenue — Other Operating", 0.02),
    ("PERS", "40", "Personnel — Salaries & Wages", 0.10),
    ("PERS", "41", "Personnel — Bonuses", 0.06),
    ("PERS", "42", "Personnel — Social Security", 0.04),
    ("PERS", "43", "Personnel — Pension", 0.03),
    ("PERS", "44", "Personnel — Other Benefits", 0.02),
    ("MAT", "50", "Materials & Supplies", 0.03),
    ("MAT", "51", "IT Hardware & Software", 0.04),
    ("MAT", "52", "Consulting & Professional Fees", 0.03),
    ("DEPR", "60", "Depreciation — Tangible", 0.03),
    ("DEPR", "61", "Depreciation — Intangible", 0.02),
    ("DEPR", "62", "Amortization", 0.02),
    ("OTHER", "70", "Travel & Entertainment", 0.02),
    ("OTHER", "71", "Rent & Occupancy", 0.03),
    ("OTHER", "72", "Communication & Postage", 0.01),
    ("OTHER", "73", "Insurance", 0.01),
    ("OTHER", "74", "Regulatory & Compliance", 0.01),
    ("OTHER", "80", "Extraordinary Items", 0.01),
    ("OTHER", "81", "Currency Translation", 0.01),
    ("OTHER", "90", "Statistical Accounts", 0.01),
]

GL_ACCOUNT_DESCRIPTIONS = {
    "10": ["Cash in Hand {}", "Bank Account — {} Main", "Cash Pool {}", "Nostro Account {}"],
    "11": ["Money Market Deposit {}", "Short-term Bond {}", "Treasury Bill {}"],
    "12": ["Trade Receivable {}", "Client Receivable {}", "Brokerage Receivable {}"],
    "13": ["IC Receivable — {}", "Intercompany Loan {}", "IC Settlement {}"],
    "14": ["Provision for Doubtful {}", "Asset Accrual {}", "Valuation Adjustment {}"],
    "15": ["Prepaid Rent {}", "Prepaid Insurance {}", "Deferred Charge {}"],
    "16": ["Buildings {}", "IT Equipment {}", "Furniture & Fixtures {}", "Leasehold {}"],
    "17": ["Software License {}", "Goodwill {}", "Client Relationships {}"],
    "20": ["Trade Payable {}", "Vendor Payable {}", "AP — External {}"],
    "21": ["IC Payable — {}", "IC Settlement {}", "IC Loan Received {}"],
    "22": ["VAT Payable {}", "Corporate Tax {}", "Withholding Tax {}"],
    "23": ["Accrued Salary {}", "Accrued Bonus {}", "Accrued Interest {}"],
    "24": ["Term Loan {}", "Bond Issued {}", "Subordinated Debt {}"],
    "25": ["Share Capital {}", "Additional Paid-in Capital {}"],
    "26": ["Retained Earnings {}", "Other Comprehensive Income {}"],
    "30": ["Advisory Fee {}", "Mgmt Fee {}", "Commission {}"],
    "31": ["Interest Income — {}", "Loan Interest {}", "Bond Coupon {}"],
    "32": ["FX Trading {}", "Equity Trading {}", "Derivatives {}"],
    "33": ["Dividend Income {}", "Rental Income {}", "Other Revenue {}"],
    "40": ["Base Salary {}", "Overtime {}", "Shift Allowance {}"],
    "41": ["Discretionary Bonus {}", "Performance Award {}", "Deferred Comp {}"],
    "42": ["Employer SSC {}", "Unemployment Ins {}", "Health Ins Contrib {}"],
    "43": ["Pension Contrib {}", "Defined Benefit {}", "Retirement Fund {}"],
    "44": ["Employee Training {}", "Relocation {}", "Company Car {}"],
    "50": ["Office Supplies {}", "Cleaning Materials {}", "Printed Forms {}"],
    "51": ["Server Hardware {}", "Desktop Licenses {}", "Cloud Hosting {}"],
    "52": ["Legal Counsel {}", "Audit Fees {}", "IT Consulting {}"],
    "60": ["Depr — Buildings {}", "Depr — Equipment {}", "Depr — Vehicles {}"],
    "61": ["Amort — Software {}", "Amort — Licenses {}", "Amort — Goodwill {}"],
    "62": ["Amort — Leasehold {}", "Amort — Patents {}"],
    "70": ["Business Travel {}", "Client Entertainment {}", "Conference {}"],
    "71": ["Office Rent {}", "Parking {}", "Building Maintenance {}"],
    "72": ["Telephone {}", "Internet {}", "Postal {}"],
    "73": ["Property Insurance {}", "Liability Insurance {}"],
    "74": ["Regulatory Fee {}", "Compliance System {}", "AML Screening {}"],
    "80": ["One-off Restructuring {}", "Litigation Settlement {}"],
    "81": ["FX Revaluation {}", "Translation Difference {}"],
    "90": ["Statistical Headcount {}", "FTE Counter {}", "Desk Count {}"],
}


def generate_gl_accounts(
    session: Session,
    rng: random.Random,
    n_accounts: int = 100_000,
) -> list[str]:
    """Generate n_accounts GL accounts (10-digit SAP SAKNR) and insert as SKA1 rows.

    Returns the list of generated account numbers for use in balance generation.
    """
    print(f"Generating {n_accounts:,} GL accounts...")
    started = time.monotonic()

    # Distribute accounts across classes by weight.
    total_weight = sum(w for _, _, _, w in GL_ACCOUNT_CLASSES)
    class_counts: list[int] = []
    remainder = n_accounts
    for i, (_, _, _, w) in enumerate(GL_ACCOUNT_CLASSES):
        if i == len(GL_ACCOUNT_CLASSES) - 1:
            class_counts.append(remainder)
        else:
            cnt = round(n_accounts * w / total_weight)
            class_counts.append(cnt)
            remainder -= cnt

    accounts: list[str] = []
    rows_buffer: list[dict] = []
    batch_size = 5000
    n_inserted = 0

    for (cls_code, prefix, _desc_prefix, _w), count in zip(
        GL_ACCOUNT_CLASSES, class_counts, strict=True
    ):
        descs = GL_ACCOUNT_DESCRIPTIONS.get(prefix, ["{} General"])
        for seq in range(count):
            # Build a 10-digit account: prefix (2 digits) + 8-digit sequence
            suffix = str(seq).zfill(8)
            saknr = prefix + suffix
            desc_template = descs[seq % len(descs)]
            txt20 = desc_template.format(seq + 1)[:20]
            txt50 = desc_template.format(seq + 1)[:50]

            accounts.append(saknr)
            rows_buffer.append(
                {
                    "scope": SCOPE,
                    "data_category": DATA_CATEGORY,
                    "ktopl": "UBS01",
                    "saknr": saknr,
                    "xbilk": "X" if prefix < "30" else "",
                    "ktoks": cls_code,
                    "glaccount_type": "X" if prefix < "30" else "N",
                    "txt20": txt20,
                    "txt50": txt50,
                }
            )

            if len(rows_buffer) >= batch_size:
                session.bulk_insert_mappings(GLAccountSKA1, rows_buffer)
                n_inserted += len(rows_buffer)
                rows_buffer.clear()
                progress("GL accounts", n_inserted, n_accounts, started)

    if rows_buffer:
        session.bulk_insert_mappings(GLAccountSKA1, rows_buffer)
        n_inserted += len(rows_buffer)
        progress("GL accounts", n_inserted, n_accounts, started)

    session.commit()
    print(f"  → {len(accounts):,} GL accounts inserted.")
    return accounts


# ── Hierarchies ───────────────────────────────────────────────────────────


def build_entity_hierarchy(session: Session, entities: list[GeneratedEntity]) -> None:
    """5-level entity hierarchy: Group → Region → Country → Type → Entity.

    Stored as a single ``Hierarchy`` row with setclass='ENT' plus parent→child
    edges in ``hierarchy_node`` and the entity-ccode leaves in
    ``hierarchy_leaf``.
    """
    print("Building entity hierarchy...")
    h = Hierarchy(
        scope=SCOPE,
        data_category=DATA_CATEGORY,
        setclass="ENT",
        setname="UBS_GROUP_ENT",
        label="UBS Group entity hierarchy",
        description="Group → Region → Country → Type → Entity (5 levels)",
        coarea=COAREA,
        is_active=True,
    )
    session.add(h)
    session.flush()
    hid = h.id

    nodes: list[dict] = []
    leaves: list[dict] = []

    # L1 → L2: Group → Regions
    regions = {ent.country.region for ent in entities}
    for region in sorted(regions):
        nodes.append(
            {
                "hierarchy_id": hid,
                "parent_setname": "UBS_GROUP_ENT",
                "child_setname": f"REG_{region}",
                "seq": 0,
            }
        )

    # L2 → L3: Region → Country
    seen_country = set()
    for ent in entities:
        key = (ent.country.region, ent.country.code)
        if key in seen_country:
            continue
        seen_country.add(key)
        nodes.append(
            {
                "hierarchy_id": hid,
                "parent_setname": f"REG_{ent.country.region}",
                "child_setname": f"CTRY_{ent.country.code}",
                "seq": 0,
            }
        )

    # L3 → L4: Country → Type
    seen_type = set()
    for ent in entities:
        key = (ent.country.code, ent.type)
        if key in seen_type:
            continue
        seen_type.add(key)
        nodes.append(
            {
                "hierarchy_id": hid,
                "parent_setname": f"CTRY_{ent.country.code}",
                "child_setname": f"TYPE_{ent.country.code}_{ent.type}",
                "seq": 0,
            }
        )

    # L4 → L5 (entity-as-set name) and leaves under it
    for ent in entities:
        type_set = f"TYPE_{ent.country.code}_{ent.type}"
        ent_set = f"ENT_{ent.ccode}"
        nodes.append(
            {
                "hierarchy_id": hid,
                "parent_setname": type_set,
                "child_setname": ent_set,
                "seq": 0,
            }
        )
        leaves.append(
            {
                "hierarchy_id": hid,
                "setname": ent_set,
                "value": ent.ccode,
                "seq": 0,
            }
        )

    for chunk in chunked(nodes, 5000):
        session.bulk_insert_mappings(HierarchyNode, chunk)
    for chunk in chunked(leaves, 5000):
        session.bulk_insert_mappings(HierarchyLeaf, chunk)
    session.commit()
    print(f"  entity hierarchy: {len(nodes)} edges, {len(leaves)} leaves")


def build_cc_hierarchy(session: Session, ccs: list[GeneratedCC]) -> None:
    """6-level CC hierarchy: Bank → Division → Function → Dept → Team → CC.

    The leaves are CC codes (KOSTL). ``hierarchy_leaf.value`` stores the cctr
    so leaf-level lookups work the same way as in the V2 routines.
    """
    print("Building cost center hierarchy...")
    h = Hierarchy(
        scope=SCOPE,
        data_category=DATA_CATEGORY,
        setclass="CC",
        setname="UBS_GROUP_CC",
        label="UBS Group cost center hierarchy",
        description="Bank → Division → Function → Dept → Team → CC (6 levels)",
        coarea=COAREA,
        is_active=True,
    )
    session.add(h)
    session.flush()
    hid = h.id

    nodes: list[dict] = []
    leaves: list[dict] = []

    # Build a single canonical setname per (level, division, function, dept, team)
    # so the same parent isn't repeated. Use a set to dedupe edges.
    edges: set[tuple[str, str]] = set()
    leaf_keys: list[tuple[str, str]] = []
    started = time.monotonic()
    total = len(ccs)

    for i, cc in enumerate(ccs):
        l2 = f"DIV_{cc.division.code}"
        # Truncate function/dept/team to keep setname under DB cap (40 chars).
        fn_safe = cc.function.replace(" ", "_").replace("&", "and").replace("/", "_")[:12]
        dp_safe = cc.department.replace(" ", "_")[:8]
        tm_safe = cc.team.replace(" ", "_")[:8]
        l3 = f"FN_{cc.division.code}_{fn_safe}"[:40]
        l4 = f"DP_{cc.division.code}_{fn_safe}_{dp_safe}"[:40]
        l5 = f"TM_{cc.division.code}_{fn_safe}_{dp_safe}_{tm_safe}"[:40]

        edges.add(("UBS_GROUP_CC", l2))
        edges.add((l2, l3))
        edges.add((l3, l4))
        edges.add((l4, l5))
        leaf_keys.append((l5, cc.cctr))

        if i % 5000 == 0:
            progress("cc hierarchy build", i, total, started)
    progress("cc hierarchy build", total, total, started)

    for parent, child in edges:
        nodes.append(
            {
                "hierarchy_id": hid,
                "parent_setname": parent,
                "child_setname": child,
                "seq": 0,
            }
        )
    for setname, value in leaf_keys:
        leaves.append(
            {
                "hierarchy_id": hid,
                "setname": setname,
                "value": value,
                "seq": 0,
            }
        )

    started = time.monotonic()
    n = 0
    for chunk in chunked(nodes, 5000):
        session.bulk_insert_mappings(HierarchyNode, chunk)
        n += len(chunk)
        progress("cc hierarchy nodes", n, len(nodes), started)

    started = time.monotonic()
    n = 0
    for chunk in chunked(leaves, 5000):
        session.bulk_insert_mappings(HierarchyLeaf, chunk)
        n += len(chunk)
        progress("cc hierarchy leaves", n, len(leaves), started)

    session.commit()
    print(f"  cc hierarchy: {len(nodes)} edges, {len(leaves)} leaves")


# ── Main ──────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate UBS-flavored sample data for ampliFi.")
    parser.add_argument(
        "--centers",
        type=int,
        default=130_000,
        help="Number of cost centers to generate (default: 130000)",
    )
    parser.add_argument(
        "--entities", type=int, default=600, help="Number of legal entities (default: 600)"
    )
    parser.add_argument(
        "--employees",
        type=int,
        default=60_000,
        help="Number of employees to generate, distributed across entities (default: 60000)",
    )
    parser.add_argument(
        "--months", type=int, default=12, help="Months of historical balances (default: 12)"
    )
    parser.add_argument(
        "--retire-pct",
        type=float,
        default=0.30,
        help="Target share of retire candidates (default: 0.30)",
    )
    parser.add_argument(
        "--sharing-pct",
        type=float,
        default=0.40,
        help="Target share of CCs in 1:n PC groups (default: 0.40)",
    )
    parser.add_argument("--seed", type=int, default=20260506, help="RNG seed for reproducibility")
    parser.add_argument(
        "--reset", action="store_true", help="Wipe the cleanup scope before generating"
    )
    parser.add_argument(
        "--wipe-only",
        action="store_true",
        help="Wipe the cleanup scope and exit — don't regenerate anything",
    )
    parser.add_argument(
        "--purge",
        action="store_true",
        help="With --reset / --wipe-only: also delete waves, runs and review scopes "
        "(by default these are kept; their entity/CC links go away via cascade)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Plan only, don't write to DB")
    args = parser.parse_args()

    # --wipe-only short-circuits the whole pipeline.
    if args.wipe_only:
        if args.dry_run:
            print("(dry-run — no DB writes)")
            print(f"\nWould wipe scope='{SCOPE}'{' including waves/runs' if args.purge else ''}.")
            return 0
        print(
            f"Wipe-only mode — clearing scope='{SCOPE}'"
            f"{' (purging waves/runs too)' if args.purge else ''}\n"
        )
        with SessionLocal() as session:
            reset_scope(session, purge_waves=args.purge)
        print("\n✓ Done.")
        return 0

    rng = random.Random(args.seed)
    today = date.today()

    print("ampliFi sample-data generator")
    print(f"  centers   : {args.centers:>10,}")
    print(f"  entities  : {args.entities:>10,}")
    print(f"  employees : {args.employees:>10,}")
    print(f"  months    : {args.months:>10}")
    print(f"  retire    : {args.retire_pct:>10.1%}")
    print(f"  sharing   : {args.sharing_pct:>10.1%}")
    print(f"  seed      : {args.seed:>10}")
    print(f"  scope     : {SCOPE}")
    print(f"  coarea    : {COAREA}")
    print()

    print(f"Database: {os.environ.get('DATABASE_URL', '(default)')}")
    if args.dry_run:
        print("(dry-run — no DB writes)")

    # Phase 1: build in-memory plan
    t_start = time.monotonic()
    print("\n[1/6] Planning entities...")
    entities = build_entities(rng, args.entities)
    n_countries = len({e.country.code for e in entities})
    print(f"  generated {len(entities)} entities across {n_countries} countries")

    print("\n[2/6] Planning employees...")
    employees = build_employees(rng, entities, args.employees)
    n_managers = sum(1 for e in employees if e.is_manager)
    print(f"  generated {len(employees):,} employees ({n_managers:,} managers)")
    rank_hist: dict[str, int] = {}
    for e in employees:
        rank_hist[e.rank_code] = rank_hist.get(e.rank_code, 0) + 1
    rank_summary = ", ".join(f"{code}:{rank_hist.get(code, 0):,}" for code, _, _, _ in RANK_LADDER)
    print(f"  rank mix: {rank_summary}")

    print("\n[3/6] Planning cost centers...")
    ccs = build_cost_centers(
        rng, entities, employees, args.centers, args.retire_pct, args.sharing_pct
    )
    n_retire = sum(1 for c in ccs if c.will_retire)
    n_share = sum(1 for c in ccs if c.pc_group_key)
    n_unique_pc = len({c.pctr for c in ccs})
    print(f"  generated {len(ccs):,} cost centers")
    print(f"    retire candidates: {n_retire:,} ({n_retire / len(ccs):.1%})")
    print(f"    in 1:n PC groups: {n_share:,} ({n_share / len(ccs):.1%})")
    print(f"    distinct PCs:     {n_unique_pc:,} (CC:PC ratio = {len(ccs) / n_unique_pc:.2f})")

    if args.dry_run:
        return 0

    # Phase 2: write
    db_url = os.environ.get("DATABASE_URL", "")
    print(f"\nUsing DB: {db_url or '(default from app config)'}")

    with SessionLocal() as session:
        if args.reset:
            reset_scope(session, purge_waves=args.purge)

        print("\n[4/7] Inserting entities + employees...")
        insert_entities(session, entities)
        # Employees MUST be inserted before cost centers because the CC
        # responsible_employee_id FK references the now-real employee row
        # ids. insert_employees() also mutates each GeneratedEmployee's
        # db_id field so the CC inserts can pick it up.
        insert_employees(session, employees)

        print("\n[5/7] Inserting cost centers + profit centers...")
        insert_cost_centers(session, ccs, today)
        insert_profit_centers(session, ccs)

        print("\n[6/7] Generating GL accounts...")
        gl_accounts = generate_gl_accounts(session, rng, n_accounts=100_000)

        print("\n[7/7] Inserting balances + hierarchies...")
        insert_balances(session, rng, ccs, args.months, today, gl_accounts=gl_accounts)
        build_entity_hierarchy(session, entities)
        build_cc_hierarchy(session, ccs)

    elapsed = time.monotonic() - t_start
    print(f"\n✓ Done in {elapsed:.1f}s.")
    print("\nSample queries:")
    print("  -- count by division:")
    # These are docs strings — false positive flagged S608 (SQL injection
    # vector). The user just sees these printed in the terminal.
    print(f"  SELECT txtmi FROM cleanup.legacy_cost_center WHERE scope='{SCOPE}' LIMIT 5;")  # noqa: S608
    print(f"  SELECT count(*) FROM cleanup.balance WHERE scope='{SCOPE}';")  # noqa: S608
    print("\nNext: open http://YOUR-PI/cockpit and create a wave with this scope.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
