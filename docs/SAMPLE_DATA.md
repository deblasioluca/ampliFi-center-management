# Sample data generator

For demos, training, and load-testing without touching production data.
Produces a UBS-flavored corpus structurally inspired by UBS AG's public
disclosures (Q1 2026 report, US Resolution Plans, corporate-governance
pages). The cost-center population, balance amounts, posting patterns and
responsible owners are all generated — no internal UBS data is reproduced.

## What it produces (defaults)

| Object                   | Count    | Notes |
|--------------------------|----------|-------|
| Legal entities           | 600      | Across 26 countries, biased toward Switzerland and the US to match real UBS concentration. Real UBS legal-entity names (UBS AG, UBS Switzerland AG, UBS Europe SE, UBS Securities LLC, UBS AG London Branch, etc.) are used where known; the rest are filled synthetically. |
| Cost centers             | 130,000  | Distributed across the 5 business divisions per the Q1 2026 operating-expense split (GWM ~50%, IB ~26%, P&C ~14%, AM ~5%, NCL ~3%, Group Items ~2%). |
| Profit centers           | ~95,000  | Includes ~14,000 shared-PC groups for the m:1 migration case. |
| Balances                 | ~1.56 M  | 12 months × 130k cost centers. Active centers have noisy monthly trends; retired centers have zero-or-near-zero amounts; non-USD currencies translated to USD. |
| Entity hierarchy         | 5 levels | Group → Region → Country → Type → Entity. |
| Cost-center hierarchy    | 6 levels | Bank → Division → Function → Department → Team → CC. |

## Distribution shape (default flags)

- **~30%** of cost centers are retire candidates. **NCL** is heavily skewed
  (~85% retire) — this matches the Q1 2026 report's portrayal of Non-core
  and Legacy as a wind-down book. Other divisions sit around 30%.
- **~35%** of cost centers are in **1:n PC groups** (multiple CCs sharing
  one profit center). Groups have **2–6 members each**, average 3.3 — that's
  the canonical SAP m:1 migration shape. Singletons fall through to their
  own PC.
- Activity levels follow a Gaussian distribution clipped to [0, 1]; retire
  candidates have ≤ 5% activity.
- Currency mix per entity matches the country (CHF for Switzerland, USD for
  US/Bermuda/Cayman, EUR for Eurozone, GBP for UK and Jersey, JPY for
  Japan, etc.). Balances are stored in transaction currency AND group
  currency (USD).

## Running

The generator writes to `scope='cleanup'` using `bulk_insert_mappings` for
speed.

```bash
# On the Pi or wherever the backend lives:
cd ~/ampliFi-center-management/backend
source .venv/bin/activate

# Default: 130k CCs, 600 entities, 12 months — about 3-5 min on a Pi 5
# with local Postgres, ~10 min on SQLite.
python scripts/generate_sample_data.py --reset

# Smaller dataset for quick iteration (~30 seconds):
python scripts/generate_sample_data.py --reset --centers 5000 --entities 100 --months 3

# Bigger if you want to stress-test:
python scripts/generate_sample_data.py --reset --centers 500000 --months 24

# See what would be generated without writing anything:
python scripts/generate_sample_data.py --dry-run
```

## All flags

| Flag                | Default     | Meaning |
|---------------------|-------------|---------|
| `--centers`         | 130000      | Number of legacy cost centers |
| `--entities`        | 600         | Number of legal entities |
| `--months`          | 12          | Months of historical balances |
| `--retire-pct`      | 0.30        | Target share of retire candidates |
| `--sharing-pct`     | 0.40        | Target share of CCs in 1:n PC groups |
| `--seed`            | 20260506    | RNG seed for reproducibility |
| `--reset`           | off         | Wipe `scope='cleanup'` before writing |
| `--dry-run`         | off         | Plan only, don't write to DB |

`--seed` is honoured for **everything** — entity codes, CC codes, PC
groupings, balance amounts. Re-running with the same seed produces
byte-identical output, which is useful for screenshots and demos.

## After generation

In the cockpit at `/cockpit`, create a wave with scope `cleanup`. The new
600 entities should appear in the entity picker, the 130k cost centers in
the data browser, and the 12-month history in the balances table. The
existing analyser pipeline (V2) will pick up the m:1 sharing groups
automatically because they share a `pctr` value.

The simulated retire candidates and shared-PC groups give the rule tree
plenty to chew on — expect roughly the same 30% RETIRE / 35% MERGE_MAP
verdicts in the run results once analysis completes, give or take routine
parameters. The new ML predictor and LLM advisor (PR #62, #64) will both
work against this dataset.

## Limitations

- The generator targets **PostgreSQL** (or any other engine that supports
  the `JSONB` column type used in the model layer). It does not work
  against SQLite because the model schema includes `JSONB` columns; for
  SQLite you'd need to substitute `JSON` first — out of scope for this
  script.
- Balances use a single synthetic GL account (`9000`, account_class
  `PERS`). If you need diverse account-class distributions for testing,
  extend `insert_balances` accordingly.
- Hierarchy edges are written deterministically by traversal order. Real
  SAP hierarchies may have additional metadata (e.g. node descriptions,
  status flags) that this script does not populate.
