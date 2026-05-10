# 02 — Architecture

## 2.1 Tech stack (decided)

| Layer | Technology |
|---|---|
| Frontend | **Astro 4.x** (static build served by backend), TypeScript, Tailwind, React islands (via `@astrojs/react`) |
| Backend API | **FastAPI 0.110+** (Python 3.11+), Uvicorn, Pydantic v2 |
| ORM / migrations | SQLAlchemy 2.x + Alembic |
| Background workers | **Celery 5** (preferred) on Redis 7. RQ is acceptable if Celery is overkill for the deploy target |
| Local DB | **PostgreSQL 15+** (recommended) |
| Cloud DB | **SAP Datasphere** on BTP (configurable as the active store) |
| Object cache | Redis (also broker) |
| LLM | Azure OpenAI + SAP BTP Generative AI Hub (abstracted) |
| Email | SMTP (v1), Microsoft Graph (v2 alongside EntraID) |
| Auth | Local (bcrypt + JWT) v1; Azure Entra ID MSAL SPA (PKCE) v2 |
| Observability | OpenTelemetry traces, Prometheus metrics, structured JSON logs |
| Container | Docker; docker-compose for local dev; production deploy is Kubernetes-ready |

## 2.2 Service decomposition (monorepo)

```
ampliFi-center-management/
├── frontend/                       # Astro app (built to static, served by backend)
│   ├── src/
│   │   ├── pages/                  # Astro pages (file-based routing)
│   │   │   ├── index.astro         # Dashboard (wave progress, system stats)
│   │   │   ├── login.astro         # Authentication (local + optional Entra ID)
│   │   │   ├── cockpit/            # Waves & Analysis cockpit
│   │   │   │   ├── index.astro     # Wave list, scope coverage, global stats
│   │   │   │   ├── wave.astro      # Wave detail (tabs: Scope, Analysis, Simulation, Proposals, Review, Progress)
│   │   │   │   ├── analytics.astro # Analytics charts
│   │   │   │   ├── compare.astro   # Run comparison
│   │   │   │   ├── pipeline.astro  # Pipeline editor
│   │   │   │   ├── run.astro       # Run detail view
│   │   │   │   └── ...             # housekeeping, mdg-export, cluster, data-quality, llm-review
│   │   │   ├── admin/              # System administration
│   │   │   │   ├── index.astro     # Users, Decision Trees, Routines, App Config, System Health
│   │   │   │   ├── sap.astro       # SAP connections
│   │   │   │   ├── rules.astro     # Rule builder
│   │   │   │   ├── templates.astro # Wave templates
│   │   │   │   └── ...             # llm, email, naming, datasphere, explorer, audit, jobs, logs
│   │   │   ├── data/index.astro    # Data management (upload, browse)
│   │   │   ├── explore.astro       # Public data explorer
│   │   │   ├── review/             # Stakeholder review (token-based)
│   │   │   ├── tasks.astro         # My Tasks
│   │   │   ├── activity.astro      # Activity feed
│   │   │   └── setup.astro         # Setup wizard
│   │   └── components/             # Reusable components
│   ├── public/
│   ├── astro.config.mjs
│   └── package.json
├── backend/                        # FastAPI service
│   ├── app/
│   │   ├── main.py                 # FastAPI app factory (+ serves frontend static files)
│   │   ├── config.py               # Settings (env-driven, pydantic-settings)
│   │   ├── api/                    # Routers (admin, auth, configs, waves, runs, review, stats, etc.)
│   │   ├── domain/                 # Pure business logic (no I/O)
│   │   │   ├── decision_tree/      # V1 cleansing + mapping rules (§04)
│   │   │   │   └── routines/       # Built-in routines (posting, ownership, redundancy, etc.)
│   │   │   ├── ml/                 # Feature builders, models (§05)
│   │   │   ├── naming/             # Naming convention engine (§07)
│   │   │   └── proposal/           # Proposal builder, lock state machine
│   │   ├── services/               # Business services
│   │   │   ├── analysis.py         # V1 analysis execution
│   │   │   ├── analysis_v2.py      # V2 CEMA-based migration engine
│   │   │   ├── seed.py             # Data seeding
│   │   │   └── upload_*.py         # Upload processors (cc_hierarchy, sap_hierarchy, etc.)
│   │   ├── infra/                  # I/O adapters (DB, LLM, OData, email)
│   │   ├── auth/                   # Local + Entra ID (MSAL) strategies
│   │   └── models/                 # SQLAlchemy ORM
│   ├── alembic/                    # Database migrations
│   ├── tests/                      # Test suite (V1 + V2 engine, API tests)
│   ├── pyproject.toml
│   └── Dockerfile
├── scripts/                        # Setup and deployment scripts
├── Implementation_Plan/            # Specification documents (21 files)
├── docs/                           # Deployment guides
├── Makefile                        # Project commands (start, stop, setup, update, etc.)
├── .env.example                    # Environment template
└── docker-compose.yml              # Docker deployment (optional)
```

Domain code (`backend/app/domain/`) MUST NOT import from `backend/app/infra/`. The
direction of dependency is `api → domain → infra` (via interfaces). This keeps the
decision tree, ML, and naming engines unit-testable without a database.

## 2.3 Runtime topology

```
                         +-----------------------------------+
   Browser <-- HTTP/S -> |  FastAPI (Uvicorn, port 8180)    |
                         |  - serves static frontend        |
                         |  - REST API (/api/*)             |
                         |  - JWT / Entra ID (MSAL) auth    |
                         +-----+--------+-------------------+
                               |        |
            +------------------+        +---------------------+
            v                                                v
     +-------------+                                +------------------+
     |  Postgres   |                                | Redis (cache +   |
     |  (active or |                                | Celery broker,   |
     |  Datasphere)|                                | optional)        |
     +------+------+                                +--------+---------+
            ^                                                |
            |                                                v
            +<--------- Celery workers ------------ +------------------+
                          |       |                 | Worker pool      |
                          v       v                 | - odata.refresh  |
                  +-----------+ +-----------+       | - ml.score       |
                  | SAP OData | |  LLM      |       | - email.send     |
                  | endpoints | |  (Azure / |       | - mdg.export     |
                  +-----------+ |   BTP)    |       +------------------+
                                +-----------+
```

> **Note (implementation status):** The frontend is built to static HTML/JS/CSS by Astro
> and served by the FastAPI backend via `StaticFiles`. There is no separate Node SSR process
> in production. The Astro pages use client-side `<script>` blocks for interactivity
> (fetch calls to `/api/*`).

## 2.4 Data store strategy

The application supports **two** data stores: Postgres (local) and SAP Datasphere on BTP.
At any moment exactly one is the **active** store (writes go there + analyses read from
there). The other is a **shadow** store that can be kept in sync.

Configuration (admin UI, §07):
- `db.active = "postgres" | "datasphere"`
- `db.shadow_enabled = true | false`
- `db.shadow = "postgres" | "datasphere" | null`

OData refresh writes to **both** when `shadow_enabled` is true. Analyses always read
from the active store.

Implementation: a `DataStore` interface in `backend/app/infra/db/` with two
implementations. The session factory selects based on the active config row, cached for
the request lifetime.

## 2.5 Scope segregation

The application maintains **three distinct scopes** of data, each representing a separate
logical workspace. All scope-aware tables (cost centers, profit centers, entities,
hierarchies, balances, GL accounts, employees, uploads) carry a `scope` column:

| Scope | Purpose | Application |
|---|---|---|
| `cleanup` | Primary workspace — center rationalisation, analysis, wave management | Cockpit (all wave/analysis/review pages) |
| `housekeeping` | Staging area — periodic health checks, inactive center flagging | Housekeeping cycles |
| `explorer` | Public data visualisation — read-only curated view | Data Explorer standalone (`/explore`) |

**Key rules:**
- Each scope is a **completely independent data universe**. Data loaded into `cleanup` never
  bleeds into `explorer` or vice-versa.
- Within each scope, data is further classified by **data category**: `legacy` (source SAP)
  or `target` (ampliFi-processed / MDG-ready).
- The **Data Management** page (`/data`) allows the user to select scope + category at the
  top; all operations (upload, SAP extraction, KPIs, data display) are scoped accordingly.
- The **Cockpit** pages always operate in `cleanup` scope.
- The **Data Explorer** standalone page always operates in `explorer` scope (hardcoded in backend).
- Upload batches, SAP bindings, and all data records are tagged with both `scope` and
  `data_category` for consistent filtering.

### Target vs Legacy tables

| Category | Tables | Description |
|---|---|---|
| Legacy | `legacy_cost_center`, `legacy_profit_center`, `entity`, `balance`, `hierarchy`, `gl_account_ska1`, `gl_account_skb1`, `employee` | Source SAP data as-is |
| Target | `target_cost_center`, `target_profit_center` (dedicated tables) + `entity`, `hierarchy`, `gl_account_ska1`, `gl_account_skb1` (filtered by `data_category='target'`) | ampliFi-processed data ready for MDG export |

## 2.6 Configuration & secrets

- Settings layered (lowest → highest): defaults in code → `.env` → environment variables
  → admin-UI overrides persisted in the `app_config` table → request-scoped overrides
  (rare).
- Secrets (DB passwords, OData credentials, LLM API keys, SMTP passwords) are stored
  encrypted at rest in `app_config_secret` using AES-GCM with a key from `APP_SECRET_KEY`
  env var (or BTP credential store in BTP-deployed mode).
- Never log secrets. Pydantic `SecretStr` everywhere they appear in DTOs.

## 2.7 Background jobs catalogue

| Task | Trigger | SLA |
|---|---|---|
| `odata.full_refresh` | Manual (admin) or scheduled (cron) | ≤ 4h |
| `odata.delta_refresh` | Scheduled hourly | ≤ 30 min |
| `ml.score_universe` | Triggered by analysis run | ≤ 30 min |
| `dt.run_cleansing` | Triggered by analyst | ≤ 5 min |
| `dt.run_mapping` | Triggered after cleansing | ≤ 5 min |
| `proposal.lock` | Analyst action | seconds |
| `email.send_batch` | After lock / housekeeping | rate-limited |
| `mdg.export_files` | After sign-off | seconds |
| `mdg.api_push` | Future / phase 3 | seconds |
| `housekeeping.monthly` | Cron (1st of month) | hours |

Each task is idempotent: tasks accept a `run_id` (UUID), persist progress in
`task_run`, and reading the same `run_id` returns the same result. Retries are safe.

## 2.8 Frontend ↔ backend contract

- Backend exposes OpenAPI 3.1 at `/api/openapi.json`.
- A typed TS client is generated into `frontend/src/lib/api.ts` (e.g. `openapi-typescript`
  + `openapi-fetch`). CI fails if the client is out-of-date relative to the spec.
- Astro pages are built to static HTML and served by the FastAPI backend. Client-side
  `<script>` blocks call `/api/...` directly with the JWT from `localStorage`.

## 2.9 Local development

```
docker compose up
# ↳ brings up: postgres, redis, mailhog, backend (uvicorn --reload), frontend (astro dev)
```

Seed:

```
docker compose exec backend python -m app.cli seed --sample
```

This loads a small synthetic dataset (~1,000 centers, 5 LEs, 6 months of balances) so the
implementer can demo the full flow without SAP access.

## 2.10 Environments

| Env | Purpose | DB | LLM |
|---|---|---|---|
| `dev` | Local | Postgres in compose | Azure dev tenant |
| `test` | CI | Postgres ephemeral | LLM mocked |
| `uat` | UAT | Postgres on shared host | Azure prod tenant (rate-limited) |
| `prod` | Live | Postgres → migrate to Datasphere | Azure prod / BTP |

## 2.11 Diagrams to generate (implementer task)

Implementer MUST produce, in `spec/diagrams/`:
- C4 Level 1 (system context)
- C4 Level 2 (containers)
- ER diagram of the application DB (auto-generated from SQLAlchemy)
- Sequence diagrams for: wave lock → review → sign-off; OData refresh; housekeeping cycle
