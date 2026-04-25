# 02 — Architecture

## 2.1 Tech stack (decided)

| Layer | Technology |
|---|---|
| Frontend | **Astro 4.x** with the `@astrojs/node` SSR adapter, TypeScript, Tailwind, Astro UI islands (React or Svelte — implementer choice) |
| Backend API | **FastAPI 0.110+** (Python 3.11+), Uvicorn, Pydantic v2 |
| ORM / migrations | SQLAlchemy 2.x + Alembic |
| Background workers | **Celery 5** (preferred) on Redis 7. RQ is acceptable if Celery is overkill for the deploy target |
| Local DB | **PostgreSQL 15+** (recommended) |
| Cloud DB | **SAP Datasphere** on BTP (configurable as the active store) |
| Object cache | Redis (also broker) |
| LLM | Azure OpenAI + SAP BTP Generative AI Hub (abstracted) |
| Email | SMTP (v1), Microsoft Graph (v2 alongside EntraID) |
| Auth | Local (bcrypt + JWT) v1; Azure EntraID OIDC v2 |
| Observability | OpenTelemetry traces, Prometheus metrics, structured JSON logs |
| Container | Docker; docker-compose for local dev; production deploy is Kubernetes-ready |

## 2.2 Service decomposition (monorepo)

```
ampliFi-cleanup/
├── frontend/                       # Astro app (SSR)
│   ├── src/
│   │   ├── pages/                  # Astro pages (file-based routing)
│   │   │   ├── index.astro         # Entry / login
│   │   │   ├── cockpit/            # Analyst cockpit
│   │   │   ├── wave/[id]/          # Wave detail
│   │   │   ├── review/[token]/     # Stakeholder review (tokenised)
│   │   │   ├── housekeeping/       # Owner sign-off form
│   │   │   └── admin/              # Admin views
│   │   ├── components/
│   │   ├── lib/api.ts              # Typed FastAPI client (generated from OpenAPI)
│   │   └── stores/                 # Client state (nano stores or similar)
│   ├── public/
│   ├── astro.config.mjs
│   └── package.json
├── backend/                        # FastAPI service
│   ├── app/
│   │   ├── main.py                 # FastAPI app factory
│   │   ├── config.py               # Settings (env-driven, pydantic-settings)
│   │   ├── api/                    # Routers, one per resource
│   │   ├── domain/                 # Pure business logic (no I/O)
│   │   │   ├── decision_tree/      # Cleansing + Mapping rules (§04)
│   │   │   ├── ml/                 # Feature builders, models (§05)
│   │   │   ├── naming/             # Naming convention engine (§07)
│   │   │   └── proposal/           # Proposal builder, lock state machine
│   │   ├── infra/                  # I/O adapters (DB, LLM, OData, email)
│   │   │   ├── db/
│   │   │   ├── llm/                # azure_openai.py, sap_btp.py + base interface
│   │   │   ├── odata/
│   │   │   ├── email/
│   │   │   └── mdg/                # MDG file export + future API client
│   │   ├── workers/                # Celery tasks
│   │   ├── auth/                   # Local + EntraID strategies
│   │   └── models/                 # SQLAlchemy ORM
│   ├── alembic/
│   ├── tests/
│   ├── pyproject.toml
│   └── Dockerfile
├── workers/                        # (thin wrapper; tasks live in backend/app/workers)
├── infra/
│   ├── docker-compose.yml          # Postgres, Redis, backend, frontend, mailhog
│   ├── k8s/                        # Manifests (deferred)
│   └── seed/                       # Seed scripts for dev
└── spec/                           # This spec bundle
```

Domain code (`backend/app/domain/`) MUST NOT import from `backend/app/infra/`. The
direction of dependency is `api → domain → infra` (via interfaces). This keeps the
decision tree, ML, and naming engines unit-testable without a database.

## 2.3 Runtime topology

```
                         +------------------------+
   Browser <-- HTTPS --> |  Astro SSR (Node)      |
                         |  - serves UI           |
                         |  - calls FastAPI on    |
                         |    server side         |
                         +-----------+------------+
                                     |
                                     v
                         +------------------------+
                         |  FastAPI (Uvicorn)     |
                         |  - REST API            |
                         |  - JWT / OIDC auth     |
                         +-----+--------+---------+
                               |        |
            +------------------+        +---------------------+
            v                                                v
     +-------------+                                +------------------+
     |  Postgres   |                                | Redis (cache +   |
     |  (active or |                                | Celery broker)   |
     |  Datasphere)|                                +--------+---------+
     +------+------+                                         |
            ^                                                v
            |                                       +------------------+
            +<--------- Celery workers ------------ | Worker pool      |
                          |       |                 | - odata.refresh  |
                          v       v                 | - ml.score       |
                  +-----------+ +-----------+       | - email.send     |
                  | SAP OData | |  LLM      |       | - mdg.export     |
                  | endpoints | |  (Azure / |       +------------------+
                  +-----------+ |   BTP)    |
                                +-----------+
```

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

## 2.5 Configuration & secrets

- Settings layered (lowest → highest): defaults in code → `.env` → environment variables
  → admin-UI overrides persisted in the `app_config` table → request-scoped overrides
  (rare).
- Secrets (DB passwords, OData credentials, LLM API keys, SMTP passwords) are stored
  encrypted at rest in `app_config_secret` using AES-GCM with a key from `APP_SECRET_KEY`
  env var (or BTP credential store in BTP-deployed mode).
- Never log secrets. Pydantic `SecretStr` everywhere they appear in DTOs.

## 2.6 Background jobs catalogue

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

## 2.7 Frontend ↔ backend contract

- Backend exposes OpenAPI 3.1 at `/api/openapi.json`.
- A typed TS client is generated into `frontend/src/lib/api.ts` (e.g. `openapi-typescript`
  + `openapi-fetch`). CI fails if the client is out-of-date relative to the spec.
- Astro pages (SSR) call the backend with the user's JWT forwarded; client islands call
  through `/api/...` proxied via Astro middleware (so the JWT cookie travels).

## 2.8 Local development

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

## 2.9 Environments

| Env | Purpose | DB | LLM |
|---|---|---|---|
| `dev` | Local | Postgres in compose | Azure dev tenant |
| `test` | CI | Postgres ephemeral | LLM mocked |
| `uat` | UAT | Postgres on shared host | Azure prod tenant (rate-limited) |
| `prod` | Live | Postgres → migrate to Datasphere | Azure prod / BTP |

## 2.10 Diagrams to generate (implementer task)

Implementer MUST produce, in `spec/diagrams/`:
- C4 Level 1 (system context)
- C4 Level 2 (containers)
- ER diagram of the application DB (auto-generated from SQLAlchemy)
- Sequence diagrams for: wave lock → review → sign-off; OData refresh; housekeeping cycle
