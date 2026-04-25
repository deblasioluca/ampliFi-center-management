# 11 — REST API Contracts

OpenAPI 3.1 lives at `/api/openapi.json`. This file is a curated index of the public
endpoints (the implementer LLM should generate the full OpenAPI from FastAPI route
annotations + Pydantic models).

All endpoints are prefixed with `/api`. JSON in/out unless noted.

## 11.1 Auth

| Method | Path | Purpose |
|---|---|---|
| GET  | `/auth/info` | Returns active provider (`local` / `entraid`) + login URL |
| POST | `/auth/login` | Local login (email, password) → access token + sets refresh cookie |
| POST | `/auth/refresh` | Refresh access token using cookie |
| POST | `/auth/logout` | Invalidates refresh token + clears cookie |
| POST | `/auth/password/reset/request` | Send reset email |
| POST | `/auth/password/reset/confirm` | Confirm with token + new password |
| GET  | `/auth/oidc/start` | Begin OIDC flow (redirect) |
| GET  | `/auth/oidc/callback` | OIDC callback handler |
| GET  | `/auth/me` | Current user info |

## 11.2 Admin: users

| Method | Path | Purpose |
|---|---|---|
| GET / POST / PATCH / DELETE | `/admin/users(/{id})` | CRUD |
| POST | `/admin/users/bulk` | Bulk upload CSV/XLSX |
| POST | `/admin/users/{id}/reset_password` | Send reset link |

## 11.3 Admin: configuration

| Method | Path | Purpose |
|---|---|---|
| GET / PUT | `/admin/config/databases` | DB connections |
| POST | `/admin/config/databases/test` | Test connection |
| GET / PUT | `/admin/config/llm` | LLM endpoints + models |
| POST | `/admin/config/llm/test` | Probe LLM with a small prompt |
| GET / PUT | `/admin/config/odata` | (Legacy alias kept; superseded by `/admin/sap`) |
| GET / POST | `/admin/sap` | SAP connections list / create |
| GET / PATCH / DELETE | `/admin/sap/{id}` | Connection detail / update / remove |
| POST | `/admin/sap/{id}/test` | Test a connection (per-protocol probe) |
| GET  | `/admin/sap/{id}/probes` | Probe history |
| POST | `/admin/sap/{id}/discover/{what}` | Discovery (e.g. `hierarchies?class=0101`, `services`, `tables`) |
| GET / POST | `/admin/sap/{id}/bindings` | Object bindings (cost_center, profit_center, hierarchies, balances, …) |
| PATCH / DELETE | `/admin/sap/{id}/bindings/{bid}` | Update / remove binding |
| POST | `/admin/sap/{id}/bindings/{bid}/preview` | Pull a sample (50 rows) for the wizard |
| POST | `/admin/sap/{id}/bindings/{bid}/pull` | Pull full payload into a new upload_batch |
| GET / PUT | `/admin/config/email` | Email provider + templates |
| GET / PUT | `/admin/config/naming/{object_type}` | Naming convention (cc / pc / wbs) |
| POST | `/admin/config/naming/{object_type}/preview` | Preview new IDs for sample |
| POST | `/admin/config/naming/{object_type}/reserve` | Reserve a sequence range |

## 11.4 Admin: uploads (manual ingest)

| Method | Path | Purpose |
|---|---|---|
| POST | `/admin/uploads` | Multipart upload (kind, file) |
| GET  | `/admin/uploads` | List with filters |
| GET  | `/admin/uploads/{id}` | Detail |
| POST | `/admin/uploads/{id}/validate` | Run validation |
| POST | `/admin/uploads/{id}/load` | Load into live tables |
| POST | `/admin/uploads/{id}/rollback` | Rollback |
| GET  | `/admin/uploads/{id}/errors` | Errors (paginated; CSV via Accept) |

## 11.5 Admin: routines

| Method | Path | Purpose |
|---|---|---|
| GET  | `/admin/routines` | List registered routines |
| POST | `/admin/routines/dsl` | Create DSL custom rule |
| PATCH | `/admin/routines/{code}` | Toggle / edit DSL params |
| POST | `/admin/routines/reload` | Re-import plugins + DSL |

## 11.6 Analytics: configurations & runs

| Method | Path | Purpose |
|---|---|---|
| GET / POST | `/configs` | List / create analytical configurations |
| GET  | `/configs/{code}` | Get latest version of a config |
| GET  | `/configs/{code}/versions` | List all versions |
| POST | `/configs/{code}/fork` | Fork (create child) |
| POST | `/configs/{code}/amend` | Amend (new version under same code) |
| POST | `/configs/{code}/validate` | Schema-validate the config |
| POST | `/configs/{code}/preview` | Sample-impact preview |

| Method | Path | Purpose |
|---|---|---|
| POST | `/waves/{id}/runs` | Start an analysis_run with a config |
| GET  | `/waves/{id}/runs` | List runs for a wave |
| GET  | `/runs/{id}` | Run detail (KPIs + status) |
| POST | `/runs/{id}/cancel` | Cancel a running run |
| GET  | `/runs/{id}/proposals` | Paginated list of proposals (filterable) |
| GET  | `/runs/{id}/proposals/{proposal_id}/why` | Why-panel content |
| POST | `/runs/{id}/proposals/{proposal_id}/override` | Analyst override |
| GET  | `/runs/{a}/diff/{b}` | Compare two runs |
| GET  | `/runs/{id}/llm` | LLM review pass details + transcripts |

## 11.7 Waves

| Method | Path | Purpose |
|---|---|---|
| GET / POST / PATCH | `/waves(/{id})` | CRUD |
| POST | `/waves/{id}/cancel` | Cancel pre-lock |
| POST | `/waves/{id}/proposal/lock` | Lock |
| POST | `/waves/{id}/proposal/unlock` | Unlock (only if no scopes invited) |
| POST | `/waves/{id}/signoff` | Mark signed off (auto when all scopes complete) |
| POST | `/waves/{id}/close` | Close + trigger MDG export |
| GET / POST | `/waves/{id}/scopes` | List/create review scopes |
| POST | `/waves/{id}/scopes/{scope_id}/invite` | Send invite emails |
| POST | `/waves/{id}/scopes/{scope_id}/remind` | Send reminder |
| GET  | `/waves/{id}/progress` | Progress dashboard |
| GET  | `/waves/{id}/exports` | List generated exports |
| POST | `/waves/{id}/exports/regenerate` | Re-generate exports (admin) |

## 11.8 Reviewer (token-scoped)

| Method | Path | Purpose |
|---|---|---|
| GET  | `/review/{token}` | Scope summary |
| GET  | `/review/{token}/items` | Items in scope (filter, paginate, mode-aware) |
| POST | `/review/{token}/items/{item_id}/decide` | Approve / Not required / Comment |
| POST | `/review/{token}/items/bulk-decide` | Bulk approve a hierarchy node / list |
| POST | `/review/{token}/requests` | Request a new center |
| POST | `/review/{token}/complete` | Final sign-off |

## 11.9 Housekeeping

| Method | Path | Purpose |
|---|---|---|
| GET  | `/admin/housekeeping/cycles` | List cycles |
| POST | `/admin/housekeeping/run` | Run an ad-hoc cycle |
| GET  | `/admin/housekeeping/cycles/{id}` | Detail + summary |
| GET  | `/housekeeping/{cycle_id}/owner/{token}` | Owner view |
| POST | `/housekeeping/{cycle_id}/owner/{token}/decide` | Owner per-item decision |
| POST | `/housekeeping/{cycle_id}/owner/{token}/complete` | Owner submission |

## 11.10 Reference data (read endpoints)

| Method | Path | Purpose |
|---|---|---|
| GET | `/entities` | Entity list |
| GET | `/legacy/cost-centers` | Filtered legacy CCs |
| GET | `/legacy/profit-centers` | Filtered legacy PCs |
| GET | `/legacy/hierarchies` | Hierarchies (sets, with optional tree expansion) |
| GET | `/balances/aggregates` | Aggregate balance queries (ccode, period, account_class) |

## 11.11 Health & ops

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | Liveness |
| GET | `/readyz` | Readiness (DB, Redis, LLM provider reachable) |
| GET | `/metrics` | Prometheus metrics |
| GET | `/admin/jobs` | Task runs |
| POST | `/admin/jobs/{run_id}/cancel` | Cancel task |

## 11.12 Conventions

- Pagination: `?page=N&size=M` (default size 100, max 1000); responses include
  `total`, `page`, `size`, `items`.
- Filtering: per-resource query params (e.g. `?outcome=KEEP&cctr=23*`).
- Sorting: `?sort=field,-other_field`.
- Errors: RFC 7807 problem+json with `type`, `title`, `status`, `detail`,
  `instance`, plus `errors[]` for validation issues.
- Idempotency: state-changing endpoints accept `Idempotency-Key` header; results are
  cached for 24h keyed by `(actor, key)`.
