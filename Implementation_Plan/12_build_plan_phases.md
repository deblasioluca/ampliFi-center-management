# 12 — Phased Build Plan

This is the recommended delivery sequence for an LLM-assisted build, biased toward
**making the manual-upload + decision-tree path usable end-to-end as fast as possible**.
Each phase has explicit exit criteria; nothing in a later phase blocks an earlier
demo.

## Phase 0 — Skeleton & plumbing (week 1)

- Monorepo per §02.2 (frontend, backend, workers, infra).
- docker-compose: postgres, redis, mailhog, backend, frontend, minio.
- Alembic baseline migration with §03 schema.
- FastAPI app with `/healthz`, `/readyz`.
- Astro app with one page hitting `/healthz`.
- CI (lint + tests + OpenAPI spec generation + frontend build).

**Exit:** `docker compose up` works; healthchecks pass; CI green.

## Phase 1 — Auth, users, admin shell (week 2)

- Local auth (login, JWT, password reset) + RBAC dependencies.
- User CRUD + bulk upload.
- Admin shell (navigation, audit log viewer placeholder).
- Database connections config UI (read/write `app_config`).
- LLM endpoint config UI (no actual calls yet).
- Email config UI + SMTP integration with MailHog.

**Exit:** an admin can log in, create users, configure SMTP, send a test email.

## Phase 2 — Manual ingest (week 3)

- Upload wizard for all kinds (§07.7).
- Parsers for the provided MDG `0G` shapes + balance file + hierarchy tables.
- Validation, error CSV download, atomic load with `refresh_batch`.
- Rollback.
- Materialised views (§03.3) and refresh trigger after each load.

**Exit:** an admin can upload the four sample files and the cockpit shows real data
counts on a dashboard.

## Phase 3 — Decision tree (deterministic) (weeks 4–5)

- Routine framework (§04.6) — registry, JSON-Schema params, plugin entry points.
- All built-in rule routines for both trees (§04.1, §04.2).
- DSL rule engine + admin editor.
- `analysis_config` CRUD with versioning.
- `analysis_run` execution, persistence in `routine_output` and `center_proposal`.
- Run cockpit (universe table, why-panel, KPIs).
- Property-based determinism tests + golden corpus tests.

**Exit:** for the loaded sample data, an analyst can run an analysis and see
deterministic KEEP/RETIRE/MERGE_MAP outcomes plus per-center "why".

## Phase 4 — ML routines (weeks 6–7)

- Feature builder pipeline (`feature_set` mview + on-demand fallback).
- LightGBM `outcome_classifier` + `target_object_classifier`. Train on golden corpus.
- Sentence-transformer embeddings + HNSW for `duplicate_cluster`.
- `naming_purpose` head.
- `anomaly_detector` for housekeeping.
- ML routines wired into the registry; SHAP explainability surfaced in why-panel.
- Cockpit: cluster explorer, ML score histograms.

**Exit:** an analyst can enable/disable ML routines and observe their effect on
proposals, with explainability.

## Phase 5 — LLM review (weeks 7–8)

- LLMProvider interface + Azure adapter + BTP adapter.
- Prompt templates (§13).
- SINGLE / SEQUENTIAL / DEBATE pass orchestrators.
- Cost guardrails (per-call, per-pass, daily cap).
- Cache layer (Redis).
- LLM transcript viewer in cockpit.

**Exit:** SINGLE and SEQUENTIAL passes work against Azure; DEBATE works on a small
scope. Cost telemetry visible.

## Phase 6 — Wave lifecycle, naming, sign-off (weeks 8–9)

- Wave CRUD, scope (entities), `is_full_scope` + `exclude_prior` logic.
- Naming convention engine (CC/PC/WBS) with templates, sequences, locking.
- Proposal lock → target_* drafts created with new IDs.
- Review scope CRUD (entity / hierarchy node / list).
- Stakeholder review UI (`/review/{token}`) with three viewing modes.
- Per-item decisions, bulk approve, request new center, completion.
- Wave progress dashboard.

**Exit:** end-to-end demo: upload data → analyse → lock → invite reviewer → reviewer
ticks off → wave moves to `signed_off`.

## Phase 7 — MDG export + comparison (week 10)

- File-based MDG export for CC + PC.
- Run comparison page (§05.8) with cached `analysis_run_diff`.
- Wave close action; export download.

**Exit:** a closed wave produces valid MDG-format files matching the 0G templates.

## Phase 8 — Housekeeping cycle (week 11)

- Monthly cron + manual run.
- Anomaly detector + flag generation.
- Owner email + `/housekeeping/.../owner/{token}` UI.
- Owner sign-off → MDG closure queue.

**Exit:** a monthly cycle runs end-to-end against the target data.

## Phase 9 — OData ingest (weeks 12–13)

- Generic OData client (auth variants, paging, retries, schema introspection).
- Connector configs per source.
- Parallel write to local + datasphere when shadow enabled.
- Scheduled jobs.

**Exit:** an OData connector can refresh balances/master/hierarchy on the same
loader as manual upload.

## Phase 10 — EntraID + Microsoft Graph email (week 14)

- OIDC flow, role mapping.
- Migrate email transport to Graph (parallel SMTP-as-fallback retained).
- User invitation flow in EntraID world.

**Exit:** EntraID becomes the active provider; SMTP only used in dev.

## Phase 11 — MDG API push (weeks 15–16)

- MDG API client + idempotency.
- Status polling.
- Feature flag.

**Exit:** signed-off proposals can be pushed directly to MDG, status reflected back
into the app.

## Phase 12 — Datasphere active store (week 17)

- Migrate active store to Datasphere; local Postgres remains as shadow.
- Performance tuning for analytical queries.

## Phase 13 — Hardening (week 18)

- Load tests at 250k centers, 25 concurrent analysts, 200 concurrent reviewers.
- Penetration test fixes.
- Backup / restore drill.
- Documentation polish.

## 12.1 Risk register & mitigations

| Risk | Mitigation |
|---|---|
| Decision-tree drift between deck and code | Golden-corpus regression tests gate every PR |
| ML training data scarcity (no labels yet) | v1 ships with the rule engine + minimal LightGBM trained on hand-labelled 200; first signed-off wave seeds richer training data |
| LLM cost runaway | Per-pass + daily caps + cache; default mode is SINGLE; DEBATE gated by scope filters |
| OData schema drift | `$metadata` introspection + schema fingerprint; refresh fails fast on drift, raises ops ticket |
| Naming collision when waves overlap | Per-wave reserved sequence ranges + collision policy at engine level |
| Reviewer fatigue at scale | Bulk approve, hierarchy-cascade ticks, three viewing modes, reminders |
| EntraID rollout delays | Auth strategy interface decouples; local auth retained; migration is config-only |
| MDG API not available at v1 | File export is canonical; API client behind feature flag from day one |

## 12.2 Definition of "done" per phase

Each phase has acceptance criteria in §14. CI gates: lint, type-check (mypy + tsc),
unit tests ≥ 90% on `domain/`, integration tests on critical flows, frontend e2e
(Playwright) for the smoke paths.
