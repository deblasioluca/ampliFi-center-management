# 14 — Acceptance Criteria (Definition of Done per module)

Each section below mirrors a module in this spec and lists the verifiable conditions
that must hold before the module is considered done.

## 14.1 Architecture & build (§02)

- [ ] `docker compose up` brings up postgres, redis, mailhog, backend, frontend,
  minio. `/healthz` and `/readyz` return 200.
- [ ] CI: lint (ruff + eslint), type-check (mypy + tsc), unit + integration tests
  pass on every PR. Coverage on `backend/app/domain/*` ≥ 90%.
- [ ] OpenAPI spec generated; TS client builds; CI fails on drift.
- [ ] Domain layer has zero imports from `infra/`.

## 14.2 Data ingest (§03, §07.7)

- [ ] All four sample files (`balance_structure.xlsx`, `Center_structure.xlsx`,
  `0G_Cost center upload_1.3 3.xlsx`, `0G_Profit center upload_1.3 2.xlsx`) load
  cleanly through the upload wizard.
- [ ] Hierarchy round-trip: load SETHEADER/SETNODE/SETLEAF; render the tree; export
  it back as identical CSVs.
- [ ] Validation produces actionable, downloadable error CSVs.
- [ ] Atomic load: a failure mid-load leaves the live tables untouched.
- [ ] Rollback restores prior `refresh_batch` and analyses pinned to the older batch
  remain functional.

## 14.3 Decision tree (§04)

- [ ] Both trees implemented; outcomes match the deck on 100% of the golden corpus
  (200 centers).
- [ ] Property-based tests (Hypothesis) prove determinism: same input → same output
  across 10k random executions.
- [ ] Every routine has unit tests for true / false / UNKNOWN paths.
- [ ] Why-panel shows the rule path with verdicts and feature values.
- [ ] DSL rule editor: an admin creates a rule via the UI and it executes within the
  same run (no redeploy).

## 14.4 Analytics framework (§05)

- [ ] Analytical configurations support save / amend / fork / archive with version
  history.
- [ ] Run comparison (`/runs/{a}/diff/{b}`) shows outcome matrix, target-object
  matrix, changed centers, and renders within 1s on cached results.
- [ ] LightGBM models trained, registered, scored; SHAP top-5 visible per
  prediction.
- [ ] Duplicate clustering produces stable cluster ids across runs on the same
  snapshot.
- [ ] LLM review: SINGLE, SEQUENTIAL, DEBATE all produce per-center commentary,
  recorded with prompt hash + model + tokens + cost.
- [ ] Per-pass and daily cost caps trigger correctly when exceeded.
- [ ] Plugin entry point: a sample plugin in `examples/cleanup_plugin_example/`
  installs and registers a new routine after `routines/reload`.

## 14.5 Cockpit (§06)

- [ ] Universe table renders 250k rows interactively (virtualised).
- [ ] Hierarchy view side-by-side (legacy vs proposed) cross-highlights nodes.
- [ ] Sankey, coverage map, cluster explorer, naming preview all live and update
  with run filters.
- [ ] Override: analyst can override a proposal with a required reason; original
  rule_path is preserved in audit.

## 14.6 Wave lifecycle (§08)

- [ ] States transition only along the documented graph; illegal transitions return
  409.
- [ ] `is_full_scope=true` + `exclude_prior=true` correctly excludes entities from
  past/active waves.
- [ ] Reserved naming sequence ranges; conflict between two waves blocked; admin can
  resolve.
- [ ] Lock creates draft target_* rows with new IDs per the active naming
  convention.
- [ ] Cancel releases reserved naming ranges and removes draft target_* rows.

## 14.7 Stakeholder review (§06.7–6.9)

- [ ] Three viewing modes (legacy list / legacy hierarchy / proposed) all render
  the reviewer's scope correctly.
- [ ] Per-item: Approve, Not Required, Comment, Request New Center.
- [ ] Bulk approve via hierarchy node cascades correctly.
- [ ] Final sign-off blocked while any item is PENDING; clear list of blockers.
- [ ] Review token: scoped, expires after 30 days, revocable, audited.

## 14.8 Housekeeping (§08.5)

- [ ] Monthly cron creates a `housekeeping_cycle`; ad-hoc trigger works.
- [ ] Anomaly detection + posting-activity flags labelled correctly.
- [ ] One digest email per owner; T+7 / T+14 / T+21 reminders sent automatically.
- [ ] Owner sign-off captures KEEP / CLOSE / DEFER per item; CLOSE queues MDG
  closure.

## 14.9 Integrations (§09)

- [ ] OData connector loads balances/master/hierarchy from a stub SAP service into
  the same tables as manual upload (using shared validation/loader).
- [ ] Azure + BTP LLM providers both pass health checks and complete a small prompt
  with cost telemetry.
- [ ] SMTP send works with rate-limit; MailHog shows messages in dev.
- [ ] MDG file export produces XLSX matching the 0G template (header rows, sheet name
  `Data`, exact columns).
- [ ] (Phase 2) MDG API push is feature-flagged and can submit a test change request.

## 14.10 Auth & security (§10)

- [ ] Local login works with bcrypt + JWT + httpOnly refresh cookie.
- [ ] Brute-force lockout triggers after 5 failures.
- [ ] EntraID OIDC: full flow with PKCE works end-to-end against a tenant; group
  mapping populates roles.
- [ ] CSP, HSTS, X-Frame-Options, X-Content-Type-Options, Referrer-Policy all set.
- [ ] CSRF: state-changing endpoints require `X-Requested-With` header.
- [ ] Audit log captures all listed actions; append-only.
- [ ] Secrets never appear in logs (verified by automated log scan in CI).

## 14.11 Performance

- [ ] Deterministic tree on 250k centers: < 5 min.
- [ ] LightGBM scoring on 250k centers: < 10 min.
- [ ] LLM SEQUENTIAL pass on 5k centers (3 stages, gpt-4o): < 30 min.
- [ ] Cockpit interactive filtering: < 250ms client / < 1.5s server.
- [ ] Concurrent reviewers ≥ 200 without throughput collapse.

## 14.12 Documentation

- [ ] This spec bundle remains in `spec/` and is updated alongside any breaking
  change.
- [ ] Runbook (`docs/RUNBOOK.md`) covers: deploy, restart, rollback, common
  incidents.
- [ ] User guide (`docs/USER_GUIDE.md`) covers: analyst flow, reviewer flow, owner
  flow.
- [ ] Admin guide (`docs/ADMIN_GUIDE.md`) covers all `/admin/*` views.
