# 21 — Instructions for the Implementing LLM

These are the standing rules the implementer LLM (and any human collaborator) must
follow when building, extending, or maintaining this application. They take
precedence over inferred conventions, but are subordinate to explicit user
requests in the project repo.

## 21.1 Documentation discipline (rule #1)

- **Always document.** Every feature ships with documentation, in the same change
  set. No PR may merge that adds a feature, route, configuration, or skill
  without:
  - updating the relevant **spec/** file (functional spec — this folder),
  - updating the **code-level docs** (docstrings, type hints, code comments where
    rationale is non-obvious),
  - updating the **application documentation** (`docs/admin/`, `docs/analyst/`,
    `docs/operator/`, etc., per §19),
  - updating the **end-user documentation** surfaced from the in-app Help drawer
    (per §19.2),
  - if relevant, updating the **runbook** (`docs/RUNBOOK.md`).
- **Keep documentation up to date.** When code changes, the docs change in the
  same PR. CI lint enforces:
  - All `helpKey`s referenced from the frontend exist as docs.
  - All admin / cockpit pages declare a `helpKey`.
  - No `TODO` / `TBD` / `Lorem ipsum` placeholders.
  - Every doc page's `updated:` front-matter is bumped when its content changed.
- **Use the spec as the source of truth.** When the spec and code disagree, fix
  whichever is wrong — but do not silently diverge. Add an ADR
  (`docs/adr/NNNN-title.md`) explaining the change and link it from the spec
  section that changed.
- **ADR for every notable decision.** Architectural choices, library swaps,
  schema decisions, breaking API changes → ADR. Use the
  Michael-Nygard short ADR template.

## 21.2 Engineering principles

- **Spec first, code second.** Read the entire `spec/` bundle before writing code
  in a new module. If the spec is ambiguous, write the question into
  `spec/15_open_questions.md` and resolve before implementing.
- **Domain pure, infra dirty.** `backend/app/domain/*` MUST NOT import from
  `backend/app/infra/*`. Cross via interfaces (Protocols).
- **Determinism is sacred.** The decision tree's outcomes are deterministic for a
  given (data_snapshot, analysis_config). Property-based tests prove it.
  ML scores and LLM commentary may vary; tree verdicts cannot.
- **No silent defaults.** Every default is documented in the spec or an ADR. Hidden
  conventions are not allowed.
- **Strict types end-to-end.** Pydantic v2 models on every boundary; mypy strict
  on `domain`; TypeScript strict in the frontend.
- **No `eval`. No raw SQL from user input.** The DSL rule engine (§04.6.C) is a
  whitelisted interpreter.
- **Idempotency by default.** State-changing endpoints accept `Idempotency-Key`;
  Celery tasks accept `run_id`. Retries are safe.
- **Feature flags for risky paths.** OData ingestion, MDG API, EntraID,
  Datasphere active-store — all behind feature flags surfaced in `/admin/config`.

## 21.3 Code quality bar

- Lint: ruff + black for Python, eslint + prettier for TypeScript, sqlfluff for
  SQL.
- Types: mypy `--strict` on `backend/app/domain/`; tsc `--strict`.
- Tests: pytest for backend, vitest + Playwright for frontend.
- Coverage: domain ≥ 90%, infra ≥ 75%.
- CI gates: lint, type, unit, integration, OpenAPI consistency, docs lint, golden
  corpus regression, security scan (bandit + npm audit).

## 21.4 Security & privacy defaults

- Pydantic `SecretStr` for all secrets; no secrets in logs (CI grep blocks).
- Tokens scoped, expiring, revocable.
- All state-changing endpoints write audit log rows.
- LLM inputs fenced; data sanitised.
- Default to "deny" on permission checks; widen explicitly.

## 21.4b Universal download rule (cross-cutting)

**Wherever data is displayed, the user MUST be able to download it.** This
applies equally to admin and end-user surfaces.

### What "download" means

- Every table, chart, hierarchy view, transcript, audit table, statistics tile,
  proposal list, or review list MUST expose a **Download** affordance.
- The download menu offers, at minimum:
  - **CSV** (raw underlying rows respecting current filters).
  - **XLSX** (formatted, sheet-named, useful for downstream pivot work).
  - For charts: **PNG**, **SVG**, plus the underlying CSV.
  - For documents (run summary, scope review pack, housekeeping summary):
    **PDF** and **DOCX**.
  - For exports that target other systems: their native format (e.g. MDG XLSX
    matching the 0G template).
- The download contains **only what the user is permitted to see**. RBAC and
  scope filters are enforced server-side; the download endpoint MUST NOT bypass
  authorisation just because the user can see the rendered view.

### How to implement consistently

- Each list endpoint accepts `Accept: text/csv` and `Accept: application/vnd.openxmlformats…`
  in addition to JSON. Frontend tables call the same endpoint with the new Accept
  header.
- Each chart endpoint accepts `?format=png|svg|csv` (see §20.4).
- A shared `<DownloadMenu>` component sits in the frontend; every page using a
  table or chart is REQUIRED to include it.
- Generated downloads of more than ~50 MB run as a Celery `export.generate` job
  and are emailed to the user when ready, with a deep-link to download from
  `/api/exports/{id}/file`. (Avoid timing-out user requests on huge exports.)
- Filenames are deterministic and informative:
  `{wave|run|cycle}_{kind}_{YYYYMMDDTHHMM}.{ext}` (e.g.
  `wave-2026-q3-apac_proposals_20260425T1530.xlsx`).

### Audit & rate-limit

- Every download is audit-logged with: actor, route, filter context, byte size,
  format.
- Anomaly detection on exports (mass exfil patterns) raises an alert and (if
  configured) requires a TOTP confirmation for further large exports in the
  session.
- Rate-limit configurable: default 30 downloads/min/user, with size-aware
  throttling for very large XLSX/CSV.

### Acceptance

- [ ] Universal `<DownloadMenu>` shipped; every list and chart in the application
  uses it.
- [ ] CSV/XLSX/PNG/SVG/PDF export works for every visible data surface.
- [ ] Big-export job mode triggers the email-when-ready flow.
- [ ] RBAC is enforced on every export endpoint (regression tests).
- [ ] Audit log captures every export.

## 21.5 Performance discipline

- Materialised views for hot aggregates; refresh on data load and run completion.
- Query plans reviewed for any new endpoint serving > 1k rows.
- Use streaming JSON parsing on large inputs.
- Pre-compute statistics (§20.8) into Redis cache; TTL 60s, busted on events.

## 21.6 LLM cost discipline

- Default temperature 0.0 for analytical and review calls.
- Default mode SINGLE; SEQUENTIAL and DEBATE only when explicitly configured.
- Per-call max tokens capped (default 1024).
- Per-pass and daily caps enforced; circuit-breaker disables LLM features when
  caps exceeded.
- Prompt cache (Redis, SHA-256 keyed) on every call.
- Track `cost_usd` on every routine_output and chat_message.

## 21.7 Working with the reference repo (`sap-ai-consultant`)

- When the PAT is provided: clone, identify reusable modules per §16.9 / §18.6,
  graft them in.
- Until then: implement to the spec and isolate behind interfaces so the graft is
  drop-in.
- Document every reused fragment via `docs/adr/NNNN-reuse-from-sap-ai-consultant.md`
  with provenance.

## 21.8 Communication & decision protocol

- Surface ambiguities **before** building. Write into
  `spec/15_open_questions.md` with sufficient detail for the user to answer in one
  pass.
- Prefer **smallest demonstrable increment**. Phase 0 first, then Phase 1, etc.
  Don't begin Phase N without exit criteria of Phase N-1 met.
- Never delete a passing test to make a new feature green; fix the underlying
  cause.

## 21.9 Naming & repo hygiene

- Branches: `feat/<short>`, `fix/<short>`, `docs/<short>`, `refactor/<short>`.
- Commits: imperative, ≤ 72 chars subject; body explains why, not what.
- PRs: link to spec section(s) implemented or amended; include screenshots for UI
  changes.
- Files: `snake_case.py` for Python, `kebab-case.astro` for Astro pages,
  `PascalCase.tsx` for React components.

## 21.10 Observability requirements

- Every external call (DB, Redis, OData, LLM, SMTP, MDG) emits an OpenTelemetry
  span with the canonical attributes (§09.7).
- Every Celery task emits start / finish / failure events with run_id.
- Every audit-worthy action writes both an audit row and a structured log line.
- Prometheus metrics for queue depth, request rate, error rate, LLM cost,
  housekeeping coverage.

## 21.11 Backwards compatibility

- API versioning: `/api/v1/...` from day one. Breaking changes go to `/api/v2/...`
  with a deprecation period documented in the runbook.
- Database migrations: forward-only via Alembic; destructive migrations require
  an ADR and a manual approval gate.

## 21.12 Definition of "done" per change

A change is done only when **all** of the following are true:

1. Spec updated (this folder).
2. Code merged with tests passing.
3. Application docs updated (per §19).
4. End-user docs updated (per §19) when user-visible.
5. ADR written if architectural.
6. Acceptance criteria (§14) for the touched module remain green.
7. CI green.

## 21.13 Immediate first actions for the implementer

1. Read every file in `spec/` in numerical order. Note any contradictions you
   spot in `spec/15_open_questions.md`.
2. Stand up Phase 0 from `spec/12_build_plan_phases.md` end-to-end.
3. Resolve open questions with the user before starting Phase 3 (decision
   tree).
4. Commit small, document continuously, never let the docs fall behind the code.
