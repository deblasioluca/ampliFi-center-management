# 08 — Waves & Housekeeping Cycles

This module specifies the wave lifecycle, the full-scope analysis variant, and the
recurring monthly housekeeping cycle on the new ampliFi environment.

## 8.1 Wave lifecycle

```
draft  ──► analysing  ──► proposed  ──► locked  ──► in_review  ──► signed_off  ──► closed
   \________ cancelled at any pre-locked state ________________/
```

| State | Meaning | Allowed transitions |
|---|---|---|
| `draft` | Wave created, scope being defined | analysing, cancelled |
| `analysing` | At least one analysis_run started | proposed, cancelled |
| `proposed` | Analyst has selected a "preferred" run for the wave | locked, analysing (re-run), cancelled |
| `locked` | Proposal locked, target_* drafts created, naming reserved | in_review (when first scope invited), draft (unlock if no invitations) |
| `in_review` | At least one review scope invited | signed_off, locked (recall — admin only, audit logged) |
| `signed_off` | All review scopes complete | closed |
| `closed` | MDG export generated; wave is final | — |
| `cancelled` | Terminal cancellation pre-lock | — |

Transitions enforced by a state-machine in `backend/app/domain/proposal/`. Each
transition writes an audit row.

## 8.2 Wave configuration object

A wave aggregates:

- `wave` row (state, dates).
- `wave_entity` (the scope, by Legal Entity).
- A default `analysis_config` (forked from `STD-CLEANSING-V2`).
- 0..N `analysis_run`s, one of which is marked **preferred** (drives the proposal).
- 1..N `review_scope`s once locked.
- A reserved naming-sequence range per object type (CC / PC / WBS).

## 8.3 Refresh and re-analysis

- The data the wave operates on is pinned via `analysis_run.data_snapshot`
  (a `refresh_batch` UUID — §03.4).
- An admin can trigger a fresh **upload** (§07.7) or **OData refresh** (§09.1) at any
  time. New refresh batches do not invalidate prior runs.
- An analyst can **re-run** the analysis with a new data_snapshot. The new run is added
  alongside existing runs; the analyst can compare versions (§05.8) before promoting one
  to "preferred".

## 8.3b Simulation → Activation Workflow (implemented)

**Added in PRs #51-52.** Analysis runs operate in one of two modes:

### Simulation Mode
- Run analysis (V1 or V2) in "simulation" mode — no permanent IDs assigned
- Temporary IDs: `CT...` for cost centers, `PT...` for profit centers
- Can run on a specific wave OR globally (all centers, with option to exclude
  already-completed scopes via `excluded_scopes` JSON)
- Results shown in both **tabular** and **hierarchical** views:
  - Which centers migrate vs retire
  - PC groupings (1:1 vs 1:n) for V2
  - CC → PC linkage
- Simulations are **versioned** — store multiple runs with different configs and labels
- Compare simulation versions side-by-side
- Progress tracked via `total_centers` / `completed_centers` on the run

### Activation
- When satisfied with a simulation, "activate" it:
  - `POST /api/waves/simulations/{run_id}/activate`
- At activation: assign real PC/CC IDs (P00137+, C00001+)
- Run marked as preferred run for the wave
- Only one activated run per wave
- Valid state transition: `simulation` → `activated` (enforced server-side)

### API Endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/waves/{id}/analyse-v2` | Run V2 analysis (simulation or activated) |
| GET | `/waves/{id}/runs/{run_id}/export-v2` | Export V2 results as Excel |
| GET | `/waves/{id}/runs/{run_id}/proposals-v2` | V2 proposals (paginated) |
| POST | `/waves/global/simulate-v2` | Global V2 simulation |
| GET | `/waves/simulations/v2` | List V2 simulation runs |
| POST | `/waves/simulations/{run_id}/activate` | Activate a simulation |

### Wave Progress Pipeline (implemented)

The wave detail page tracks progress through 8 steps:

```
Create → Defining Scope → Analyse → Simulation → Proposals → Review Scopes → Progress → Export
```

Each step unlocks the next set of tabs in the wave detail view. Previous steps
remain accessible. The progress indicator shows the current step with visual
highlighting.

## 8.4 Full-scope (non-wave) analysis

A wave with `is_full_scope=true` represents a strategic, **non-sign-off** analysis over
the full universe — optionally **excluding entities already in any past or active wave**
(`exclude_prior=true`).

- Scope: `entity − wave_entity (where wave.state ≠ 'cancelled')` if `exclude_prior=true`.
- Lifecycle stops at `proposed`. Lock / Review / Sign-off / Close are disabled.
- Output is a strategic snapshot (e.g. "if we ran today on everything not yet done,
  how many target PCs would emerge, what does the hierarchy look like, what's the
  naming-coverage gap").
- Full-scope runs can still spawn an **LLM review** for narrative reporting but cannot
  produce MDG exports.

## 8.5 Housekeeping cycle (monthly)

The housekeeping cycle re-uses the same analytics framework but runs on the **target**
data (`target_cost_center`, `target_profit_center`) instead of legacy. Owners of
flagged centers (sourced from `target_*.responsible`) are emailed for sign-off.

### 8.5.1 Lifecycle

```
scheduled  ──► running  ──► review_open  ──► closed
                              \______ cancelled (admin) _____/
```

### 8.5.2 Triggers

- **Cron schedule**: by default 03:00 UTC on the 1st of each month. Configurable via
  `app_config['housekeeping.cron']`.
- **Manual run**: admin can trigger `POST /api/admin/housekeeping/run` (creates an
  ad-hoc cycle).

### 8.5.3 Configuration

A separate `analysis_config` is used for housekeeping (default code
`HK-MONTHLY-V1`). Suggested pipeline:

```
rule.posting_activity (12-month window, threshold 0)
rule.ownership
ml.anomaly_detector       (IsolationForest on target features)
rule.bs_relevance         (still has B/S balance? else flag)
aggregate.combine_outcomes  (HK-specific: KEEP / REVIEW_LOW_USE / CLOSE_CANDIDATE)
llm.review_single         (one model commentary per flagged center)
```

### 8.5.4 Per-center flags (housekeeping_item.flag)

| Flag | Definition |
|---|---|
| `UNUSED` | No postings in the rolling 12-month window |
| `LOW_VOLUME` | < N postings or < € X balance in the window (configurable) |
| `NO_OWNER` | `responsible` is empty or unresolved against the user/contact list |
| `ANOMALY` | IsolationForest score above threshold |
| `STILL_USED` | Used in active allocations (informational; suppresses CLOSE) |

Multiple flags can attach to one center.

### 8.5.5 Owner notification & sign-off

- For each `housekeeping_item`, the cycle resolves the center owner via:
  `target_*.responsible → users.email` (preferred), or `attrs.owner_email`, or a fallback
  list configured per company code.
- One **digest email** per owner per cycle (one email containing all centers needing
  their decision), with a deep link `/housekeeping/{cycle_id}/owner/{token}`.
- The owner sees a list with three actions per item: `KEEP`, `CLOSE`, `DEFER` (with a
  required comment for `CLOSE` and `DEFER`).
- Reminder cadence: T+7 days, T+14 days, escalation to delegate at T+21.

### 8.5.6 Closure

- When the owner submits, decisions are recorded on `housekeeping_item`.
- Cycle moves to `closed` when the owner-response deadline passes (default T+30).
- All `CLOSE` decisions queue MDG closures (§09.4).
- A summary report is emailed to the admin team and stored in
  `cleanup.housekeeping_cycle.summary` (counts, owner response rate, list of closures).

### 8.5.7 SLA & monitoring

- Cycle should complete the analysis (running → review_open) in < 4 hours.
- Owner response rate is tracked as a KPI on the admin dashboard.
- Repeat offenders (centers flagged in 3+ consecutive cycles) get a tag and surface
  to the analyst team for proactive cleanup.

## 8.6 Wave administration UI (cross-ref)

Wave CRUD and progress dashboards live under `/admin/waves` (§07.9) and
`/wave/{id}` (§06). The admin view is a superset; analysts have the same operations
constrained to waves they own / are assigned to.

## 8.7 Cancelling a wave

- Allowed in any pre-`locked` state.
- Sets `state='cancelled'`.
- Releases reserved naming-sequence ranges back to the pool.
- Drafts of `target_cost_center` / `target_profit_center` created during analysis are
  hard-deleted (they were never approved); reserved IDs are released.
- Audit logged.

## 8.8 Re-opening a closed wave (admin-only emergency)

- Action: `POST /api/admin/waves/{id}/reopen` requires `reason`. Allowed only if the
  wave's MDG export has not yet been ingested in target system.
- Sets state back to `signed_off` (or `locked`, depending on `level` parameter).
- Heavy audit; email to all reviewers; analyst confirmation required.

## 8.9 Concurrency and isolation between waves

Two waves may legitimately propose conflicting target centers if their entity scopes
overlap (which `exclude_prior` is meant to prevent). Hard guardrails:

- A center proposed in two locked waves with conflicting outcomes raises a
  **conflict** that must be resolved by an admin before either wave can close.
- Naming sequences are reserved per wave to avoid ID collisions.
- The MDG export pipeline checks: same `pctr_id` / `cctr_id` cannot be exported by
  two waves with conflicting payloads.

## 8.10 Wave KPIs (rendered on `/admin/waves`)

- Waves by state.
- Avg time per state.
- Reviewer SLA (% of scopes completed within target days).
- Outcome distribution (KEEP / RETIRE / MERGE_MAP / REDESIGN).
- Open requests for new centers.
- Housekeeping closure rate over time.
