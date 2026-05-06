# 06 — End-User Module (Analyst Cockpit + Stakeholder Review)

The end-user module covers two distinct audiences:

- **Analysts** (Group Finance) build, run, lock and circulate proposals.
- **Stakeholders / reviewers** (LE finance, business owners) tick off proposals.

## 6.1 Navigation

```
/cockpit                 → home, lists waves & full-scope runs
/wave/new                → create wave
/wave/{id}               → wave detail (configure, run, review)
/wave/{id}/scope         → scope (entities)
/wave/{id}/pipeline      → analytical configuration editor
/wave/{id}/runs          → list of analysis runs (versions)
/wave/{id}/run/{run_id}  → run detail (cockpit on a single run)
/wave/{id}/runs/compare  → diff two runs
/wave/{id}/proposal      → proposal page (lock / unlock)
/wave/{id}/scopes        → review scopes (assign reviewers)
/wave/{id}/progress      → review progress dashboard
/review/{token}          → stakeholder view (no login in v1; tokenised)
/housekeeping            → housekeeping cycles
/admin/...               → admin module (§07)
```

## 6.2 Wave creation

Analyst clicks **New wave** → form:

- Code (auto-suggested, editable)
- Name
- Description
- **Scope type**: `entities` (multi-select Legal Entities) or `full_scope`
- For `full_scope`: checkbox "Exclude entities already in any past or active wave"
  (default ON). When ON, the entity list = `entity − wave_entity` (across all wave states
  except `cancelled`).
- Default analysis_config (a copy of `STD-CLEANSING-V2` is auto-attached; analyst can
  fork/edit).

Persisted state on `wave` row; entities pinned to the wave on `wave_entity`.

### 6.2.1 Entity Picker (implemented)

**Added in PR #52.** The entity selection for wave scoping uses a sophisticated
multi-select picker designed for 600+ entities:

- **Search/filter**: Typeahead search on entity name and company code
- **Select All / Deselect All**: Buttons to select/deselect all (respects current filter)
- **Count badge**: Shows number of selected entities
- **Grouped display**: Clear list with entity code and name

## 6.3 Pipeline editor (analytical configuration)

Located at `/wave/{id}/pipeline`.

Layout:

```
┌─────────────────────────────────────────────────────────────────┐
│ Config: WAVE-APAC-V2  v2   [Save] [Save as…] [Fork] [Compare…]  │
├──────────────────────────┬──────────────────────────────────────┤
│ Routine library          │ Pipeline (drag to reorder)           │
│ ─ rules                  │ ☑ rule.posting_activity   [params]   │
│   rule.posting_activity  │ ☑ rule.ownership          [params]   │
│   rule.ownership         │ ☑ ml.duplicate_cluster    [params]   │
│   …                      │ ☑ rule.redundancy         [params]   │
│ ─ ml                     │ ☐ rule.cross_system_dep   [params]   │
│   ml.outcome_classifier  │ … (drag to reorder, toggle on/off)   │
│   …                      │                                       │
│ ─ llm                    │ LLM Review Pass                      │
│   llm.review_single      │  Mode: ( ) SINGLE  ( ) SEQUENTIAL    │
│   llm.review_sequential  │        (●) DEBATE                    │
│   llm.review_debate      │  Models: [+ add]                     │
│ ─ custom rules (DSL)     │  Scope: outcomes ⊆ {MERGE_MAP, …}    │
│   [+ New rule]           │         min_balance: 100,000 EUR     │
│                          │         max centers: 5,000           │
│                          │  Prompt template: prompt.review.v3   │
└──────────────────────────┴──────────────────────────────────────┘
                  [Validate]  [Preview impact]  [Run]
```

- **Validate** runs JSON-Schema validation server-side.
- **Preview impact** runs the deterministic part on a 1% sample → shows estimated
  outcome distribution.
- **Run** kicks off `dt.run_cleansing` + `dt.run_mapping` + (optional) `ml.score` +
  (optional) `llm.review` Celery jobs.

## 6.4 Run detail cockpit

Header KPIs: total centers, KEEP %, RETIRE %, MERGE_MAP %, REDESIGN %, ML coverage,
LLM coverage, run status, data_snapshot timestamp.

Tabs:

1. **Universe** — virtualised table (250k rows). Columns: ccode, cctr, txtmi, owner,
   last_posting_period, total_balance, outcome, target_object, ml_confidence, "why".
   Row click opens the **Why panel**.
2. **Sankey** — legacy outcome → target object.
3. **Hierarchy old vs new** — split view: legacy hierarchy on the left, proposed target
   structure on the right; selecting a node highlights connected nodes on the other side.
4. **Clusters** — ML-detected duplicate clusters; per cluster show member centers,
   proposed survivor, merge rationale.
5. **Coverage** — entities × outcome heatmap.
6. **LLM transcripts** — transcript viewer for SEQUENTIAL/DEBATE passes.
7. **Overrides** — list of analyst overrides with reasons.

### Why panel (per center)

```
Center: 23472 — SDM Brazil
Owner: Leandro Bolsoni
Profit center (legacy): 23472     Outcome: KEEP → Target: PC

Rule path:
  ✔ rule.posting_activity      → PASS  (last posting 2026-03; 8 months activity)
  ✔ rule.ownership             → PASS  (owner active)
  ✖ ml.duplicate_cluster       → none
  ✔ rule.bs_relevance          → has B/S balance € 2.4m
  ✔ rule.has_direct_revenue    → revenue € 6.1m
  ✔ rule.has_operational_costs → opex € 0.4m  →  CC_AND_PC?

ML scores
  outcome_classifier: KEEP 0.94 | RETIRE 0.01 | MERGE 0.04 | REDESIGN 0.01
  target_classifier:  PC 0.42 | CC_AND_PC 0.55 | CC 0.02 | …  (SHAP top: rev_amt, has_feeder)

LLM commentary (DEBATE, judge: gpt-4o)
  "Center has consistent direct revenue and operational costs from feeder; both
   profitability and cost accountability apply. Recommend CC_AND_PC."

[Override outcome / target]    [Add to comparison]    [View raw payload]
```

## 6.5 Proposal lock

When the analyst is satisfied:

`POST /api/waves/{id}/proposal/lock` → state: `proposed` → `locked`. Side effects:
- Snapshots the chosen `analysis_run_id` onto the wave.
- Creates `target_cost_center` and `target_profit_center` rows in **draft** form
  (`is_active=false`) using the naming convention engine (§07.5).
- Locks the analysis_config used (`is_immutable=true`).
- Allows scope assignment (§6.7).

Unlocking is allowed only if no scope has been invited yet.

## 6.6 Run comparison page

`/wave/{id}/runs/compare?a={run_a}&b={run_b}` renders the diff defined in §05.8: outcome
matrix, target-object matrix, changed-centers table, ML histograms, and a side-by-side
LLM commentary diff for centers where both passes generated commentary.

## 6.7 Review scopes

Analyst defines one or more **review_scope** rows per wave:

- **By entity**: pick LEs the reviewer covers.
- **By hierarchy node**: pick a `setname` (cost center or profit center group).
- **By list**: explicit list of centers (CSV upload supported).

Each scope has: name, reviewer (user, may be a stakeholder created on the fly), an
auto-generated `invite_token` (UUID v4, 30-day expiry, refreshable), and an invite email.

`POST /api/waves/{id}/scopes` creates scopes. `POST /api/scopes/{id}/invite` enqueues an
email (§09.3) with the link `/review/{token}`.

## 6.8 Stakeholder review UI (`/review/{token}`)

Token-based access in v1; no login required. Token grants read+write to that scope only,
with rate-limiting and audit. (When EntraID is on, scope can be linked to a user identity
instead of token.)

Layout:

```
┌──────────────────────────────────────────────────────────────────┐
│ Wave 2026 Q3 APAC – Reviewer: Patrick / TWN-FINANCE              │
├──────────────────────────────────────────────────────────────────┤
│ Tabs:  [Legacy list]  [Legacy hierarchy]  [Proposed (new)]        │
│                                                                  │
│ Filters: outcome ▾  target ▾  entity ▾  search ▾  status ▾        │
│                                                                  │
│  ☑ 23472  SDM Brazil           KEEP →  CC_AND_PC                 │
│  ☐ 23498  Brazil shared svcs   RETIRE                            │
│  …                                                               │
│                                                                  │
│ [Approve all visible]  [Mark Not Required]                       │
│                                                                  │
│ Bottom panel for selected center:                                │
│   Outcome  KEEP →  CC_AND_PC                                     │
│   Rationale: …                                                   │
│   [Approve] [Not required] [Request new center] [Comment]        │
│                                                                  │
│  [I have completed my review →  Submit final sign-off]           │
└──────────────────────────────────────────────────────────────────┘
```

### Three viewing modes (mandatory)

1. **Legacy list** — flat table of legacy centers in the reviewer's scope.
2. **Legacy hierarchy** — tree of cost-center / profit-center groups (SETNODE) with
   leaves expanded. Tick at any node = tick all descendant leaves.
3. **Proposed (new)** — the future structure (target PCs grouping target CCs) with
   counts per group.

### Per-item actions

- **Approve** — sets `decision='APPROVE'`.
- **Not required** — sets `decision='NOT_REQUIRED'`; flips the linked target center's
  `is_active=false` if the wave closes with that decision.
- **Request new center** — creates a `review_item` with `proposal_id=NULL`, decision
  `NEW_REQUEST`, and a small form (purpose, B/S relevance, expected target object,
  responsible person).
- **Comment** — free-text comment, attached to the item.

### Bulk actions

- Tick a hierarchy node to apply the same decision to all descendant items in scope.
- "Approve all visible" approves the currently filtered set.
- Undo per item until `Submit final sign-off`.

### Final sign-off

`POST /api/scopes/{id}/complete` requires every item in scope to be in a terminal state
(`APPROVE`, `NOT_REQUIRED`, or `NEW_REQUEST`). Items left as `PENDING` block submission
with a clear list. Once submitted: scope is frozen, reviewer email confirmation goes out
(§09.3), wave progress dashboard updates.

## 6.9 Wave progress dashboard

`/wave/{id}/progress` shows:

- Per scope: % complete, last activity, reviewer.
- Aggregate: total items, by decision.
- Outstanding requests: list of NEW_REQUEST items for analyst triage.
- Reminder controls: "Send reminder to all stuck > N days".

When **all** scopes are completed → wave state moves to `signed_off` automatically.
Analyst then runs **Close wave**: triggers MDG export (§09.4) and final email.

## 6.10 Tab State Management (implemented)

**Added in PR #52.** The wave detail page uses a progress-based tab system.
Tabs are disabled/enabled based on the current wave step:

| Wave Step | Available Tabs |
|-----------|---------------|
| Create | Scope only |
| Defining Scope | Scope |
| Analyse | Scope, Analysis |
| Simulation | Scope, Analysis, Simulation |
| Proposals | Scope, Analysis, Simulation, Proposals |
| Review Scopes | All tabs through Review Scopes |
| Progress | All tabs |
| Export | All tabs |

Disabled tabs appear grayed out and are not clickable, preventing premature
access to steps that aren't relevant yet.

## 6.11 Engine & Config Selection (implemented)

**Added in PR #52.** When running analysis from the wave page:

1. **Engine selector**: Dropdown to choose V1 (Decision Tree) or V2 (CEMA Migration)
2. **Config version selector**: Dropdown listing all available analysis configs with
   version numbers, filtered to show only configs compatible with the selected engine
3. Both selectors persist their values on the `AnalysisRun` record

## 6.12 Scope Coverage Dashboard (implemented)

**Added in PR #52.** The cockpit page (`/cockpit`) displays scope coverage statistics:

- **Entities**: X / Y analysed (with percentage bar)
- **Cost Centers (analysed)**: X / Y with percentage — only counts active CCs
  (not orphaned analysis runs)
- **Profit Centers**: X / Y with percentage
- **Per-wave breakdown**: Table showing entities, CC count, and coverage per wave
- **Unassigned row**: Orphaned analysis runs (no wave) are shown separately so
  numbers always add up correctly

On 401 (JWT expired), the dashboard redirects to login instead of showing stale/empty data.

## 6.13 Full-scope strategic run

A wave with `is_full_scope=true` skips the review/sign-off phase. It produces the same
analysis run + cockpit views, intended for strategic analysis (e.g. "if we ran clean-up
on everything excluding waves already done, what would the future PC structure look
like?"). The Lock / Scopes / Sign-off actions are hidden for these waves.

## 6.14 Performance targets

| Action | Target |
|---|---|
| Cockpit list filtering 250k rows | < 1.5s server-side, < 250ms client interaction |
| Why-panel load | < 400ms |
| Sankey rendering (aggregate fetch) | < 1s |
| Run a full deterministic tree on 250k centers | < 5 min |
| LLM SEQUENTIAL pass over 5k centers (3 stages, gpt-4o) | < 30 min |
| Comparison page (cached diff) | < 800ms |
