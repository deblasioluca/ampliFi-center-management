# Changelog

All notable changes to **ampliFi Center Management** are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Each entry references the merged pull request(s) and groups changes by area.

---

## 2026-05-07 — Day 2: hierarchy bugs, async runs, dashboard restructure (PRs #75–#91)

A 17-PR session driven by operator testing on real data (130k cost
centers, 600 entities, 12 months of balances). The work split into
four arcs:

* **Wave detail polish + hierarchy correctness** (#75, #76, #85, #86)
* **Async long-running jobs + progress UX** (#77, #81, #88)
* **Cockpit / scope / context fixes** (#78, #79, #80, #82, #83, #84)
* **Data-browser & analytics dashboard at scale** (#87, #88, #89, #90, #91)

### Notable caveat: PR #89 almost vanished

PR #88 and PR #89 were both merged inside a 22-second window. PR #89
was based on `feat/analytics-restructure-and-cluster-async`
(PR #88's branch); when GitHub merged PR #88 first and deleted that
branch, the subsequent PR #89 merge commit (`f47d8c4`) landed on the
deleted feature branch instead of `main`. Symptom: the PR shows
`merged=true` in the API but its content is not in `main`.

PR #90 cherry-picked PR #89's commit (`4949f5b`) onto a fresh branch
off `main` and added 8 follow-up fixes. **If you ever see a similar
"merged but not in main" pattern, check whether the base branch was
deleted around the same time.**

### Added

#### Async cluster duplicate-check + analytics dashboard restructure — _PR #88_

Cluster Explorer's duplicate-check was synchronous and blocked the
HTTP request for several minutes on real data. Now dispatches via
`POST /api/cluster/check-duplicates` (returns `job_id`) and the
client polls `GET /api/cluster/jobs/{id}`. Job state lives in an
in-memory dict (`_cluster_jobs`); fine for single-instance, would
need Redis for multi-instance.

Analytics dashboard reorganised into two rows: "View" (scope/run
selection, read-only) vs "New run" (engine/config picker + Run
Global Analysis button). KPI fallback widened so "centers analysed"
shows up even when a run finishes with `completed_centers=0`. View
toggle changed from two stacked buttons to a segmented control.

#### Hierarchical view rebuild for /data — _PR #89, restored via #90_

Data-browser hierarchical view rewritten from scratch: tree panel
on the left, leaf table on the right that mirrors the tabular
columns. Selection is sticky across re-renders. Per-column filter
inputs added to the tabular sub-header row with focus restoration
(prevents losing what the operator just typed when a re-render
fires). Mapping page gets flat/hierarchical mode toggle; empty
state is now actionable with a `POST /api/center-mappings/auto-derive`
endpoint that creates `CenterMapping` rows from `MERGE_MAP`
proposals (idempotent).

Balances tab gets its own hierarchical view via
`GET /api/legacy/balances/by-hierarchy` (server-side aggregation,
no row-by-row work).

#### Server-side aggregations for the analytics dashboard — _PR #90_

New `GET /api/runs/{id}/aggregates` endpoint computes
`outcome_counts`, `target_per_outcome`, `outcome_by_entity` (with
entity-name lookup), `confidence_histogram` (10 fixed bins),
`balance_activity` and `outcome_target_flow` server-side via SQL
`GROUP BY`. The dashboard's chart layer switched to use it,
eliminating the **5,000-proposal cap** that had been showing
"5,000 KEEP" in the donut while the KPI strip correctly showed
18,490.

#### Smart setclass-aware hierarchy resolver — _PR #90_

`_resolve_paths_for_ccs` and `_resolve_paths_for_pcs` pick the
right lookup field based on the hierarchy's setclass:

* `0101` (CC hierarchy)     → leaves are cctrs   → match by `cc.cctr`
* `0104` (PC hierarchy)     → leaves are pctrs   → match by `cc.pctr`
* `0106` (Entity hierarchy) → leaves are ccodes  → match by `cc.ccode`

Returns paths keyed by `cctr`/`pctr` regardless, so callers don't
have to care about the indirection. Fixes "L0..LX columns disappear
when an Entity hierarchy is picked on the Cost Centers tab".

`balances/by-hierarchy` extended in the same way: 0106 joins
`balance.ccode`, 0104 joins through `legacy_cc.pctr`, 0101 keeps
the original `balance.cctr` join. Response carries
`hierarchy_setclass` so the frontend can label.

#### Engine-aware config dropdown — _PR #90_

`ConfigOut` exposes `engine_version` (detected from routine
prefixes — any `v2.*` → v2, otherwise v1). `GET /api/configs`
accepts an `?engine=v1|v2` filter. Dashboard's engine selector now
reloads configs with the active filter, and the dropdown suffix
reflects the engine the config will actually run on (was config
revision number, which was meaningless to operators).

#### L0..LX columns and hierarchy picker for Cost Centers / Profit Centers — _PR #88_

Both tabs already had their own hierarchy picker; this PR added
the L0..LX columns the picker promises. The CC tab also gained a
2-row toolbar so the Hierarchical button doesn't get cut off on
narrow viewports. PC hierarchy picker broadened to include CC
hierarchies (label calls out the CC↔PC 1:1 caveat).

#### Wave detail: real CC hierarchy tree + L0..LX in tabular — _PR #86_

Wave detail page now uses the real CC hierarchy_path for the
Hierarchical view (was a flat group-by). Tabular view in
proposals-v2 gets L0..LX columns via
`?include_paths=true&hierarchy_id=N`. All wave subpages got a
consistent `← Back to Cockpit` nav.

#### Stored hierarchies for the data browser — _PR #76_

CC and PC views in the data browser now read from
`stored_hierarchy` records (Hierarchy, HierarchyNode, HierarchyLeaf
tables) instead of computing trees on the fly from the legacy
table. This is what made the L0..LX work in #86 and #88 possible.

#### Async global analysis runs — _PR #77_

`POST /api/runs/global` now creates the run row with
`status='queued'`, dispatches the actual pipeline to a background
thread, and returns the `run_id` immediately. Clients poll
`GET /api/runs/{id}` to drive a progress UI; `total_centers` and
`completed_centers` on the response let them render a progress bar.
Cancel via `POST /api/runs/{id}/cancel` — the engine checks every
10 centers and exits cleanly if it sees `status=cancelled`.

#### Wave delete + archive — _PR #84_

`DELETE /api/waves/simulations/{run_id}` now also accepts wave-
level delete for waves in `in_review` (was simulation-only).
Terminal waves (`activated`, `cancelled`) get an archive flow
instead — `POST /api/waves/{id}/archive` — that hides them from
the default list without losing the audit trail.

#### Wave hierarchy modal fix + decision-tree UX — _PR #75_

`GET /api/legacy/hierarchies/{id}/nodes` was returning the raw
edge list (`{id, parent, child, seq}`) so the "Add Hierarchy Nodes
to Scope" modal rendered every checkbox with `value=""` and no
label. Rewrote to walk edges, BFS from roots, return one item per
distinct setname with computed level + parent. Decision Tree tab
got a delete button per simulation run + a selected-state
indicator + clearer "Centers Analyzed" wording.

#### Mapping auto-derive — _PR #89, restored via #90_

`POST /api/center-mappings/auto-derive` creates `CenterMapping`
rows from existing `MERGE_MAP` proposals. Idempotent. Wired to an
empty-state CTA on the Mapping page so the operator has an
obvious next step instead of a dead empty list.

#### Config delete + admin cleanup — _PR #83_

Custom `AnalysisConfig` rows can now be deleted (was no UI for it).
The duplicate "Decision Trees (legacy)" entry in the admin nav was
removed — same destination as the rule-catalog browser.

#### `make verify` diagnostic + fail-loud `make update` — _PR #80_

`make update` was silently exiting 0 when it failed, which made
the office-server lag invisible. Now exits non-zero on any error.
New `make verify` runs a non-destructive read of the deployed
state and reports versions / migration head / config sanity.

### Changed

#### Data-browser perf: paginate + opt-in heavy parts — _PR #87_

`GET /api/data/browser` redesigned for performance on real data:
server-side pagination (`?page=1&size=200`, default 200, cap 500
— later raised to 200_000 in PR #91), opt-in `include_balances`
and `include_hierarchies` flags (default off; the heavy joins
only run when the operator explicitly clicks "+ Show monthly
balances" / "+ Show hierarchy levels"), page-bounded PC lookup,
server-side `?search=` ILIKE filter, and removal of per-column
auto-filter dropdowns (the dominant cost was DOM construction —
100k+ nodes for the full set, locking the browser for many
seconds).

The cap was raised again to 200_000 in PR #91 because the
hierarchical view needs the complete set for leaf-counts and
detail-table rendering.

#### Cockpit scope coverage split between scoped waves and global — _PR #78_

Scope dropdown previously mixed wave-scoped and global-scoped runs
into one list, which made it ambiguous what "Run Global Analysis"
would actually analyse when a wave was already selected. Now the
two are explicitly separated: picking a wave disables the
"Global" button and surfaces a link to the wave's own decision
tree.

#### Cockpit context-aware buttons + coarea picker + comparison diagnostics — _PR #79_

Buttons now know which scope they apply to and render the
appropriate label. CO Area picker added where it was missing (the
previous freetext input let operators type non-existent codes).
Engine comparison view surfaces diagnostics when an engine is
unavailable instead of silently dropping it from the chart.

#### Engine comparison stability — _PR #67_ (already in changelog as a date-range fix)

500 on `/api/runs/compare/...`, cluster 405, LLM review empty
dropdown — all addressed.

### Fixed

#### Data Browser → Hierarchical → HTTP 422 — _PR #91_

PR #90 set `dbSize=10000` for hierarchy mode but the
`/api/data/browser` endpoint capped `size` at `le=500`. Result: a
red `Error: HTTP 422` on first click. Cap raised to `200_000`,
high enough for the 130k CC scale and still rejecting absurd inputs.

#### Hierarchy tree indentation broken — _PR #91_

`renderHierTree` built Tailwind utility classes dynamically:
```js
'ml-' + Math.min(depth * 4, 12)
```
Tailwind JIT only emits utilities it sees as literal strings in
source; the concatenated `ml-4`/`ml-8`/`ml-12` never landed in the
bundle and sub-nodes rendered flush-left. Switched to inline
`style="margin-left:Npx"`.

#### CC / PC tab Hierarchical "(none — flat list)" rendered nothing — _PR #91_

Picker label promised a flat list when no hierarchy was selected
but the code bailed with "Pick a hierarchy from the dropdown".
`_renderCCHierarchy` and `_renderPCHierarchy` now actually render
a flat table when no hierarchy is picked.

#### CC / PC Hierarchical: silent empty when no rows mapped — _PR #91_

When the smart resolver couldn't link any CC/PC to the picked
hierarchy (e.g. CC hierarchy on PC tab — not applicable; or
entity hierarchy whose ccodes aren't in the loaded set), the
tree rendered empty. Now surfaces an actionable amber diagnostic
with the count and a hint about scope/category.

#### Expand-all / Collapse-all toolbar — _PR #91_

Operator request: trees are 5 levels deep on UBS_GROUP_ENT and
walking each caret manually is tedious. Toolbar at the top of
every `renderHierTree` widget toggles every `[data-coll-tgt]` div
and syncs the caret glyphs.

#### Run #N shows N centers in KPIs but 0 proposals — _PR #90_

Diagnostic (cached KPIs but proposals were deleted) is now surfaced
immediately rather than after a 5000-row fetch. The aggregates
endpoint returns `total_proposals=0` which the frontend matches
to the existing inline message.

#### Activity unread-count 404 — _PR #82_

Frontend was calling `/api/activity/unread-count` which didn't
exist (returned 404 in the browser console). Endpoint added,
returning `{count: int}`. Read-only, cheap (one COUNT query).

#### Wave entity lookup, hierarchy scope persistence, PC hierarchy filter — _PR #85_

Three independent bugs: (1) wave detail looked up entity by `id`
where the data has `ccode`; (2) hierarchy_id selected in scope was
not persisted across reloads; (3) PC hierarchy filter was excluded
when applied to PCs (filter logic only checked CC).

#### Alembic phase26 robust to either constraint name — _PR #74_

`Phase 26` migration assumed a specific FK constraint name that
SQLAlchemy autogenerates differently across Postgres versions.
Now it tries both names and skips if neither is present, so
upgrade works on dev and prod without an `alembic stamp` workaround.

#### CI: backend-test, backend-lint were red on every PR — _PR #72 (bundled)_

Already documented in the section below. Mentioned here for
completeness — these fixes unblocked everything from #75 onwards.

### Tests added in this arc

* `test_runs_aggregates.py` — outcome_counts, target_per_outcome,
  histogram bins (with 1.0 clamp), entity-by-total sort, 404 path
* `test_smart_resolver.py` — 0101/0104/0106 lookup keys for CCs and
  PCs, unknown setclass fallback, missing hierarchy returns empty
* `test_configs_engine_filter.py` — detection from `v2.*` / `rule.*`
  / mixed / empty / legacy `routines` key / string entries; engine
  filter on `list_configs`
* `test_balances_by_hierarchy_setclass.py` — JOIN selection per
  setclass, missing-hierarchy returns empty
* `test_data_browser_pagination.py::test_pagination_size_capped_at_200000`
  updated for the new 200k cap
* `test_proposals_v2_include_paths.py` — `include_paths=true` +
  `hierarchy_id=N` round-trip on proposals-v2 (PR #86)
* `test_data_browser_path_hierarchy.py` — `path_hierarchy_id` on
  the data-browser endpoint (PR #88)
* `test_runs_kpi_fallback.py` — KPI fallback when
  `completed_centers=0` (PR #88)
* `test_cluster_async.py` — async dispatch + polling lifecycle
  (PR #88)
* `test_wave_hierarchy_and_simulation_delete.py` — hierarchy nodes
  walk + simulation delete endpoint (PR #75)
* `test_center_mapping_auto_derive.py` — auto-derive idempotency
  (PR #89, restored via PR #90)

357 of 358 backend unit tests pass. The single failure
(`test_population_anomalies_runs_on_real_population`) is
environmental and predates this arc.

---

## 2026-05-06 — Day 1 evening: CI repair, alembic robustness (PRs #73–#74)

### Added

#### Initial CHANGELOG — _PR #73_

This file. Documents the 2026-05-06 PR burst (#69–#72) in the
"Keep a Changelog" format. Earlier history (PR #53 onwards) was
backfilled later in PR #92 (this PR).

### Fixed

#### Alembic phase26 constraint-name robustness — _PR #74_

(See the duplicate entry above — listed in PR #91's arc because
that's where the test surface added it. Same fix.)

---



A multi-PR session covering documentation, data-browser UX, a 3-stage LLM
assistant for the decision tree, and a CI cleanup that had been blocking
merges since at least PR #66.

### Added

#### LLM Q&A on the built-in rule catalog — _PR #71_

Stage 1 of 3 of the LLM Decision-Tree Assistant. A read-only "Ask the
assistant" panel on `/admin/rule-catalog` lets analysts and admins ask
questions about the catalog in natural language and get answers grounded
on the actual rule metadata.

- **Endpoint:** `POST /api/admin/rule-catalog/qa` (admin role required)
  - Stateless. Client passes prior turns in `history`; server caps replay at
    the most recent 10 to bound prompt size.
  - Optional `rule_code` narrows the LLM's grounding to one specific rule
    plus the broader catalog index. Without it the LLM gets the full index.
  - Reads LLM config from `AppConfig.key='llm'` (Azure OpenAI or SAP BTP),
    same source as the existing `llm.advisor` routine and chat assistant.
  - **Read-only:** no DB writes. Errors at any stage (no config, unknown
    provider, init failure, network failure) collapse to
    `{available: false, reason: ...}` so the UI shows a clear message
    instead of an HTTP error.
- **Frontend:** new "Ask the assistant" section between the rule list and
  the detail drawer, with a scope dropdown that auto-selects the open rule
  when the drawer is in use, multi-turn bubbles, backtick→`<code>`
  rendering, and a "Clear conversation" reset.
- **Tests:** 10 unit tests covering the grounding helper (index/detail
  modes, unknown rule_code), the system prompt builder, the unavailable
  fallback for missing/unknown providers, history cap at 10 turns, malformed
  history entries silently dropped, and provider-call failure surfaced as
  `available: false`.

#### LLM Drafter + Configurator for pipeline variants — _PR #72_

Stages 2 and 3 of the LLM Decision-Tree Assistant. An "✨ AI Assistant"
panel on `/admin/configs` turns plain-language goals into editable pipeline
configurations. Two modes share the same panel; both end with an editable
preview the user reviews and saves through the existing
`POST /api/configs` endpoint. Nothing is persisted server-side until the
user explicitly saves.

- **Quick draft (Drafter):** single-shot. Describe the goal, get a complete
  draft.
- **Guided (Configurator):** three steps — `clarify` → `propose` → `refine`.
  The LLM asks 1–3 short questions with tappable options first, then folds
  the answers into a complete draft, with an optional refine loop on top.
- **Endpoints:**
  - `POST /api/admin/configs/draft-from-description` — single-shot
  - `POST /api/admin/configs/configure-stepwise` — multi-step
    (`step: 'clarify' | 'propose' | 'refine'`)
- **Helpers** (extracted for testability and shared across endpoints):
  - `_build_pipeline_grounding(engine)` — engine-filtered catalog (V1
    excludes `v2.*`, V2 only `v2.*`) with per-routine blocks: code, label,
    description, decides, tunable params with defaults and help text
  - `_build_drafter_system_prompt` — strict JSON output shape, anchors the
    LLM in the catalog, requires V1 pipelines to end with
    `aggregate.combine_outcomes`
  - `_build_configurator_clarify_prompt` — instructs the LLM to ask 1–3
    short questions with tappable options, or return `[]` if already clear
  - `_build_configurator_refine_prompt` — same JSON shape as drafter,
    framed as a revision based on user feedback
  - `_call_llm_json` — strips markdown fences, parses JSON, surfaces all
    failure modes as `available: false` with raw text preserved for
    debugging
  - `_validate_pipeline_config` — checks every proposed routine code exists
    in the engine-filtered catalog. Surfaced to the UI so users see
    hallucinated codes flagged in red.
- **Frontend:** new modal with mode picker, dynamic blocks for clarifying
  questions, draft preview with rationale + warnings + validation flags,
  refine block stays available after the first draft renders, save flow
  with code + name inputs that POSTs to the existing
  `/api/configs` endpoint.
- **Tests:** 21 unit tests covering all three system prompts, JSON parsing
  edge cases (markdown fences, malformed JSON), validation of routine
  codes, full happy paths for both endpoints, engine validation, step
  validation, history replay, and provider call failure.

#### GL Accounts data browser + Tabular ↔ Hierarchical views — _PR #70_

GL Accounts had no list endpoint or browser tab — uploads worked, browsing
didn't. This PR adds the missing tab and extends the existing
Tabular ↔ Hierarchical toggle pattern (which previously existed only on
the unified Data Browser tab) to every per-type browser.

- **Endpoint:** `GET /api/legacy/gl-accounts` — lists `GLAccountSKA1`
  records with optional `scope` / `data_category` / `ktopl` / `bukrs` /
  `saknr` (prefix) / `search` filters, plus a best-effort `GLAccountSKB1`
  join that exposes `bukrs` / `stext_skb1` / `waers` from the first
  matching company-code row (`None` when no SKB1 entry exists).
- **Frontend:** new "GL Accounts" tab + count box (counts grid grows from
  7 → 8 columns). Tabular ↔ Hierarchical toggle UI on Entities, Cost
  Centers, Profit Centers, and the new GL tab. A generic
  `renderHierTree(containerId, items, levelDefs, leafRenderer)` helper
  drives all four trees with configurable key extractors per level,
  expandable nodes with descendant counts, and sorted keys.
- **Hierarchies** (per browser):
  | Browser | Level 1 | Level 2 | Leaf |
  | --- | --- | --- | --- |
  | GL accounts | `saknr[0]` (Account class — `1xxxxxxxxx`) | `saknr[0:5]` (Account group — `10000xxxxx`) | full 10-digit `saknr` |
  | Cost centers | `ccode` (Entity) | `cctrcgy` (Category) | `cctr` |
  | Profit centers | `ccode` (Entity) | `segment` | `pctr` |
  | Entities | `country` | `region` | `ccode` |
- **Tests:** 6 unit tests covering response structure, the SKB1 join (when
  present, when absent, first-match-wins), empty results, filter param
  acceptance, and pagination round-trip through `PaginationParams`.

### Changed

#### Page sizes for browse endpoints — _PR #70_

Default page size for CC / PC / Entity / GL browsers raised from **50 → 500**
so the hierarchical view can group across the full result set. Affects
`/api/legacy/cost-centers`, `/api/legacy/profit-centers`,
`/api/entities`, and the new `/api/legacy/gl-accounts`.

If you've scripted against these with a custom `size`, behaviour is
unchanged — the change is in the frontend default only.

#### Deployment guide rewrite — _PR #69_

`docs/DEPLOYMENT.md` was rewritten around three explicit upgrade tiers so
the office Linux server (and any other deployment) can be brought current
without spelunking through individual PR descriptions.

- **Tier 1 — recent merges (PR #60 → main):** zero-infra changes; just
  `git pull && make update` keeps existing deployments current.
- **Tier 2 — PR #60 era:** the database-migration boundary; explicit
  Alembic upgrade steps and config notes.
- **Tier 3 — PR #57 or earlier:** legacy paths + cross-references to
  `docs/DEPLOYMENT-RHEL.md` for proxy/SSL specifics.

A PR-to-feature reference map at the bottom (PR #57 → main) lets operators
see at a glance which behaviors landed when.

### Fixed

#### CI: backend-test was failing for every PR — _PR #72 (bundled)_

The `backend-test` GitHub Actions job had been failing on **every PR** since
at least PR #66 with six collection errors all rooted in:

```
ModuleNotFoundError: No module named 'aiosqlite'
```

Root cause: `backend/tests/conftest.py` defaults `DATABASE_ASYNC_URL` to
`sqlite+aiosqlite:///:memory:`, but `aiosqlite` was never added to the
`[dev]` optional-dependencies group in `backend/pyproject.toml`. Local
venvs picked it up transitively; CI's clean install did not.

**Fix:** added `aiosqlite>=0.19.0` to `[project.optional-dependencies].dev`.
Restores `backend-test` to green and unblocks all downstream PRs.

#### CI: backend-lint was failing for every PR — _PR #72 (bundled)_

`backend-lint` had been failing with two errors:

- `E501 Line too long (105 > 100)` in `backend/app/api/reference.py:307`
  (the `stext_skb1` SKB1-join expression added in PR #70 was 105 chars)
- `E501 Line too long (106 > 100)` in `backend/app/api/admin.py` (a JSON
  template line in the new drafter system prompt)

**Fix:** both lines split across two string literals or one expression
across multiple lines via `ruff format`. No behavior change.

After both fixes, `ruff check` and `ruff format --check` are clean.

---

## 2026-05-06 — Day 1 morning: V2 engine, ML/LLM, sample data, UX foundation (PRs #53–#68)

Backfill of the morning of 2026-05-06, before this CHANGELOG was
created in PR #73. Less detail than the live entries above; titles
plus one short paragraph each. Refer to the merged PRs on
[GitHub](https://github.com/deblasioluca/ampliFi-center-management/pulls?q=is%3Apr+is%3Aclosed)
for full bodies.

### Added

#### Multi-engine comparison: rule tree + ML + LLM — _PR #62_

Three engines, one read-only diagnostic. New `routines/ml_outcome_predictor.py`
(feature-based scoring, IsolationForest anomaly detection) and
`routines/llm_advisor.py` (independent LLM verdict via the existing
`AzureOpenAIProvider`). Comparison page at `/cockpit/engines` runs
all three on the same wave sample and classifies agreement (unanimous /
majority / split). Backend orchestrator at `services/engine_comparison.py`
+ `GET /api/runs/compare/wave/{id}`. 16 new tests.

#### Business-friendly decision panel + on-demand ML/LLM opinions — _PR #64_

Reasoning view rewritten so analysts see why the rule tree picked an
outcome in business language (not pseudo-code). Per-proposal "Get ML
opinion" and "Get LLM opinion" buttons run the alternate engines on
demand. Replaces the JSON dump that was there before.

#### EntityPicker + EmployeePicker UX components — _PR #61_

Comma-separated text inputs replaced with a typeahead picker that
searches entity / employee tables and chips the selected items. Same
component reused on wave scope, scope-form, and the cockpit filters.

#### UI prompt helpers + scope-form context-sensitive labels — _PR #63_

Form labels and helper text adapt to what the operator already
selected (e.g. "Scope" wording differs between cleanup and migration
mode). Reduces support questions about ambiguous fields.

#### UBS-flavored sample data generator — _PR #65_

`scripts/generate_sample_data.py` produces realistic test data at
production scale: 130k cost centers, 600 entities, 12 months of
balances. Names follow a UBS-style taxonomy (GWM / IB / PC / etc.)
so screenshots are recognisable for stakeholder demos.

#### Sample data wipe + 60k employees + FK linkage — _PRs #66, #68_

`--wipe-only` and `--purge` flags so re-running the generator doesn't
double up. Postgres `pctr` length fix (was overflowing 12-char column).
60k employees added with FK linkage from CC and PC owner fields, so
the EntityPicker has realistic targets.

#### Rule catalog browser + wave tab consolidation + deployment guide — _PR #57_

The first major UX consolidation. `/admin/rule-catalog` lets analysts
browse every rule with description, parameters, and verdict meanings.
Wave tabs (Decision Tree / Tabular / Hierarchical / Decision Detail)
unified into a single tab strip. First version of `docs/DEPLOYMENT.md`.

### Changed

#### Decision-tree variant UX + review polish — _PR #54_

Variant picker on `/cockpit` upgraded with description tooltips and
an "active" badge. Several review-comment cleanups across the codebase.

### Fixed

#### `/rule-catalog` 404 + English UI — _PR #58_

The configs router's `/{code}` catch-all was matching `rule-catalog`
as a value for the `code` path parameter, producing a 404. Reordered
the route registrations so static paths win first. Same PR replaced
the residual German strings on the page with English.

#### Auth: JWT bearer token in new admin/cockpit pages — _PR #59_

The new pages added in #57 / #58 were calling `fetch()` without the
auth header, so every API call returned 401. Added the standard
`authHdr` helper that the rest of the app uses.

#### Configs: list parsing, id-vs-code, residual German — _PR #60_

Config picker was using `id` as the value but trying to look up by
`code` on edit, breaking the round-trip. List parsing was case-
sensitive when the API was case-insensitive. Last German string in
the config editor replaced with English.

#### Engine comparison 500, cluster 405, LLM review empty dropdown — _PR #67_

Three independent UI bugs from the multi-engine work: comparison
endpoint failing with a 500 on certain sample sizes; Cluster Explorer
returning a 405 on the duplicate-check endpoint; LLM Review's wave
dropdown empty because the filter was wrong. Naming consistency pass
across the comparison view.

### Documentation

#### Initial CHANGELOG seed (for #50–#52) — _PR #53_

Documentation pass for the V2 engine, simulation mode, config admin,
and entity picker work that landed before this CHANGELOG existed.

#### Celery Beat schedule overrides + EXPLORER_REQUIRE_AUTH — _PR #55_

`docs/CONFIG.md` (or equivalent) added env-var docs for two
under-documented controls operators kept asking about.

#### CI: SQLite-aware engine + lint/format compliance — _PR #56_

PR #54's CI was broken because it used Postgres-only constructs.
Engine creation now picks SQLite when `DATABASE_URL` starts with
`sqlite:`; remaining lint / format violations cleaned up.

---

## PR-to-feature reference

For operators who want to know "what changed in PR #N":

| PR | Date | Theme | One-liner |
|----|------|-------|-----------|
| #53 | 2026-05-06 | Docs | CHANGELOG seed for #50–#52 |
| #54 | 2026-05-06 | UX | Decision-tree variant picker polish |
| #55 | 2026-05-06 | Docs | Celery Beat + EXPLORER_REQUIRE_AUTH env vars |
| #56 | 2026-05-06 | CI | SQLite-aware engine + lint/format |
| #57 | 2026-05-06 | Feat | Rule catalog browser, wave tabs, deployment guide |
| #58 | 2026-05-06 | Fix | `/rule-catalog` 404 + English UI |
| #59 | 2026-05-06 | Fix | JWT bearer token in new pages |
| #60 | 2026-05-06 | Fix | Config picker id-vs-code round-trip |
| #61 | 2026-05-06 | Feat | EntityPicker + EmployeePicker components |
| #62 | 2026-05-06 | Feat | Multi-engine comparison (tree + ML + LLM) |
| #63 | 2026-05-06 | UX | Context-sensitive form labels |
| #64 | 2026-05-06 | Feat | Reasoning panel + on-demand engine opinions |
| #65 | 2026-05-06 | Feat | UBS-flavored sample data generator (130k CCs) |
| #66 | 2026-05-06 | Feat | --wipe-only / --purge for sample data |
| #67 | 2026-05-06 | Fix | Engine comparison 500, cluster 405, LLM dropdown |
| #68 | 2026-05-06 | Feat | 60k employees + FK linkage from CC/PC owner |
| #69 | 2026-05-06 | Docs | Deployment guide rewrite |
| #70 | 2026-05-06 | Feat | Tabular ↔ Hierarchical views on data browsers |
| #71 | 2026-05-06 | Feat | LLM Q&A on rule catalog |
| #72 | 2026-05-06 | Feat | LLM Drafter + Configurator (also CI repair) |
| #73 | 2026-05-06 | Docs | Initial CHANGELOG creation |
| #74 | 2026-05-06 | Fix | Alembic phase26 constraint-name robustness |
| #75 | 2026-05-07 | Fix | Wave hierarchy modal + decision-tree UX |
| #76 | 2026-05-07 | Feat | Stored hierarchies for CC + PC views |
| #77 | 2026-05-07 | Feat | Async global runs with progress polling |
| #78 | 2026-05-07 | Fix | Cockpit scope coverage split (scoped vs global) |
| #79 | 2026-05-07 | Fix | Context-aware buttons, coarea picker, comparison |
| #80 | 2026-05-07 | Fix | `make update` fail-loud + `make verify` |
| #81 | 2026-05-07 | Fix | V2 async + delete-bug + dashboard cleanup |
| #82 | 2026-05-07 | Fix | `/api/activity/unread-count` endpoint |
| #83 | 2026-05-07 | Feat | Config delete + remove duplicate admin nav |
| #84 | 2026-05-07 | Feat | Wave delete in_review + archive flow |
| #85 | 2026-05-07 | Fix | Wave entity lookup, hierarchy scope, PC filter |
| #86 | 2026-05-07 | Feat | Wave hierarchy tree + L0..LX in tabular |
| #87 | 2026-05-07 | Perf | Data-browser paginate + opt-in heavy parts |
| #88 | 2026-05-07 | Feat | Async cluster, dashboard restructure, L0..LX |
| #89 | 2026-05-07 | Feat | Hierarchical view rebuild, column filters, mapping auto-derive (lost merge — restored via #90) |
| #90 | 2026-05-07 | Fix | 5k chart cap, engine/config, smart resolver, balances tree |
| #91 | 2026-05-07 | Fix | Hierarchy 422, indentation, flat-list, expand/collapse |
| #92 | 2026-05-07 | Docs | This CHANGELOG backfill (PRs #53–#91) |

