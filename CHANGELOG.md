# Changelog

All notable changes to **ampliFi Center Management** are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Each entry references the merged pull request(s) and groups changes by area.

---

## 2026-05-06 — Data browsing, LLM assistant trilogy, deployment guide

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

## Earlier history

Pre-2026-05-06 changes are not retroactively backfilled into this file.
Refer to the merged PR list on
[GitHub](https://github.com/deblasioluca/ampliFi-center-management/pulls?q=is%3Apr+is%3Aclosed)
or the commit history for context on prior work.
