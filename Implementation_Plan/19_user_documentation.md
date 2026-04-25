# 19 — User Documentation (in-app + standalone)

A full documentation suite ships **with the app** and is **available from the UI**.
Documentation is treated as a first-class deliverable: every feature merges with
its docs in the same PR. CI fails when a feature flag is added without a docs entry.

## 19.1 Documentation set (full)

| Audience | Document | Format | Where to find it |
|---|---|---|---|
| End-user (analyst) | Analyst User Guide | Markdown rendered in-app | `/docs/analyst/*` |
| End-user (reviewer) | Reviewer Guide | Markdown rendered in-app | `/docs/reviewer/*` and per-page tooltips |
| End-user (owner) | Housekeeping Owner Guide | Markdown rendered in-app | `/docs/owner/*` |
| Admin | Admin Manual | Markdown rendered in-app | `/docs/admin/*` |
| Operator (ops) | Runbook + Install/Upgrade | Markdown in repo | `docs/RUNBOOK.md`, `docs/INSTALL.md` |
| Implementer / dev | Spec bundle (this folder) + ADRs | Markdown in repo | `spec/`, `docs/adr/` |
| API consumer | OpenAPI 3.1 + Redoc | Generated | `/api/docs` (Swagger), `/api/redoc` |
| Stakeholder | One-pager + executive deck | Word + PPT | `docs/stakeholder/` |

## 19.2 In-app documentation surface

The application exposes documentation through a **Help drawer** and a dedicated
`/docs` route:

```
┌────────────────────────────────────────────────────────────────────┐
│  Cockpit ▸ Run #42                                       [?] Help  │
├────────────────────────────────────────────────────────────────────┤
│ … cockpit content …                                                │
│                                                                    │
│   ┌──── Help drawer (slides in from right when [?] clicked) ────┐  │
│   │ Topic detected: "Run cockpit"                              │  │
│   │  • Overview of this page                                   │  │
│   │  • What is an outcome?                                     │  │
│   │  • How to override a proposal                              │  │
│   │  • [Open full guide ▸]                                     │  │
│   │                                                            │  │
│   │  Search: [_______________]                                 │  │
│   └────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────┘
```

- **Context-aware**: every page declares a `helpKey` (e.g. `cockpit.run`); the
  drawer auto-loads the matching doc.
- **Search**: full-text across all docs (Postgres `tsvector` index on a `doc_chunk`
  table, no external service needed).
- **Tooltips**: small `?` next to non-obvious labels open a popover with a one-line
  explanation; "Read more" link jumps to the full doc.
- **Walkthroughs**: optional product-tour overlays for first-time users
  (Driver.js or Shepherd; admin can disable). Each walkthrough is a JSON file
  shipped with the docs: `docs/tours/<page>.json`.
- **Skill-aware**: when a chat skill (§17) handles a question that matches a doc
  topic, the response includes a "See: Reviewer Guide ▸ Approving items" link.

## 19.3 Authoring conventions

- All docs live as Markdown under `docs/` (or `frontend/src/content/docs/` if
  Astro Content Collections is used). Front-matter:

  ```yaml
  ---
  audience: reviewer | analyst | admin | owner | operator
  helpKey: review.scope.flat_list
  title: Reviewing items in flat list mode
  order: 30
  tags: [review, scope]
  updated: 2026-04-25
  ---
  ```

- Cross-link with relative paths.
- Screenshots stored under `docs/_assets/`. Implementer must capture from the
  current UI for every release where the screen changed.
- Every code-related doc page links back to the relevant spec section in `spec/`.

## 19.4 Coverage checklist (the "full set")

### 19.4.1 Reviewer Guide (must cover)

- What is the cleanup project, in 5 sentences.
- How to access your scope (link from email; how the token works).
- The three viewing modes (flat list, hierarchy, proposed) — with screenshots.
- Per-item actions: Approve, Not Required, Comment, Request New Center.
- Bulk approve via hierarchy nodes.
- What "outcome" and "target object" mean (PC, CC, CC_AND_PC, WBS_REAL,
  WBS_STAT) — with everyday-language examples.
- How to use the chat assistant to ask questions.
- How to submit final sign-off.
- FAQ.

### 19.4.2 Analyst User Guide

- Wave creation and scope definition.
- Pipeline editor (which routines do what).
- Running an analysis; reading the cockpit.
- The Why panel.
- Override protocol.
- Comparing runs (versioning).
- LLM review modes (when to use each).
- Locking, scope assignment, reviewer invites, reminders.
- Closing a wave; MDG export.
- Housekeeping cycles.

### 19.4.3 Admin Manual

- User CRUD + bulk upload.
- DB connection setup (local + Datasphere).
- LLM endpoint setup (Azure + BTP).
- OData connector setup (when phase 2).
- Email config + templates.
- Naming convention engine.
- Manual upload (per file kind).
- Routine registry + DSL editor.
- Skill registry (§17).
- Audit log.
- Backup / restore.

### 19.4.4 Owner Guide (housekeeping)

- What the email means.
- How to make a decision (KEEP / CLOSE / DEFER).
- The chat assistant for owners.
- What happens after you submit.

### 19.4.5 Operator Runbook

- Install / upgrade / rollback (mirrors §18.2).
- Common alerts and remediation.
- Backup verification drill.
- Performance tuning checklist.
- How to rotate secrets.

## 19.5 Stakeholder one-pager + deck

- Word one-pager (`docs/stakeholder/onepager.docx`): goal, approach, key decisions,
  timeline, KPIs.
- Executive deck (`docs/stakeholder/exec.pptx`): adapted from
  `ampliFi-CC-Cleanup.pptx` with added "What the app does" slides.

## 19.6 Documentation linting

CI checks:

- All markdown links resolve.
- All `helpKey` values referenced from the frontend exist as doc pages.
- All admin / cockpit pages declare a `helpKey`.
- Every page in `docs/{audience}/*.md` has the required front-matter fields.
- No doc page has a `updated` value older than 365 days at release time (warns).
- Words on a banlist (placeholders such as `TODO`, `TBD`, `Lorem ipsum`) fail CI.

## 19.7 Versioning

- Docs are versioned with the application: `docs/versions/2026-04/...` (snapshot
  cut at release). The in-app drawer always shows the docs for the running
  version; older versions accessible via a switcher.

## 19.8 Translation (forward-looking)

Docs are written in English first. The app's i18n layer (when added) will ingest
translations under `docs/{lang}/...`; the same `helpKey` resolves to the
appropriate language version based on the user's preference. v1 ships English only.

## 19.9 "How to learn the product" path

For first-time users, the in-app **Onboarding** card on the home page presents a
short interactive tour matching the user's role:

- Analyst: "Create a wave → load data → run analysis → lock → invite a reviewer."
- Reviewer: "Open your scope → review one item → use chat → submit."
- Admin: "Configure DB, LLM, email → upload data → create a wave."
- Owner: "Open your housekeeping email → decide → submit."

Each step is a guided overlay with a "skip" option. Completion state is stored
per-user.
