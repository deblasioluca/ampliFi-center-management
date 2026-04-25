# 17 — LLM Skills Framework + Skill Library

This module defines:

1. A **skills framework** — packaged, declarative competencies that the LLM can
   pick up and apply. Skills are extensible the same way decision-tree routines
   are (§04.6): built-in, plugin, or admin-authored.
2. A **starter library of skills** for two surfaces — analytics (the routines and
   passes from §05) and end-user consulting (the chat assistant from §16).
3. A **skill template** so the team can author new skills consistently.

The skills framework is a thin layer **on top of** the prompt machinery (§13), the
LLM provider (§09.2), and the chat assistant (§16). It does not replace them; it
gives them named, reusable, swappable units.

## 17.1 What is a skill

A **skill** is a self-contained directory shipped with the application or installed
by an admin. It contains:

```
skills/
└── {skill_id}/
    ├── SKILL.md              # required — name, description, when-to-use, params
    ├── system.j2             # required — system prompt fragment
    ├── user.j2               # optional — user prompt template
    ├── output.schema.json    # optional — strict JSON output schema
    ├── tools.json            # optional — list of chat tools the skill may invoke
    ├── examples/             # optional — few-shot examples
    │   ├── 01.input.json
    │   └── 01.output.json
    └── meta.json             # required — id, version, surfaces, model defaults
```

A skill is **declarative**. It does not contain executable code. It is rendered by
the orchestrator the same way a prompt template is, with a small extra layer:
matching, applicability checks, and tool exposure.

## 17.2 SKILL.md structure (template)

```
---
id: cleansing_review
name: Cleansing-tree reviewer
version: 1.0.0
surfaces: [analytics, chat_analyst]
model_defaults:
  temperature: 0.0
  max_tokens: 700
  json_mode: true
applicability:
  any:
    - context.outcome.cleansing in ["KEEP","RETIRE","MERGE_MAP","REDESIGN"]
    - context.has.features.posting_count_window
inputs:
  required: [center, features, outcome]
  optional: [ml]
outputs:
  schema_ref: output.schema.json
tools:
  - proposal_detail
  - explain_outcome
authors: ["Group Finance Tech"]
tags: [cleansing, review, deterministic-aware]
---

# Cleansing-tree reviewer

## When to use
For per-center review of the cleansing decision (KEEP / RETIRE / MERGE_MAP / REDESIGN).
Pairs with §04.1.

## What it does
Looks at the deterministic outcome plus posting/ownership/redundancy features and
either concurs or raises a specific, evidence-grounded objection.

## Output
Strict JSON: { concur, summary, alt_outcome, objection_reason, confidence }.

## Limitations
Only operates on cleansing tree outputs. For mapping decisions use `mapping_review`.
```

## 17.3 Skill registry

Stored in DB (mirrors §04.6 routine registry):

```sql
CREATE TABLE cleanup.llm_skill (
  id              BIGSERIAL PRIMARY KEY,
  skill_id        VARCHAR(64) NOT NULL UNIQUE,
  name            TEXT NOT NULL,
  version         VARCHAR(20) NOT NULL,
  surfaces        TEXT[] NOT NULL,         -- {analytics, chat_analyst, chat_reviewer, chat_owner}
  source          VARCHAR(20) NOT NULL,    -- 'builtin' | 'plugin' | 'admin'
  storage_uri     TEXT NOT NULL,           -- where SKILL.md + files live
  meta            JSONB NOT NULL,
  enabled         BOOLEAN NOT NULL DEFAULT TRUE,
  registered_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Built-ins live under `backend/app/infra/llm/skills/`. Plugins are discovered via
Python entry point `cleanup.llm_skills` (returning a directory path). Admin-authored
skills are uploaded as a zip via `/admin/llm/skills/upload`, validated against the
skill schema, and stored in object storage.

## 17.4 Loading & applicability

At startup the registry scans built-ins + plugins + DB rows and indexes by
`skill_id`. For each skill the framework computes a tiny **applicability check**
(JSON-rule, same DSL as the no-code routine engine) so the orchestrator can pick the
right skill automatically given the context, OR the user/analyst can name a skill
explicitly.

```python
class SkillRegistry:
    def list(self, surface: str | None = None) -> list[SkillMeta]: ...
    def get(self, skill_id: str) -> Skill: ...
    def select_for(self, surface: str, context: dict) -> list[Skill]: ...   # ranked
    def reload(self) -> None: ...
```

## 17.5 Orchestrator integration

### In analytics (§05.9 review modes)

The pipeline `llm_review.skill` field, if set, names a skill instead of a raw prompt
template:

```jsonc
{
  "llm_review": {
    "mode": "SEQUENTIAL",
    "stages": [
      { "skill": "cleansing_review",  "model": "azure:gpt-4o",   "role": "drafter" },
      { "skill": "review_critic",     "model": "btp:gemini-1.5", "role": "critic" },
      { "skill": "review_finalizer",  "model": "azure:gpt-4o",   "role": "finalizer" }
    ]
  }
}
```

The orchestrator resolves each skill, renders its `system.j2 + user.j2`, attaches its
output schema, and exposes its tools. Skill version is snapshotted into the run for
audit.

### In the chat assistant (§16)

When the user sends a message, the chat service:

1. Consults `SkillRegistry.select_for("chat_reviewer", context)` to get a ranked
   list of applicable skills.
2. The top match's system fragment is **merged** with the surface's base system
   prompt (the surface system stays authoritative; skills add specialisation).
3. Tools declared by the skill are added to the available tool set (filtered through
   access control — a skill cannot grant access it doesn't have).
4. If the skill has `output.schema.json`, JSON-mode is used.

A user can also pick a skill manually from a quick menu in the chat input
("/skill explain_change_for_me").

## 17.6 Admin skill management UI

`/admin/llm/skills`:

- List of registered skills (built-in / plugin / admin) with version, surfaces,
  enabled toggle.
- "Upload skill" — zip file of a skill directory. Server-side validation (SKILL.md
  schema, prompt rendering smoke test, tool reference validity).
- "Edit" for admin-authored skills (in-place edit of SKILL.md and templates).
- "Test" — given a sample context, render the prompt and run a single LLM call,
  display result + token cost.
- "Reload registry" — rescans built-ins + plugins + DB.

API:

| Method | Path | Purpose |
|---|---|---|
| GET    | `/admin/llm/skills` | List |
| POST   | `/admin/llm/skills/upload` | Upload zip |
| POST   | `/admin/llm/skills/{id}/test` | Test render + call |
| PATCH  | `/admin/llm/skills/{id}` | Toggle / edit metadata |
| DELETE | `/admin/llm/skills/{id}` | Remove (admin-only; built-ins protected) |
| POST   | `/admin/llm/skills/reload` | Reload registry |

## 17.7 Initial skill library

### A. Analytics skills (used by analytical LLM passes)

| skill_id | Purpose | Surface |
|---|---|---|
| `cleansing_review` | Per-center review of cleansing-tree verdict (KEEP/RETIRE/MERGE_MAP/REDESIGN) | `analytics` |
| `mapping_review` | Per-center review of mapping-tree verdict (target object) | `analytics` |
| `review_critic` | Critic stage (SEQUENTIAL): finds issues with a draft review | `analytics` |
| `review_finalizer` | Final stage (SEQUENTIAL): integrates draft + critique | `analytics` |
| `debate_advocate_keep` | DEBATE advocate for KEEP-side | `analytics` |
| `debate_advocate_retire` | DEBATE advocate for RETIRE-side | `analytics` |
| `debate_judge` | DEBATE judge synthesising both sides | `analytics` |
| `cluster_summarizer` | Summarises a duplicate cluster, recommends survivor | `analytics` |
| `cluster_merge_explainer` | Explains why two centers are flagged duplicates (NLP + numeric features) | `analytics` |
| `naming_purpose_classifier` | LLM fallback for `ml.naming_purpose` (when model untrained) | `analytics` |
| `allocation_detector` | Flags centers that look like allocation vehicles based on naming + balance pattern | `analytics` |
| `risk_finder` | Scans a run and surfaces risky proposals (low confidence, B/S relevant, no owner) | `analytics` |
| `run_summary_exec` | Exec-level narrative summary of a run | `analytics` |
| `comparison_narrative` | Narrates the diff between two runs (Sankey companion) | `analytics` |
| `wave_briefing` | Pre-lock wave briefing for the analyst (what's about to happen, key risks) | `analytics` |
| `housekeeping_findings` | Summarises monthly housekeeping cycle findings | `analytics` |

### B. End-user / consulting skills (used by chat assistants & owner UI)

| skill_id | Purpose | Surface |
|---|---|---|
| `explain_change_for_me` | "What changes for me / my entity if these proposals go ahead?" | `chat_reviewer` |
| `plain_language_glossary` | Translates technical terms (CC, PC, WBS, MERGE_MAP) into business language | `chat_reviewer`, `chat_owner` |
| `impact_summary_scope` | Summarises impact across the reviewer's scope | `chat_reviewer` |
| `issue_finder_scope` | Flags potential issues in the reviewer's scope (B/S balance with no PC, owner mismatch, etc.) | `chat_reviewer` |
| `recommendation_assistant` | Suggests Approve / Not Required / Request, with rationale, for a single center | `chat_reviewer` |
| `report_my_review` | Generates a sign-off summary the reviewer can re-read before submitting | `chat_reviewer` |
| `why_this_outcome` | Explains the deterministic + ML + LLM rationale for a given proposal in plain language | `chat_reviewer`, `chat_analyst` |
| `what_about_center` | Free-form Q&A about a specific center | `chat_reviewer`, `chat_analyst` |
| `owner_outreach_helper` | Drafts a response from the owner (KEEP/CLOSE/DEFER) given housekeeping data | `chat_owner` |
| `scope_progress` | Explains the reviewer's progress and what's left | `chat_reviewer` |
| `cockpit_navigator` | Helps the analyst navigate cockpit views ("show me Sankey for entity X") | `chat_analyst` |
| `compliance_explainer` | Explains the link between an outcome and an internal policy/decision-tree slide | `chat_reviewer`, `chat_analyst` |
| `naming_explainer` | Explains why a new center got a particular ID under the active naming convention | `chat_reviewer`, `chat_analyst` |
| `chained_questions` | Helps the user formulate productive follow-ups (suggest-prompts skill) | `chat_*` |

### C. Templates (skill scaffolds, not skills themselves)

Provided in `skills/_templates/` so admins can clone-and-edit:

| Template | Use it to author … |
|---|---|
| `template_per_center_review` | New per-center review skill (deterministic verdict + LLM commentary, JSON output) |
| `template_aggregate_summary` | New aggregate-summary skill that writes a paragraph from KPIs |
| `template_chat_explainer` | New chat-side skill that explains a concept on demand |
| `template_classifier` | New strict-JSON classifier skill (label + confidence) |
| `template_extractor` | New extractor skill (e.g. extract owner email from free text) |
| `template_critic` | New SEQUENTIAL critic skill |
| `template_advocate` | New DEBATE advocate skill |
| `template_judge` | New DEBATE judge skill |

Each template ships with a fully populated `SKILL.md`, `system.j2`, `user.j2`,
`output.schema.json` (where applicable), a sanity-check unit test, and a
README describing the placeholders to fill.

## 17.8 Authoring workflow (admin)

1. Clone a template via `/admin/llm/skills/clone-template?template=template_chat_explainer&id=my_skill`.
2. Edit SKILL.md and prompt files in the in-app editor (or upload a zip).
3. Run **Test** with a sample context; review the rendered prompt and the LLM's
   response.
4. Save → version 1.0.0 enabled. Version-bump on any subsequent edit; previous
   version remains immutable for any in-flight runs.
5. Optionally **publish** to a curated list visible to all surfaces.

## 17.9 Versioning & immutability

- A skill `version` is bumped on save (semver suggested but not enforced).
- A run that uses a skill snapshots `(skill_id, version)` into the routine_output /
  llm_review_pass record so re-reading old results uses the historical skill text.
- Built-ins are upgraded by a release; admin can pin to an older version per
  analytical config.

## 17.10 Cost & telemetry

- Per-skill token usage and cost are aggregated in
  `app_config['llm.skills.daily_cost_cap_usd.<skill_id>']` (optional per-skill caps).
- Admin dashboard `/admin/llm/skills/usage` shows cost, latency p50/p95, success
  rate per skill over time.
- Skills with consistent JSON-validation failures are auto-disabled with an alert
  (configurable threshold).

## 17.11 Acceptance for the skills layer

- [ ] Skill schema validated; bad uploads rejected with actionable errors.
- [ ] At least 3 analytics skills and 3 chat skills shipped with v1.
- [ ] Templates compile to working skills via "clone-template".
- [ ] Skills picked automatically (`select_for`) match documented applicability
  rules in unit tests.
- [ ] Versioning honoured: re-running a historical run uses the historical skill text.
- [ ] Admin can upload, test, enable/disable a skill without redeploy.
