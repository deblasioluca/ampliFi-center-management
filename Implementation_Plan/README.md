# ampliFi — Cost & Profit Center Clean-Up Application

**Spec version:** 0.1 (DRAFT FOR DISCUSSION)
**Date:** 2026-04-25
**Owner:** Luca De Blasio — Group Finance Technology
**Status:** Implementation specification, ready for an LLM-assisted build

---

## What this application does (one paragraph)

The company is migrating to a new ERP ("ampliFi") and must rationalise ~216,000 legacy
SAP cost centers into a clean target structure of cost centers, profit centers and WBS
elements. The 1:1 cost-center ↔ profit-center relationship is being dissolved in favour
of the canonical SAP m:1 model (one profit center groups many cost centers). The roll-out
runs in **waves** across 500+ legal entities over multiple years, so usage of centers
shifts over time and analyses must be repeatable on refreshed data. This application
ingests SAP balances + master data + hierarchies, applies a configurable **decision
tree** plus **ML classification** to propose KEEP / RETIRE / MERGE / MAP outcomes,
lets the project team build, lock and circulate review packages to stakeholders, captures
sign-off, exports approved centers in MDG upload format (and later via MDG API), and
finally runs a **monthly housekeeping cycle** on the new environment to keep it clean.

## Document map (read in this order)

| # | File | What it covers |
|---|---|---|
| 0 | `README.md` (this file) | Index, glossary, conventions |
| R | `00_requirements.md` | The original user requirements, cleaned up and structured (source brief) |
| 1 | `01_business_overview.md` | Business context, scope, stakeholders, glossary |
| 2 | `02_architecture.md` | Tech stack, services, deployment topology, runtime diagram |
| 3 | `03_data_model.md` | Source data structures + application database schema |
| 4 | `04_decision_trees.md` | Cleansing tree + Mapping tree, codified as deterministic rules |
| 5 | `05_ml_and_analytics.md` | Feature engineering, classical ML models, explainability, analytics tools catalog |
| 6 | `06_module_end_user.md` | End-user cockpit: analyse → propose → lock → review → sign-off |
| 7 | `07_module_admin.md` | Admin: users, DB/LLM/OData config, naming convention, wave CRUD |
| 8 | `08_module_waves_and_housekeeping.md` | Wave lifecycle + monthly housekeeping cycle |
| 9 | `09_integrations.md` | SAP OData ingestion, LLM endpoints (Azure + BTP), email, MDG export & API |
| 10 | `10_auth_and_security.md` | Simple auth v1, Azure EntraID v2, RBAC, secrets, audit |
| 11 | `11_api_contracts.md` | REST endpoint inventory (OpenAPI-style summary) |
| 12 | `12_build_plan_phases.md` | Phased delivery plan with milestones & exit criteria |
| 13 | `13_llm_prompts.md` | Prompt templates for analytics narrative, duplicate clustering, naming inference |
| 14 | `14_acceptance_criteria.md` | Definition-of-Done per module |
| 15 | `15_open_questions.md` | Items to confirm with stakeholders |
| 16 | `16_llm_chat_assistant.md` | LLM chat in cockpit (analyst) and in stakeholder review |
| 17 | `17_llm_skills.md` | Skills framework + initial library + skill templates |
| 18 | `18_operations_and_logging.md` | Application logs, lifecycle scripts, systemd service, install-as-service |
| 19 | `19_user_documentation.md` | Full doc set + in-app Help drawer + onboarding tours |
| 20 | `20_visualizations.md` | Visualisations on analysis results + universal statistics strip |
| 21 | `21_implementer_instructions.md` | Standing rules for the implementing LLM (documentation, downloads, security) |

## Glossary (used consistently across all files)

| Term | Definition |
|---|---|
| **Center** | Generic word for an SAP cost center, profit center, or WBS element |
| **CC / PC / WBS** | Cost Center / Profit Center / Work Breakdown Structure element |
| **Legacy** | The current SAP environment with ~216k active centers |
| **Target / ampliFi** | The new ERP target environment |
| **Wave** | A scoped delivery unit, defined by a set of Legal Entities, that moves through analyse → propose → sign-off → close |
| **Outcome** | The decision tree verdict for a center: KEEP / RETIRE / MERGE-MAP / REDESIGN |
| **Mapping target** | For a kept center, the target SAP object: Cost Center, Profit Center, both, WBS-real, WBS-statistical, or PC-only |
| **MDG** | SAP Master Data Governance — destination system for approved centers |
| **Active DB** | Whichever of the local DB or SAP Datasphere is configured as authoritative |
| **Sign-off scope** | A subset of centers (by entity, hierarchy node, or list) assigned to a stakeholder for review |

## Conventions used in this spec

- Code samples use **Python 3.11+** for the backend and **TypeScript** for the Astro frontend.
- Database identifiers are `snake_case`; API JSON keys are `camelCase`.
- All timestamps stored as `TIMESTAMPTZ` (UTC).
- All money fields stored as `NUMERIC(23, 2)` plus a `currency_code CHAR(3)`.
- "MUST", "SHOULD", "MAY" follow RFC 2119.
- Where the spec is intentionally open (so the implementer can choose), it is marked
  `// IMPLEMENTER CHOICE:` with the constraints that bind the choice.

## High-level architecture choices already made

These are decided. The build plan does not need to re-litigate them.

1. **Frontend:** Astro (Node SSR adapter), TypeScript, Tailwind, lightweight component
   library (shadcn-style or DaisyUI — implementer choice).
2. **Backend:** Python 3.11 + **FastAPI**, SQLAlchemy 2.x, Alembic for migrations,
   Pydantic v2 for DTOs.
3. **Workers:** Celery or RQ for long-running jobs (OData refresh, ML scoring, email
   blasts). Redis as broker.
4. **DB v1:** PostgreSQL (recommended over MySQL — better window functions, JSONB,
   `tablesample`).
5. **DB v2:** SAP Datasphere on BTP. Application is configurable to switch the **active**
   data store between local Postgres and Datasphere; both can be loaded from OData.
6. **ML stack:** scikit-learn + LightGBM + sentence-transformers (for naming embeddings).
   Tracked with MLflow (optional but recommended).
7. **LLM access:** abstracted behind a `LLMProvider` interface with Azure OpenAI and SAP
   BTP Generative AI Hub adapters. No raw vendor SDKs leak into business logic.
8. **Auth v1:** username/password + bcrypt + JWT session. **Auth v2:** Azure EntraID
   (OIDC). The auth layer is an interchangeable strategy from day one.
9. **Email v1:** SMTP relay (env-config). **Email v2:** Microsoft Graph
   (consistent with EntraID move).
10. **Naming convention:** Structured / coded (not speaking) — per the deck recommendation.
    Convention is data-driven and editable in admin (see §07).

## What the implementer LLM should do first

1. Read the **whole** spec in the order above before writing code.
2. Generate a **monorepo skeleton** matching `02_architecture.md` (frontend, backend,
   workers, infra-compose).
3. Build feature flags so each analytics tool / ML routine in `05_ml_and_analytics.md`
   can be toggled on/off — this is the "dynamic decision tree" requirement.
4. Treat `04_decision_trees.md` as the ground truth for outcomes; ML augments, never
   replaces, the deterministic tree.
5. Build the **wave** abstraction first; the "full scope" run is just a wave with
   `scope = ALL_ENTITIES − previously_in_scope`.
6. Stop and ask clarification on items in `15_open_questions.md` before locking
   data-shape decisions.
