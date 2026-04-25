# 15 — Open Questions

These items are intentionally unresolved in this draft. Each must be confirmed
with the relevant stakeholder before the dependent module is finalised.

| # | Topic | Question | Default in v1 | Decision needed by | Owner |
|---|---|---|---|---|---|
| 1 | DB choice | Postgres or MySQL for the local DB? Spec assumes Postgres. | Postgres | Phase 0 (week 1) | Group Finance Tech |
| 2 | Account-class ranges | Confirm the SAP G/L account ranges that map to B/S, REVENUE, OPEX, OTHER (used by the mapping tree). Default ranges are placeholders. | See §03.5 default | Phase 3 (week 4) | Patrick / GL data owner |
| 3 | Cost-center "feeder" flag | Where does `attrs.feeder_flag` come from in source data? Today there is no canonical column. | LLM/heuristic placeholder | Phase 3 | SAP master-data team |
| 4 | Allocation cycle data | Do we have access to allocation-cycle config tables to drive the "vehicle for allocation" branches? | Default to UNKNOWN; rule emits flag | Phase 3 | SAP CO team |
| 5 | Naming convention syntax | Templates and placeholders sketched in §07.5. Confirm the exact CC/PC formats and whether they should be relatable (e.g. CC ID derives from PC ID). | `PC-{coarea}-{seq:6}` / `CC-{pc_root}-{seq:4}` | Phase 6 (week 8) | Group Finance |
| 6 | Naming reuse policy | When a legacy ID survives, do we reuse the legacy ID or always issue a new ID? Spec defaults to NEW. | Always new ID | Phase 6 | Group Finance |
| 7 | LLM provider precedence | Default provider for analytics/chat: Azure or BTP? | Azure | Phase 5 (week 7) | Architecture |
| 8 | DEBATE mode usage | Which scope should DEBATE be enabled for by default? Defaults to MERGE_MAP / REDESIGN with min-balance gate. | per §05.6 | Phase 5 | Group Finance |
| 9 | EntraID timing | When is EntraID expected to replace local auth? | Phase 10 (week 14) | Phase 1 | Identity team |
| 10 | OData services list | Concrete list of SAP OData endpoints for balances/master/hierarchies and their auth methods. | Manual upload only | Phase 9 (week 12) | SAP integrations |
| 11 | MDG API endpoint | Confirm the target MDG OData entity set (legacy `USMD_PROCESS_REQUESTS` vs newer API), idempotency keys, and approval workflow. | File export only | Phase 11 (week 15) | MDG team |
| 12 | Owner email resolution | When a target_*.responsible field is a free-text string (e.g. "00130563 A.König-Wid"), how do we reliably resolve it to an email? Need an HR / directory mapping. | LLM extractor + fallback list | Phase 8 (week 11) | HR / IT |
| 13 | Reviewer onboarding | Tokenised links by default; should the org also offer EntraID-based identity tying? | Token in v1; identity in v2 | Phase 6 | Identity / Finance |
| 14 | Housekeeping cadence | Monthly default. Confirm window, weekend behaviour, and SLA. | 1st of month, 30-day deadline | Phase 8 | Group Finance |
| 15 | Datasphere active store | When are we expected to flip from Postgres to Datasphere? | After phase 11 | Phase 12 | Architecture |
| 16 | Reference repo access | Provide a PAT for `github.com/deblasioluca/sap-ai-consultant` so the implementer can lift `.env`, LLM config, docgen patterns, and SAP connection client. | Spec is self-contained | Phase 0 | Luca |
| 17 | LLM cost cap | What is the daily / wave LLM cost cap (USD)? | $250/day default | Phase 1 | Finance ops |
| 18 | Skill governance | Who can author / approve admin-uploaded LLM skills? Limited to admin role; policy needed for change management. | Admin-only | Phase 5 | Group Finance Tech |
| 19 | Stat baselines | What is "the start of project" baseline used by the statistics strip's `vs start of project` comparison? | First successful refresh | Phase 2 | Group Finance |
| 20 | Visualization toolkit | Recharts vs ECharts default? Spec defers to implementer. | Recharts | Phase 3 | Frontend |
| 21 | Hierarchy non-compliance | If a center sits in 0 hierarchy nodes, REDESIGN or RETIRE? Spec defaults to REDESIGN under strict mode only. | Strict mode off | Phase 3 | Group Finance |
| 22 | Legal entity mapping | Confirm the canonical Legal Entity → Company Code mapping. Is one entity ever split across multiple company codes? | 1:1 | Phase 2 | Master data |
| 23 | Wave conflict resolution | When two waves propose conflicting outcomes for the same center, what is the precedence rule? | Admin manual resolve | Phase 6 | Group Finance |
| 24 | Run summary generation | Should the run summary doc be auto-generated on every run, or only on lock? | On lock only | Phase 7 | Group Finance |
| 25 | Reviewer SLA | Default 14-day review window with 7/14-day reminders. Confirm. | 14 days | Phase 6 | Group Finance |
| 26 | Mapping-tree canonical version | Slide 5 vs Slide 12 of the deck differ (B/S pre-step, project-vs-operational order, feeder sub-check, fall-through name). | **RESOLVED 2026-04-25** — slide 12 is canonical. §04.2 updated accordingly. | — | Luca |
| 27 | Cleansing-tree branch outcomes | Whether Q4 (hierarchy non-compliance) and Q5 (cross-system dependency) are decision branches or informational flags. | **RESOLVED 2026-04-25** — slide-4 arrows confirm both are decision branches routing to MERGE/MAP. §04.1 updated. | — | Luca |
