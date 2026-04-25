# 01 — Business Overview

## 1.1 Why this app exists

The company runs ~216,000 active SAP cost centers today. They will not all migrate to
the new ampliFi ERP. The Group Finance team has chosen a **hybrid Clean & Carry +
Clean & Rebuild** approach (per `ampliFi-CC-Cleanup.pptx` slide 3):

- ~80% of the legacy centers are structurally sound → carry forward after cleansing.
- ~20% are broken (country-specific anomalies, project centers mixed with operational,
  obsolete naming) → redesign bottom-up.

Two decision trees drive every decision (§04):

1. **Cleansing tree** — for the active 216k centers: KEEP / RETIRE-DEACTIVATE / MERGE-MAP.
2. **Mapping tree** — for centers flagged KEEP or MERGE/MAP: assign the right target SAP
   object (Cost Center, Profit Center, both, WBS real, WBS statistical, PC-only).

The **profit-center model is changing**: the legacy 1:1 CC↔PC relationship is dropped.
The target model is the canonical SAP **m:1** relation — one Profit Center groups many
Cost Centers. Identifying which Profit Centers must exist, and which CCs map to each, is
a primary output of the analysis.

## 1.2 Initial findings (from slide 10 of the deck)

These numbers shape the analytics features:

| Slice | Count | Notes |
|---|---|---|
| Total active CCs | ~217,000 | Starting universe |
| CCs with Balance Sheet activity | ~96,000 | Strongly indicates a Profit Center is needed |
| CCs with Revenue activity | ~76,000 | Candidates for PC-only or PC+CC |
| CCs with OPEX activity | ~61,000 | Candidates for CC + PC |
| CCs of "technical" nature | ~75,000 (~⅓) | Need review against the future booking model |
| CCs in Group Functions with B/S activity | ~45,000 | Major PC-creation driver |
| CCs in Taiwan | ~1,300 | First priority (Taiwan-specific roll-out) |
| Inactive legacy CCs (excluded) | ~80,000 | Not in S/4 considerations |

## 1.3 Scope of v1 (in / out)

### In scope

- Ingest legacy SAP master data (CC + PC + hierarchies + balances) via OData and/or file.
- Run the cleansing decision tree + ML augmentation.
- Run the mapping decision tree to determine the target SAP object per surviving center.
- Manage **waves** (CRUD, scope by Legal Entity, lifecycle states).
- "Full scope" analysis with an option to **exclude** entities already in waves.
- Cockpit with toggleable analytical tools and ML routines (dynamic decision tree).
- Build, lock, distribute and capture sign-off on review packages.
- Stakeholder review UI: tick-off per center, per node, or per scope; request new center;
  mark "not required"; close work as final.
- Export approved centers as MDG-format files.
- Naming-convention engine (configurable; can re-use legacy IDs or generate new IDs).
- Email notifications to stakeholders (publish, remind, confirm, housekeeping).
- Monthly housekeeping cycle on the new environment with owner-driven sign-off.
- Admin module: users + user upload, DB/LLM/OData config.
- Authentication: simple user/password v1, Azure EntraID v2.

### Out of scope (v1, but acknowledged)

- Direct write-back to MDG via API (planned; design must allow it — see §09).
- Project / WBS hierarchy ingestion (only WBS classification is in scope; element
  creation deferred).
- Allocation cycle simulation (only flag whether a CC is **used** as an allocation
  vehicle; the cycle itself is not modelled).
- Multi-tenant separation (single deployment, one organisation).

## 1.4 Stakeholders & roles

| Role | Permissions | Notes |
|---|---|---|
| **Admin** | Full system | Group Finance Tech leads. Manages config, users, waves. |
| **Analyst** | Run analyses, build proposals, lock waves, manage scopes | Group Finance core team |
| **Reviewer / Stakeholder** | View assigned scope, tick-off, request, mark not-required, complete | LE finance leads, business owners; usually external to the core team |
| **Center Owner** (housekeeping) | Confirm "still required" / "can close" on owned centers | Anyone listed as `CCTRRESPP` on a target center |
| **Auditor** (read-only) | View everything, export reports, no edit | Internal audit / compliance |

RBAC is enforced at the API layer (§10).

## 1.5 Top-level user journeys (each fully specified in §06)

1. **Wave kick-off → sign-off**: Analyst creates a wave for a set of LEs → loads/refreshes
   data → activates a tailored set of analyses → reviews proposed outcomes → locks the
   proposal → assigns scopes to reviewers → reviewers tick off → analyst closes the wave.
2. **Full-scope strategic run**: Analyst runs analytics across all entities except those
   already in any past or active wave → produces a strategic view (e.g. how many PCs
   total, distribution, naming coverage) → no sign-off, just a snapshot.
3. **Monthly housekeeping**: Cron triggers an analysis on the **new** environment →
   identifies underused centers → emails owners with a sign-off form → owners confirm
   keep/close → closures are queued for MDG.
4. **Naming & MDG export**: Once a wave (or housekeeping run) is signed off, generate
   MDG upload files (CC + PC) using the configured naming convention; queue for direct
   MDG API push when that integration is enabled.

## 1.6 Non-functional requirements

| Area | Requirement |
|---|---|
| **Scale** | Must handle 250k centers, 5–10 years of monthly balances, 500+ entities |
| **Refresh** | Full OData refresh ≤ 4h; incremental delta ≤ 30 min |
| **Analysis runtime** | Cockpit interactive queries < 2s on cached aggregates; ML score of full universe < 30 min |
| **Concurrency** | 25 concurrent analysts; 200 concurrent reviewers during a sign-off campaign |
| **Auditability** | Every outcome change must be traceable: who, when, prior value, reason |
| **Determinism** | Decision-tree rules must be deterministic and reproducible — re-running on the same data MUST produce the same outcomes (ML scores can drift; tree verdicts cannot) |
| **Data residency** | Local DB + SAP Datasphere only; LLM calls go to org-approved Azure / BTP endpoints. No third-party LLM SaaS without admin override |
| **Email** | Digestable + individual modes; rate-limited to avoid mail-relay throttling |

## 1.7 Reference repository

The reference repo `github.com/deblasioluca/sap-ai-consultant` is private (auth required
to fetch). The implementer should request read access via a GitHub PAT and align:

- SAP OData client patterns (auth, paging, retry, schema introspection).
- LLM provider abstraction (Azure + BTP) — interface, retry, token handling.
- Project layout and module decomposition style.

If the repo is unavailable at build time, the patterns described in §09 are sufficient
to implement everything from scratch.
