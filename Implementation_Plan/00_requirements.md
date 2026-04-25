# 00 — Source Requirements (cleaned up)

This file is a faithful, well-structured restatement of the requirements you provided.
It is the **source brief** that the rest of this spec implements. Where the brief was
ambiguous, an assumption is captured here and resolved in the design files (§02 onward).

---

## R1. Purpose of the application

> We are moving to a new ERP solution ("ampliFi") and need to clean up cost centers
> and produce a rationalised target structure. The current 1:1 cost-center ↔ profit-center
> relationship is being dissolved; we are introducing the proper SAP model where one
> profit center can have many cost centers assigned.

The application supports the team that performs this cleanup by combining (a) source
SAP data (master, hierarchies, balances), (b) a deterministic decision tree, and (c) ML
+ LLM-based augmentation, into an analyse → propose → sign-off → export workflow.

---

## R2. Analytical capability

### R2.1 Active vs inactive determination

The app analyses current balances by multiple means to decide whether a center is:

- actively used,
- a candidate to be closed because of small balances,
- a candidate to be closed because of low transaction volume,
- otherwise relevant.

### R2.2 Decision tree

Analysis follows the **decision tree documented in `ampliFi-CC-Cleanup.pptx`** (slides 4
and 5/12), composed of two stages:

1. **Cleansing tree** — for the 216k active legacy centers, classifies each as
   KEEP / RETIRE-DEACTIVATE / MERGE-MAP / REDESIGN.
2. **Mapping tree** — for centers flagged KEEP or MERGE/MAP, decides the target SAP
   object: Cost Center, Profit Center, both (CC + PC), PC-only, WBS real, WBS statistical
   or NONE (candidate for closing).

### R2.3 Machine learning support

ML supports decisions alongside the deterministic tree. Models classify centers,
detect duplicates, and infer purpose from naming. ML augments the tree (auditable),
never silently overrides it.

### R2.4 Multiple analysis runs with named, versioned configurations

a) **Define / save / amend** analytical configurations (which routines are active,
   thresholds, ML models, LLM mode).
b) **Save analytical results in versions** — every run is preserved.
c) **Compare results across versions** — outcome diffs, target-object diffs,
   per-center deltas.
d) **Comprehensive LLM review** with three modes: **Single LLM**, **Sequential
   pipeline**, **Full debate** (multi-agent debate + judge), producing per-center
   commentary on every center and on overall results.

### R2.5 Extensible analysis framework

The framework MUST be extensible: new routines (rules, ML models, LLM steps) can be
added to the decision tree without re-architecting. Three extension paths are required:
built-in code, plugin packages, and a no-code rule DSL.

---

## R3. Scope: full vs waves

### R3.1 Wave concept

The new ERP rolls out to **500+ entities in waves over multiple years**. Center usage
moves over time, so analyses must be repeatable on refreshed data. Scopes are defined
by Legal Entity and managed through a lifecycle:

```
analyse → propose → sign-off → close
```

### R3.2 Data refresh

The app must refresh data on a regular basis (load new balances, new lists of cost and
profit centers, hierarchies, etc.). The analysis tells where new profit centers are
needed and which cost centers map to them.

### R3.3 Full-scope analysis with exclusions

The app must support running an analysis on the **full dataset** (current state).
When running full-scope, there must be an option to **exclude entities that are or
were already in scope of a wave** (so analyses don't double-count). Default ON.

---

## R4. Cockpit (after data load)

### R4.1 Sophisticated analytical tools, ML routines, dynamic reports

After load, the user has access to a cockpit covering:

- analytical tools (universe explorer, hierarchy view, Sankey, coverage map,
  inactivity heatmap, cluster explorer, naming preview, run comparison, LLM transcripts),
- ML routines (duplicate clustering, outcome classifier, target-object classifier,
  naming-purpose classifier, anomaly detector for housekeeping),
- dynamic reports on results, hierarchies, attributes.

### R4.2 Toggleable tools (dynamic decision tree)

Tools and routines must be **activatable / deactivatable** so the user can compose
their own decision tree dynamically. Configurations are saved (R2.4 a).

### R4.3 Lock and circulate the proposal

Once happy with the proposal, the user can **lock it** and provide it to stakeholders
for review.

---

## R5. Stakeholder review

### R5.1 Flexible scope assignment

The user can flexibly invite reviewers and dynamically **assign scope**. Scope can be
defined by:
- Legal Entity (entities),
- Hierarchy node (cost-center group / profit-center group),
- Explicit list of centers.

### R5.2 Three viewing modes

The reviewer must be able to see:
- **Old centers as a flat list**,
- **Old centers in hierarchical view**,
- The **new (proposed)** structure.

### R5.3 Per-item review actions

The reviewer can:
- tick off individual cost / profit centers,
- tick off whole scopes,
- tick off via hierarchy nodes (cascade to leaves),
- **request a new center** if something is missing,
- **mark a center as not required**,
- mark their **work as done** — at which point everything available in their scope is
  considered signed off.

---

## R6. Housekeeping cycle (post-go-live)

The same analytical capabilities run on the **new** environment as a recurring
housekeeping cycle (e.g. monthly):

- Identify centers that are not actively used or that the decision tree / ML deem
  unneeded.
- Notify the **center owner** (sourced from the cost center master) for review.
- The owner confirms either "still required" or "can be closed".

This forms a continuous quality-assurance loop on the new structure.

---

## R7. Technology stack

| Concern | Decision |
|---|---|
| Frontend | Astro |
| Backend | Python |
| DB v1 (local) | MySQL or Postgres (Postgres recommended in the design — see §02) |
| DB v2 (cloud) | SAP Datasphere on BTP |
| Active store | The active DB is a configuration choice (the other can act as shadow) |
| SAP integration | **OData, ADT, and SOAP/RFC** for loading balances, masters, hierarchies into either DB. The user can choose the protocol per object. |
| Auth v1 | Simple username + password |
| Auth v2 | Azure EntraID |
| LLM access | Azure endpoints AND SAP BTP endpoints |

---

## R8. Data ingest — file upload OR direct from SAP (admin-only)

For each ingestible object the admin must choose between:

- **Manual file upload** of:
  - balances,
  - cost centers,
  - profit centers,
  - hierarchies (header / nodes / leaves).
- **Direct load from a configured SAP system** via one of three protocols:
  - **OData**,
  - **ADT** (ABAP Development Tools REST),
  - **SOAP / RFC** (`pyrfc` primary, SOAP fallback).

Both paths share the same staging, validation, and loader logic. Object catalogue
includes companies (T001), cost centers (CSKS/CSKT), profit centers (CEPC/CEPCT),
balances, account-class ranges, and hierarchies (CC groups class `0101`, PC groups
class `0106`). For hierarchies the admin **picks** which `SETNAME`s to download
(single, multiple, pattern, or all in a class).

Patterns for the SAP connection layer are reused from
`github.com/deblasioluca/sap-ai-consultant` once the PAT is provided.

---

## R9. Application structure

### R9.1 End-user view

Covers all of the above (R2 → R6).

### R9.2 Admin view (admin-only features)

The admin view covers:

- **User administration**, including bulk **user list upload** with email.
- **Configuration of DB connections** (local DB + Datasphere, choose active).
- **Configuration of LLM endpoints** (Azure + BTP).
- **Configuration of SAP OData** (endpoints, credentials, schedule).
- **Cleanup waves**: cockpit to **create and manage waves** (CRUD), run analytics,
  view progress, manage reviews.
- **Manual upload of source data** (R8).

---

## R10. Naming convention

- The app must support a **naming-convention engine** for the new centers.
- Profit centers and cost centers may have **individual** conventions, but may also
  have a **relation** (e.g. CC ID derives from PC ID).
- For legacy centers identified as "should survive", the new ID is generated by the
  naming engine — but a **legacy cost center may become a profit center** (this is part
  of the analysis and decision tree).

---

## R11. Email notifications

A built-in email engine is required to:

- send invitations to stakeholders to review,
- send reminders,
- send confirmations once review work is closed,
- inform stakeholders when a wave is published for review,
- inform owners about housekeeping results,
- and other transactional events.

---

## R12. Export to MDG

- **Download functionality** for the new cost and profit centers in MDG-compatible
  upload format (per the provided 0G upload templates).
- Direct push to **SAP MDG via API** is desirable as a phase-2 capability.

---

## R13. Data structures (provided)

### R13.1 Balance structure

`balance_structure.xlsx` — header columns: `COMPANY_CODE`, `SAP_MANAGEMENT_CENTER`,
`CURR_CODE_ISO_TC`, `SUM(P.GCR_POSTING_AMT_TC)`, `SUM(P.GCR_POSTING_AMT_GC2)`, `COUNT(*)`.

### R13.2 Cost / profit center master

Standard SAP cost center structure (CSKS-equivalent) plus the wide attribute table
(`Center_structure.xlsx`). MDG upload format (data model `0G`, entity types `CCTR` and
`PCTR`).

### R13.3 Hierarchies

Standard SAP set framework:
- `SETHEADER` / `SETHEADERT` — header data and language texts.
- `SETNODE` — parent-child relationships between nodes.
- `SETLEAF` — leaf values (cost / profit centers, single or BT range).

---

## R14. Reference repository

`github.com/deblasioluca/sap-ai-consultant` — used as a style and integration-pattern
reference (SAP integration patterns, LLM integration). The repo is private; the
implementer should request a PAT to align integration patterns.

---

## R15. Mapping to design files

| Requirement | Implemented in |
|---|---|
| R1, R2.1–R2.3 | §04 Decision Trees, §05 ML & Analytics |
| R2.4 (configs, versioning, comparison, LLM modes) | §05.6 – §05.9, §03.2.2 |
| R2.5 (extensibility) | §04.6 |
| R3 (waves, refresh, full-scope, exclusions) | §08, §03.2.2 |
| R4 (cockpit, toggleable, lock) | §06.3 – §06.5 |
| R5 (review scopes, modes, actions) | §06.7 – §06.8 |
| R6 (housekeeping) | §08.5 |
| R7 (stack) | §02 |
| R8 (manual upload) | §07.7 |
| R9 (app structure, admin) | §06, §07 |
| R10 (naming convention) | §07.5 |
| R11 (email) | §09.3 |
| R12 (MDG export + API) | §09.4 |
| R13 (data structures) | §03.1 |
| R14 (reference repo) | §02.1, §09.1 |
