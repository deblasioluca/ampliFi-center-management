# 04 — Decision Trees (codified) + Extensible Routine Framework

This module is the heart of the application. It encodes the two decision trees from the
deck (`ampliFi-CC-Cleanup.pptx` slides 4 and 5/12) as deterministic, auditable rules,
and exposes them through a **pluggable framework** so new routines can be added without
modifying core code.

> **Determinism rule**: Re-running a tree on the same data snapshot MUST produce the
> same outcomes. ML scores and LLM commentary may augment but never override the
> deterministic verdicts unless the analyst explicitly enables an "ML override" routine
> in the config (§04.6).

## 4.1 Tree A — Cleansing tree

Applies only to the **active** legacy centers (~216k). Outcome ∈ {KEEP, RETIRE,
MERGE_MAP, REDESIGN}.

This pseudocode is the **literal arrow-by-arrow transcription** of slide 4
(`ampliFi-CC-Cleanup.pptx` / `Folie4.jpeg`). All five numbered criteria are
decision branches, not just informational flags:

```
INPUT: a legacy cost center C with attached features
       (balances, last_posting_period, owner, hierarchy_membership, dependencies)

# ① Posting activity
CHECK posting_activity(C):
  IF C.months_since_last_posting > posting_inactivity_threshold        # default 12
     AND C.posting_count_window <= posting_minimal_threshold:           # default 0
       RETURN RETIRE  with reason "posting.inactive"

# ② Business ownership
CHECK ownership(C):
  IF NOT has_valid_owner(C):
       RETURN RETIRE  with reason "ownership.no_owner"

# ③ Redundancy (duplicate / overlap with another center's purpose)
CHECK redundancy(C):
  duplicate_cluster = find_duplicates(C, similarity_threshold)          # naming/balance/hier
  IF duplicate_cluster IS NOT NULL:
       RETURN MERGE_MAP  with reason "redundancy.duplicate" plus cluster_id

# ④ Hierarchy compliance — must fit in exactly one hierarchy node.
#    If not, the center needs redesign / remap.
CHECK hierarchy_compliance(C):
  IF C.hierarchy_membership_count != 1:
       RETURN MERGE_MAP  with reason "hierarchy.non_compliant"
       # (Strict-mode flag retained: when strict_hierarchy_mode=true AND count==0
       #  the spec records sub-reason "hierarchy.orphan" so the analyst can split
       #  pure orphans from multi-membership cases at review time.)

# ⑤ Cross-system dependency — used in BW extractors, models, GRC, intercompany, etc.
#    Per slide rule "high dependency → careful migration, mapping required",
#    such a center needs to go through the mapping process (i.e. MERGE_MAP),
#    not a clean keep.
CHECK cross_system_dependency(C):
  IF C.in_bw_extractors OR C.in_grc OR C.in_intercompany:
       RETURN MERGE_MAP  with reason "dependency.high"

# Default — all five checks passed cleanly
DEFAULT:
  RETURN KEEP
```

### Default thresholds (overridable in any analysis_config)

| Param | Default | Notes |
|---|---|---|
| `posting_inactivity_threshold` (months) | 12 | Per deck "no activity 6–12 months → deactivate" |
| `posting_minimal_threshold` (count in window) | 0 | A few stray reversals shouldn't save a center |
| `similarity_threshold` (duplicate detection) | 0.92 | Cosine on name embedding + numeric features (§05) |
| `strict_hierarchy_mode` | false | When true, hierarchy-orphan sub-reason is escalated to REDESIGN at review time; default keeps it as MERGE_MAP |

## 4.2 Tree B — Mapping tree

Applies to centers flagged KEEP or MERGE_MAP. Outcome (`target_object`) ∈
{COST_CENTER, PROFIT_CENTER_ONLY, PROFIT_CENTER_AND_COST_CENTER, WBS_REAL,
WBS_STAT, CANDIDATE_FOR_CLOSING}.

> **Canonical source: slide 12** (`Folie12.jpeg`) — the refined version in the
> appendix. It differs from slide 5 in three meaningful ways:
> (a) the standalone B/S-relevance pre-step is removed,
> (b) "project costs" is evaluated **before** "operational costs",
> (c) the operational-costs branch does **not** carry a feeder-system sub-check —
> operational costs alone yields Cost Center.
> The fall-through outcome is explicitly **"Candidate for closing"** rather than
> the looser "Redesign / Naming convention" footer on slide 5.

```
INPUT: a kept/mapped legacy center C

# ① Direct revenues booking (now or in future)
IF has_direct_revenue(C):
   # ② (left, after Yes-①): Feeder system?
   IF has_feeder_costs(C):
       RETURN PROFIT_CENTER_AND_COST_CENTER         # operations + profitability
   ELSE:
       RETURN PROFIT_CENTER_ONLY                    # profitability only

# ② (right, after No-①): Project costs
IF collects_project_costs(C):
   RETURN WBS_REAL                                  # project accountability

# ③ Direct operational costs (HR, IT, etc.)
IF has_operational_costs(C):
   RETURN COST_CENTER                               # operations accountability

# ④ Vehicle for revenue allocation
IF used_for_revenue_allocation(C):
   RETURN WBS_REAL                                  # revenue-sharing vehicle

# ⑤ Vehicle for cost allocation
IF used_for_cost_allocation(C):
   RETURN COST_CENTER                               # cost-allocation vehicle

# ⑥ Information-only tracking
IF used_for_info_only(C):
   RETURN WBS_STAT                                  # statistical / parallel reporting

# Fall-through
RETURN CANDIDATE_FOR_CLOSING                        # = NONE in the data model
```

### Mapping outcome ↔ database enum

The DB type `cleanup.target_object` (§03.2.2) maps as:
`COST_CENTER`→`CC`, `PROFIT_CENTER_ONLY`→`PC_ONLY`, `PROFIT_CENTER_AND_COST_CENTER`→`CC_AND_PC`,
`WBS_REAL`→`WBS_REAL`, `WBS_STAT`→`WBS_STAT`, `CANDIDATE_FOR_CLOSING`→`NONE`.
The original `PC` enum (used as a "PC tentatively because of B/S postings") is no
longer produced by the canonical slide-12 flow and is reserved for legacy data
imports only. New runs MUST emit one of the six terminal outcomes above.

### Feature definitions (used by both trees)

These are computed once per center per data_snapshot in `mv_balance_per_center`,
`mv_account_class_split`, etc. (see §03.3) and made available to every routine.

| Feature | Source | Definition |
|---|---|---|
| `months_since_last_posting` | `balance` | `current_period - max(period_id where posting_count > 0)` |
| `posting_count_window` | `balance` | `SUM(posting_count) where period_id in last N months` |
| `has_valid_owner` | `legacy_cost_center.responsible` + `attrs.owner` | non-empty AND resolvable to an active person/cost-element |
| `has_bs_postings` | `mv_account_class_split` | `bs_amt != 0` over window |
| `has_direct_revenue` | `mv_account_class_split` | `rev_amt != 0` AND not coming from allocation cycles |
| `has_operational_costs` | `mv_account_class_split` | `opex_amt != 0` AND not coming from allocation cycles |
| `has_feeder_costs` | `attrs.feeder_flag` (provided source-side) | TRUE if feeder system posts to C |
| `collects_project_costs` | `attrs.cost_pattern` or LLM classifier on naming | naming/usage matches project-style |
| `used_for_revenue_allocation` | allocation cycles table (when available) | C is a sender/receiver in revenue cycle |
| `used_for_cost_allocation` | allocation cycles table | C is part of cost-allocation cycle |
| `used_for_info_only` | derived | non-zero stats but no real cost/revenue postings |

**Where the data isn't available** (e.g. allocation cycles), the routine MUST emit a
verdict of `UNKNOWN` and contribute a flag to `routine_output.payload`. The tree's
default behaviour on UNKNOWN is to **skip that branch and continue**, never to silently
fail.

## 4.3 Outcome combination rules

For each center the run records:
- `cleansing_outcome` ∈ {KEEP, RETIRE, MERGE_MAP, REDESIGN}
- `target_object` ∈ {CC, PC, CC_AND_PC, PC_ONLY, WBS_REAL, WBS_STAT, NONE} (only set if
  cleansing_outcome ∈ {KEEP, MERGE_MAP})
- `rule_path`: ordered list of routine codes that fired with their verdicts
  (audit trail)
- `ml_confidence`: optional, 0..1, from the ML classifier (§05)
- `rationale`: optional LLM-written explanation (§05.7)

## 4.4 Override semantics

An analyst can override any proposal:

```
override(proposal_id, new_outcome, new_target_object?, reason: required)
```

Overrides are stored on the proposal row (`override_by`, `override_reason`,
`override_at`). The original deterministic verdict and rule_path are **not** modified —
they remain in `routine_output` for audit. Re-running the same analysis_run cancels
overrides only if the analyst explicitly opts to.

## 4.5 Engine implementation (sketch)

`backend/app/domain/decision_tree/engine.py`:

```python
from typing import Protocol
from dataclasses import dataclass

@dataclass(frozen=True)
class CenterContext:
    center_id: int
    features: dict          # populated per data_snapshot
    flags: dict             # in/out

@dataclass(frozen=True)
class RoutineResult:
    code: str
    verdict: str            # routine-specific
    score: float | None
    payload: dict
    comment: str | None     # only when an LLM is involved
    short_circuit: bool     # if True, halts the tree for this center

class Routine(Protocol):
    code: str
    kind: str               # 'rule' | 'ml' | 'llm' | 'aggregate'
    def run(self, ctx: CenterContext, params: dict) -> RoutineResult: ...

class Engine:
    def __init__(self, registry: 'RoutineRegistry'):
        self._registry = registry

    def execute(self, config: dict, ctx: CenterContext) -> list[RoutineResult]:
        results: list[RoutineResult] = []
        for step in config['pipeline']:
            if not step.get('enabled', True):
                continue
            routine = self._registry.get(step['routine'])
            res = routine.run(ctx, step.get('params', {}))
            results.append(res)
            if res.short_circuit:
                break
        return results
```

`config` is the analytical configuration (§05.6). The pipeline order is preserved exactly
so re-runs are deterministic.

## 4.6 Extensibility — adding new routines

The framework is **plug-in based**. A new routine can be added in three ways:

### A. Built-in (preferred for stable rules)

1. Implement a class in `backend/app/domain/decision_tree/routines/<name>.py`
   that conforms to the `Routine` protocol.
2. Decorate with `@register_routine(code="rule.<your_name>", kind="rule")`.
3. Provide a JSON-Schema (`schema.json` next to it) describing parameters.
4. On boot, the registry scans the package and inserts/updates a row in
   `cleanup.routine`.

### B. Plugin module (third-party / customer-specific)

A separate Python package shipped as `cleanup_plugin_<name>`. The application
auto-discovers plugins via Python entry points (`cleanup.routines`):

```toml
# in plugin's pyproject.toml
[project.entry-points."cleanup.routines"]
my_check = "cleanup_plugin_acme.routines:MyCheck"
```

On startup the registry imports each entry point, validates it implements the
Protocol, and registers it. Plugins can also declare ML model artifacts (loaded from
S3/MinIO).

### C. No-code / declarative (rules only)

A simple **rule DSL** stored in `cleanup.routine` rows where `source='custom'`. Example
shape stored in `routine.schema`/`default_params`:

```json
{
  "kind": "rule",
  "expression": {
    "all": [
      { "feature": "months_since_last_posting", "op": ">", "value": 12 },
      { "feature": "posting_count_window",     "op": "<=", "value": 0 }
    ]
  },
  "verdict_when_true":  { "outcome": "RETIRE", "reason": "custom.inactive_strict" },
  "verdict_when_false": "passthrough"
}
```

The `rule_dsl` evaluator is a small whitelisted interpreter: only `==`, `!=`, `<`, `<=`,
`>`, `>=`, `in`, `not_in`, plus boolean `all`, `any`, `not`. **No `eval()`. No code
execution.** This lets analysts (admin role) author and test new rules entirely from the
UI (§07.6).

### Registry contract

```python
class RoutineRegistry:
    def get(self, code: str) -> Routine: ...
    def list(self, kind: str | None = None) -> list[RoutineMeta]: ...
    def reload(self) -> None: ...           # rescans builtin + plugins + DSL rows

# singleton; thread-safe; reload() is admin-only
```

The cockpit pulls `list()` to render the pipeline editor (§06.3). Each routine entry
exposes its JSON-Schema so the UI can render a parameter form automatically.

## 4.7 Tree C — V2 CEMA-based Migration Decision Tree (implemented)

**Added in PR #51.** This tree automates the CEMA-based center migration workflow
described by Patrick's production steps document. It ingests the flattened CEMA
hierarchy (from CC-with-hierarchy upload) and determines which centers to migrate,
whether to create 1:1 or 1:n PC-CC relationships, and assigns sequential PC/CC IDs.

### 4.7.1 V2 Pipeline Routines

The V2 engine uses 4 pipeline routines executed in sequence:

```
INPUT: a legacy cost center C with CEMA hierarchy path + balance data

# ① Retire flag
ROUTINE v2.retire_flag:
  IF C.description CONTAINS "_RETIRE":
      SET C.retire = true
      RETURN verdict "retire" with reason "description_contains_retire"

# ② Balance-based migration check
ROUTINE v2.balance_migrate:
  CHECK balance categories: Assets, Liabilities, Equity, Statistical,
        Revenues, Direct Costs, Hard Cost Allocations, Allocated Costs
  IF ANY category has non-zero balance AND NOT C.retire:
      SET C.migrate = "Y"
      RETURN verdict "migrate" with reason "has_balance"
  ELSE:
      SET C.migrate = "N"
      RETURN verdict "no_migrate" with reason "no_balance" or "retired"

# ③ PC approach determination
ROUTINE v2.pc_approach:
  LOOKUP C's CEMA hierarchy path against configurable approach rules:
    - Rules reference hierarchy nodes selected by the admin (e.g. "Group Functions", "Asset Management")
    - Matching nodes → approach = "1:n" (one PC for the block, name from level 3)
    - All others → approach = "1:1" (each center gets its own PC, name from lowest hierarchy level)
  RETURN verdict with approach type and pc_name

# ④ Combine migration results
ROUTINE v2.combine_migration:
  AGGREGATE results from routines 1-3
  SET final: migrate, approach, pc_name, retire status
  RETURN combined verdict
```

### 4.7.2 PC/CC ID Assignment (post-pipeline)

After the pipeline completes, IDs are assigned to migrated centers:

- **PC IDs**: `P00001` – `PZZZZZ`, configurable start (default `P00137`)
  - 1:1 centers: each gets an incremental PC ID
  - 1:n centers: one PC ID shared by all centers in the block
- **CC IDs**: `C00001` – `CZZZZZ`, sequential for all migrated centers
- Centers with `migrate = "N"` receive no IDs
- Sort order follows hierarchy path (Ext_L0 → L13) for deterministic ID assignment

### 4.7.3 PC Approach Configuration

The approach rules are stored as JSON in the `AnalysisConfig` parameters and
are editable through the admin UI (Decision Trees page):

```json
{
  "pc_approach_rules": [
    {
      "name": "Group Functions",
      "match": {"field": "ext_l1", "op": "in", "values": ["GF_NODE_01", ...]},
      "approach": "1:n",
      "pc_level": 3
    },
    {
      "name": "Asset Management",
      "match": {"field": "ext_l1", "op": "==", "value": "AM_NODE"},
      "approach": "1:n",
      "pc_level": 3
    }
  ],
  "default_approach": "1:1",
  "pc_start_id": 137,
  "cc_start_id": 1
}
```

The hierarchy nodes in the rules can be selected via a hierarchical picker in
the admin UI (referencing actual `HierarchyNode` records).

### 4.7.4 V2 Export

`GET /api/waves/{id}/runs/{run_id}/export-v2` generates an Excel workbook with
3 sheets: PC template, CC template, and Legacy→Target mapping.

### 4.7.5 Simulation vs Activation

V2 runs support two modes (see §08 for lifecycle details):

- **Simulation** (`mode = "simulation"`): Assigns temporary IDs (`CT...`, `PT...`).
  Multiple simulation versions can be saved and compared.
- **Activated** (`mode = "activated"`): Assigns real PC/CC IDs. Marks the run
  as the preferred run for the wave. Only one activated run per wave.

## 4.8 Configuration shape (cross-reference)

The exact JSON shape of an analytical configuration that selects which routines fire,
their order, and parameter values is defined in **§05.6**. The decision tree engine
consumes that shape verbatim.

## 4.9 Decision Tree Configuration Admin UI (implemented)

**Added in PR #52.** The admin UI provides a full configuration management interface
at `/admin` → Decision Trees:

- **List all configs**: Grouped by code, shows latest version + status
- **Create new config**: Select routines (checkboxes), set parameters (JSON editor),
  code/name/description
- **Edit existing config**: Creates new immutable version (for reproducibility)
- **Clone/fork configs**: Create variants based on existing configurations
- **Version history**: View all versions for any config with timestamps

When running analysis or simulation on a wave, the analyst selects:
1. **Engine version** (V1 Decision Tree or V2 CEMA Migration) via dropdown
2. **Config version** from available configs via dropdown

The selected config is passed as `config_id` in the analysis POST body and stored
on the `AnalysisRun` record for traceability.

## 4.10 Testing requirements

- Each built-in routine must have a unit test covering: positive verdict, negative
  verdict, missing feature (UNKNOWN), boundary values.
- A **golden corpus** of ~200 hand-labelled centers is checked into the repo
  (`backend/tests/fixtures/golden_corpus.csv`); CI fails if any classification on
  the corpus changes silently.
- Property-based tests (Hypothesis) MUST verify determinism: same input → same output.
