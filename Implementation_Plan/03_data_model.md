# 03 — Data Model

This section defines (a) the **source data structures** the app must ingest and (b) the
**application database schema** for analysis, proposal, sign-off, and housekeeping.

---

## 3.0 High-Level Database Table Overview

All tables reside in the default PostgreSQL schema. Tables with scope segregation carry
a `scope` column (`cleanup` | `housekeeping` | `explorer`) and a `data_category` column
(`legacy` | `target`). See §02.5 for the scope segregation architecture.

### Master / Reference Data

| Table | Contents | Scope-aware | Key fields |
|---|---|---|---|
| `entity` | Legal entities (company codes) — T001 | Yes | `ccode`, `name`, `country`, `region` |
| `employee` | Employee master (GPN-based) | Yes | `gpn`, `bs_name`, `email`, `job_desc` |
| `legacy_cost_center` | Cost centers from SAP CSKS | Yes | `coarea`, `cctr`, `ccode`, `txtsh`, `responsible` |
| `legacy_profit_center` | Profit centers from SAP CEPC | Yes | `coarea`, `pctr`, `ccode`, `txtsh`, `responsible` |
| `target_cost_center` | Target/processed cost centers (MDG-ready) | Yes | Same as legacy + `approved_in_wave` |
| `target_profit_center` | Target/processed profit centers (MDG-ready) | Yes | Same as legacy + `approved_in_wave` |
| `balance` | Posting balances (period-level, multi-currency) | Yes | `ccode`, `cctr`, `coarea`, `fiscal_year`, `period` |
| `gl_account_ska1` | GL Accounts at chart-of-accounts level | Yes | `ktopl`, `saknr`, `txt20`, `txt50` |
| `gl_account_skb1` | GL Accounts at company-code level | Yes | `bukrs`, `saknr`, `stext` |
| `hierarchy` | Hierarchy header (set framework) | Yes | `setclass`, `setname`, `label`, `coarea` |
| `hierarchy_node` | Hierarchy internal nodes (parent-child) | No (via hierarchy FK) | `hierarchy_id`, `setname`, `parent_setname` |
| `hierarchy_leaf` | Hierarchy leaves (center assignments) | No (via hierarchy FK) | `hierarchy_id`, `setname`, `value` |

### Analysis & Wave Management

| Table | Contents | Key fields |
|---|---|---|
| `wave` | A named batch of centers for analysis/review | `code`, `name`, `state` |
| `wave_entity` | Entities (company codes) assigned to a wave | `wave_id`, `entity_id` |
| `wave_hierarchy_scope` | Hierarchy nodes defining wave scope | `wave_id`, `hierarchy_id`, `setname` |
| `analysis_config` | Saved decision-tree configurations | `code`, `config` (JSONB) |
| `routine` | Pluggable analysis routines (rules, ML, LLM) | `code`, `kind`, `schema` |
| `analysis_run` | A single execution of an analysis config | `wave_id`, `analysis_config_id`, `status` |
| `routine_output` | Per-center output from each routine in a run | `run_id`, `center_id`, `routine_id` |
| `center_proposal` | Proposed action for a center (merge, rename, keep, etc.) | `run_id`, `cctr`, `decision` |
| `center_mapping` | Legacy→Target center mapping (1:N) | `legacy_cctr`, `target_cctr`, `mapping_type` |

### Review Workflow

| Table | Contents | Key fields |
|---|---|---|
| `review_scope` | A subset of proposals assigned to a reviewer | `wave_id`, `reviewer_id`, `entity_ccodes` |
| `review_item` | Individual item within a review scope | `scope_id`, `proposal_id`, `status` |
| `llm_review_pass` | LLM-generated review comments | `run_id`, `proposal_id`, `verdict` |

### Data Quality

| Table | Contents | Key fields |
|---|---|---|
| `data_quality_issue` | DQ issues flagged during upload (VERAK, orphan nodes, etc.) | `batch_id`, `object_type`, `field`, `issue_type`, `status` |

### Upload & Integration

| Table | Contents | Key fields |
|---|---|---|
| `upload_batch` | A single file/API upload event | `scope`, `data_category`, `object_type`, `status` |
| `upload_error` | Errors captured during upload validation/load | `batch_id`, `row_num`, `error_code`, `message` |
| `sap_connection` | SAP system connection config (RFC/OData) | `system_id`, `host`, `client` |
| `sap_object_binding` | Which SAP objects to extract for which scope | `connection_id`, `scope`, `data_category`, `object_type` |
| `sap_connection_probe` | Connection health check results | `connection_id`, `status`, `latency_ms` |
| `datasphere_config` | SAP Datasphere connection settings | `space_id`, `schema`, `enabled` |
| `explorer_display_config` | Column display config for Data Explorer per object type | `object_type`, `table_columns`, `column_labels` |
| `explorer_source_config` | Which run/table to source explorer data from | `object_type`, `source_type` |

### Housekeeping

| Table | Contents | Key fields |
|---|---|---|
| `housekeeping_cycle` | Monthly/quarterly health-check run | `period`, `status`, `started_at` |
| `housekeeping_item` | Individual finding in a cycle (inactive, orphan, etc.) | `cycle_id`, `cctr`, `issue_type` |

### System / Admin

| Table | Contents | Key fields |
|---|---|---|
| `app_user` | Application users (local + Entra ID) | `username`, `email`, `role` |
| `app_config` | Application configuration key-value store | `key`, `value` |
| `app_config_secret` | Encrypted secrets (AES-GCM) | `key`, `encrypted_value` |
| `audit_log` | Audit trail for admin actions | `user_id`, `action`, `resource` |
| `task_run` | Background task execution tracking | `task_type`, `status`, `progress` |
| `activity_feed_entry` | User-facing activity feed entries | `actor_id`, `action`, `resource` |
| `wave_template` | Reusable wave configuration templates | `code`, `config` |

### Naming & GL

| Table | Contents | Key fields |
|---|---|---|
| `naming_sequence` | Naming convention sequences (for center ID generation) | `prefix`, `next_value` |
| `naming_pool` | Pool of pre-allocated center IDs | `prefix`, `pool_size` |
| `naming_allocation` | Individual ID allocations from the pool | `pool_id`, `allocated_id` |
| `gl_account_class_range` | GL account classification rules (balance/P&L/etc.) | `from_account`, `to_account`, `class` |

---

## 3.1 Source structures (read-only inputs)

### 3.1.1 Balances feed

Provided format (file `balance_structure.xlsx`, header row only — actual data
is volume-loaded via OData / file upload):

| Field | Type | Notes |
|---|---|---|
| `COMPANY_CODE` | varchar(4) | Legal Entity / company code |
| `SAP_MANAGEMENT_CENTER` | varchar(10) | The cost center ID being measured |
| `CURR_CODE_ISO_TC` | char(3) | Transaction currency |
| `SUM(P.GCR_POSTING_AMT_TC)` | numeric(23,2) | Sum of postings in transaction currency |
| `SUM(P.GCR_POSTING_AMT_GC2)` | numeric(23,2) | Sum of postings in group reporting currency 2 |
| `COUNT(*)` | bigint | Posting line count |

Implementer note: the feed is an **aggregation** per (company, center, currency). The
ingest layer must capture the **period** dimension separately — either as a column from
the source query (`PERIOD_YYYYMM`), or as the file's load batch label. **Add `period_id`
as a mandatory column** in the application table.

### 3.1.2 Cost center master (SAP CSKS-equivalent, MDG 0G shape)

Provided file `0G_Cost center upload_1.3 3.xlsx` — header row of the data sheet:

| Field | Type | Description |
|---|---|---|
| `COAREA` | varchar(4) | Controlling area (e.g. `UBS`) |
| `CCTR` | varchar(10) | Cost center ID |
| `TXTSH` | varchar(20) | Short text |
| `TXTMI` | varchar(40) | Medium text |
| `CCTRRESPP` | varchar(40) | Responsible person (free-form: number + name) |
| `CCTRCGY` | char(1) | Category (`K` = standard cost center) |
| `CCODECCTR` | varchar(4) | Company code |
| `CURRCCTR` | char(3) | Currency |
| `PCTRCCTR` | varchar(10) | Assigned profit center |

Plus the wide attribute table `Center_structure.xlsx` (~63 columns of optional
attributes — owner, business segment, region, postal code, GEAR LED ID, FMD comp,
HC stat, certifier, etc.). All optional attributes ingested into a JSONB column
`attrs` on the cost-center table; a small whitelist promoted to dedicated columns
(see §3.2).

### 3.1.3 Profit center master (MDG 0G shape)

From `0G_Profit center upload_1.3 2.xlsx`:

| Field | Type | Description |
|---|---|---|
| `COAREA` | varchar(4) | Controlling area |
| `PCTR` | varchar(10) | Profit center ID |
| `TXTMI` | varchar(40) | Medium text |
| `TXTSH` | varchar(20) | Short text |
| `PCTRDEPT` | varchar(40) | Department |
| `PCTRRESPP` | varchar(40) | Responsible person |
| `PC_SPRAS` | char(2) | Language key |
| `PCTRCCALL` | char(1) | Allow all CCs flag (X / blank) |

### 3.1.4 Hierarchies (SAP set framework)

Three native SAP tables:

- **SETHEADER** — header row per set/group (set class `0101` = cost-center groups,
  `0106` = profit-center groups). Key fields: `SETCLASS`, `SETNAME`, `SUBCLASS`,
  `LINEID`, `VALSIGN`, `VALOPTION`, `VALFROM`, `VALTO`, ...
- **SETHEADERT** — language-dependent text per set: `LANGU`, `SETCLASS`, `SUBCLASS`,
  `SETNAME`, `DESCRIPT`.
- **SETNODE** — parent-child relations. Key fields: `SETCLASS`, `SUBCLASS`, `SETNAME`,
  `LINEID`, `SUBSETCLS`, `SUBSETSCLS`, `SUBSETNAME`.
- **SETLEAF** — leaf membership: `SETCLASS`, `SUBCLASS`, `SETNAME`, `LINEID`, `VALSIGN`,
  `VALOPTION`, `VALFROM`, `VALTO`. `VALOPTION='EQ'` is a single value;
  `VALOPTION='BT'` is a range (FROM..TO inclusive).

The application stores hierarchies in a normalised form (§3.2) but **always keeps the
source rows** for round-trip and audit.

---

## 3.2 Application database schema (PostgreSQL)

All tables live in schema `cleanup`. Naming: `snake_case`, plural table names, surrogate
`id BIGSERIAL` PK + a natural-key `UNIQUE` constraint where one exists.

### 3.2.1 Reference / master tables

```sql
-- legal entities (companies)
CREATE TABLE cleanup.entity (
  id              BIGSERIAL PRIMARY KEY,
  company_code    VARCHAR(4) NOT NULL UNIQUE,
  name            TEXT NOT NULL,
  region          TEXT,
  is_active       BOOLEAN NOT NULL DEFAULT TRUE,
  attrs           JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- legacy cost centers (snapshot from latest refresh)
CREATE TABLE cleanup.legacy_cost_center (
  id              BIGSERIAL PRIMARY KEY,
  coarea          VARCHAR(4)  NOT NULL,
  cctr            VARCHAR(10) NOT NULL,
  txtsh           VARCHAR(20),
  txtmi           VARCHAR(40),
  ccode           VARCHAR(4)  NOT NULL,
  currency        CHAR(3),
  responsible     TEXT,
  category        CHAR(1),
  pctr_legacy     VARCHAR(10),                  -- legacy 1:1 PC link
  is_active       BOOLEAN NOT NULL DEFAULT TRUE,
  attrs           JSONB NOT NULL DEFAULT '{}'::jsonb,
  refreshed_at    TIMESTAMPTZ NOT NULL,
  refresh_batch   UUID NOT NULL,
  UNIQUE (coarea, cctr)
);
CREATE INDEX ON cleanup.legacy_cost_center (ccode);
CREATE INDEX ON cleanup.legacy_cost_center (pctr_legacy);
CREATE INDEX ON cleanup.legacy_cost_center USING gin (attrs);

CREATE TABLE cleanup.legacy_profit_center (
  id              BIGSERIAL PRIMARY KEY,
  coarea          VARCHAR(4)  NOT NULL,
  pctr            VARCHAR(10) NOT NULL,
  txtsh           VARCHAR(20),
  txtmi           VARCHAR(40),
  department      TEXT,
  responsible     TEXT,
  language        CHAR(2),
  allow_all_cc    BOOLEAN,
  attrs           JSONB NOT NULL DEFAULT '{}'::jsonb,
  refreshed_at    TIMESTAMPTZ NOT NULL,
  refresh_batch   UUID NOT NULL,
  UNIQUE (coarea, pctr)
);

-- balances (period-level granularity, multi-currency)
CREATE TABLE cleanup.balance (
  id              BIGSERIAL PRIMARY KEY,
  ccode           VARCHAR(4)  NOT NULL,
  cctr            VARCHAR(10) NOT NULL,
  period_id       INTEGER NOT NULL,             -- YYYYMM
  currency_tc     CHAR(3),
  amount_tc       NUMERIC(23, 2),
  amount_gc2      NUMERIC(23, 2),
  posting_count   BIGINT,
  account_class   VARCHAR(8),                   -- B/S, REVENUE, OPEX, OTHER (derived)
  refresh_batch   UUID NOT NULL,
  UNIQUE (ccode, cctr, period_id, currency_tc, account_class)
);
CREATE INDEX ON cleanup.balance (cctr);
CREATE INDEX ON cleanup.balance (period_id);

-- hierarchies, normalised
CREATE TABLE cleanup.hierarchy (
  id              BIGSERIAL PRIMARY KEY,
  setclass        VARCHAR(4)  NOT NULL,        -- 0101 = CC groups, 0106 = PC groups
  setname         VARCHAR(40) NOT NULL,
  description     TEXT,
  refresh_batch   UUID NOT NULL,
  UNIQUE (setclass, setname)
);

CREATE TABLE cleanup.hierarchy_node (
  id              BIGSERIAL PRIMARY KEY,
  hierarchy_id    BIGINT NOT NULL REFERENCES cleanup.hierarchy(id) ON DELETE CASCADE,
  setname         VARCHAR(40) NOT NULL,
  parent_setname  VARCHAR(40),                  -- null at root
  line_id         INTEGER NOT NULL,
  description     TEXT,
  UNIQUE (hierarchy_id, setname, line_id)
);
CREATE INDEX ON cleanup.hierarchy_node (parent_setname);

CREATE TABLE cleanup.hierarchy_leaf (
  id              BIGSERIAL PRIMARY KEY,
  hierarchy_id    BIGINT NOT NULL REFERENCES cleanup.hierarchy(id) ON DELETE CASCADE,
  setname         VARCHAR(40) NOT NULL,         -- the lowest-level node this leaf hangs under
  val_sign        CHAR(1),                      -- I/E (include / exclude)
  val_option      CHAR(2),                      -- EQ / BT
  val_from        VARCHAR(10) NOT NULL,
  val_to          VARCHAR(10),
  line_id         INTEGER NOT NULL
);
CREATE INDEX ON cleanup.hierarchy_leaf (setname);
CREATE INDEX ON cleanup.hierarchy_leaf (val_from, val_to);
```

### 3.2.2 Wave, analytical configuration & analysis tables

The analysis framework is **configurable and versioned**. The same wave can host many
analysis runs, each with a named configuration that selects which decision-tree
routines, which ML models, and which LLM review mode are applied. Every run produces a
**versioned result** that is preserved for diffing and audit.

```sql
-- a saved, reusable analytical configuration ("decision tree recipe")
CREATE TABLE cleanup.analysis_config (
  id                BIGSERIAL PRIMARY KEY,
  code              VARCHAR(64) NOT NULL UNIQUE,    -- e.g. "STD-CLEANSING-V2"
  name              TEXT NOT NULL,
  description       TEXT,
  parent_config_id  BIGINT REFERENCES cleanup.analysis_config(id),  -- amendments inherit
  version           INTEGER NOT NULL DEFAULT 1,     -- bumped on each save
  is_immutable      BOOLEAN NOT NULL DEFAULT FALSE, -- locked after first run uses it
  config            JSONB NOT NULL,                 -- see §05.6 schema
  created_by        BIGINT NOT NULL REFERENCES cleanup.app_user(id),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON cleanup.analysis_config (parent_config_id);
CREATE INDEX ON cleanup.analysis_config USING gin (config);

-- registry of pluggable routines (see §04.6)
-- a routine is any decision-tree rule, ML model, or LLM-review module that can be
-- toggled inside an analysis_config. Routines are registered at boot from code +
-- DB rows so the framework is extensible without redeploy.
CREATE TABLE cleanup.routine (
  id              BIGSERIAL PRIMARY KEY,
  code            VARCHAR(64) NOT NULL UNIQUE,     -- e.g. "rule.posting_activity"
  kind            VARCHAR(20) NOT NULL,            -- 'rule' / 'ml' / 'llm' / 'aggregate'
  name            TEXT NOT NULL,
  description     TEXT,
  version         VARCHAR(20) NOT NULL DEFAULT 'v1',
  schema          JSONB NOT NULL,                  -- JSON-Schema for the routine's params
  default_params  JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_enabled      BOOLEAN NOT NULL DEFAULT TRUE,
  source          VARCHAR(20) NOT NULL DEFAULT 'builtin',  -- 'builtin' / 'plugin' / 'custom'
  registered_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TYPE cleanup.wave_state AS ENUM ('draft', 'analysing', 'proposed', 'locked', 'in_review', 'signed_off', 'closed', 'cancelled');

CREATE TABLE cleanup.wave (
  id              BIGSERIAL PRIMARY KEY,
  code            VARCHAR(40) NOT NULL UNIQUE,    -- e.g. WAVE-2026-Q3-APAC
  name            TEXT NOT NULL,
  description     TEXT,
  state           cleanup.wave_state NOT NULL DEFAULT 'draft',
  is_full_scope   BOOLEAN NOT NULL DEFAULT FALSE, -- "full scope" run flag
  exclude_prior   BOOLEAN NOT NULL DEFAULT TRUE,  -- exclude entities already in waves
  created_by      BIGINT NOT NULL REFERENCES cleanup.app_user(id),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_at       TIMESTAMPTZ,
  signed_off_at   TIMESTAMPTZ,
  closed_at       TIMESTAMPTZ
);

CREATE TABLE cleanup.wave_entity (
  wave_id         BIGINT NOT NULL REFERENCES cleanup.wave(id) ON DELETE CASCADE,
  entity_id       BIGINT NOT NULL REFERENCES cleanup.entity(id),
  PRIMARY KEY (wave_id, entity_id)
);

-- a single analysis run within a wave (or full-scope / housekeeping); results are versioned
CREATE TABLE cleanup.analysis_run (
  id                  BIGSERIAL PRIMARY KEY,
  wave_id             BIGINT REFERENCES cleanup.wave(id) ON DELETE CASCADE,  -- NULL for full-scope
  cycle_id            BIGINT REFERENCES cleanup.housekeeping_cycle(id) ON DELETE CASCADE,
  analysis_config_id  BIGINT NOT NULL REFERENCES cleanup.analysis_config(id),
  config_snapshot     JSONB NOT NULL,             -- frozen copy of config at run time
  data_snapshot       UUID NOT NULL,              -- references refresh_batch for deterministic re-run
  version_label       VARCHAR(40),                -- e.g. "v1.2 - tighter inactivity"
  parent_run_id       BIGINT REFERENCES cleanup.analysis_run(id),  -- for diffing lineage
  started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at         TIMESTAMPTZ,
  status              VARCHAR(16) NOT NULL DEFAULT 'running',  -- running / done / failed
  metrics             JSONB NOT NULL DEFAULT '{}'::jsonb,      -- counts per outcome, model AUC, etc.
  CHECK ((wave_id IS NOT NULL) OR (cycle_id IS NOT NULL) OR (wave_id IS NULL AND cycle_id IS NULL))  -- full-scope allowed
);
CREATE INDEX ON cleanup.analysis_run (wave_id);
CREATE INDEX ON cleanup.analysis_run (analysis_config_id);
CREATE INDEX ON cleanup.analysis_run (parent_run_id);

-- pre-computed comparison between two runs (cached for cockpit speed)
CREATE TABLE cleanup.analysis_run_diff (
  id                  BIGSERIAL PRIMARY KEY,
  run_a_id            BIGINT NOT NULL REFERENCES cleanup.analysis_run(id) ON DELETE CASCADE,
  run_b_id            BIGINT NOT NULL REFERENCES cleanup.analysis_run(id) ON DELETE CASCADE,
  computed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  summary             JSONB NOT NULL,             -- counts: changed, same, only_in_a, only_in_b
  UNIQUE (run_a_id, run_b_id)
);

-- per-center, per-routine output trace (so we can show which rule fired and what each
-- LLM said). This is the auditable record of a run.
CREATE TABLE cleanup.routine_output (
  id                  BIGSERIAL PRIMARY KEY,
  analysis_run_id     BIGINT NOT NULL REFERENCES cleanup.analysis_run(id) ON DELETE CASCADE,
  legacy_cc_id        BIGINT NOT NULL REFERENCES cleanup.legacy_cost_center(id),
  routine_code        VARCHAR(64) NOT NULL,
  step_index          INTEGER NOT NULL,           -- order within the run (sequential pipeline)
  verdict             VARCHAR(40),                -- routine-specific
  score               NUMERIC(8,4),
  payload             JSONB NOT NULL,             -- structured detail
  comment             TEXT,                       -- LLM commentary if applicable
  llm_model           VARCHAR(80),                -- which model produced the comment
  cost_tokens         INTEGER,
  latency_ms          INTEGER
);
CREATE INDEX ON cleanup.routine_output (analysis_run_id, legacy_cc_id);
CREATE INDEX ON cleanup.routine_output (routine_code);

-- LLM review pass (single / sequential / debate). One row per pass; per-center detail
-- lives in routine_output. The pass record carries cost + summary.
CREATE TYPE cleanup.llm_review_mode AS ENUM ('SINGLE', 'SEQUENTIAL', 'DEBATE');

CREATE TABLE cleanup.llm_review_pass (
  id                  BIGSERIAL PRIMARY KEY,
  analysis_run_id     BIGINT NOT NULL REFERENCES cleanup.analysis_run(id) ON DELETE CASCADE,
  mode                cleanup.llm_review_mode NOT NULL,
  models              JSONB NOT NULL,             -- e.g. ["azure:gpt-4o", "btp:gemini-1.5"]
  scope_filter        JSONB,                      -- subset of centers covered (else all)
  started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at         TIMESTAMPTZ,
  total_tokens        BIGINT,
  total_cost_usd      NUMERIC(10,4),
  summary             JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- per-center outcome from one analysis run
CREATE TYPE cleanup.cleansing_outcome AS ENUM ('KEEP', 'RETIRE', 'MERGE_MAP', 'REDESIGN');
CREATE TYPE cleanup.target_object  AS ENUM ('CC', 'PC', 'CC_AND_PC', 'PC_ONLY', 'WBS_REAL', 'WBS_STAT', 'NONE');

CREATE TABLE cleanup.center_proposal (
  id                      BIGSERIAL PRIMARY KEY,
  analysis_run_id         BIGINT NOT NULL REFERENCES cleanup.analysis_run(id) ON DELETE CASCADE,
  legacy_cc_id            BIGINT NOT NULL REFERENCES cleanup.legacy_cost_center(id),
  cleansing_outcome       cleanup.cleansing_outcome NOT NULL,
  target_object           cleanup.target_object,
  rationale               TEXT,                       -- LLM-generated narrative
  rule_path               JSONB NOT NULL,             -- which rules fired (audit)
  ml_confidence           NUMERIC(5,4),               -- 0..1
  proposed_target_cc_id   VARCHAR(40),                -- new ID after naming engine
  proposed_target_pc_id   VARCHAR(40),
  override_by             BIGINT REFERENCES cleanup.app_user(id), -- if analyst overrode
  override_reason         TEXT,
  override_at             TIMESTAMPTZ,
  UNIQUE (analysis_run_id, legacy_cc_id)
);
CREATE INDEX ON cleanup.center_proposal (cleansing_outcome);
CREATE INDEX ON cleanup.center_proposal (target_object);
```

### 3.2.3 Sign-off / review tables

```sql
CREATE TABLE cleanup.review_scope (
  id              BIGSERIAL PRIMARY KEY,
  wave_id         BIGINT NOT NULL REFERENCES cleanup.wave(id) ON DELETE CASCADE,
  name            TEXT NOT NULL,
  scope_type      VARCHAR(16) NOT NULL,    -- 'entity' / 'hier_node' / 'list'
  selector        JSONB NOT NULL,          -- e.g. { entity_ids:[...], setname: "X" }
  reviewer_id     BIGINT NOT NULL REFERENCES cleanup.app_user(id),
  invite_token    UUID NOT NULL UNIQUE,
  invited_at      TIMESTAMPTZ,
  completed_at    TIMESTAMPTZ
);

CREATE TYPE cleanup.review_decision AS ENUM ('APPROVE', 'NOT_REQUIRED', 'NEW_REQUEST', 'PENDING');

CREATE TABLE cleanup.review_item (
  id              BIGSERIAL PRIMARY KEY,
  scope_id        BIGINT NOT NULL REFERENCES cleanup.review_scope(id) ON DELETE CASCADE,
  proposal_id     BIGINT REFERENCES cleanup.center_proposal(id),
  -- For NEW_REQUEST entries proposal_id is NULL and the request body lives in attrs
  decision        cleanup.review_decision NOT NULL DEFAULT 'PENDING',
  comment         TEXT,
  decided_at      TIMESTAMPTZ,
  attrs           JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX ON cleanup.review_item (scope_id, decision);
```

### 3.2.4 Target (post sign-off) and housekeeping tables

```sql
-- the new ampliFi cost & profit centers, populated from approved proposals
CREATE TABLE cleanup.target_cost_center (
  id                  BIGSERIAL PRIMARY KEY,
  cctr_id             VARCHAR(40) NOT NULL UNIQUE,    -- per naming convention
  txtsh               VARCHAR(20),
  txtmi               VARCHAR(40),
  ccode               VARCHAR(4),
  currency            CHAR(3),
  category            CHAR(1) NOT NULL DEFAULT 'K',
  responsible         TEXT,
  pctr_id             VARCHAR(40),                    -- assigned profit center
  source_legacy_ids   BIGINT[] NOT NULL,
  attrs               JSONB NOT NULL DEFAULT '{}'::jsonb,
  approved_in_wave    BIGINT REFERENCES cleanup.wave(id),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  is_active           BOOLEAN NOT NULL DEFAULT TRUE,
  closed_at           TIMESTAMPTZ
);

CREATE TABLE cleanup.target_profit_center (
  id                  BIGSERIAL PRIMARY KEY,
  pctr_id             VARCHAR(40) NOT NULL UNIQUE,
  txtsh               VARCHAR(20),
  txtmi               VARCHAR(40),
  department          TEXT,
  responsible         TEXT,
  language            CHAR(2) DEFAULT 'EN',
  allow_all_cc        BOOLEAN DEFAULT FALSE,
  source_legacy_ids   BIGINT[] NOT NULL DEFAULT '{}',
  attrs               JSONB NOT NULL DEFAULT '{}'::jsonb,
  approved_in_wave    BIGINT REFERENCES cleanup.wave(id),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  is_active           BOOLEAN NOT NULL DEFAULT TRUE,
  closed_at           TIMESTAMPTZ
);

-- housekeeping cycles (monthly)
CREATE TYPE cleanup.hk_state AS ENUM ('scheduled','running','review_open','closed','cancelled');

CREATE TABLE cleanup.housekeeping_cycle (
  id              BIGSERIAL PRIMARY KEY,
  period_id       INTEGER NOT NULL,         -- YYYYMM
  state           cleanup.hk_state NOT NULL DEFAULT 'scheduled',
  config          JSONB NOT NULL,           -- analytics + ML config
  run_at          TIMESTAMPTZ,
  closed_at       TIMESTAMPTZ
);

CREATE TABLE cleanup.housekeeping_item (
  id              BIGSERIAL PRIMARY KEY,
  cycle_id        BIGINT NOT NULL REFERENCES cleanup.housekeeping_cycle(id) ON DELETE CASCADE,
  target_cc_id    BIGINT REFERENCES cleanup.target_cost_center(id),
  target_pc_id    BIGINT REFERENCES cleanup.target_profit_center(id),
  flag            VARCHAR(32) NOT NULL,     -- e.g. 'UNUSED', 'LOW_VOLUME', 'NO_OWNER'
  rationale       TEXT,
  ml_confidence   NUMERIC(5,4),
  owner_email     TEXT NOT NULL,
  decision        VARCHAR(32) NOT NULL DEFAULT 'PENDING',  -- KEEP / CLOSE / DEFER
  decided_at      TIMESTAMPTZ,
  decided_by      TEXT
);
```

### 3.2.5 Admin: uploads, users, config, audit

```sql
-- manual file uploads (admin-only ingest path for v1; OData ingest writes to the same
-- target tables but creates a different upload_batch row with source='odata')
CREATE TYPE cleanup.upload_kind AS ENUM (
  'balance', 'cost_center', 'profit_center',
  'hierarchy_set', 'hierarchy_node', 'hierarchy_leaf',
  'entity', 'gl_account_class'
);
CREATE TYPE cleanup.upload_state AS ENUM ('uploaded','validating','validated','loading','loaded','failed','rolled_back');

CREATE TABLE cleanup.upload_batch (
  id              BIGSERIAL PRIMARY KEY,
  batch_uuid      UUID NOT NULL UNIQUE,
  kind            cleanup.upload_kind NOT NULL,
  source          VARCHAR(20) NOT NULL DEFAULT 'manual', -- 'manual' / 'odata' / 'adt' / 'soap_rfc'
  sap_connection  TEXT,                                  -- when source != 'manual'
  sap_binding_id  BIGINT,                                -- references sap_object_binding.id
  filename        TEXT,
  storage_uri     TEXT,                                  -- where the raw file is parked
  rows_total      BIGINT,
  rows_loaded     BIGINT,
  rows_rejected   BIGINT,
  state           cleanup.upload_state NOT NULL DEFAULT 'uploaded',
  uploaded_by     BIGINT REFERENCES cleanup.app_user(id),
  uploaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  validated_at   TIMESTAMPTZ,
  loaded_at       TIMESTAMPTZ,
  error_summary   JSONB,
  notes           TEXT
);
CREATE INDEX ON cleanup.upload_batch (kind, state);

-- per-row validation errors (capped; full report exported to CSV on demand)
CREATE TABLE cleanup.upload_error (
  id              BIGSERIAL PRIMARY KEY,
  upload_batch_id BIGINT NOT NULL REFERENCES cleanup.upload_batch(id) ON DELETE CASCADE,
  row_number      BIGINT NOT NULL,
  column_name     TEXT,
  error_code      TEXT NOT NULL,
  error_message   TEXT NOT NULL,
  raw_row         JSONB
);
CREATE INDEX ON cleanup.upload_error (upload_batch_id);

-- SAP system connections (one row per logical SAP system)
CREATE TYPE cleanup.sap_protocol AS ENUM ('odata','adt','soap_rfc');

CREATE TABLE cleanup.sap_connection (
  id              BIGSERIAL PRIMARY KEY,
  name            VARCHAR(64) NOT NULL UNIQUE,
  description     TEXT,
  system_id       VARCHAR(8),                  -- SID, e.g. 'P01'
  client          VARCHAR(3),
  default_lang    CHAR(2) DEFAULT 'EN',
  protocols       cleanup.sap_protocol[] NOT NULL DEFAULT '{}',
  config          JSONB NOT NULL,              -- per-protocol blocks (see §09.1.1)
  enabled         BOOLEAN NOT NULL DEFAULT TRUE,
  created_by      BIGINT REFERENCES cleanup.app_user(id),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- bindings — for each connection, which objects we pull and how
CREATE TABLE cleanup.sap_object_binding (
  id              BIGSERIAL PRIMARY KEY,
  connection_id   BIGINT NOT NULL REFERENCES cleanup.sap_connection(id) ON DELETE CASCADE,
  kind            cleanup.upload_kind NOT NULL,
  protocol        cleanup.sap_protocol NOT NULL,
  request         JSONB NOT NULL,              -- entity_set/url for OData; ddic block for ADT; rfm block for SOAP/RFC
  field_mapping   JSONB NOT NULL DEFAULT '{}'::jsonb,
  hierarchy_pick  JSONB,                       -- only for hierarchy kinds: single/multi/pattern/all
  cron_schedule   VARCHAR(40),
  delta_field     TEXT,
  enabled         BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (connection_id, kind, protocol)
);
CREATE INDEX ON cleanup.sap_object_binding (connection_id, kind);

-- per-protocol probe history for the "Test connection" button
CREATE TABLE cleanup.sap_connection_probe (
  id              BIGSERIAL PRIMARY KEY,
  connection_id   BIGINT NOT NULL REFERENCES cleanup.sap_connection(id) ON DELETE CASCADE,
  protocol        cleanup.sap_protocol NOT NULL,
  ok              BOOLEAN NOT NULL,
  latency_ms      INTEGER,
  details         TEXT,
  warnings        JSONB,
  probed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  probed_by       BIGINT REFERENCES cleanup.app_user(id)
);
CREATE INDEX ON cleanup.sap_connection_probe (connection_id, probed_at DESC);
```


```sql
CREATE TABLE cleanup.app_user (
  id              BIGSERIAL PRIMARY KEY,
  email           CITEXT NOT NULL UNIQUE,
  display_name    TEXT,
  role            VARCHAR(20) NOT NULL,     -- admin / analyst / reviewer / auditor / owner
  is_active       BOOLEAN NOT NULL DEFAULT TRUE,
  password_hash   TEXT,                     -- null when EntraID is used
  external_id     TEXT,                     -- EntraID OID
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at   TIMESTAMPTZ
);

CREATE TABLE cleanup.app_config (
  key             TEXT PRIMARY KEY,
  value_json      JSONB NOT NULL,
  description     TEXT,
  updated_by      BIGINT REFERENCES cleanup.app_user(id),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE cleanup.app_config_secret (
  key             TEXT PRIMARY KEY,
  ciphertext      BYTEA NOT NULL,           -- AES-GCM
  nonce           BYTEA NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE cleanup.audit_log (
  id              BIGSERIAL PRIMARY KEY,
  ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
  actor_id        BIGINT REFERENCES cleanup.app_user(id),
  action          TEXT NOT NULL,            -- e.g. 'wave.lock'
  entity          TEXT NOT NULL,            -- e.g. 'wave:42'
  before_json     JSONB,
  after_json      JSONB,
  ip              INET,
  user_agent      TEXT
);
CREATE INDEX ON cleanup.audit_log (entity);
CREATE INDEX ON cleanup.audit_log (actor_id);

-- task runs (idempotency + progress)
CREATE TABLE cleanup.task_run (
  run_id          UUID PRIMARY KEY,
  task_name       TEXT NOT NULL,
  args            JSONB NOT NULL,
  state           VARCHAR(16) NOT NULL,     -- queued / running / done / failed
  progress        NUMERIC(5,2) NOT NULL DEFAULT 0,
  started_at      TIMESTAMPTZ,
  finished_at     TIMESTAMPTZ,
  error           TEXT,
  result          JSONB
);
```

## 3.3 Derived / cached views

For cockpit performance, the application materialises:

- `mv_balance_per_center(period_id, ccode, cctr, total_tc, total_gc2, posting_count, last_seen_period)`
- `mv_account_class_split(ccode, cctr, bs_amt, rev_amt, opex_amt, other_amt)` —
  driven by an account_class mapping table sourced from SAP G/L master.
- `mv_center_inactivity(cctr, months_since_last_posting)` — used by the cleansing tree.
- `mv_hierarchy_compliance(cctr, hierarchy_id, hits)` — flags centers in 0 or >1 hier node.

Refreshed by `odata.full_refresh` and `odata.delta_refresh`.

## 3.4 Snapshots and time travel

Every refresh writes a fresh `refresh_batch` UUID. Analyses lock to a single batch via
`analysis_run.data_snapshot`. The implementer should implement **WAL-style** snapshots
by NOT deleting old `legacy_cost_center` rows immediately — instead, mark them with
`refreshed_at`/`refresh_batch` so an analysis pinned to an older batch still reads
consistent data. A retention job purges batches older than `config.snapshot_retention_days`.

## 3.5 Account-class derivation rule

The split into B/S, REVENUE, OPEX, OTHER (used heavily by the mapping tree) is derived
from the SAP G/L account classification. Until a real G/L account feed is wired:

- Use a simple range-based rule from `app_config` (`account_class.ranges`).
- Default ranges: `1xxxxxx`–`3xxxxxx` ⇒ B/S; `4xxxxxx` ⇒ REVENUE; `5xxxxxx`–`7xxxxxx` ⇒ OPEX;
  rest ⇒ OTHER. **These defaults MUST be confirmed before go-live** (open question §15).
