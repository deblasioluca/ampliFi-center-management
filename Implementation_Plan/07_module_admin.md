# 07 — Admin Module

The admin module is restricted to users with role `admin`. It covers user management,
runtime configuration (DB / LLM / OData / email / naming), wave administration,
**manual file upload** of source data (the v1 ingest path), and a routine/rule editor
for the extensible decision-tree framework (§04.6).

## 7.1 Admin landing & navigation

```
/admin                 → admin home (system status tiles)
/admin/users           → user CRUD + bulk upload
/admin/databases       → DB connection config (local + Datasphere)
/admin/llm             → LLM endpoint config (Azure + BTP)
/admin/sap             → SAP system connections (OData / ADT / SOAP-RFC) + object catalogue
/admin/email           → Email provider config + templates
/admin/naming          → Naming convention engine
/admin/uploads         → Manual upload of source data (v1)
/admin/routines        → Routine registry + DSL rule editor
/admin/waves           → Wave administration (create / manage / view)
/admin/audit           → Audit log viewer
/admin/jobs            → Background job monitor (Celery / RQ)
```

## 7.1b Decision Tree Configuration (implemented)

**Added in PR #52.** A dedicated Decision Tree Config section in the admin panel
provides full lifecycle management for analysis configurations:

- **List all configs**: Grouped by `code`, showing latest version, engine type, status
- **Create new config**: Select engine (V1/V2), name, code, description; toggle routines
  on/off, set parameters (JSON editor), review before saving
- **Edit**: Creates a new immutable version (preserving audit trail)
- **Clone/fork**: Create a variant based on an existing config
- **Version history**: Full timeline of versions per config code with timestamps
  and change metadata

Configurations are immutable once used in an analysis run (for reproducibility).
The config version is stored on each `AnalysisRun` record.

API:
- `GET /api/configs` — list all configs (latest version per code)
- `POST /api/configs` — create new config
- `GET /api/configs/{code}/versions` — version history
- `POST /api/configs/{code}/fork` — fork
- `POST /api/configs/{code}/amend` — create new version

## 7.2 User administration

### 7.2.1 User CRUD

Standard list / create / edit / disable. Fields: email (unique), display name, role
(admin / analyst / reviewer / auditor / owner), active. v1 password mode shows a
"reset password" button that emails a one-time link. v2 EntraID mode hides password
entirely; new users are auto-provisioned on first OIDC login (admins only need to
pre-set roles).

### 7.2.2 Bulk user upload (REQUIRED)

Admin uploads a CSV/XLSX with columns:
`email, display_name, role, active`.

Behaviour:
- Pre-validate (email format, role enum, duplicates).
- For new users: create row, send invitation email (v1: with a one-time password link;
  v2: just a "you've been added" notification — actual login happens via OIDC).
- For existing users: update display_name / role / active if changed (audit log).
- Row-level errors reported in a downloadable error CSV.

API: `POST /api/admin/users/bulk` (multipart form upload).

### 7.2.3 Employee Picker for User Creation (implemented)

**Added in PR #52.** When creating a new user in Admin > Users, the form includes
an **employee picker** that searches the employee table (loaded via data upload):

- Typeahead search on first name and last name
- Select an employee to auto-fill the user creation form with:
  - First name, last name, email, GPN (Global Personnel Number)
- This eliminates manual data entry and ensures consistency with HR records

API: `GET /api/reference/employees?search=<term>` — returns matching employees

### 7.2.4 Entra ID Claims Popup (implemented)

**Added in PR #52.** When the user logs in via Microsoft Entra ID (MSAL SPA flow),
a popup/dialog appears showing all token claims received:

- name, preferred_username, email, oid (Object ID)
- tid (Tenant ID), groups, roles, app roles
- Any other claims from the ID token

This helps administrators identify which data is available from Entra ID to
populate the user table and configure group-to-role mappings.

## 7.3 Database connection configuration

Two named connections — `local` (Postgres) and `datasphere`. For each:

- Host, port, database, schema, username, password (encrypted at rest).
- "Test connection" action.
- Active store toggle: `local` or `datasphere`.
- Shadow toggle: `enabled` + `shadow_connection_name`.

Persisted via `cleanup.app_config` (key `db.connections`, `db.active`, `db.shadow_*`).

## 7.4 LLM endpoint configuration

Two providers can each carry multiple model entries:

- **Azure**: `endpoint`, `deployment_name`, `api_key` (secret), `api_version`,
  `model_id` (logical name used in pipeline configs, e.g. `azure:gpt-4o`).
- **SAP BTP Generative AI Hub**: `service_url`, `client_id`, `client_secret` (secret),
  `xsuaa_url`, `model_id` (e.g. `btp:gemini-1.5`).

Per model: temperature default, max_tokens default, cost-per-1k tokens (for
`llm_review_pass.total_cost_usd`).

"Test" button invokes a tiny `complete()` call to verify connectivity. Errors are
surfaced (auth failure, throttle, model not found).

## 7.4b SAP connections — OData / ADT / SOAP-RFC

`/admin/sap` lists configured SAP systems. Each connection bundles credentials
plus the protocols enabled on it (any of OData, ADT, SOAP/RFC). Full schema in
§09.1.1.

### Per connection

- **General**: name, description, system ID, client, default language.
- **Protocols enabled**: any combination of OData, ADT, SOAP/RFC.
- **OData panel**: base URL, auth type (basic / OAuth2 client credentials /
  x-CSRF), credentials.
- **ADT panel**: base URL, auth type (basic / x509), credentials.
- **SOAP/RFC panel**: host, sysnr, optional saprouter, SNC settings, auth, and
  either an RFC destination or a SOAP endpoint as the runtime channel.
- **Test connection** action runs a per-protocol probe (§09.1.6) and shows
  results inline plus the last 20 historical probes.

### Object catalogue per connection

For each enabled connection, the admin curates which objects are pulled from it
and via which protocol:

| Object | Protocol picker | Per-protocol params |
|---|---|---|
| `companies` (T001) | OData / ADT / SOAP-RFC | service path, table read, RFM args |
| `cost_centers` (CSKS/CSKT) | OData / ADT / SOAP-RFC | as above |
| `profit_centers` (CEPC/CEPCT) | OData / ADT / SOAP-RFC | as above |
| `cc_hierarchies` (set class 0101) | OData / ADT / SOAP-RFC | **hierarchy picker** (one / many / pattern / all) |
| `pc_hierarchies` (set class 0106) | OData / ADT / SOAP-RFC | **hierarchy picker** |
| `balances` | OData / ADT / SOAP-RFC | ledger, period range, company filter |
| `gl_account_class` | OData / ADT / SOAP-RFC | ranges source |

The hierarchy picker pre-discovers available `SETNAME`s on the connected system
(§09.1.2) so the admin selects from a populated list with descriptions in their
language.

### Schedules

Each `sap_object_binding` carries an optional cron schedule and a `delta_field`.
Bindings without a schedule are pull-on-demand (used as part of an upload
wizard, §7.7).

API: see §11.3a.

The naming convention is **data-driven**. Per object type (CC, PC, WBS) the admin
defines:

- A **template** with placeholders, e.g. `PC-{coarea}-{seq:6}` or
  `CC-{pc_root}-{seq:4}`.
- Allowed placeholders: `{coarea}`, `{ccode}`, `{region}`, `{pc_root}`, `{seq:N}`
  (zero-padded sequence), `{legacy_id}`, `{owner_id}`, `{date:YYYYMM}`.
- A **legacy survival rule**: when a legacy ID survives, may we reuse the legacy ID
  (default: NO — issue a new ID), or use it as input to the template
  (`{legacy_id}` placeholder).
- A **CC ↔ PC relation rule**: when the CC depends on the PC ID, the template may
  reference `{pc_root}`. The engine resolves PCs first, then CCs, in dependency order.
- A **collision policy**: error / append `-N` / re-issue from sequence.
- **Reservation**: a sequence range can be reserved for a wave (e.g. PCs 100000–199999
  for 2026 Q3 APAC).
- **Preview**: for a sample of proposals, render the new IDs alongside legacy IDs.
- **Lock per wave**: once a wave is locked, the resolved IDs are pinned and not
  regenerated.

Storage: `cleanup.app_config['naming.cc'] / ['naming.pc'] / ['naming.wbs']` JSON
documents, plus `cleanup.naming_sequence(table_name, last_value)` for atomic sequence
reservation under transactional locking.

API:
- `GET/PUT /api/admin/naming/{object_type}` — convention CRUD.
- `POST /api/admin/naming/{object_type}/preview` — preview against a sample.
- `POST /api/admin/naming/{object_type}/reserve` — reserve a range for a wave.

## 7.6 Routine registry & DSL rule editor

Surfaces the registered routines (§04.6) with:
- code, kind, version, schema preview, enabled toggle.
- For DSL custom rules: visual builder (`feature ▾  op ▾  value`) and JSON view.
- Test against the sampled center set (returns counts of TRUE / FALSE / UNKNOWN).
- Reload registry button (re-imports plugins, re-reads custom rules).

API:
- `GET /api/admin/routines` — list.
- `POST /api/admin/routines/dsl` — create custom DSL rule.
- `PATCH /api/admin/routines/{code}` — toggle / edit DSL.
- `POST /api/admin/routines/reload` — reload registry (admin-only).

## 7.7 Data ingest — file upload OR direct from SAP

The ingest wizard at `/admin/uploads` is **dual-source**: for any object that
supports it, the admin chooses between

- **File upload** — pick an `.xlsx` / `.csv` from disk (the v1 default), or
- **Direct from SAP** — pick a configured SAP connection (§7.4b) and a
  protocol (OData / ADT / SOAP-RFC); the application pulls the object
  directly via the matching adapter (§09.1).

Either path lands in the same staging table, runs the same validation rules
(§7.7.3), and writes the same `upload_batch` row — only `source` differs
(`manual` vs `odata` / `adt` / `soap_rfc`).

For **profit centers**, **cost centers**, and **hierarchies** (CC and PC groups),
the SAP-direct option includes the **hierarchy picker** described in §09.1.2: a
discover call lists available `SETNAME`s, and the admin picks one, many, a
pattern, or all hierarchies in a class before pulling.

Admins upload source files. Each upload becomes an `upload_batch` row (§03.2.5) and
populates the same target tables that OData would, with `source='manual'`. The user can
preview, validate, and load — or roll back.

### 7.7.1 Supported uploads

| Kind | Accepted formats | Required columns |
|---|---|---|
| `balance` | `.xlsx`, `.csv` | `COMPANY_CODE, SAP_MANAGEMENT_CENTER, PERIOD_YYYYMM, CURR_CODE_ISO_TC, SUM_TC, SUM_GC2, COUNT, ACCOUNT_CLASS?` |
| `cost_center` | `.xlsx`, `.csv` | `COAREA, CCTR, TXTSH, TXTMI, CCTRRESPP, CCTRCGY, CCODECCTR, CURRCCTR, PCTRCCTR` (matches MDG 0G template) |
| `profit_center` | `.xlsx`, `.csv` | `COAREA, PCTR, TXTMI, TXTSH, PCTRDEPT, PCTRRESPP, PC_SPRAS, PCTRCCALL` |
| `hierarchy_set` | `.xlsx`, `.csv` | `SETCLASS, SETNAME, DESCRIPT, SUBCLASS?` (header rows from SETHEADER/T) |
| `hierarchy_node` | `.xlsx`, `.csv` | `SETCLASS, SETNAME, LINEID, SUBSETNAME, SUBCLASS?` (parent-child edges) |
| `hierarchy_leaf` | `.xlsx`, `.csv` | `SETCLASS, SETNAME, LINEID, VALSIGN, VALOPTION, VALFROM, VALTO?` |
| `entity` | `.xlsx`, `.csv` | `COMPANY_CODE, NAME, REGION?` |
| `gl_account_class` | `.xlsx`, `.csv` | `ACCOUNT_FROM, ACCOUNT_TO, CLASS` (B/S, REVENUE, OPEX, OTHER) |
| `cc_with_hierarchy` | `.xlsx` | Cost centers with embedded CEMA hierarchy columns (Ext_L0..L13 + descriptions) — flattened format **(PR #50)** |
| `sap_flat_hierarchy` | `.xlsx`, `.csv` | SAP flat hierarchy (SETCLASS, SETNAME, LINEID, SUBSETNAME, VALFROM, VALTO) |
| `gcr_balance` | `.xlsx`, `.csv` | GCR aggregated balance (company code, center, period, amounts by account class) |
| `target_cost_center` | `.xlsx`, `.csv` | Target cost centers (full SAP CSKS structure) |
| `target_profit_center` | `.xlsx`, `.csv` | Target profit centers (full SAP CEPC structure) |
| `center_mapping` | `.xlsx`, `.csv` | Legacy → target center mapping |
| `employee` | `.xlsx`, `.csv` | Employee records (GPN, FIRST_NAME, LAST_NAME, EMAIL, COMPANY_CODE, ...) |

For files exported from MDG that include the leading `*COAREA …` metadata row (as in
the provided sample files), the parser auto-detects and skips those header lines.

### 7.7.2 Upload flow

```
   Admin selects kind  →  uploads file  →  parser detects shape  →
   validation report (counts, errors per row)  →  Admin confirms  →
   loader writes to staging  →  swap into live tables under a new refresh_batch
```

Stages:

1. **Upload** — file stored under `storage_uri` (S3 / MinIO / local path); `state='uploaded'`.
2. **Validate** — schema check, duplicate-row check, FK resolution. Errors written to
   `upload_error` (capped at e.g. 5,000 rows for UI; full report exported on demand).
   `state='validated'` if zero errors OR admin opts "load anyway with errors skipped".
3. **Load** — atomic transaction:
   - For balances: append new rows with new `refresh_batch`.
   - For master data (CC / PC / entity): UPSERT by natural key with new `refresh_batch`.
   - For hierarchies: replace rows for the affected set names within a new
     `refresh_batch`.
   - For gl_account_class: replace ranges configuration.
4. **Loaded** — analyses can pin to this `refresh_batch`.
5. **Rollback** — admin can `POST /api/admin/uploads/{id}/rollback`. The loader
   keeps a previous `refresh_batch`, so rollback re-points the active batch and marks
   the rolled-back batch as superseded; analyses pinned to a specific batch keep
   working.

### 7.7.3 Validation rules (per kind)

- `balance`: `PERIOD_YYYYMM` must be six digits, month 01–12; numeric columns must
  parse; currency length 3; COMPANY_CODE must already exist (enforce or warn — config
  flag `upload.balance.strict_company_code`); negative `COUNT` rejected.
- `cost_center`: `CCTR` non-empty, length ≤ 10; `CCTRCGY ∈ {K,1,2,…}`; if `PCTRCCTR`
  present, must exist in the latest profit-center batch (warn-only by default, since
  the migration may legitimately create them in subsequent uploads).
- `profit_center`: `PCTR` unique within file; `PC_SPRAS ∈ ISO 639-1`.
- `hierarchy_*`: referential integrity within the set (every `SUBSETNAME` must appear
  as a `SETNAME`), ranges in leaves are inclusive, `VALOPTION ∈ {EQ, BT}`.
- All kinds: any column not in the spec is preserved into `attrs` JSONB — never lost.

### 7.7.4 UI

`/admin/uploads` — list of `upload_batch` rows with state, counts, age, actions
(Validate, Load, Download error CSV, Rollback). New upload wizard:

```
Step 1: Pick kind                  [Cost Center ▾]
Step 2: Pick source                ( ) File upload   (●) Direct from SAP
        ── if File upload:
              [browse...]
        ── if Direct from SAP:
              Connection: [S4_LEGACY_PROD ▾]
              Protocol:   ( ) OData  (●) SOAP/RFC  ( ) ADT
              For hierarchies: [Pick hierarchies ▸]
                  (Single ▸ All in class 0101 ▸ Multi-select ▸ Pattern)
              Optional filter: company codes / period range / etc.
              [Discover sample (50 rows)]
Step 3: Map columns (auto-mapped from object_binding, fix if needed)
Step 4: Preview first 50 rows + validation summary
Step 5: Load                       [Cancel] [Load now]
```

When **Direct from SAP** is selected, Step 5 enqueues a `sap.pull[connection,kind]`
Celery task; once it completes, the standard validation/load path proceeds.

### 7.7.5 API summary

```
POST   /api/admin/uploads               (multipart: kind, file)        → upload_batch
GET    /api/admin/uploads               list
GET    /api/admin/uploads/{id}          detail
POST   /api/admin/uploads/{id}/validate run validation (sync or async)
POST   /api/admin/uploads/{id}/load     run loader (Celery)
POST   /api/admin/uploads/{id}/rollback rollback
GET    /api/admin/uploads/{id}/errors   paginated errors (or CSV via Accept header)
```

## 7.8 Email administration

- SMTP relay config (host, port, TLS, username, password, from-address).
- Per-template subject + body editor (Jinja2; sandboxed).
- Preview with a sample center / wave context.
- "Send test email" action.
- Rate-limit settings (max emails/min, max emails/hour).
- Templates: see §09.3.

## 7.9 Wave administration (admin-only operations)

The wave **CRUD + cockpit lives under `/admin/waves`** so admins can:

- Create, edit, cancel, archive waves.
- View any wave the analyst is working on (read access to all waves).
- Trigger refresh for a wave's data snapshot.
- Re-issue review invite tokens (e.g. when a stakeholder changes).
- Export a wave's audit pack (zip: configs, runs, proposals, decisions, emails sent).

This is a superset of the analyst-side wave UI (§06).

## 7.10 Audit log viewer

`/admin/audit` shows `cleanup.audit_log` filtered by actor, entity, action, date range.
Export to CSV. Mandatory entries (logged automatically):

- user.login, user.create, user.update, user.bulk_upload
- config.update (db / llm / odata / email / naming)
- upload.create, upload.validate, upload.load, upload.rollback
- routine.register, routine.dsl_create, routine.dsl_update, routine.reload
- wave.create, wave.lock, wave.unlock, wave.signoff, wave.close
- analysis_config.save, analysis_run.start, analysis_run.complete
- proposal.override
- email.send (subject + recipient hash)
- mdg.export, mdg.api_push

## 7.11 Jobs monitor

`/admin/jobs` shows the Celery / RQ task table (`cleanup.task_run`) with state, queue
length, latency, recent failures. Per-task: payload, logs, retry, cancel.

## 7.12 RBAC summary (cross-ref §10)

| Capability | admin | analyst | reviewer | owner | auditor |
|---|:-:|:-:|:-:|:-:|:-:|
| User CRUD / bulk upload | ✔ | | | | |
| DB / LLM / OData / Email / Naming config | ✔ | | | | |
| Manual data upload | ✔ | | | | |
| Routine registry / DSL editor | ✔ | | | | |
| Wave create / lock / close | ✔ | ✔ | | | |
| Run analyses, build proposals | ✔ | ✔ | | | |
| Review (tick-off) | | | ✔ (own scope) | | |
| Housekeeping owner sign-off | | | | ✔ (own centers) | |
| View audit log | ✔ | | | | ✔ |
| Read all data | ✔ | ✔ | (own scope) | (own centers) | ✔ |
