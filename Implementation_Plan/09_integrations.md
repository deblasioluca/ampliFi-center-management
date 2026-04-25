# 09 — Integrations: SAP OData, LLM, Email, MDG

This module specifies the external interfaces. All integrations are isolated behind a
provider interface in `backend/app/infra/`, so the domain layer never imports vendor SDKs.

## 9.1 SAP integration — three protocols

The application supports **three** SAP protocols for ingest, each suited to
different objects:

| Protocol | Best for | Notes |
|---|---|---|
| **OData** | Modern S/4HANA / BW exposed services; balances, masters, hierarchies via standard CDS-based services | HTTP/JSON, cheap to wire, `$metadata` introspection |
| **ADT** (ABAP Development Tools REST) | Reading raw DDIC tables, CDS view results, and select transports/sets where no OData service exists | HTTP/XML; basic-auth or x509; uses ADT discovery endpoints |
| **SOAP / RFC** | Classic backends without OData/ADT exposure; pulling `T001`, `CSKS`, `CEPC`, set framework (`SETHEADER`/`SETNODE`/`SETLEAF`) via BAPI/RFM | RFC via `pyrfc` (SAP NW RFC SDK) for production, SOAP web services as a fallback when RFC is blocked |

> Until phase-2 connectors land, manual file upload (§07.7) remains the canonical
> ingest path — and any of the three protocols can populate the same target tables
> via the same loader.

The reference repo `github.com/deblasioluca/sap-ai-consultant` already implements
these three protocols; once the PAT is provided, lift its connection-config layer,
test/validate flow, and request-helpers into `backend/app/infra/sap/` (see §9.1.7).

### 9.1.1 Connection configuration (admin §07.4b)

A unified `sap_connection` record captures one logical SAP system. The protocol
chosen on the connection determines which fields are required.

```jsonc
{
  "name": "S4_LEGACY_PROD",
  "description": "Legacy ECC – read-only finance user",
  "system_id": "P01",
  "client":    "100",
  "default_language": "EN",

  // Protocols enabled on this connection (one or more)
  "protocols": ["odata", "adt", "soap_rfc"],

  "odata": {
    "base_url": "https://s4.example.com/sap/opu/odata/sap/",
    "auth_type": "basic | oauth2_client_credentials | x_csrf_token",
    "username": "{secret:S4_USER}",
    "password": "{secret:S4_PWD}",
    "default_format": "json"
  },

  "adt": {
    "base_url": "https://s4.example.com/sap/bc/adt/",
    "auth_type": "basic | x509",
    "username": "{secret:S4_USER}",
    "password": "{secret:S4_PWD}"
  },

  "soap_rfc": {
    "host": "s4.example.com",
    "sysnr": "00",
    "router": "/H/saprouter/H/",                       // optional
    "snc": { "enabled": false, "partner_name": "..." },
    "auth_type": "user_password | x509",
    "username": "{secret:S4_USER}",
    "password": "{secret:S4_PWD}",
    "soap_endpoint": "https://s4.example.com/sap/bc/srt/scs/sap/...",  // SOAP fallback
    "rfc_destination": "RFC_S4_PROD"                                  // logical name when using saprfc.ini
  }
}
```

Each connection has a **Test connection** action that validates whichever
protocols are enabled (separate probes — see §9.1.6).

### 9.1.2 Object catalogue (what to download)

Per connection, an admin defines which objects to pull. v1 ships the following
objects with default protocols and per-object query templates; admins can enable,
disable, parameterise, and (for advanced cases) add custom objects.

| Object | Source (default) | Protocol options | Notes |
|---|---|---|---|
| `companies` (T001) | `T001` table | OData ⟶ `BusinessCompanyCodeSet` (CDS), ADT ⟶ DDIC table read, SOAP/RFC ⟶ `RFC_READ_TABLE` on `T001` | Must include `BUKRS`, `BUTXT`, `LAND1`, `WAERS` |
| `cost_centers` | `CSKS` + `CSKT` | OData ⟶ `CostCenterSet` (CDS `I_CostCenter`), ADT ⟶ table read, SOAP/RFC ⟶ `BAPI_COSTCENTER_GETLIST` / `RFC_READ_TABLE` | Mirrors MDG 0G `CCTR` shape |
| `profit_centers` | `CEPC` + `CEPCT` | OData ⟶ `ProfitCenterSet` (CDS `I_ProfitCenter`), ADT ⟶ table read, SOAP/RFC ⟶ `BAPI_PROFITCENTER_GETLIST` / `RFC_READ_TABLE` | Mirrors MDG 0G `PCTR` shape |
| `cc_hierarchies` (CC groups) | Set framework, class `0101` | OData ⟶ `SetSet`/`SetNodeSet`/`SetLeafSet` (custom CDS where exposed), ADT ⟶ table read, SOAP/RFC ⟶ `RFC_READ_TABLE` on `SETHEADER`/`SETHEADERT`/`SETNODE`/`SETLEAF` filtered to class `0101` | Hierarchy picker: select one or many `SETNAME`s |
| `pc_hierarchies` (PC groups) | Set framework, class `0106` | Same as above filtered to class `0106` | Hierarchy picker |
| `balances` | Posting aggregates per the format in §03.1.1 | OData ⟶ `BalanceSet` (custom CDS or BW query), ADT ⟶ CDS view result via `/sap/bc/adt/datapreview/ddic`, SOAP/RFC ⟶ `BAPI_ACC_DOCUMENT_*` aggregations or `RFC_READ_TABLE` over `FAGLFLEXT` | Period-grained; admin chooses `ledger`, `period_from/to`, `company_codes` |
| `gl_account_class` (account ranges) | `T001`/`SKB1`/`T030` | OData / ADT / RFC | Drives the B/S / Revenue / OPEX classification (§03.5) |

#### Selection behaviour for hierarchies

Because there are many hierarchy `SETNAME`s in production, the admin selects:

- **Single hierarchy** by exact `SETNAME`.
- **Multiple hierarchies** via multi-select (with search by name and description).
- **All hierarchies in a class** (e.g. all `0101`).
- **Pattern**: `SETNAME like 'GFXX%'`.

A pre-discovery call (`POST /api/admin/sap/{conn}/discover/hierarchies?class=0101`)
lists available hierarchies (with descriptions in the user's language) so the
admin can pick from a populated list.

### 9.1.3 Per-object download definition

Each object maps to a `sap_object_binding` row that ties:

```
sap_connection × upload_kind × protocol × params × schedule
```

Example:

```jsonc
{
  "connection": "S4_LEGACY_PROD",
  "kind": "cost_center",
  "protocol": "soap_rfc",
  "request": {
    "rfm": "RFC_READ_TABLE",
    "table": "CSKS",
    "fields": ["BUKRS","KOSTL","KOKRS","DATBI","DATAB","TXTKZ","KOSAR","VERAK","VERAK_USER","WAERS","PRCTR"],
    "where": "DATBI >= sy-datum"
  },
  "field_mapping": {
    "BUKRS": "ccode",
    "KOSTL": "cctr",
    ...
  },
  "schedule": "0 3 * * *",
  "delta_field": null
}
```

For OData and ADT, `request` carries `entity_set`/`url`/`query_template` instead of
the RFM block. The mapping table is shared across protocols so downstream loader
code is protocol-agnostic.

### 9.1.4 Client implementations

#### OData client (`backend/app/infra/sap/odata.py`)

- HTTP/JSON, paged via server-driven `__next` or `$top`/`$skip` fallback.
- `$metadata` discovery cached per refresh.
- CSRF handshake supported even though we mostly read.
- Streaming JSON parsing (`ijson`) to keep memory bounded on large pages.
- Retries: exponential on 5xx, single re-auth on 401, honour `Retry-After` on 429.

#### ADT client (`backend/app/infra/sap/adt.py`)

- Discovery endpoint: `GET /sap/bc/adt/discovery`.
- Table read: `POST /sap/bc/adt/datapreview/ddic` (XML body) — for select system
  tables (`T001`, `CSKS`, `CEPC`, `SETHEADER`, etc.) when OData is unavailable.
- CDS preview: `POST /sap/bc/adt/datapreview/ddic?rowNumber=n` against a CDS view
  name.
- Auth: HTTP basic or client certificate (mTLS); CSRF token handshake required.
- The client is a thin XML/JSON wrapper; results normalised to the same
  `Iterable[dict]` shape OData returns so downstream code is identical.

#### SOAP / RFC client (`backend/app/infra/sap/soap_rfc.py`)

- Primary: **RFC** via `pyrfc` (the official SAP NW RFC SDK Python binding).
- Connection params from `soap_rfc` block in the connection config; supports
  `saprouter` and SNC.
- Standard remote function modules used:
  - `RFC_READ_TABLE` for direct table reads (T001, CSKS, CEPC, SETHEADER, SETNODE,
    SETLEAF) — with the well-known 512-char per-row limit; the client implements
    the canonical `RFC_READ_TABLE` chunking workaround (request fields in
    batches, reassemble per row).
  - `BAPI_COSTCENTER_GETLIST`, `BAPI_PROFITCENTER_GETLIST` where available.
  - `BAPI_ACC_DOCUMENT_*` aggregations / classic FI extracts for balances.
- **SOAP fallback** (when RFC is blocked or `pyrfc` cannot be installed): same
  RFMs exposed as SAP SOAP services (`SRT/SRT_SCS_RFC`); the client uses `zeep`
  with HTTPS + basic auth and the same payload shape.
- Result normalisation: output rows normalised to the same `dict` shape produced
  by OData/ADT.

#### Reuse from `sap-ai-consultant`

When the PAT is provided, the implementer should lift:
- Connection-config Pydantic models (single source of truth).
- Auth helpers (CSRF handshake, OAuth2 client credentials, x509 wiring).
- The `RFC_READ_TABLE` chunking helper (this is the trickiest piece of the SOAP/RFC
  layer and the reference repo already has a battle-tested version).
- Test/validate flow used by the admin "Test connection" button.
- Logging conventions (so application logs match across systems).

### 9.1.5 Common ingestion pipeline (protocol-agnostic)

```
trigger (manual / cron / wave-driven)
  ↓
SAP connection + object_binding chosen
  ↓
worker sap.pull[connection,kind] (Celery)
  ↓
protocol adapter fetches rows  → normalised iterator[dict]
  ↓
field_mapping applied           → upload_kind staging rows
  ↓
validation (shared with manual upload, §07.7.3)
  ↓
swap into live tables under new refresh_batch
  ↓
emit "data.refreshed" event → caches invalidate, stats strip refreshes
```

This means: **whether data arrives via file upload, OData, ADT, or SOAP/RFC, the
loader code is the same**. Only the adapter at the front of the pipeline differs.

When `db.shadow_enabled=true` the worker writes to both stores in parallel using
the same `refresh_batch` UUID so the two stores can be reconciled.

### 9.1.6 Connection test / validate

`POST /api/admin/sap/{conn}/test` runs a per-protocol probe:

- **OData**: GET `$metadata` from the configured base_url with the configured auth.
  Asserts XML parses; reports the discovered services.
- **ADT**: GET `/sap/bc/adt/discovery`. Asserts response 200 + valid AsciiDoc.
- **SOAP/RFC**: invoke `RFC_PING` (or the SOAP equivalent) to confirm the system
  is reachable and the user is logged in; on success follow with `RFC_GET_FUNCTION_INTERFACE`
  for `RFC_READ_TABLE` to confirm authorisation.

Each probe returns:
```jsonc
{
  "protocol": "soap_rfc",
  "ok": true,
  "latency_ms": 314,
  "details": "Pinged P01/100 as S4_USER; RFC_READ_TABLE callable.",
  "warnings": []
}
```

Test results are surfaced in the admin UI and persisted (latest 20 per connection).

### 9.1.7 SAP infra layer layout (drop-in for `sap-ai-consultant` patterns)

```
backend/app/infra/sap/
├── __init__.py              # registers protocols
├── base.py                  # SAPProtocol Protocol, ConnectionConfig models
├── odata.py
├── adt.py
├── soap_rfc.py
├── object_catalogue.py      # T001 / CSKS / CEPC / set framework / balances objects
├── field_mapping.py
├── tests.py                 # test_connection() per protocol
└── utils/
    ├── csrf.py
    ├── rfc_read_table.py    # chunking helper
    ├── pagination.py
    └── retry.py
```

`backend/app/infra/sap/base.py` exposes a single Protocol so the worker doesn't
care which client is in use:

```python
class SAPProtocol(Protocol):
    name: str   # 'odata' | 'adt' | 'soap_rfc'
    def test(self, conn: ConnectionConfig) -> TestResult: ...
    def discover(self, conn: ConnectionConfig, what: str, params: dict) -> list[dict]: ...
    def pull(self, conn: ConnectionConfig, binding: ObjectBinding) -> Iterable[dict]: ...
```

### 9.1.8 Phase positioning

- v1 ships **manual file upload** end-to-end (§07.7).
- Phase 2 (build plan §12, weeks 12–13) wires **OData**.
- Phase 2b adds **ADT** and **SOAP/RFC** in parallel; once any protocol is
  available, the admin can switch a `sap_object_binding` from manual to that
  protocol with no other changes downstream.

## 9.2 LLM providers

### 9.2.1 Common interface

```python
@runtime_checkable
class LLMProvider(Protocol):
    name: str

    def complete(
        self,
        model: str,
        messages: list[Message],
        *,
        temperature: float = 0.0,
        max_tokens: int = 1024,
        json_schema: dict | None = None,
        metadata: dict | None = None,
    ) -> Completion: ...

    def estimate_cost(self, completion: Completion) -> float: ...
```

`Message`, `Completion` are Pydantic models. Streaming is internal to the provider;
callers always receive the final `Completion`. `json_schema` (when provided) instructs
the provider to do JSON-mode generation when the underlying API supports it (Azure
OpenAI does); otherwise the provider falls back to "respond with JSON only" prompt
suffix and validates.

### 9.2.2 Azure OpenAI provider

- Endpoint + API key + deployment name + api version from admin config (§07.4).
- SDK: `openai` Python client v1.x with Azure-specific config.
- Token counting via `tiktoken`.
- Logs: prompt hash, model, latency_ms, prompt_tokens, completion_tokens. **Never log
  raw prompt or response** by default; admin can enable verbose logging for a single
  request via header for debugging.

### 9.2.3 SAP BTP Generative AI Hub provider

- Endpoint + XSUAA OAuth2 client credentials.
- Token cached in Redis until expiry; renewed proactively at 80% of TTL.
- Resource group + deployment ID per logical model.
- Same `complete()` semantics; cost mapping configured per model.

### 9.2.4 Cost guardrails

- Per-call max_tokens cap (default 1024 unless overridden).
- Per-pass max-cost cap (e.g. €100/pass) — pass aborts and writes `summary.aborted`
  with reason if the cap is reached.
- Daily org cap (`app_config['llm.daily_cost_cap_usd']`) — checked before each call
  via Redis counter.

### 9.2.5 Caching

SHA-256(`provider:model:temperature:max_tokens:messages_canonical_json`) ⇒ Redis
key holds the `Completion`. TTL configurable (default 30 days). Cache hits are still
recorded in `routine_output` but flagged `cache_hit=true`.

### 9.2.6 PII / data-classification rule

By default, the prompt MAY include cost-center IDs, descriptions, balances and owner
names. It MUST NOT include personal data beyond what's already in the SAP master.
Admin can enable a "strict" mode that hashes owner names before inclusion.

## 9.3 Email

### 9.3.1 Provider

- v1: SMTP relay (configured §07.8). MailHog used in dev.
- v2: Microsoft Graph (`/sendMail`) under EntraID app permissions, parallel to OIDC
  rollout. Same template / queue layer; only the transport changes.

### 9.3.2 Templates (Jinja2, sandboxed)

| Template | Trigger | Default subject |
|---|---|---|
| `wave.review_invite` | Scope invited | `[ampliFi] Please review your scope: {{wave.code}}` |
| `wave.review_reminder` | T+7 / T+14 | `[ampliFi] Reminder: {{wave.code}} review pending` |
| `wave.review_complete_ack` | Scope submitted | `[ampliFi] Thank you — review submitted for {{scope.name}}` |
| `wave.published` | Wave entered `in_review` | `[ampliFi] Wave {{wave.code}} is now open for review` |
| `wave.signed_off` | Wave fully signed off | `[ampliFi] Wave {{wave.code}} is signed off` |
| `wave.closed` | Wave closed | `[ampliFi] Wave {{wave.code}} closed — MDG export attached` |
| `housekeeping.invite` | Cycle review_open | `[ampliFi] Monthly housekeeping — your action required ({{cycle.period}})` |
| `housekeeping.reminder` | T+7 / T+14 | `[ampliFi] Reminder: housekeeping action pending` |
| `housekeeping.summary` | Cycle closed | `[ampliFi] Housekeeping {{cycle.period}} summary` |
| `user.invite` | New user created | `[ampliFi] You've been added — sign in here` |
| `user.password_reset` | Reset requested | `[ampliFi] Reset your password` |

Each template lives in `backend/app/infra/email/templates/{name}/(subject.txt|body.html|body.txt)`.
Admin can override per-instance copies via `app_config['email.templates.{name}']`.

### 9.3.3 Queue, rate-limits, audit

- All sends enqueued via Celery `email.send_batch`.
- Rate-limits configurable: e.g. 30/min, 1000/hour (avoid relay throttling).
- Each send writes an `audit_log` row with a hash of the recipient, subject, template
  name, success/failure status. Body is **not** stored verbatim; it is reproducible
  from the template + context if needed (which is also stored).
- Bounce handling: SMTP error captured; admin sees a "delivery issues" panel.

### 9.3.4 Tokenised links

Review and housekeeping emails contain links like
`https://app/review/{token}` (UUID v4, 30-day expiry, single-scope binding,
revocable). The token is bound to the scope/cycle, not the user identity, so it works
without login (v1) and overlays cleanly with EntraID-bound identities (v2).

## 9.4 MDG export

### 9.4.1 File-based export (v1)

Trigger: wave moved to `closed`, or housekeeping cycle closed with closures.

For an MDG-format export, generate two XLSX files matching the provided 0G upload
templates. The header rows (the rows starting with `*`) are reproduced verbatim, then
the data rows are appended.

**Cost center file** — sheet `Data`, header lines:

```
*COAREA  CCTR  TXTSH  TXTMI  CCTRRESPP  CCTRCGY  CCODECCTR  CURRCCTR  PCTRCCTR
```

**Profit center file** — sheet `Data`, header lines:

```
*COAREA  PCTR  TXTMI  TXTSH  PCTRDEPT  PCTRRESPP  PC_SPRAS  PCTRCCALL
```

The leading meta-rows include: download timestamp, data model `0G`, entity types
`CCTR` / `PCTR`, variant, edition (`USMD_EDITION` — must be configurable per export),
selection (the wave code).

The exporter writes **only approved (signed-off) target centers** belonging to the
wave (or housekeeping cycle). Existing centers with `closed_at` get a `Closed`
indicator if the MDG variant supports it (configurable).

Output files saved to `storage_uri` and offered as download from
`/api/waves/{id}/exports`. A zip is also produced (`exports/{wave_code}.zip`) bundling:
- `cost_centers.xlsx`
- `profit_centers.xlsx`
- `closures.xlsx` (housekeeping)
- `audit.csv` (which proposals were exported, with proposal_ids)

### 9.4.2 Direct push to MDG via API (phase 2)

Skeleton already in `backend/app/infra/mdg/api_client.py`. Behaviour:

- Auth: same SAP-OData-style (oauth2 client credentials or basic).
- For each target CC / PC: call MDG OData (`USMD_PROCESS_REQUESTS` or the modern API)
  with the same field set. Errors map back to proposal/MDG status.
- Idempotency: the request payload includes a deterministic `change_request_id`
  derived from `wave_code + cctr_id`. Re-pushing the same proposal must not create
  duplicates.
- Status polling: a worker polls MDG for change-request approval and updates
  `target_cost_center.is_active`, `mdg_status`, `mdg_change_request_id`.

The implementer should **build the file export first** (which is the sole certain
requirement) and stub the API client behind a feature flag `feature.mdg_api`.

## 9.5 Datasphere connectivity

When the active store is Datasphere:

- The DB driver swaps to the Datasphere SQL endpoint (HANA SQL or via Datasphere SQL
  service).
- Most ORM operations work; large analytical scans are pushed down via raw SQL where
  needed.
- Migrations: a separate alembic environment writes to the Datasphere schema (admin
  ops only).
- Secrets: BTP credential store integration (`infra/secrets/btp.py`) takes precedence
  over local env vars.

## 9.6 File storage

For uploaded files and generated exports:

- v1 default: local filesystem under `storage/uploads/`, `storage/exports/`.
- Configurable to S3-compatible (MinIO in dev, AWS S3 / BTP Object Store in prod).

Storage URIs use a uniform scheme (`s3://`, `file://`) so the rest of the code is
storage-agnostic.

## 9.7 Observability

- Every external call (OData, LLM, SMTP, MDG) emits an OpenTelemetry span with
  `peer.service`, `http.status_code`, `latency_ms`, `tokens_used` (LLM only),
  `cost_usd` (LLM only).
- Failed calls surface in `/admin/jobs` and in dedicated dashboards.
