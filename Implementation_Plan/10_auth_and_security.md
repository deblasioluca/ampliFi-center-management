# 10 — Authentication, Authorisation, Security & Audit

## 10.1 Auth strategies

Both strategies live behind the same `AuthProvider` interface in
`backend/app/auth/`. The active provider is selected by `app_config['auth.provider']`:

- `local` (v1) — username + password.
- `entraid` (v2) — Azure EntraID (Microsoft Entra ID) via OIDC.

The frontend reads `/api/auth/info` on bootstrap to decide whether to render the
local login form or redirect to `/api/auth/oidc/start`.

### 10.1.1 Local auth (v1)

- Passwords hashed with `bcrypt` (cost factor 12).
- Login returns a JWT (HS256, 30-min access token + refresh token in a httpOnly,
  Secure, SameSite=Lax cookie).
- Reset flow: admin "send reset link" generates a one-time token (UUID v4, 24h TTL,
  single-use), email via `user.password_reset` template (§09.3).
- Brute-force defense: 5 failed attempts → 5-min lockout (configurable).
- Password policy: min 12 chars, ≥ 1 letter + ≥ 1 digit, not in HaveIBeenPwned k-anon
  list (`pwnedpasswords` API; can be disabled in air-gapped deploys).

### 10.1.2 EntraID auth (v2)

- OIDC authorization code flow with PKCE.
- App registered in EntraID; client ID + tenant ID + client secret stored as secrets.
- On callback: validate `id_token` (signature, issuer, audience, nbf/exp), look up
  user by `oid` (Object ID, mapped to `app_user.external_id`), upsert user record on
  first login (display_name from claims; role defaults to `reviewer` unless an admin
  pre-provisioned).
- Sessions still use JWT (issued by the app), refreshed via the EntraID refresh token
  silently; logout clears local cookies AND calls EntraID `end_session_endpoint`.
- Group-to-role mapping: configurable `app_config['auth.entraid.role_map']`
  e.g. `{"GroupAdminGUID": "admin", "GroupAnalystGUID": "analyst"}`. Group claims
  must be enabled on the EntraID app.

### 10.1.3 Tokenised stakeholder links (no-login mode)

- Used for `/review/{token}` and `/housekeeping/.../owner/{token}`.
- Token = UUID v4, scoped to one `review_scope` or `housekeeping_cycle` + recipient
  email. 30-day TTL, refreshable by an admin/analyst, revocable.
- Coexists with both auth strategies: when EntraID is active and the recipient is also
  a known user, the token can be transparently upgraded to a full session on click;
  otherwise it grants scoped access without a login.

## 10.2 Authorization (RBAC)

Roles: `admin`, `analyst`, `reviewer`, `auditor`, `owner`.

Permissions are enforced at the API layer via FastAPI dependencies:

```python
@router.post("/waves/{id}/lock", dependencies=[Depends(require_role("analyst","admin"))])
def lock_wave(id: int, ...): ...
```

Resource-level checks (e.g. reviewer can only operate on their own scope) are enforced
in domain services with the `actor` passed explicitly:

```python
def submit_review(scope_id, decision, actor: User):
    scope = repo.get_scope(scope_id)
    if actor.role == "reviewer" and scope.reviewer_id != actor.id:
        raise PermissionDenied
    ...
```

A complete RBAC matrix is in §07.12.

## 10.3 Secrets management

- Encryption at rest: AES-GCM with `APP_SECRET_KEY` (rotated via re-encryption job).
- In BTP deploy: secrets pulled from BTP Credential Store / Destination service.
- No secrets in logs (Pydantic `SecretStr`, structured-log redaction).
- `.env.example` checked in; real `.env` git-ignored.

## 10.4 CSRF, CORS, security headers

- API: stateless JWT. CSRF protection by `SameSite=Lax` cookie + custom header check
  for state-changing endpoints (`X-Requested-With: ampliFi`).
- CORS: locked to the configured frontend origin.
- HSTS, X-Frame-Options DENY, Content-Security-Policy `default-src 'self'`,
  X-Content-Type-Options nosniff, Referrer-Policy `same-origin`.
- All cookies `Secure`, `HttpOnly`, `SameSite=Lax`.

## 10.5 Input validation & rate-limiting

- Pydantic v2 models on every endpoint.
- Server-side schema validation on uploaded files (§07.7).
- Rate limits (Redis): default 60 rpm/user, 30 rpm for unauthenticated review token
  endpoints, 10 rpm for login attempts per IP.

## 10.6 Audit log (mandatory entries)

See §07.10 for the full list. Every state-changing API endpoint MUST emit an audit
record. The log is append-only at the application layer (no UPDATE/DELETE permissions
on `audit_log` granted to the app role).

## 10.7 Data classification & retention

| Class | Examples | Retention |
|---|---|---|
| Master data | CC, PC, hierarchies | Indefinite |
| Balances | Per-period postings | 7 years |
| Audit log | All actions | 7 years |
| Email send records | Recipient hash, template | 2 years |
| LLM prompts/responses | Per-call detail | 90 days (cache TTL) |
| Uploaded source files | Original spreadsheets | 1 year |
| Snapshots (refresh batches) | All historical batches | Configurable; default 12 months |

Retention enforced by a periodic worker; configuration lives in `app_config`.

## 10.8 Backup & recovery

- Postgres: daily full + WAL archiving; PITR window 14 days.
- Datasphere: covered by BTP-side backup policies.
- Object storage: lifecycle policy with versioning enabled.
- Disaster-recovery RPO ≤ 1 hour, RTO ≤ 4 hours.

## 10.9 Threat model summary

- **Unauthorised data access** → mitigated by RBAC + scoped tokens + audit.
- **Privilege escalation via review token** → tokens scoped to one `review_scope`,
  validated server-side, revocable, expire.
- **Mass exfiltration** → rate limits, audit, anomaly detection on export endpoints
  (alert when > N exports/hour by one user).
- **Prompt injection from cost-center descriptions** → all LLM prompts pass through a
  prompt-construction helper that escapes user-supplied strings into a fenced block;
  the system prompt explicitly instructs models to ignore instructions inside the
  fenced block.
- **Supply-chain risk for plugin routines** → admin-only registration; reload action
  audited; plugins must be installed by ops, not arbitrary users.
- **Email enumeration** → password reset and login responses are deliberately
  ambiguous ("if the email exists, a link has been sent").
