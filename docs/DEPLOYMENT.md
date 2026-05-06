# Deployment Guide

For upgrading an existing ampliFi Center Management installation to current `main`.

This guide is organized by **how stale your install is**. Pick the section that matches:

| Your last sync | Use section |
|---|---|
| Within the last few days, on `main` | [Recent install](#recent-install--quick-update) |
| Around PR #58–#60 (e.g. office Linux server) | [Upgrading from PR #60](#upgrading-from-pr-60-eg-office-linux-server) |
| PR #57 or earlier (Celery/Redis not yet active) | [Upgrading from PR #57 or earlier](#upgrading-from-pr-57-or-earlier--full-infra-setup) |

> **For the corporate RHEL server:** after following the steps in this guide, also see
> [DEPLOYMENT-RHEL.md](./DEPLOYMENT-RHEL.md) for proxy / SSL-interception specifics
> (these only need to be configured once and should already be in place at PR #60).

---

## Recent install — quick update

```bash
cd ~/Development/ampliFi-center-management
git checkout main && git pull
make update
```

Done. `make update` pulls, rebuilds frontend, reinstalls backend deps, runs Alembic
migrations, and restarts services.

If you've never installed the systemd units (e.g. you're running services manually),
see step 6 of the [PR #57 upgrade](#upgrading-from-pr-57-or-earlier--full-infra-setup).

---

## Upgrading from PR #60 (e.g. office Linux server)

This is the **expected case for the office Linux server**, which has been running PR #60
since the configs-list-parsing fix landed. The good news: **PR #60 → current `main` is
purely additive**. No new dependencies, no new env vars, no schema migrations, no new
systemd units. Just code (backend services and frontend pages).

### What you're picking up

PRs merged since #60, in order:

| PR | What it adds | Where to spot-check |
|---|---|---|
| #61 | EntityPicker + EmployeePicker UX (replaces comma-separated inputs) | Any wave / config form with entity or employee fields |
| #62 | ML predictor + LLM advisor routines + side-by-side comparison page | `/cockpit/engines` (new page) |
| #63 | UI prompts + scope context | Across cockpit pages |
| #64 | Business-friendly reasoning panel + on-demand ML/LLM opinions | Any wave's Decision Tree tab |
| #65 | UBS-flavored sample data generator | `make load-sample` |
| #66 | `--wipe-only` / `--purge` flags for sample data + Postgres pctr length fix | `make delete-sample`, then `make load-sample` |

> **Postgres users — note on PR #66:** if you're on Postgres (not SQLite) and previously hit
> "value too long for type character varying" errors when generating sample data with profit
> center IDs longer than the old column width, that's fixed in #66. No migration needed; the
> fix is in the data generation logic.

### Steps

```bash
cd /path/to/ampliFi-center-management

# 1. Verify clean working tree on main
git status                       # should be clean
git log --oneline -1             # should show 4290e69 ... (PR #60 merge) or close to it

# If you have uncommitted local changes (logs, build artifacts), they're harmless.
# If you have real local changes you care about: stash them first.
git stash -u                     # only if needed

# 2. Pull
git fetch origin
git checkout main                # if you weren't already on main
git pull origin main

# 3. Update
make update
```

`make update` will:
- rebuild the frontend (you'll see the new pages including `/cockpit/engines`)
- reinstall backend deps from `pyproject.toml` (no actual changes since #60, but harmless)
- run `alembic upgrade head` (no-op since #60, but harmless)
- restart backend, frontend, worker, beat

### Verify

```bash
# Services should all be active
sudo systemctl status amplifi-backend amplifi-frontend amplifi-worker amplifi-beat --no-pager

# No new env vars expected — but sanity check that Redis + Celery still reachable
redis-cli ping                   # → PONG
sudo journalctl -u amplifi-worker -n 5 --no-pager | grep -i ready
```

### Smoke test

In the browser:

1. **`/cockpit`** — landing should load as before.
2. **`/cockpit/engines`** — new page; Engine Comparison view (Rule Tree vs ML vs LLM).
   Read-only, doesn't persist anything. Pick a wave, run the comparison, see three columns.
3. **`/cockpit/wave?id=<any>`** — open the Decision Tree tab; the new business-friendly
   reasoning panel should be visible. There should still be only one Decision Tree tab
   (consolidated in PR #57 — confirms the merge worked).
4. **Any form with entity or employee fields** — the picker should be the new
   EntityPicker / EmployeePicker (typeahead, not a comma-separated string field).
5. **Sample data**: optionally try the new wipe-and-regenerate flow, but **only on a
   non-production install** — it will delete sample data:
   ```bash
   make delete-sample             # remove existing sample data (keeps admin user + routines)
   make load-sample               # regenerate UBS-flavored sample dataset
   ```

That's it. No infra changes, no migrations to worry about.

---

## Upgrading from PR #57 or earlier — full infra setup

Use this when your install predates the Celery/Redis activation. Symptoms: no
`amplifi-worker.service` or `amplifi-beat.service` on the host, no `CELERY_BROKER_URL` in
`.env`, no Redis installed.

### 1. Verify your current state

```bash
cd /path/to/ampliFi-center-management
git status                       # branch + clean working tree?
git log --oneline -5             # last commits — confirm where you are
```

If you have uncommitted local changes you care about, stash or commit them now.

### 2. Pull current main

```bash
git fetch origin
git checkout main
git pull origin main
```

If checkout fails because of untracked or modified files: `git stash -u`.

### 3. Install Redis (if not already present)

Celery requires Redis as broker.

```bash
# Debian / Ubuntu
sudo apt update
sudo apt install -y redis-server
sudo systemctl enable --now redis-server

# RHEL / Rocky / Alma
sudo dnf install -y redis
sudo systemctl enable --now redis

# Verify
sudo ss -tlnp | grep redis       # → 127.0.0.1:6379 listening
redis-cli ping                   # → PONG
```

### 4. Update `.env`

Append the Celery + Explorer variables if missing:

```bash
cd /path/to/ampliFi-center-management

grep -E "REDIS|CELERY|EXPLORER" .env

# If missing, append:
cat >> .env <<'EOF'

# --- Celery (added by upgrade) ---
CELERY_BROKER_URL=redis://127.0.0.1:6379/1
CELERY_RESULT_BACKEND=redis://127.0.0.1:6379/2

# --- Explorer auth gate (default off, preserves existing public UX) ---
EXPLORER_REQUIRE_AUTH=false

# --- Redis main ---
REDIS_URL=redis://127.0.0.1:6379/0
EOF
```

Optional: Beat schedule overrides. Defaults are 1st of month 03:00 UTC for housekeeping,
daily 09:00/09:30 UTC for reminders. See `.env.example` for variable names (all commented
out by default).

### 5. Run `make update`

```bash
make update
```

Watch the output — Alembic should report applying migrations including
`phase26_housekeeping_pc`. If you see errors, **stop and report**; running with a
half-migrated DB is bad.

Verify:

```bash
cd backend && source .venv/bin/activate
alembic current                  # → phase26_housekeeping_pc (head)
deactivate; cd ..
```

### 6. Install systemd services

```bash
ls /etc/systemd/system/amplifi-*.service 2>/dev/null
```

If empty, install all four:

```bash
sudo cp systemd/amplifi-backend.service /etc/systemd/system/
sudo cp systemd/amplifi-frontend.service /etc/systemd/system/
sudo cp systemd/amplifi-worker.service /etc/systemd/system/
sudo cp systemd/amplifi-beat.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now amplifi-backend amplifi-frontend amplifi-worker amplifi-beat
```

If only worker + beat are missing (rest already running):

```bash
sudo cp systemd/amplifi-worker.service /etc/systemd/system/
sudo cp systemd/amplifi-beat.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now amplifi-worker amplifi-beat
```

> **Hardcoded paths in the unit files:** `User=macan` and
> `WorkingDirectory=/home/macan/Development/ampliFi-center-management` were written for the
> Pi. On a server with a different user or path, edit
> `/etc/systemd/system/amplifi-*.service` after copying, then `sudo systemctl daemon-reload`.
> Specifically check `User=`, `Group=`, `WorkingDirectory=`, `EnvironmentFile=`, `ExecStart=`.

### 7. Verify

```bash
sudo systemctl status amplifi-backend amplifi-frontend amplifi-worker amplifi-beat --no-pager
sudo journalctl -u amplifi-worker -n 20 --no-pager     # → "celery@<host> ready", "Connected to redis://"
sudo journalctl -u amplifi-beat   -n 10 --no-pager     # → "beat: Starting..."
```

If "Cannot connect to redis://localhost:6380" — `CELERY_BROKER_URL` is wrong. Re-check
step 4, then `sudo systemctl restart amplifi-worker amplifi-beat`.

### 8. Smoke test

Then continue with the [PR #60 smoke test list](#smoke-test) above — those features (engines
page, entity pickers, business-friendly reasoning, sample-data flow) are all present in
current `main`.

---

## Common upgrade pitfalls

**`git pull` says "branch is behind origin/main, can be fast-forwarded" but you're not on main:**
check `git branch --show-current`. Switch to `main` first.

**`make update` succeeds but new files (e.g. `amplifi-beat.service`) aren't there:**
you're not on `main`. See above.

**Worker logs show `Cannot connect to redis://localhost:6380`:** `.env` is missing
`CELERY_BROKER_URL` and `CELERY_RESULT_BACKEND` — code falls through to the default which
is `:6380`. See PR #57 step 4.

**Beat says `Unit amplifi-worker.service not found`:** worker unit isn't installed. See
PR #57 step 6.

**`alembic upgrade head` complains about a missing migration:** your local DB schema is
older than expected. Check `alembic current` and `alembic history`. Apply migrations in
order. **Don't** `alembic stamp` to skip them — that breaks data integrity.

**Multiple Celery Beat instances:** only ONE machine in your cluster should run
`amplifi-beat`. Workers can run on multiple machines. Separate environments (Pi + office
server with their own DBs) each run their own beat — those aren't a cluster.

**Frontend rebuild seems to work but `/cockpit/engines` 404s after upgrade from PR #60:**
the frontend dist wasn't rebuilt. `make update` should handle this, but you can force it
with `cd frontend && npm run build` and then `sudo systemctl restart amplifi-frontend`.

**Postgres "value too long" errors when running `make load-sample`:** you're on a build
older than #66. Either upgrade further (recommended) or shorten the test data manually. The
fix in #66 keeps profit center identifiers within column width without a migration.

---

## Rollback

If an upgrade goes wrong:

```bash
# Get back to the previous commit
git reflog                        # find the previous HEAD sha
git reset --hard <previous-sha>

# Roll back DB migrations if any were applied
cd backend && source .venv/bin/activate
alembic downgrade -1              # one step back
# or to a specific named revision:
alembic downgrade phase25_merge
deactivate; cd ..

# Restart services
sudo systemctl restart amplifi-backend amplifi-frontend amplifi-worker amplifi-beat
```

Note: not every migration's `downgrade()` is loss-free. Check the migration file before
downgrading, especially if it dropped or modified columns.

For PR #60 → current upgrades specifically, rollback is trivial: there are no schema
migrations, so `git reset --hard 4290e69` (PR #60 merge) and `make update` is enough.

---

## Quick reference: PR-to-feature map (PR #57 → main)

For tracing where a feature came from when debugging:

- **#57** — Rule catalog browser + Decision Tree tab consolidation + first deployment guide
- **#58** — Configs routing + i18n fixes
- **#59** — JWT auth in new pages
- **#60** — Configs list parsing fix
- **#61** — EntityPicker + EmployeePicker (UX)
- **#62** — ML/LLM routines + side-by-side engine comparison (`/cockpit/engines`)
- **#63** — UI prompts + scope context
- **#64** — Business-friendly reasoning panel + on-demand ML/LLM opinions
- **#65** — UBS-flavored sample data generator (`make load-sample`)
- **#66** — `--wipe-only` / `--purge` flags + Postgres pctr length fix (`make delete-sample`)
