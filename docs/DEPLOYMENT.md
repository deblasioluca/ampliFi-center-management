# Deployment Guide

For upgrading an existing ampliFi Center Management installation to current `main`.

This guide covers two scenarios:

1. **Recent install** (last sync was within last few days): standard `make update` flow
2. **Stale install** (last sync was 5+ PRs ago — e.g. you're at PR #50/#51): full upgrade, including new dependencies

If you set up the Pi recently and have been running `make update` regularly, the **Recent install** flow is enough. The **Stale install** flow is for installations that missed multiple feature drops, like the office Linux server.

---

## Recent install — quick update

```bash
cd ~/Development/ampliFi-center-management
git checkout main && git pull
make update
```

If `systemd/amplifi-beat.service` is new for your install (it shipped in PR #54), once-per-host setup is also needed:

```bash
sudo cp systemd/amplifi-beat.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now amplifi-beat
sudo systemctl status amplifi-beat
```

Done.

---

## Stale install — full upgrade (e.g. office server at PR #50/#51)

Use this when you missed several PRs of work and need to bring everything up to current main, including new dependencies and database migrations.

### 1. Verify your current state

```bash
cd /path/to/ampliFi-center-management
git status                      # branch + clean working tree?
git log --oneline -5            # last commits — figure out where you are
```

If you have uncommitted local changes you care about, stash or commit them now. If they're junk (logs, PIDs, build artifacts), they'll be ignored by checkout.

### 2. Pull current main

```bash
git fetch origin
git checkout main
git pull origin main
```

If the checkout fails because of untracked or modified files, stash them: `git stash -u`. After the upgrade you can review with `git stash show` and either restore or drop.

### 3. New dependency: Redis

Recent PRs activated Celery for real (was previously stubs). Celery requires Redis. If your office server doesn't have Redis yet, install it:

```bash
# Debian / Ubuntu
sudo apt update
sudo apt install -y redis-server
sudo systemctl enable --now redis-server

# verify
sudo ss -tlnp | grep redis      # → 127.0.0.1:6379 listening
redis-cli ping                  # → PONG
```

Default port is 6379. If something is already on 6379 you'd need to configure a different port — but on a fresh install this is rare.

### 4. Update `.env`

Your existing `.env` is missing variables that the new code reads. Add the missing ones:

```bash
cd /path/to/ampliFi-center-management

# Show what you currently have
grep -E "REDIS|CELERY|EXPLORER" .env

# If CELERY_BROKER_URL or CELERY_RESULT_BACKEND are missing, append them.
# Use the same Redis instance, different DB numbers.
cat >> .env <<'EOF'

# --- Celery (added by upgrade) ---
CELERY_BROKER_URL=redis://127.0.0.1:6379/1
CELERY_RESULT_BACKEND=redis://127.0.0.1:6379/2

# --- Explorer auth gate (default off, preserves existing public UX) ---
EXPLORER_REQUIRE_AUTH=false
EOF
```

If `REDIS_URL` is also missing:

```bash
echo 'REDIS_URL=redis://127.0.0.1:6379/0' >> .env
```

Optional: Beat schedule overrides. Defaults are 1st of month 03:00 UTC for housekeeping, daily 09:00/09:30 UTC for reminders. If you want different times, see `.env.example` for the variable names — all commented out by default.

### 5. Run `make update`

Pulls latest code (already done if you followed step 2), rebuilds frontend, installs Python deps, runs Alembic migrations, restarts services.

```bash
make update
```

Watch the output — Alembic should report applying migrations including `phase26_housekeeping_pc` (the new one from PR #54). If you see errors here, **stop and report** — running with a half-migrated DB is bad.

Verify the migration ran:

```bash
cd backend && source .venv/bin/activate
alembic current
# → should show: phase26_housekeeping_pc (head)
deactivate; cd ..
```

### 6. Install systemd services

If you've been running the backend manually (`make backend-start` or similar) and never set up systemd units:

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

If you only need the new ones (worker + beat were added in PR #54):

```bash
sudo cp systemd/amplifi-worker.service /etc/systemd/system/
sudo cp systemd/amplifi-beat.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now amplifi-worker amplifi-beat
```

> **Important about hardcoded paths:** the systemd unit files use `User=macan` and `WorkingDirectory=/home/macan/Development/...` because they were written for the Pi. If your office server has a different user or path, edit `/etc/systemd/system/amplifi-*.service` after copying, then run `sudo systemctl daemon-reload`. Specifically check the lines `User=`, `Group=`, `WorkingDirectory=`, `EnvironmentFile=`, and `ExecStart=`. The venv path and the `.env` location both reference the working directory.

### 7. Verify

```bash
# All four services should be active (running)
sudo systemctl status amplifi-backend amplifi-frontend amplifi-worker amplifi-beat --no-pager

# Worker should show "celery@<hostname> ready" and "Connected to redis://"
sudo journalctl -u amplifi-worker -n 20 --no-pager

# Beat should show "beat: Starting..."
sudo journalctl -u amplifi-beat -n 10 --no-pager
```

If you see "Cannot connect to redis://localhost:6380" — that means CELERY_BROKER_URL still points at the wrong port. Re-check step 4 and `sudo systemctl restart amplifi-worker amplifi-beat`.

### 8. Smoke test

Open the frontend in your browser. Head to:
- `/admin/rule-catalog` — should list all built-in decision tree rules with descriptions
- `/admin/configs` — should let you create variants from presets
- `/cockpit/wave?id=N` (any wave) — only one Decision Tree tab should be visible (was previously two: Analysis + Simulation)

Run a small simulation on a wave to verify the worker picks up the task:

```bash
sudo journalctl -u amplifi-worker -f
# in another terminal, trigger a sim from the UI; you should see the task being received and processed
```

---

## Common upgrade pitfalls

**`git pull` says "Your branch is behind origin/main by N commits, can be fast-forwarded"** but you're not on main: check `git branch --show-current`. Switch to `main` first.

**`make update` succeeds but the new files (e.g. `amplifi-beat.service`) aren't there:** you're not on `main`. See above.

**Worker logs show `Cannot connect to redis://localhost:6380`:** `.env` is missing `CELERY_BROKER_URL` and `CELERY_RESULT_BACKEND` — code falls through to the default which is `:6380`. See step 4.

**Beat says `Unit amplifi-worker.service not found`:** worker unit isn't installed. See step 6.

**`alembic upgrade head` complains about a missing migration:** your local DB schema is older than expected. Check `alembic current` and `alembic history` — you may need to apply migrations in order. Don't run `alembic stamp` to skip them; it loses data integrity.

**Multiple Celery Beat instances would multiply schedules:** only ONE machine in your cluster should run `amplifi-beat`. Workers can run on multiple machines. If you have separate infrastructures (e.g. Pi + office server with their own DBs), each runs its own beat — those aren't a cluster.

---

## Rollback

If an upgrade goes wrong and you need to roll back:

```bash
# Get back to the commit you were at before
git reflog                       # find the previous HEAD sha
git reset --hard <previous-sha>

# Roll back the database migration if needed
cd backend && source .venv/bin/activate
alembic downgrade -1             # one step back
# or
alembic downgrade phase25_merge  # to a specific named revision
deactivate; cd ..

# Restart services
sudo systemctl restart amplifi-backend amplifi-frontend amplifi-worker amplifi-beat
```

Note: not every migration's `downgrade()` is loss-free. Check the migration file before downgrading, especially if it dropped or modified columns.
