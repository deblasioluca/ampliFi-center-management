# ampliFi Center Management

ERP migration support application for rationalizing legacy SAP cost centers through a two-stage decision tree framework (cleansing + mapping) with wave-based rollout.

## Tech Stack

| Layer      | Technology                                    |
|------------|-----------------------------------------------|
| Backend    | FastAPI 0.110+, Python 3.11+, Uvicorn         |
| ORM        | SQLAlchemy 2.x + Alembic                      |
| Workers    | Celery 5 on Redis 7                           |
| Database   | PostgreSQL 15+                                |
| Frontend   | Astro 4.x with SSR, TypeScript, Tailwind      |
| Auth       | Local bcrypt + JWT                            |
| Container  | Docker + docker-compose (optional)            |

## Quick Start (Makefile)

All commands run from the **project root** directory. Type `make help` to see all available targets.

### First-time setup

```bash
# 1. Clone the repo
git clone https://github.com/deblasioluca/ampliFi-center-management.git
cd ampliFi-center-management

# 2. Copy and edit .env
cp .env.example .env
# Edit .env with your database URL, ports, etc.

# 3. Configure Git credentials (so git pull works without prompting)
make git-setup
# Prompts for your GitHub username and PAT (Personal Access Token)
# Create a PAT at: https://github.com/settings/tokens/new (select 'repo' scope)

# 4. Run full setup (creates venv, installs deps, creates DB tables, seeds data)
make setup
```

### Day-to-day commands

```bash
make start          # Start the backend server
make stop           # Stop the backend server
make restart        # Restart the backend server
make status         # Show whether the backend is running + health check
make logs           # Tail the backend log file
```

### Updating after code changes

```bash
make update         # git pull + rebuild frontend + reinstall backend + restart
```

### Sample data

```bash
make load-sample    # Generate sample data (entities, cost centers, balances, etc.)
make delete-sample  # Delete all sample data (keeps admin user and routines)
make seed           # Full seed: admin user + sample data + built-in routines
```

### All Makefile targets

| Target           | Description                                                         |
|------------------|---------------------------------------------------------------------|
| `make help`      | Show all available targets                                          |
| `make start`     | Start the backend server (port from .env, default 8180)             |
| `make stop`      | Stop the backend server                                             |
| `make restart`   | Restart the backend server                                          |
| `make status`    | Show running status + health check                                  |
| `make setup`     | First-time setup: venv, deps, DB tables, seed data                  |
| `make update`    | Pull latest code, rebuild frontend, reinstall backend, restart      |
| `make load-sample` | Generate sample data                                              |
| `make delete-sample` | Delete sample data (keeps admin user + routines)                |
| `make seed`      | Full seed (admin user + sample data + routines)                     |
| `make logs`      | Tail the backend log                                                |
| `make git-setup` | Configure Git credentials (run once, prompts for PAT)               |

## Deployment Guides

- **[RHEL / CentOS / Rocky Linux](docs/DEPLOYMENT-RHEL.md)** — Full guide for Red Hat Enterprise Linux 8/9 including proxy configuration, PostgreSQL setup, systemd services, firewall, and nginx reverse proxy.

## Default Credentials

| What       | Value                                |
|------------|--------------------------------------|
| Admin user | `admin@amplifi.dev` / `admin`        |

## Configuration

All configuration is via the `.env` file. Key settings:

```bash
# Ports (configurable per deployment target)
BACKEND_PORT=8180
FRONTEND_PORT=4321
POSTGRES_PORT=5433
REDIS_PORT=6380

# Database
DATABASE_URL=postgresql+psycopg2://amplifi:amplifi@localhost:5433/amplifi_cleanup

# CORS (production only — dev allows all origins)
# CORS_ALLOWED_ORIGINS=http://localhost:4321,https://app.example.com
```

See `.env.example` for the full list of options.

## API Documentation

Once the backend is running, interactive API docs are available at:

- Swagger UI: `http://<host>:<BACKEND_PORT>/api/docs`
- ReDoc: `http://<host>:<BACKEND_PORT>/api/redoc`

### Admin Sample Data API

These endpoints allow managing sample data via the API (require admin auth):

```bash
# Get sample data status (counts)
GET /api/admin/sample-data

# Generate sample data
POST /api/admin/sample-data

# Delete sample data
DELETE /api/admin/sample-data
```

### Data Management API

Selective and bulk delete for all imported data tables (require admin auth):

```
DELETE /api/data/entities              # by ids or ccode
DELETE /api/data/entities/all
DELETE /api/data/legacy/cost-centers   # by ids, ccode, or coarea
DELETE /api/data/legacy/cost-centers/all
DELETE /api/data/balances              # by ids, ccode, coarea, or fiscal_year
DELETE /api/data/balances/all
DELETE /api/data/hierarchies           # by ids or coarea (cascades)
DELETE /api/data/hierarchies/all
DELETE /api/data/purge-all             # delete ALL imported data
GET    /api/data/counts                # counts of all data tables
```

## Project Structure

```
ampliFi-center-management/
  backend/                 # FastAPI application
    app/
      api/                 # API route handlers
      auth/                # Authentication service
      domain/              # Business logic (decision tree engine)
      infra/               # Infrastructure (DB, SAP client, logging)
      models/              # SQLAlchemy ORM models
      services/            # Shared services (seed, etc.)
    alembic/               # Database migrations
    tests/                 # Test suite
  frontend/                # Astro SSR frontend
    src/
      pages/               # Page routes
      components/          # Reusable components
  scripts/                 # Setup and deployment scripts
  Implementation_Plan/     # Specification documents
  Makefile                 # Project commands
  .env.example             # Environment template
  docker-compose.yml       # Docker deployment (optional)
```

## Deployment

### Native (RPI / bare-metal)

1. Ensure PostgreSQL and Redis are installed and running
2. Clone the repo and `cp .env.example .env`
3. Edit `.env` with your database credentials and port preferences
4. Run `make setup` for initial installation
5. Run `make start` to start the backend
6. Use `make update` after pulling new code

### Docker

```bash
docker-compose up -d
```

All ports are configurable via `.env`.
