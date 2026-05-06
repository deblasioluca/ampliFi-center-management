# ampliFi Center Management

ERP migration support application for rationalizing ~216,000 legacy SAP cost centers into a clean target structure of cost centers, profit centers and WBS elements. Uses a configurable decision tree framework (V1 cleansing + mapping, V2 CEMA-based migration) with wave-based rollout, simulation mode, and integrated review workflow.

## Tech Stack

| Layer      | Technology                                    |
|------------|-----------------------------------------------|
| Backend    | FastAPI 0.110+, Python 3.11+, Uvicorn         |
| ORM        | SQLAlchemy 2.x + Alembic                      |
| Workers    | Celery 5 on Redis 7                           |
| Database   | PostgreSQL 15+                                |
| Frontend   | Astro 4.x (static build), TypeScript, Tailwind, React islands |
| Auth       | Local bcrypt + JWT; optional Microsoft Entra ID (MSAL SPA) |
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
- **[Generic upgrade guide](docs/DEPLOYMENT.md)** — Tiered upgrade paths for existing deployments (PR #60 → current, including database migrations).

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for a record of notable changes.

## Default Credentials

| What       | Value                                |
|------------|--------------------------------------|
| Admin user | `admin` / `admin` (or `admin@amplifi.dev` / `admin`) |

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

## Key Features

| Feature | Description |
|---------|-------------|
| **Decision Tree V1** | Cleansing + mapping rules (posting activity, ownership, redundancy, hierarchy compliance, cross-system dependency) |
| **Decision Tree V2** | CEMA-based migration engine (retire flag, balance migrate, PC approach 1:1/1:n, combine migration) with PC/CC ID assignment |
| **Decision Tree Config Admin** | Create, version, clone configs; toggle routines, set parameters; immutable once used in a run |
| **Wave Management** | Create waves scoped by entities/hierarchy; 8-step progress pipeline (Create → Scope → Analyse → Review → Lock → Stakeholder → Sign Off → Export) |
| **Simulation Mode** | Run analysis in simulation (temporary CT/PT IDs); compare versions; activate to assign real PC/CC IDs |
| **Entity Picker** | Multi-select with search, Select All, count badge — built for 600+ entities |
| **Employee Picker** | Typeahead search on employee table for user creation (auto-fills name, email) |
| **Scope Coverage** | Dashboard showing entities and cost centers analysed per wave, with "Unassigned" row for orphaned runs |
| **Tab State Management** | Wave detail tabs disabled based on progress step (prevents premature access to Analysis, Proposals, Review, etc.) |
| **Entra ID Claims** | Popup showing all token claims after MSAL login (name, email, groups, etc.) |
| **Data Upload** | Excel/CSV upload with templates: CC with hierarchy, SAP flat hierarchy, GCR balances, entities, GL accounts, target objects |
| **Explore Page** | Public data explorer with configurable display columns and hierarchical tree view |
| **Review Workflow** | Token-based stakeholder review; assign reviewers by entity/hierarchy node; progress tracking |
| **MDG Export** | Generate SAP MDG upload files for approved proposals |

## Project Structure

```
ampliFi-center-management/
  backend/                 # FastAPI application
    app/
      api/                 # API route handlers (admin, auth, configs, waves, runs, review, etc.)
      auth/                # Authentication service (local + Entra ID)
      domain/              # Business logic (decision tree engine, routines, naming)
      infra/               # Infrastructure (DB, SAP client, logging)
      models/              # SQLAlchemy ORM models
      services/            # Analysis services (V1, V2), seed, upload processing
    alembic/               # Database migrations
    tests/                 # Test suite (21+ unit tests for V2 engine)
  frontend/                # Astro frontend (built to static, served by backend)
    src/
      pages/               # Page routes (dashboard, cockpit, admin, data, explore, etc.)
      components/          # Reusable components
  scripts/                 # Setup and deployment scripts
  Implementation_Plan/     # Specification documents (21 files)
  Makefile                 # Project commands
  .env.example             # Environment template
  docker-compose.yml       # Docker deployment (optional)
```

## Architecture

The backend (FastAPI) serves both the REST API and the built frontend as static files on a single port (default 8180). There is no separate frontend server in production.

```
Browser  ──► FastAPI (Uvicorn, port 8180)
               ├── /api/*          → REST API (JWT auth)
               ├── /api/docs       → Swagger UI
               ├── /api/redoc      → ReDoc
               └── /*              → Static frontend (Astro build output)
                    ├── /           → Dashboard
                    ├── /cockpit    → Waves & Analysis
                    ├── /admin      → System Administration
                    ├── /data       → Data Management
                    ├── /explore    → Public Explorer
                    ├── /login      → Authentication
                    └── /review/*   → Stakeholder Review (token-based)
```

Backend → PostgreSQL (primary store) + Redis (cache/broker, optional).

## Deployment

### Native (RPI / bare-metal)

1. Ensure PostgreSQL and Redis are installed and running
2. Clone the repo and `cp .env.example .env`
3. Edit `.env` with your database credentials and port preferences
4. Run `make setup` for initial installation
5. Run `make start` to start the backend (serves both API and frontend)
6. Use `make update` after pulling new code

### Docker

```bash
docker-compose up -d
```

All ports are configurable via `.env`.

## Admin Pages

The System (Admin) section includes:

| Section | Path | Purpose |
|---------|------|---------|
| Users | `/admin` | User CRUD, employee picker for creation, role management |
| SAP Connections | `/admin/sap` | SAP system connections (OData/ADT/SOAP-RFC) |
| Data Explorer Sources | `/admin/explorer` | Configure which data sources appear in Explorer |
| Explorer Display Config | `/admin/explorer-display` | Column labels and visibility per object type |
| Data Storage | `/admin/datasphere` | SAP Datasphere connection and DDL export |
| LLM Settings | `/admin/llm` | Azure OpenAI / BTP AI Hub model configuration |
| Email Settings | `/admin/email` | SMTP provider and template configuration |
| Naming Conventions | `/admin/naming` | PC/CC/WBS naming rules |
| App Config | `/admin` (App Config) | Global application settings |
| **Decision Trees** | `/admin` (Decision Trees) | Create, version, clone analysis configs; toggle routines, set parameters |
| Rule Builder | `/admin/rules` | Custom DSL rule editor |
| Wave Templates | `/admin/templates` | Reusable wave configuration templates |
| Routines | `/admin` (Routines) | Registered analysis routines (built-in + plugins + DSL) |
| Jobs | `/admin/jobs` | Background job monitor |
| Audit Log | `/admin/audit` | Audit trail viewer |
| Application Logs | `/admin/logs` | Application log viewer |
| System Health | `/admin` (System Health) | DB, Redis, system status |
| Setup Wizard | `/setup` | Initial setup guide |
