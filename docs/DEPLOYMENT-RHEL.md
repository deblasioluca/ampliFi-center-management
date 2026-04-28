# Deploying ampliFi Center Management on Red Hat Enterprise Linux (RHEL)

This guide covers a fresh installation on RHEL 8 or 9 (also works on CentOS Stream, Rocky Linux, AlmaLinux).

---

## Prerequisites

| Component | Minimum Version | Notes |
|---|---|---|
| RHEL / CentOS Stream | 8.x or 9.x | `cat /etc/redhat-release` to check |
| Python | 3.11+ | RHEL 9 ships 3.9 — you may need AppStream |
| Node.js | 18+ | For the Astro frontend |
| PostgreSQL | 14+ | Backend database (assumes already installed) |
| Git | 2.x | For cloning and pulling updates |
| Make | 3.x | Pre-installed on RHEL |

---

## 1. Install System Dependencies

```bash
# Enable EPEL (Extra Packages for Enterprise Linux)
sudo dnf install -y epel-release

# Install core tools
sudo dnf install -y git make gcc gcc-c++ openssl-devel bzip2-devel \
    libffi-devel zlib-devel readline-devel
```

### Python 3.11+

RHEL 9 default Python is 3.9. Install 3.11 or 3.12 via AppStream:

```bash
# RHEL 9 / CentOS Stream 9
sudo dnf install -y python3.11 python3.11-devel python3.11-pip

# Create a symlink (optional, only if python3 defaults to 3.9)
sudo alternatives --set python3 /usr/bin/python3.11
```

For RHEL 8:

```bash
sudo dnf module enable python311 -y
sudo dnf install -y python3.11 python3.11-devel python3.11-pip
```

Verify:
```bash
python3.11 --version   # Should show 3.11.x or higher
```

### Node.js 18+

```bash
# Option A: NodeSource repository
curl -fsSL https://rpm.nodesource.com/setup_18.x | sudo bash -
sudo dnf install -y nodejs

# Option B: dnf module (RHEL 9)
sudo dnf module enable nodejs:18 -y
sudo dnf install -y nodejs
```

Verify:
```bash
node --version   # Should show v18.x or higher
npm --version
```

### PostgreSQL (database setup only — assumes PostgreSQL is already installed)

Create the database and user for ampliFi:

```bash
# Create database and user
sudo -u postgres psql -c "CREATE USER amplifi WITH PASSWORD 'amplifi';"
sudo -u postgres psql -c "CREATE DATABASE amplifi_cleanup OWNER amplifi;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE amplifi_cleanup TO amplifi;"
sudo -u postgres psql -d amplifi_cleanup -c "CREATE SCHEMA IF NOT EXISTS cleanup AUTHORIZATION amplifi;"
```

If local password auth is not enabled, edit `pg_hba.conf`:
```bash
sudo vi /var/lib/pgsql/data/pg_hba.conf
# Change: local all all peer
# To:     local all all md5
sudo systemctl restart postgresql
```

---

## 2. Corporate Proxy (if applicable)

If your RHEL server goes through a corporate proxy for outbound internet access, configure it **before** running any install commands.

### System-wide proxy (for dnf, curl, wget)

```bash
# Add to /etc/environment or your shell profile
export HTTP_PROXY=http://proxy-host:port
export HTTPS_PROXY=http://proxy-host:port
export NO_PROXY=localhost,127.0.0.1,your-sap-host
```

> **Note:** Once the `.env` file is created (step 4), the Makefile automatically reads `HTTPS_PROXY` / `HTTP_PROXY` from it and applies them to `pip install` (with `--trusted-host` flags). npm and git use the system-wide proxy if set.

---

## 3. Clone the Repository

```bash
cd /opt   # or wherever you want to install
sudo git clone https://github.com/deblasioluca/ampliFi-center-management.git
sudo chown -R $(whoami):$(whoami) ampliFi-center-management
cd ampliFi-center-management
```

---

## 4. Configure

### Create `.env`

```bash
cp .env.example .env
vi .env
```

**Key settings to update:**

```env
# Database — point to your existing PostgreSQL instance
# Adjust host/port/user/password to match your environment
DATABASE_URL=postgresql+psycopg2://amplifi:amplifi@localhost:5432/amplifi_cleanup
DATABASE_ASYNC_URL=postgresql+asyncpg://amplifi:amplifi@localhost:5432/amplifi_cleanup
POSTGRES_PORT=5432

# Security — change in production!
APP_SECRET_KEY=your-random-secret-at-least-32-chars

# Proxy (if behind corporate proxy)
HTTPS_PROXY=http://proxy-host:port
HTTP_PROXY=http://proxy-host:port
NO_PROXY=localhost,127.0.0.1,your-sap-host

# Ports (adjust if needed)
BACKEND_PORT=8180
FRONTEND_PORT=4321
```

---

## 5. Initial Setup

```bash
# If python3.11 is not the default python3, edit Makefile line ~105:
# Change "python3" to "python3.11" in the venv creation command.
# Or create a symlink: sudo alternatives --set python3 /usr/bin/python3.11

make setup
```

This will:
1. Create a Python virtual environment (`backend/.venv/`)
2. Install all Python dependencies (with proxy and `--trusted-host` if configured)
3. Install Node.js dependencies and build the Astro frontend
4. Create database tables and seed default data
5. Start the application

If Python 3.11 is not the default `python3`, run setup manually:

```bash
cd backend
python3.11 -m venv .venv
source .venv/bin/activate
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --upgrade pip
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -e ".[dev]"
cd ../frontend && npm install && npm run build && cd ..
cd backend
source .venv/bin/activate
python -m alembic upgrade head
python -m app.cli seed
```

---

## 6. Start the Application

```bash
make start
```

- **Backend**: http://0.0.0.0:8180
- **Frontend**: http://0.0.0.0:4321

Default credentials: `admin` / `admin` (change after first login).

### Check status

```bash
make status
```

### View logs

```bash
make logs
# or: tail -f amplifi-backend.log
```

---

## 7. Run as a systemd Service (Production)

Create a service file so the app starts on boot:

### Backend service

```bash
sudo vi /etc/systemd/system/amplifi-backend.service
```

```ini
[Unit]
Description=ampliFi Center Management — Backend
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=your-user
Group=your-group
WorkingDirectory=/opt/ampliFi-center-management/backend
EnvironmentFile=/opt/ampliFi-center-management/.env
ExecStart=/opt/ampliFi-center-management/backend/.venv/bin/uvicorn \
    app.main:app --host 0.0.0.0 --port 8180 --log-level info
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Frontend service (optional — for dev mode; in production you may serve static build via nginx)

```bash
sudo vi /etc/systemd/system/amplifi-frontend.service
```

```ini
[Unit]
Description=ampliFi Center Management — Frontend
After=network.target

[Service]
Type=simple
User=your-user
Group=your-group
WorkingDirectory=/opt/ampliFi-center-management/frontend
ExecStart=/usr/bin/npm run dev
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable amplifi-backend
sudo systemctl start amplifi-backend

# Optional: frontend
sudo systemctl enable amplifi-frontend
sudo systemctl start amplifi-frontend

# Check status
sudo systemctl status amplifi-backend
sudo journalctl -u amplifi-backend -f
```

---

## 8. Firewall

Open the application ports:

```bash
sudo firewall-cmd --permanent --add-port=8180/tcp
sudo firewall-cmd --permanent --add-port=4321/tcp
sudo firewall-cmd --reload
```

---

## 9. Reverse Proxy with nginx (Optional)

If you want HTTPS or a single entry point:

```bash
sudo dnf install -y nginx
sudo vi /etc/nginx/conf.d/amplifi.conf
```

```nginx
server {
    listen 443 ssl;
    server_name amplifi.example.com;

    ssl_certificate     /etc/pki/tls/certs/your-cert.pem;
    ssl_certificate_key /etc/pki/tls/private/your-key.pem;

    # Frontend
    location / {
        proxy_pass http://127.0.0.1:4321;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Backend API
    location /api/ {
        proxy_pass http://127.0.0.1:8180;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

```bash
sudo systemctl enable --now nginx
```

---

## 10. Deploying Updates

After code changes are pushed to GitHub:

```bash
cd /opt/ampliFi-center-management
make update
```

This runs: `git pull` → frontend rebuild → Python deps install → DB migrations → seed → restart.

If using systemd instead of `make start`:

```bash
cd /opt/ampliFi-center-management
git pull
cd frontend && npm install && npm run build && cd ..
cd backend && source .venv/bin/activate
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -e ".[dev]"
python -m alembic upgrade head
python -m app.cli seed
cd ..
sudo systemctl restart amplifi-backend
```

---

## Troubleshooting

### `pip install` fails with SSL errors behind proxy

```bash
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -e ".[dev]"
```

The Makefile already includes `--trusted-host` flags. If you still get errors, ensure `HTTPS_PROXY` is set correctly in `.env`.

### `npm install` fails behind proxy

```bash
npm config set proxy http://proxy-host:port
npm config set https-proxy http://proxy-host:port
npm config set strict-ssl false   # only if proxy does SSL interception
```

### `git pull` fails behind proxy

Git typically does not need the corporate proxy (GitHub is accessed directly). If it does in your network:

```bash
git config --global http.proxy http://proxy-host:port
```

### PostgreSQL connection refused

```bash
# Check PostgreSQL is running
sudo systemctl status postgresql

# Check pg_hba.conf allows local connections
sudo cat /var/lib/pgsql/data/pg_hba.conf | grep -v '^#'

# Test connection
psql -U amplifi -d amplifi_cleanup -c "SELECT 1;"
```

### SELinux blocks the application

```bash
# Check for denials
sudo ausearch -m AVC -ts recent

# Allow the app to bind to its port
sudo semanage port -a -t http_port_t -p tcp 8180

# If needed, set permissive mode temporarily for troubleshooting
sudo setenforce 0
```

### Python 3.11 not available

Build from source as a last resort:

```bash
sudo dnf install -y gcc openssl-devel bzip2-devel libffi-devel zlib-devel
cd /tmp
curl -O https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz
tar xzf Python-3.11.9.tgz
cd Python-3.11.9
./configure --enable-optimizations --prefix=/usr/local
make -j$(nproc)
sudo make altinstall
# Now available as: python3.11
```
