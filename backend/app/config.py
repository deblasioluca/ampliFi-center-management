"""Application configuration (env-driven, pydantic-settings)."""

from __future__ import annotations

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env", "../.env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- application ---
    app_name: str = "ampliFi Cleanup"
    app_env: str = "dev"
    debug: bool = False

    # --- ports (configurable per deployment) ---
    backend_port: int = 8180
    frontend_port: int = 4321
    postgres_port: int = 5433
    redis_port: int = 6380

    # --- database (postgres) ---
    database_url: str = "postgresql+psycopg2://amplifi:amplifi@localhost:5433/amplifi_cleanup"
    database_async_url: str = "postgresql+asyncpg://amplifi:amplifi@localhost:5433/amplifi_cleanup"
    db_pool_size: int = 10
    db_max_overflow: int = 20

    # --- redis ---
    redis_url: str = "redis://localhost:6380/0"

    # --- auth ---
    app_secret_key: SecretStr = SecretStr("change-me-in-production-please")
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 480
    jwt_refresh_token_expire_days: int = 7
    auth_provider: str = "local"  # 'local' | 'entraid'

    # --- auth (EntraID) ---
    entraid_client_id: str = ""
    entraid_tenant_id: str = ""
    entraid_client_secret: SecretStr = SecretStr("")
    entraid_show_claims: bool = False

    # --- email ---
    smtp_host: str = "localhost"
    smtp_port: int = 1025
    smtp_username: str = ""
    smtp_password: SecretStr = SecretStr("")
    smtp_from: str = "amplifi@example.com"
    smtp_tls: bool = False

    # --- LLM (Azure OpenAI) ---
    azure_openai_endpoint: str = ""
    azure_openai_api_key: SecretStr = SecretStr("")
    azure_openai_api_version: str = "2024-02-01"
    azure_openai_deployment: str = ""

    # --- LLM (SAP BTP) ---
    btp_genai_service_url: str = ""
    btp_genai_client_id: str = ""
    btp_genai_client_secret: SecretStr = SecretStr("")
    btp_genai_xsuaa_url: str = ""

    # --- LLM cost caps ---
    llm_daily_cost_cap_usd: float = 250.0
    llm_chat_daily_cost_cap_usd: float = 50.0

    # --- storage ---
    storage_backend: str = "local"  # 'local' | 's3'
    storage_local_path: str = "storage"
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: SecretStr = SecretStr("minioadmin")
    s3_bucket: str = "amplifi-cleanup"

    # --- celery ---
    celery_broker_url: str = "redis://localhost:6380/1"
    celery_result_backend: str = "redis://localhost:6380/2"

    # --- CORS ---
    cors_allowed_origins: str = (
        ""  # comma-separated, e.g. "http://localhost:4321,https://app.example.com"
    )

    # --- SAP Datasphere ---
    datasphere_url: str = ""  # HANA JDBC/ODBC endpoint
    datasphere_schema: str = "ACM"  # target schema in Datasphere
    datasphere_user: str = ""
    datasphere_password: SecretStr = SecretStr("")
    datasphere_use_ssl: bool = True

    # --- TLS / HTTPS ---
    # Modes: "off" | "direct" (uvicorn HTTPS) | "proxy" (reverse proxy TLS)
    tls_mode: str = "off"
    tls_cert_file: str = ""  # path to PEM certificate (direct mode)
    tls_key_file: str = ""  # path to PEM private key (direct mode)
    # External URL override for Entra ID redirect URIs (e.g. https://amplifi.company.com)
    tls_external_url: str = ""

    # --- feature flags ---
    feature_mdg_api: bool = False
    feature_datasphere: bool = False
    feature_entraid: bool = False
    # When true, explorer endpoints serving sensitive PII (employees) or
    # financial data (balances) require an authenticated user with the
    # 'analyst' or 'admin' role. Default false to preserve the legacy
    # public read-only behaviour during development; flip to true in
    # production deployments.
    explorer_require_auth: bool = False


settings = Settings()
