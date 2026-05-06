"""Database engine and session factory.

Uses connection pooling on production drivers (PostgreSQL, HANA via Datasphere
adapter). For SQLite — used by the test suite via ``DATABASE_URL=sqlite://`` —
pooling args are not applicable because SQLite uses ``SingletonThreadPool``,
which raises if given ``pool_size`` / ``max_overflow``.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings


def _engine_kwargs(url: str) -> dict:
    """Return create_engine kwargs appropriate for the URL's dialect.

    SQLite (used by the test suite) doesn't accept ``pool_size`` /
    ``max_overflow`` — its default ``SingletonThreadPool`` ignores them and
    SQLAlchemy raises a TypeError when they're passed. Production drivers
    (PostgreSQL psycopg2/asyncpg, HANA via Datasphere) all accept them.
    """
    if url.startswith("sqlite"):
        return {"pool_pre_ping": True}
    return {
        "pool_size": settings.db_pool_size,
        "max_overflow": settings.db_max_overflow,
        "pool_pre_ping": True,
    }


engine = create_engine(settings.database_url, **_engine_kwargs(settings.database_url))

async_engine = create_async_engine(
    settings.database_async_url,
    **_engine_kwargs(settings.database_async_url),
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
AsyncSessionLocal = async_sessionmaker(
    bind=async_engine, class_=AsyncSession, expire_on_commit=False
)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session
