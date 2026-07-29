from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase

from config import settings

engine = create_async_engine(
    settings.database_url,
    echo=False,
    # Connection pool settings to reduce risk of exhaustion under load
    # (especially important while some upstream detail fetches are slow).
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
)
async_session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session


def _migrate_detail_flags(sync_conn) -> None:
    """Add is_detail / has_detail columns and backfill from legacy heuristics."""
    inspector = inspect(sync_conn)
    if inspector.has_table("listing_versions"):
        lv_cols = {c["name"] for c in inspector.get_columns("listing_versions")}
        if "is_detail" not in lv_cols:
            sync_conn.execute(
                text(
                    "ALTER TABLE listing_versions "
                    "ADD COLUMN is_detail BOOLEAN NOT NULL DEFAULT 0"
                )
            )
            sync_conn.execute(
                text(
                    """
                    UPDATE listing_versions
                    SET is_detail = 1
                    WHERE seller IS NOT NULL
                       OR status IS NOT NULL
                       OR image_urls IS NOT NULL
                       OR categories IS NOT NULL
                    """
                )
            )
    if inspector.has_table("listings"):
        l_cols = {c["name"] for c in inspector.get_columns("listings")}
        if "has_detail" not in l_cols:
            sync_conn.execute(
                text(
                    "ALTER TABLE listings "
                    "ADD COLUMN has_detail BOOLEAN NOT NULL DEFAULT 0"
                )
            )
            sync_conn.execute(
                text(
                    """
                    UPDATE listings
                    SET has_detail = 1
                    WHERE current_version_id IN (
                        SELECT id FROM listing_versions WHERE is_detail = 1
                    )
                    """
                )
            )


async def init_db():
    import models  # noqa: F401 — register ORM tables on Base.metadata

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.run_sync(_migrate_detail_flags)
