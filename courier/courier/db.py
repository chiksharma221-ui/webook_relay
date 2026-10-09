"""Connection pool and schema setup."""
from __future__ import annotations

import logging
from pathlib import Path

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

log = logging.getLogger(__name__)
SCHEMA = (Path(__file__).parent / "schema.sql").read_text()
NOTIFY_CHANNEL = "courier_deliveries"


async def create_pool(database_url: str, min_size: int = 2, max_size: int = 20) -> AsyncConnectionPool:
    pool = AsyncConnectionPool(database_url, min_size=min_size, max_size=max_size, open=False,
                               kwargs={"row_factory": dict_row})
    await pool.open(wait=True, timeout=30)
    return pool


async def migrate(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        # Several processes may start at once; serialise schema creation.
        await conn.execute("SELECT pg_advisory_lock(742210)")
        try:
            await conn.execute(SCHEMA)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(742210)")
    log.info("Schema ready")
