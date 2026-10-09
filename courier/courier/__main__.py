"""python -m courier <command>

  api       run the HTTP API and dashboard (port 8000)
  worker    run a delivery worker (start as many as you like)
  migrate   create the database schema and exit
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from .config import load_settings


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="courier", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["api", "worker", "migrate"])
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8000)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request is too noisy
    settings = load_settings()

    if args.command == "api":
        import uvicorn
        uvicorn.run("courier.api:app", host=args.host, port=args.port, log_level="info")
        return 0

    from .db import create_pool, migrate

    async def _migrate():
        pool = await create_pool(settings.database_url, min_size=1, max_size=1)
        await migrate(pool)
        await pool.close()

    async def _worker():
        from .worker import Worker
        # 1 connection for claims, 1 for LISTEN (separate), the rest for recording results.
        pool = await create_pool(settings.database_url, min_size=3,
                                 max_size=int(os.getenv("COURIER_DB_POOL_SIZE", 20)))
        await migrate(pool)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:  # Windows: Ctrl+C raises KeyboardInterrupt instead
                pass
        try:
            await Worker(pool, settings).run(stop)
        finally:
            await pool.close()

    try:
        asyncio.run(_migrate() if args.command == "migrate" else _worker())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
