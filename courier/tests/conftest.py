"""Tests run against a real PostgreSQL, because the interesting behaviour
(row locks, SKIP LOCKED, transactions) only exists in a real database.

Set TEST_DATABASE_URL, e.g. postgresql://postgres:postgres@localhost:5432/courier_test
(`docker compose up -d postgres` provides one).
"""
from __future__ import annotations

import os

import httpx
import pytest
import pytest_asyncio

from courier.config import Settings
from courier.db import create_pool, migrate

TEST_DB = os.getenv("TEST_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/courier_test")


def make_settings(**overrides) -> Settings:
    base = dict(database_url=TEST_DB, allow_private_urls=True, retry_schedule=(1, 2, 4),
                max_attempts=4, circuit_failure_threshold=3, circuit_cooldown_seconds=30,
                lease_seconds=30, request_timeout=2, worker_concurrency=20, batch_size=50,
                api_key=None)
    base.update(overrides)
    return Settings(**base)


@pytest.fixture
def settings():
    return make_settings()


@pytest_asyncio.fixture
async def pool():
    p = await create_pool(TEST_DB, min_size=1, max_size=10)
    await migrate(p)
    async with p.connection() as conn:
        await conn.execute("TRUNCATE delivery_attempts, deliveries, events, endpoints RESTART IDENTITY CASCADE")
    yield p
    await p.close()


class Receiver:
    """In-process fake receiver: plugs into httpx via MockTransport, no network needed."""

    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.responses: dict[str, list] = {}   # url path -> queue of status codes / exceptions
        self.default = 200

    def script(self, path: str, *outcomes):
        self.responses.setdefault(path, []).extend(outcomes)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        queue = self.responses.get(request.url.path)
        outcome = queue.pop(0) if queue else self.default
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, tuple):
            return httpx.Response(outcome[0], headers=outcome[1])
        return httpx.Response(outcome)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))


@pytest.fixture
def receiver():
    return Receiver()
