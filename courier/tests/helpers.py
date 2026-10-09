from __future__ import annotations

import json

from psycopg.types.json import Jsonb

from courier.signing import generate_secret


async def add_endpoint(pool, url="http://shop.test/hook", event_types=None, max_in_flight=10):
    async with pool.connection() as conn:
        row = await (await conn.execute(
            "INSERT INTO endpoints (url, secret, event_types, max_in_flight) VALUES (%s, %s, %s, %s) RETURNING id, secret",
            (url, generate_secret(), event_types, max_in_flight))).fetchone()
    return row


async def add_event(pool, event_type="order.paid", payload=None, n=1):
    """Insert events and fan out to all matching endpoints (mirrors the API)."""
    ids = []
    async with pool.connection() as conn:
        for _ in range(n):
            ev = await (await conn.execute(
                "INSERT INTO events (event_type, payload) VALUES (%s, %s) RETURNING id",
                (event_type, Jsonb(payload or {"amount": 500})))).fetchone()
            await conn.execute(
                """INSERT INTO deliveries (event_id, endpoint_id) SELECT %s, id FROM endpoints
                   WHERE enabled AND (event_types IS NULL OR %s = ANY(event_types))""", (ev["id"], event_type))
            ids.append(ev["id"])
    return ids


async def deliveries(pool):
    async with pool.connection() as conn:
        return await (await conn.execute("SELECT * FROM deliveries ORDER BY id")).fetchall()


async def make_due(pool):
    """Time travel: make every pending retry due now."""
    async with pool.connection() as conn:
        await conn.execute("UPDATE deliveries SET next_attempt_at = now() WHERE status = 'pending'")


async def close_circuits(pool):
    async with pool.connection() as conn:
        await conn.execute("UPDATE endpoints SET circuit_open_until = NULL")
