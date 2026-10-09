"""HTTP API (the front desk) and the dashboard.

    uvicorn courier.api:app --port 8000
"""
from __future__ import annotations

import json
import logging
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, PlainTextResponse, RedirectResponse
from psycopg.errors import ForeignKeyViolation
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field, ValidationError, field_validator

from . import __version__, signing
from .config import Settings, load_settings
from .db import NOTIFY_CHANNEL, create_pool, migrate
from .urlguard import UnsafeURL, check_url

log = logging.getLogger(__name__)
STATIC = Path(__file__).parent / "static"


# ---------------------------------------------------------------- models
class EndpointIn(BaseModel):
    url: str = Field(max_length=2000)
    description: str | None = Field(default=None, max_length=500)
    event_types: list[str] | None = Field(default=None, description="omit to receive every event type")
    max_in_flight: int | None = Field(default=None, ge=1, le=100)


class EndpointPatch(BaseModel):
    url: str | None = Field(default=None, max_length=2000)
    description: str | None = None
    event_types: list[str] | None = None
    enabled: bool | None = None
    max_in_flight: int | None = Field(default=None, ge=1, le=100)


class EventIn(BaseModel):
    event_type: str = Field(min_length=1, max_length=200, pattern=r"^[A-Za-z0-9_.:\-]+$")
    payload: dict

    @field_validator("payload")
    @classmethod
    def _object(cls, v):
        if not isinstance(v, dict):
            raise ValueError("payload must be a JSON object")
        return v


# --------------------------------------------------------------- app setup
def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.pool = await create_pool(settings.database_url)
        await migrate(app.state.pool)
        yield
        await app.state.pool.close()

    app = FastAPI(title="Courier", version=__version__, lifespan=lifespan,
                  description="Reliable webhook delivery: retries, signatures, circuit breakers, dead-letter replay.")
    app.state.settings = settings

    def auth(authorization: Annotated[str | None, Header()] = None) -> None:
        if settings.api_key and authorization != f"Bearer {settings.api_key}":
            raise HTTPException(401, "missing or invalid API key")

    def pool(request: Request):
        return request.app.state.pool

    def validate_url(url: str) -> None:
        try:
            check_url(url, settings.allow_private_urls)
        except UnsafeURL as exc:
            raise HTTPException(422, f"endpoint URL rejected: {exc}") from exc

    # ---------------------------------------------------------- endpoints
    @app.post("/v1/endpoints", status_code=201, dependencies=[Depends(auth)])
    async def create_endpoint(body: EndpointIn, pool=Depends(pool)):
        validate_url(body.url)
        secret = signing.generate_secret()
        async with pool.connection() as conn:
            row = await (await conn.execute(
                """INSERT INTO endpoints (url, description, secret, event_types, max_in_flight)
                   VALUES (%s, %s, %s, %s, %s)
                   RETURNING id, url, description, event_types, enabled, max_in_flight, created_at""",
                (body.url, body.description, secret, body.event_types,
                 body.max_in_flight or settings.default_max_in_flight))).fetchone()
        # The secret is shown once; the receiver stores it to verify signatures.
        return {**row, "secret": secret}

    @app.get("/v1/endpoints", dependencies=[Depends(auth)])
    async def list_endpoints(pool=Depends(pool)):
        async with pool.connection() as conn:
            rows = await (await conn.execute("""
                SELECT e.id, e.url, e.description, e.event_types, e.enabled, e.disabled_reason,
                       e.max_in_flight, e.consecutive_failures, e.circuit_open_until, e.created_at,
                       CASE WHEN NOT e.enabled THEN 'disabled'
                            WHEN e.circuit_open_until > now() THEN 'open'
                            WHEN e.consecutive_failures >= %s THEN 'half_open'
                            ELSE 'closed' END AS circuit,
                       count(*) FILTER (WHERE d.status = 'pending')    AS pending,
                       count(*) FILTER (WHERE d.status = 'delivering') AS delivering,
                       count(*) FILTER (WHERE d.status = 'succeeded')  AS succeeded,
                       count(*) FILTER (WHERE d.status = 'dead')       AS dead
                FROM endpoints e LEFT JOIN deliveries d ON d.endpoint_id = e.id
                GROUP BY e.id ORDER BY e.created_at""", (settings.circuit_failure_threshold,))).fetchall()
        return rows

    @app.patch("/v1/endpoints/{endpoint_id}", dependencies=[Depends(auth)])
    async def update_endpoint(endpoint_id: uuid.UUID, body: EndpointPatch, pool=Depends(pool)):
        changes = body.model_dump(exclude_unset=True)
        if not changes:
            raise HTTPException(400, "nothing to update")
        if "url" in changes:
            validate_url(changes["url"])
        sets, params = [], []
        for col, val in changes.items():
            sets.append(f"{col} = %s")
            params.append(val)
        if changes.get("enabled") is True:  # re-enabling resets the breaker
            sets += ["disabled_reason = NULL", "consecutive_failures = 0", "circuit_open_until = NULL"]
        async with pool.connection() as conn:
            row = await (await conn.execute(
                f"UPDATE endpoints SET {', '.join(sets)} WHERE id = %s "
                "RETURNING id, url, description, event_types, enabled, max_in_flight",
                (*params, endpoint_id))).fetchone()
        if not row:
            raise HTTPException(404, "endpoint not found")
        return row

    @app.delete("/v1/endpoints/{endpoint_id}", status_code=204, dependencies=[Depends(auth)])
    async def delete_endpoint(endpoint_id: uuid.UUID, pool=Depends(pool)):
        async with pool.connection() as conn:
            cur = await conn.execute("DELETE FROM endpoints WHERE id = %s", (endpoint_id,))
        if cur.rowcount == 0:
            raise HTTPException(404, "endpoint not found")
        return Response(status_code=204)

    @app.post("/v1/endpoints/{endpoint_id}/replay-dead", dependencies=[Depends(auth)])
    async def replay_dead(endpoint_id: uuid.UUID, pool=Depends(pool)):
        async with pool.connection() as conn, conn.transaction():
            cur = await conn.execute(
                """UPDATE deliveries SET status = 'pending', attempt_count = 0, next_attempt_at = now(),
                       last_error = NULL, updated_at = now()
                   WHERE endpoint_id = %s AND status = 'dead'""", (endpoint_id,))
            await conn.execute(f"NOTIFY {NOTIFY_CHANNEL}")
        return {"replayed": cur.rowcount}

    # ------------------------------------------------------------- events
    @app.post("/v1/events", status_code=202, dependencies=[Depends(auth)])
    async def create_event(request: Request, response: Response, pool=Depends(pool),
                           idempotency_key: Annotated[str | None, Header(max_length=200)] = None):
        raw = await request.body()
        if len(raw) > settings.max_payload_bytes:
            raise HTTPException(413, f"payload larger than {settings.max_payload_bytes} bytes")
        try:
            body = EventIn.model_validate_json(raw)
        except ValidationError as exc:
            raise HTTPException(422, json.loads(exc.json(include_url=False))) from exc

        # The event and its deliveries are written in ONE transaction: either the
        # event is accepted and queued for every subscriber, or nothing is.
        async with pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                """INSERT INTO events (event_type, payload, idempotency_key) VALUES (%s, %s, %s)
                   ON CONFLICT (idempotency_key) DO NOTHING RETURNING id, created_at""",
                (body.event_type, Jsonb(body.payload), idempotency_key))).fetchone()
            if row is None:  # same Idempotency-Key seen before: return the original
                existing = await (await conn.execute(
                    """SELECT ev.id, ev.created_at, count(d.id) AS deliveries
                       FROM events ev LEFT JOIN deliveries d ON d.event_id = ev.id
                       WHERE ev.idempotency_key = %s GROUP BY ev.id""", (idempotency_key,))).fetchone()
                response.status_code = 200
                return {**existing, "duplicate": True}
            cur = await conn.execute(
                """INSERT INTO deliveries (event_id, endpoint_id)
                   SELECT %s, id FROM endpoints
                   WHERE enabled AND (event_types IS NULL OR %s = ANY(event_types))""",
                (row["id"], body.event_type))
            if cur.rowcount:
                await conn.execute(f"NOTIFY {NOTIFY_CHANNEL}")  # delivered to workers on commit
        return {**row, "deliveries": cur.rowcount, "duplicate": False}

    @app.get("/v1/events", dependencies=[Depends(auth)])
    async def list_events(pool=Depends(pool), limit: int = Query(50, le=500),
                          event_type: str | None = None):
        async with pool.connection() as conn:
            return await (await conn.execute(
                """SELECT ev.id, ev.event_type, ev.created_at,
                          count(d.id) AS deliveries,
                          count(d.id) FILTER (WHERE d.status = 'succeeded') AS succeeded,
                          count(d.id) FILTER (WHERE d.status = 'dead') AS dead
                   FROM events ev LEFT JOIN deliveries d ON d.event_id = ev.id
                   WHERE (%(t)s::text IS NULL OR ev.event_type = %(t)s)
                   GROUP BY ev.id ORDER BY ev.created_at DESC LIMIT %(l)s""",
                {"t": event_type, "l": limit})).fetchall()

    @app.get("/v1/events/{event_id}", dependencies=[Depends(auth)])
    async def get_event(event_id: uuid.UUID, pool=Depends(pool)):
        async with pool.connection() as conn:
            event = await (await conn.execute(
                "SELECT id, event_type, payload, idempotency_key, created_at FROM events WHERE id = %s",
                (event_id,))).fetchone()
            if not event:
                raise HTTPException(404, "event not found")
            deliveries = await (await conn.execute(
                """SELECT d.id, d.endpoint_id, e.url, d.status, d.attempt_count, d.next_attempt_at,
                          d.last_status_code, d.last_error, d.delivered_at
                   FROM deliveries d JOIN endpoints e ON e.id = d.endpoint_id
                   WHERE d.event_id = %s ORDER BY d.id""", (event_id,))).fetchall()
            attempts = await (await conn.execute(
                """SELECT a.delivery_id, a.attempt_no, a.started_at, a.duration_ms, a.status_code, a.error
                   FROM delivery_attempts a JOIN deliveries d ON d.id = a.delivery_id
                   WHERE d.event_id = %s ORDER BY a.started_at""", (event_id,))).fetchall()
        by_delivery: dict[int, list] = {}
        for a in attempts:
            by_delivery.setdefault(a.pop("delivery_id"), []).append(a)
        for d in deliveries:
            d["attempts"] = by_delivery.get(d["id"], [])
        return {**event, "deliveries": deliveries}

    # --------------------------------------------------------- deliveries
    @app.get("/v1/deliveries", dependencies=[Depends(auth)])
    async def list_deliveries(pool=Depends(pool), limit: int = Query(50, le=500),
                              status: Literal["pending", "delivering", "succeeded", "dead"] | None = None,
                              endpoint_id: uuid.UUID | None = None):
        async with pool.connection() as conn:
            return await (await conn.execute(
                """SELECT d.id, d.event_id, ev.event_type, d.endpoint_id, e.url, d.status, d.attempt_count,
                          d.next_attempt_at, d.last_status_code, d.last_error, d.updated_at, d.delivered_at
                   FROM deliveries d JOIN events ev ON ev.id = d.event_id JOIN endpoints e ON e.id = d.endpoint_id
                   WHERE (%(s)s::text IS NULL OR d.status = %(s)s)
                     AND (%(e)s::uuid IS NULL OR d.endpoint_id = %(e)s)
                   ORDER BY d.updated_at DESC LIMIT %(l)s""",
                {"s": status, "e": endpoint_id, "l": limit})).fetchall()

    @app.post("/v1/deliveries/{delivery_id}/replay", dependencies=[Depends(auth)])
    async def replay_delivery(delivery_id: int, pool=Depends(pool)):
        async with pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                """UPDATE deliveries SET status = 'pending', attempt_count = 0, next_attempt_at = now(),
                       last_error = NULL, updated_at = now()
                   WHERE id = %s AND status IN ('dead', 'succeeded') RETURNING id, status""",
                (delivery_id,))).fetchone()
            if not row:
                raise HTTPException(409, "only dead or succeeded deliveries can be replayed")
            await conn.execute(f"NOTIFY {NOTIFY_CHANNEL}")
        return row

    # ---------------------------------------------------------- stats
    @app.get("/v1/stats", dependencies=[Depends(auth)])
    async def stats(pool=Depends(pool)):
        async with pool.connection() as conn:
            counts = await (await conn.execute(
                "SELECT status, count(*) AS n FROM deliveries GROUP BY status")).fetchall()
            window = await (await conn.execute("""
                SELECT count(*) AS attempts,
                       count(*) FILTER (WHERE status_code BETWEEN 200 AND 299) AS ok,
                       percentile_cont(0.5)  WITHIN GROUP (ORDER BY duration_ms) AS p50_ms,
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_ms
                FROM delivery_attempts WHERE started_at > now() - interval '5 minutes'""")).fetchone()
            per_min = await (await conn.execute("""
                SELECT date_trunc('minute', started_at) AS minute,
                       count(*) FILTER (WHERE status_code BETWEEN 200 AND 299) AS ok,
                       count(*) FILTER (WHERE status_code IS NULL OR status_code NOT BETWEEN 200 AND 299) AS failed
                FROM delivery_attempts WHERE started_at > now() - interval '30 minutes'
                GROUP BY 1 ORDER BY 1""")).fetchall()
            lag = await (await conn.execute("""
                SELECT extract(epoch FROM now() - min(next_attempt_at)) AS oldest_due_seconds
                FROM deliveries WHERE status = 'pending' AND next_attempt_at <= now()""")).fetchone()
        by_status = {r["status"]: r["n"] for r in counts}
        attempts = window["attempts"] or 0
        return {
            "deliveries": {s: by_status.get(s, 0) for s in ("pending", "delivering", "succeeded", "dead")},
            "last_5_min": {
                "attempts": attempts,
                "success_rate": round(window["ok"] / attempts, 4) if attempts else None,
                "p50_ms": window["p50_ms"], "p95_ms": window["p95_ms"],
            },
            "per_minute": per_min,
            "oldest_due_seconds": float(lag["oldest_due_seconds"] or 0),
        }

    # --------------------------------------------------------- metrics
    @app.get("/metrics", include_in_schema=False, response_class=PlainTextResponse)
    async def metrics(pool=Depends(pool)):
        """Prometheus exposition format. Computed from the database, so it's
        correct however many API and worker processes are running."""
        async with pool.connection() as conn:
            by_status = await (await conn.execute(
                "SELECT status, count(*) AS n FROM deliveries GROUP BY status")).fetchall()
            attempts = await (await conn.execute("""
                SELECT count(*) FILTER (WHERE status_code BETWEEN 200 AND 299) AS ok,
                       count(*) FILTER (WHERE status_code IS NULL OR status_code NOT BETWEEN 200 AND 299) AS failed
                FROM delivery_attempts""")).fetchone()
            q = await (await conn.execute("""
                SELECT percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY duration_ms) AS q
                FROM delivery_attempts WHERE started_at > now() - interval '5 minutes'""")).fetchone()
            lag = await (await conn.execute("""
                SELECT coalesce(extract(epoch FROM now() - min(next_attempt_at)), 0) AS s
                FROM deliveries WHERE status = 'pending' AND next_attempt_at <= now()""")).fetchone()
            open_circuits = await (await conn.execute(
                "SELECT count(*) AS n FROM endpoints WHERE enabled AND circuit_open_until > now()")).fetchone()
        counts = {r["status"]: r["n"] for r in by_status}
        lines = [
            "# HELP courier_deliveries Deliveries by current status.",
            "# TYPE courier_deliveries gauge",
            *[f'courier_deliveries{{status="{st}"}} {counts.get(st, 0)}'
              for st in ("pending", "delivering", "succeeded", "dead")],
            "# HELP courier_delivery_attempts_total HTTP delivery attempts by outcome.",
            "# TYPE courier_delivery_attempts_total counter",
            f'courier_delivery_attempts_total{{outcome="success"}} {attempts["ok"]}',
            f'courier_delivery_attempts_total{{outcome="failure"}} {attempts["failed"]}',
            "# HELP courier_attempt_duration_ms Attempt duration quantiles over the last 5 minutes.",
            "# TYPE courier_attempt_duration_ms gauge",
        ]
        for label, val in zip(("0.5", "0.95", "0.99"), q["q"] or [None] * 3):
            lines.append(f'courier_attempt_duration_ms{{quantile="{label}"}} {val if val is not None else "NaN"}')
        lines += [
            "# HELP courier_oldest_due_seconds Age of the oldest delivery that is due but not yet sent (queue lag).",
            "# TYPE courier_oldest_due_seconds gauge",
            f"courier_oldest_due_seconds {float(lag['s']):.3f}",
            "# HELP courier_open_circuits Endpoints whose circuit breaker is currently open.",
            "# TYPE courier_open_circuits gauge",
            f"courier_open_circuits {open_circuits['n']}",
        ]
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------ misc
    @app.get("/health")
    async def health(pool=Depends(pool)):
        async with pool.connection() as conn:
            await conn.execute("SELECT 1")
        return {"status": "ok", "version": __version__}

    @app.get("/", include_in_schema=False)
    async def root():
        return RedirectResponse("/dashboard")

    @app.get("/dashboard", include_in_schema=False)
    async def dashboard():
        return FileResponse(STATIC / "dashboard.html")

    @app.exception_handler(ForeignKeyViolation)
    async def _fk(request, exc):
        raise HTTPException(404, "referenced resource not found")

    return app


app = create_app()
