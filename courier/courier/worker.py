"""The delivery worker: claims due deliveries, sends them, records the outcome.

Run any number of these (`python -m courier worker`) against one database.
They coordinate only through PostgreSQL row locks, so there is no leader and
nothing to configure when you add or remove workers.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx
import psycopg
from psycopg_pool import AsyncConnectionPool

from . import __version__, signing
from .config import Settings
from .db import NOTIFY_CHANNEL
from .retry import backoff_seconds, parse_retry_after
from .urlguard import UnsafeURL, check_url_async

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Claiming work.
#
# 1. `capacity`: how many more requests each endpoint may receive right now:
#    its max_in_flight minus deliveries currently leased to ANY worker. An
#    endpoint whose breaker is half-open (it has been failing) gets 1 slot, so a
#    single probe request tests whether it has recovered. Endpoints with an
#    open circuit or that are disabled get no slots at all.
# 2. `candidates`: for each endpoint with free slots, its oldest due deliveries,
#    at most `free` of them (an index range scan per endpoint, so the cost
#    depends on the number of endpoints, not the size of the backlog). A
#    delivery whose lease expired (its worker crashed) counts as due.
# 3. `locked`: fair ordering. Every endpoint's 1st candidate, then every
#    endpoint's 2nd, and so on, so a backlog on one endpoint can't push other
#    endpoints' deliveries to the back of the line. FOR UPDATE SKIP LOCKED
#    takes row locks and skips rows another worker is locking right now, so
#    two workers never get the same row, and LIMIT here (not earlier) means
#    workers claiming simultaneously skip past each other instead of all
#    fighting over the same rows. The status condition is repeated on purpose:
#    if another worker's claim committed between our snapshot and our lock,
#    Postgres re-checks it against the row's new version and drops the row.
# 4. The UPDATE leases the rows to this worker.
# ---------------------------------------------------------------------------
CLAIM_SQL = """
WITH capacity AS (
    SELECT e.id AS endpoint_id,
           GREATEST(0,
             (CASE WHEN e.consecutive_failures >= %(threshold)s THEN 1 ELSE e.max_in_flight END)
             - (SELECT count(*) FROM deliveries x
                WHERE x.endpoint_id = e.id AND x.status = 'delivering' AND x.locked_until >= now())
           ) AS free
    FROM endpoints e
    WHERE e.enabled
      AND (e.circuit_open_until IS NULL OR e.circuit_open_until <= now())
),
candidates AS (
    SELECT due.id, due.due_at,
           row_number() OVER (PARTITION BY c.endpoint_id ORDER BY due.due_at, due.id) AS rn
    FROM capacity c
    CROSS JOIN LATERAL (
        SELECT * FROM (
            (SELECT d.id, d.next_attempt_at AS due_at FROM deliveries d
             WHERE d.endpoint_id = c.endpoint_id AND d.status = 'pending' AND d.next_attempt_at <= now()
             ORDER BY d.next_attempt_at LIMIT c.free)
            UNION ALL
            (SELECT d.id, d.locked_until AS due_at FROM deliveries d
             WHERE d.endpoint_id = c.endpoint_id AND d.status = 'delivering' AND d.locked_until < now()
             ORDER BY d.locked_until LIMIT c.free)
        ) both_kinds
        ORDER BY due_at LIMIT c.free
    ) due
    WHERE c.free > 0
),
locked AS (
    SELECT d.id FROM deliveries d
    JOIN candidates c ON c.id = d.id
    WHERE (d.status = 'pending' AND d.next_attempt_at <= now())
       OR (d.status = 'delivering' AND d.locked_until < now())
    ORDER BY c.rn, c.due_at
    LIMIT %(limit)s
    FOR UPDATE OF d SKIP LOCKED
)
UPDATE deliveries d
SET status = 'delivering',
    locked_until = now() + make_interval(secs => %(lease)s),
    locked_by = %(worker)s,
    updated_at = now()
FROM locked, events ev, endpoints e
WHERE d.id = locked.id AND ev.id = d.event_id AND e.id = d.endpoint_id
RETURNING d.id, d.event_id, d.endpoint_id, d.attempt_count, ev.event_type, ev.payload,
          ev.created_at AS event_created_at, e.url, e.secret
"""


@dataclass
class Job:
    id: int
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    attempt_count: int
    event_type: str
    payload: dict
    event_created_at: datetime
    url: str
    secret: str


@dataclass
class Outcome:
    ok: bool
    status_code: int | None
    error: str | None
    duration_ms: int
    started_at: datetime
    response_body: str | None = None
    retry_after: float | None = None


def build_body(job: Job) -> bytes:
    """The JSON the receiver gets. Stable across retries so the signature input is too."""
    return json.dumps({
        "id": str(job.event_id),
        "type": job.event_type,
        "timestamp": job.event_created_at.astimezone(timezone.utc).isoformat(),
        "data": job.payload,
    }, separators=(",", ":"), sort_keys=True).encode()


class Worker:
    def __init__(self, pool: AsyncConnectionPool, settings: Settings,
                 client: httpx.AsyncClient | None = None, worker_id: str | None = None):
        self.pool = pool
        self.s = settings
        self.id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(settings.request_timeout),
            follow_redirects=False,  # a redirect could point at an internal address
            limits=httpx.Limits(max_connections=settings.worker_concurrency * 2),
            headers={"user-agent": f"Courier/{__version__}"},
        )
        self._wake = asyncio.Event()
        self._inflight: set[asyncio.Task] = set()
        self._claim_conn = None  # set by run(): claims never queue behind result writes

    # -- claiming ---------------------------------------------------------
    async def claim(self, limit: int) -> list[Job]:
        if limit <= 0:
            return []
        params = {"threshold": self.s.circuit_failure_threshold, "limit": limit,
                  "lease": self.s.lease_seconds, "worker": self.id}
        if self._claim_conn is not None:
            rows = await (await self._claim_conn.execute(CLAIM_SQL, params)).fetchall()
        else:
            async with self.pool.connection() as conn:
                rows = await (await conn.execute(CLAIM_SQL, params)).fetchall()
        return [Job(**r) for r in rows]

    # -- sending ----------------------------------------------------------
    async def send(self, job: Job) -> Outcome:
        started = datetime.now(timezone.utc)
        t0 = time.perf_counter()
        ms = lambda: int((time.perf_counter() - t0) * 1000)  # noqa: E731
        try:
            await check_url_async(job.url, self.s.allow_private_urls)
        except UnsafeURL as exc:
            return Outcome(False, None, f"blocked: {exc}", ms(), started)
        body = build_body(job)
        hdrs = {"content-type": "application/json", **signing.headers(job.secret, str(job.event_id), body)}
        try:
            resp = await self.client.post(job.url, content=body, headers=hdrs)
        except httpx.TimeoutException:
            return Outcome(False, None, "timeout", ms(), started)
        except httpx.HTTPError as exc:
            return Outcome(False, None, f"{type(exc).__name__}: {exc}"[:300], ms(), started)
        ok = 200 <= resp.status_code < 300
        return Outcome(
            ok=ok, status_code=resp.status_code, error=None if ok else f"HTTP {resp.status_code}",
            duration_ms=ms(), started_at=started, response_body=resp.text[: self.s.response_body_limit],
            retry_after=parse_retry_after(resp.headers.get("retry-after")),
        )

    # -- recording --------------------------------------------------------
    def _next_state(self, job: Job, out: Outcome, now: datetime) -> tuple[str, datetime | None, bool]:
        """(new status, next_attempt_at, disable_endpoint) for a failed attempt."""
        attempts = job.attempt_count + 1
        if out.error and out.error.startswith("blocked:"):
            return "dead", None, False
        if out.status_code == 410:  # receiver says this URL is gone for good
            return "dead", None, True
        too_old = (now - job.event_created_at).total_seconds() > self.s.max_event_age_seconds
        if attempts >= self.s.max_attempts or too_old:
            return "dead", None, False
        delay = backoff_seconds(attempts, self.s.retry_schedule, self.s.retry_jitter)
        if out.retry_after is not None and out.status_code in (429, 503):
            delay = max(delay, out.retry_after)
        return "pending", now + timedelta(seconds=delay), False

    async def record(self, job: Job, out: Outcome) -> None:
        now = datetime.now(timezone.utc)
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute(
                """INSERT INTO delivery_attempts
                   (delivery_id, attempt_no, started_at, duration_ms, status_code, error, response_body, worker)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (job.id, job.attempt_count + 1, out.started_at, out.duration_ms, out.status_code,
                 out.error, out.response_body, self.id))

            if out.ok:
                cur = await conn.execute(
                    """UPDATE deliveries SET status = 'succeeded', attempt_count = attempt_count + 1,
                           delivered_at = now(), last_status_code = %s, last_error = NULL,
                           locked_until = NULL, locked_by = NULL, updated_at = now()
                       WHERE id = %s AND locked_by = %s AND status = 'delivering'""",
                    (out.status_code, job.id, self.id))
                await conn.execute(
                    """UPDATE endpoints SET consecutive_failures = 0, circuit_open_until = NULL
                       WHERE id = %s AND (consecutive_failures <> 0 OR circuit_open_until IS NOT NULL)""",
                    (job.endpoint_id,))
            else:
                status, next_at, disable = self._next_state(job, out, now)
                cur = await conn.execute(
                    """UPDATE deliveries SET status = %s, attempt_count = attempt_count + 1,
                           next_attempt_at = COALESCE(%s, next_attempt_at), last_status_code = %s,
                           last_error = %s, locked_until = NULL, locked_by = NULL, updated_at = now()
                       WHERE id = %s AND locked_by = %s AND status = 'delivering'""",
                    (status, next_at, out.status_code, out.error, job.id, self.id))
                if not (out.error or "").startswith("blocked:"):
                    await conn.execute(
                        """UPDATE endpoints SET
                               consecutive_failures = consecutive_failures + 1,
                               circuit_open_until = CASE WHEN consecutive_failures + 1 >= %s
                                   THEN now() + make_interval(secs => %s) ELSE circuit_open_until END
                           WHERE id = %s""",
                        (self.s.circuit_failure_threshold, self.s.circuit_cooldown_seconds, job.endpoint_id))
                if disable:
                    await conn.execute(
                        "UPDATE endpoints SET enabled = false, disabled_reason = %s WHERE id = %s",
                        ("receiver returned 410 Gone", job.endpoint_id))
            if cur.rowcount == 0:
                # Our lease expired and another worker took the delivery over.
                # The request already went out, so the receiver may see it twice;
                # that's the at-least-once trade-off, and why receivers dedupe on webhook-id.
                log.warning("Lost lease on delivery %s; result recorded as an attempt only", job.id)

    async def process(self, job: Job) -> None:
        try:
            out = await self.send(job)
            await self.record(job, out)
        except Exception:  # never let one bad job kill the worker; its lease will expire and it'll be retried
            log.exception("Unexpected error processing delivery %s", job.id)

    # -- main loop --------------------------------------------------------
    async def run_once(self) -> int:
        """Claim one batch and process it fully. Used by tests and the CLI's --once."""
        jobs = await self.claim(self.s.batch_size)
        await asyncio.gather(*(self.process(j) for j in jobs))
        return len(jobs)

    async def _listen(self) -> None:
        """Wake up immediately when the API announces new deliveries (LISTEN/NOTIFY)."""
        while True:
            try:
                async with await psycopg.AsyncConnection.connect(self.s.database_url, autocommit=True) as conn:
                    await conn.execute(f"LISTEN {NOTIFY_CHANNEL}")
                    async for _ in conn.notifies():
                        self._wake.set()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # fall back to polling until we can reconnect
                log.warning("LISTEN connection lost (%s); retrying in 2s", exc)
                await asyncio.sleep(2)

    async def run(self, stop: asyncio.Event) -> None:
        log.info("Worker %s started (concurrency=%d)", self.id, self.s.worker_concurrency)
        listener = asyncio.create_task(self._listen())
        claim_cm = self.pool.connection()
        self._claim_conn = await claim_cm.__aenter__()
        await self._claim_conn.set_autocommit(True)
        try:
            while not stop.is_set():
                free = self.s.worker_concurrency - len(self._inflight)
                jobs = await self.claim(min(free, self.s.batch_size)) if free > 0 else []
                for job in jobs:
                    task = asyncio.create_task(self.process(job))
                    self._inflight.add(task)
                    task.add_done_callback(self._inflight.discard)
                if jobs and len(jobs) == min(free, self.s.batch_size):
                    continue  # there may be more due right now
                # Sleep until something could have changed: new events (NOTIFY),
                # a delivery finishing (frees a slot, maybe for a capped endpoint),
                # shutdown, or the poll interval (retries that have become due).
                self._wake.clear()
                signals = [asyncio.create_task(self._wake.wait()), asyncio.create_task(stop.wait())]
                await asyncio.wait([*signals, *self._inflight], timeout=self.s.poll_interval,
                                   return_when=asyncio.FIRST_COMPLETED)
                for w in signals:
                    w.cancel()
        finally:
            listener.cancel()
            await self._claim_conn.set_autocommit(False)
            await claim_cm.__aexit__(None, None, None)
            self._claim_conn = None
            if self._inflight:
                log.info("Draining %d in-flight deliveries", len(self._inflight))
                await asyncio.wait(self._inflight, timeout=self.s.request_timeout + 5)
            await self.client.aclose()
            log.info("Worker %s stopped", self.id)
