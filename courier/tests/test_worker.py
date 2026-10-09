"""The failure-handling behaviour, each test one scenario an interviewer might ask about."""
import asyncio
import json

import httpx
import pytest

from courier import signing
from courier.worker import Worker
from tests.helpers import add_endpoint, add_event, close_circuits, deliveries, make_due

pytestmark = pytest.mark.asyncio


async def test_delivers_signed_payload(pool, settings, receiver):
    ep = await add_endpoint(pool)
    [event_id] = await add_event(pool, payload={"order": 123, "amount": 500})
    assert await Worker(pool, settings, receiver.client()).run_once() == 1

    req = receiver.requests[0]
    signing.verify(ep["secret"], req.content, dict(req.headers))  # the receiver can verify us
    body = json.loads(req.content)
    assert body["id"] == str(event_id) == req.headers["webhook-id"]
    assert body["data"] == {"order": 123, "amount": 500}
    [d] = await deliveries(pool)
    assert d["status"] == "succeeded" and d["attempt_count"] == 1 and d["delivered_at"]


async def test_failure_schedules_retry_with_backoff(pool, settings, receiver):
    await add_endpoint(pool)
    await add_event(pool)
    receiver.script("/hook", 500)
    await Worker(pool, settings, receiver.client()).run_once()
    [d] = await deliveries(pool)
    assert d["status"] == "pending" and d["attempt_count"] == 1
    assert d["last_status_code"] == 500
    delay = (d["next_attempt_at"] - d["updated_at"]).total_seconds()
    assert 0.7 <= delay <= 1.3  # first step of (1, 2, 4) with jitter


async def test_network_errors_and_timeouts_are_retried(pool, settings, receiver):
    await add_endpoint(pool)
    await add_event(pool)
    receiver.script("/hook", httpx.ConnectError("refused"), httpx.ReadTimeout("slow"))
    w = Worker(pool, settings, receiver.client())
    await w.run_once()
    await make_due(pool)
    await w.run_once()
    [d] = await deliveries(pool)
    assert d["attempt_count"] == 2 and d["last_error"] == "timeout"
    await make_due(pool)
    await w.run_once()
    assert (await deliveries(pool))[0]["status"] == "succeeded"


async def test_same_event_id_on_every_retry(pool, settings, receiver):
    """At-least-once: receivers dedupe on webhook-id, so it must never change."""
    await add_endpoint(pool)
    await add_event(pool)
    receiver.script("/hook", 503, 503)
    w = Worker(pool, settings, receiver.client())
    for _ in range(3):
        await make_due(pool)
        await w.run_once()
    assert len({r.headers["webhook-id"] for r in receiver.requests}) == 1
    assert len({r.content for r in receiver.requests}) == 1


async def test_dead_after_max_attempts_then_replay(pool, settings, receiver):
    from fastapi.testclient import TestClient  # noqa: F401  (replay is tested via SQL path below)
    await add_endpoint(pool)
    await add_event(pool)
    receiver.script("/hook", *[500] * settings.max_attempts)
    w = Worker(pool, settings, receiver.client())
    for _ in range(settings.max_attempts):
        await make_due(pool)
        await close_circuits(pool)
        await w.run_once()
    [d] = await deliveries(pool)
    assert d["status"] == "dead" and d["attempt_count"] == settings.max_attempts

    async with pool.connection() as conn:  # what POST /v1/deliveries/{id}/replay does
        await conn.execute("UPDATE deliveries SET status='pending', attempt_count=0, next_attempt_at=now() WHERE id=%s", (d["id"],))
    await close_circuits(pool)
    await w.run_once()
    assert (await deliveries(pool))[0]["status"] == "succeeded"


async def test_410_gone_kills_delivery_and_disables_endpoint(pool, settings, receiver):
    ep = await add_endpoint(pool)
    await add_event(pool)
    receiver.script("/hook", 410)
    await Worker(pool, settings, receiver.client()).run_once()
    assert (await deliveries(pool))[0]["status"] == "dead"
    async with pool.connection() as conn:
        e = await (await conn.execute("SELECT enabled, disabled_reason FROM endpoints WHERE id=%s", (ep["id"],))).fetchone()
    assert e["enabled"] is False and "410" in e["disabled_reason"]


async def test_retry_after_is_respected(pool, settings, receiver):
    await add_endpoint(pool)
    await add_event(pool)
    receiver.script("/hook", (429, {"retry-after": "120"}))
    await Worker(pool, settings, receiver.client()).run_once()
    [d] = await deliveries(pool)
    assert (d["next_attempt_at"] - d["updated_at"]).total_seconds() >= 119


async def test_circuit_breaker_isolates_broken_endpoint(pool, settings, receiver):
    """A broken shop stops getting requests; a healthy one is unaffected."""
    await add_endpoint(pool, url="http://broken.test/hook")
    await add_endpoint(pool, url="http://healthy.test/hook")
    receiver.script("/hook", *[])  # default 200
    receiver.default = 200

    def handler(request):
        receiver.requests.append(request)
        return httpx.Response(500 if request.url.host == "broken.test" else 200)

    w = Worker(pool, settings, httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    await add_event(pool, n=10)
    await w.run_once()
    async with pool.connection() as conn:
        e = await (await conn.execute(
            "SELECT consecutive_failures, circuit_open_until FROM endpoints WHERE url LIKE '%%broken%%'")).fetchone()
    assert e["consecutive_failures"] >= settings.circuit_failure_threshold and e["circuit_open_until"]

    receiver.requests.clear()
    await add_event(pool, n=5)
    await make_due(pool)
    await w.run_once()
    hosts = [r.url.host for r in receiver.requests]
    assert "broken.test" not in hosts            # circuit open: no requests at all
    assert hosts.count("healthy.test") == 5      # healthy endpoint carries on


async def test_half_open_sends_single_probe(pool, settings, receiver):
    ep = await add_endpoint(pool)
    async with pool.connection() as conn:  # breaker tripped, cooldown over -> half-open
        await conn.execute("UPDATE endpoints SET consecutive_failures = 10, circuit_open_until = now() - interval '1 second' WHERE id=%s", (ep["id"],))
    await add_event(pool, n=20)
    jobs = await Worker(pool, settings, receiver.client()).claim(50)
    assert len(jobs) == 1


async def test_per_endpoint_in_flight_cap(pool, settings, receiver):
    await add_endpoint(pool, url="http://a.test/hook", max_in_flight=3)
    await add_endpoint(pool, url="http://b.test/hook", max_in_flight=10)
    await add_event(pool, n=20)
    jobs = await Worker(pool, settings, receiver.client()).claim(100)
    by_url = {}
    for j in jobs:
        by_url[j.url] = by_url.get(j.url, 0) + 1
    assert by_url == {"http://a.test/hook": 3, "http://b.test/hook": 10}


async def test_crashed_worker_lease_expires_and_another_worker_finishes(pool, settings, receiver):
    await add_endpoint(pool)
    await add_event(pool)
    crashed = Worker(pool, settings, receiver.client(), worker_id="crashed")
    [job] = await crashed.claim(10)  # claims, then "dies" without recording anything
    rescuer = Worker(pool, settings, receiver.client(), worker_id="rescuer")
    assert await rescuer.claim(10) == []  # lease still valid: hands off
    async with pool.connection() as conn:
        await conn.execute("UPDATE deliveries SET locked_until = now() - interval '1 second'")
    assert await rescuer.run_once() == 1
    [d] = await deliveries(pool)
    assert d["status"] == "succeeded"

    # The crashed worker comes back to life and tries to record a stale result:
    # fencing on locked_by means it can't overwrite the rescuer's outcome.
    from courier.worker import Outcome
    from datetime import datetime, timezone
    await crashed.record(job, Outcome(False, 500, "HTTP 500", 5, datetime.now(timezone.utc)))
    assert (await deliveries(pool))[0]["status"] == "succeeded"


async def test_concurrent_workers_never_claim_the_same_delivery(pool, settings, receiver):
    """The SKIP LOCKED guarantee, checked under real concurrency."""
    for i in range(10):
        await add_endpoint(pool, url=f"http://shop{i}.test/hook", max_in_flight=100)
    await add_event(pool, n=60)  # 600 deliveries
    workers = [Worker(pool, settings, receiver.client(), worker_id=f"w{i}") for i in range(8)]
    claimed, rounds = [], 0
    while len(claimed) < 600 and rounds < 10:
        batches = await asyncio.gather(*(w.claim(40) for w in workers))
        claimed += [j.id for b in batches for j in b]
        rounds += 1
    assert len(claimed) == len(set(claimed)) == 600  # every row claimed exactly once
    assert rounds <= 3  # 8 workers x 40 = 320 per round: contention doesn't starve anyone


async def test_blocked_url_goes_dead_without_request(pool, settings, receiver):
    strict = settings.with_overrides(allow_private_urls=False)
    await add_endpoint(pool, url="http://127.0.0.1:5432/")
    await add_event(pool)
    await Worker(pool, strict, receiver.client()).run_once()
    [d] = await deliveries(pool)
    assert d["status"] == "dead" and d["last_error"].startswith("blocked:")
    assert receiver.requests == []


async def test_run_loop_wakes_on_notify(pool, settings, receiver):
    """New events are delivered immediately, not on the next poll."""
    await add_endpoint(pool)
    slow_poll = settings.with_overrides(poll_interval=30)
    w = Worker(pool, slow_poll, receiver.client())
    stop = asyncio.Event()
    task = asyncio.create_task(w.run(stop))
    await asyncio.sleep(0.5)
    await add_event(pool)
    async with pool.connection() as conn:
        await conn.execute("NOTIFY courier_deliveries")
    for _ in range(50):
        if (await deliveries(pool))[0]["status"] == "succeeded":
            break
        await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, 10)
    assert (await deliveries(pool))[0]["status"] == "succeeded"


async def test_fair_scheduling_backlog_does_not_starve_others(pool, settings, receiver):
    """A big, older backlog on one endpoint must not push other endpoints to the back of the line."""
    await add_endpoint(pool, url="http://backlogged.test/hook", max_in_flight=100, event_types=["bulk"])
    await add_endpoint(pool, url="http://quiet.test/hook", max_in_flight=100, event_types=["single"])
    await add_event(pool, "bulk", n=80)            # older
    await add_event(pool, "single", n=3)           # newer
    jobs = await Worker(pool, settings, receiver.client()).claim(10)
    urls = [j.url for j in jobs]
    assert urls.count("http://quiet.test/hook") == 3   # all of them, despite being newest
    assert len(jobs) == 10


async def test_claim_cost_does_not_depend_on_backlog_size(pool, settings, receiver):
    import time
    await add_endpoint(pool, max_in_flight=10)
    async with pool.connection() as conn:  # 20k-delivery backlog
        await conn.execute("""INSERT INTO events (id, event_type, payload)
                              SELECT gen_random_uuid(), 'x', '{}'::jsonb FROM generate_series(1, 20000)""")
        await conn.execute("INSERT INTO deliveries (event_id, endpoint_id) SELECT ev.id, e.id FROM events ev, endpoints e")
        await conn.execute("ANALYZE deliveries")
    w = Worker(pool, settings, receiver.client())
    t0 = time.perf_counter()
    jobs = await w.claim(100)
    assert len(jobs) == 10                     # endpoint cap respected
    assert time.perf_counter() - t0 < 0.5      # index range scan, not a sort of 20k rows


async def test_claim_skips_rows_another_worker_is_locking(pool, settings, receiver):
    """SKIP LOCKED is about throughput: a worker never waits for another worker's lock."""
    await add_endpoint(pool)
    await add_event(pool, n=5)
    async with pool.connection() as other, other.transaction():
        # Another worker is mid-claim and holds a lock on the oldest delivery.
        await other.execute("SELECT id FROM deliveries ORDER BY id LIMIT 1 FOR UPDATE")
        jobs = await asyncio.wait_for(Worker(pool, settings, receiver.client()).claim(10), timeout=2)
    assert len(jobs) == 4  # took the other four immediately instead of blocking


async def test_stale_worker_cannot_overwrite_new_lease_owner(pool, settings, receiver):
    """Fencing: a worker whose lease expired must not touch a delivery another worker now owns."""
    from datetime import datetime, timezone
    from courier.worker import Outcome
    await add_endpoint(pool)
    await add_event(pool)
    slow = Worker(pool, settings, receiver.client(), worker_id="slow")
    [job] = await slow.claim(10)
    async with pool.connection() as conn:
        await conn.execute("UPDATE deliveries SET locked_until = now() - interval '1 second'")
    [_] = await Worker(pool, settings, receiver.client(), worker_id="new-owner").claim(10)

    await slow.record(job, Outcome(True, 200, None, 5, datetime.now(timezone.utc)))  # late result
    [d] = await deliveries(pool)
    assert d["status"] == "delivering" and d["locked_by"] == "new-owner"
