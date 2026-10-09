"""A deliberately unreliable webhook receiver for chaos testing.

    uvicorn chaos.receiver:app --port 9000

Each URL path is a "shop" with its own behaviour:

    /hook/healthy   always 200, fast
    /hook/flaky     fails 30% of requests with a random 500/502/503/timeout
    /hook/slow      200, but takes 1-3 seconds
    /hook/outage    200, except during an outage window (then 503 for everything)

It verifies every signature, and records which webhook ids it accepted, so the
runner can prove that nothing was lost and count duplicates.
"""
from __future__ import annotations

import asyncio
import json
import random
import statistics
import time
from collections import defaultdict
from datetime import datetime

from fastapi import FastAPI, Request, Response

from courier.signing import InvalidSignature, verify

app = FastAPI(title="Chaos receiver")

PROFILES = {"healthy", "flaky", "slow", "outage"}
FLAKY_FAILURE_RATE = 0.30


class State:
    def __init__(self):
        self.secrets: dict[str, str] = {}
        self.outage_until: dict[str, float] = {}
        self.requests = defaultdict(int)
        self.accepted: dict[str, dict[str, float]] = defaultdict(dict)  # profile -> id -> first accept time
        self.duplicate_accepts = defaultdict(int)
        self.invalid_signatures = defaultdict(int)
        self.latencies_ms: dict[str, list[float]] = defaultdict(list)


S = State()


@app.post("/hook/{profile}")
async def hook(profile: str, request: Request):
    if profile not in PROFILES:
        return Response(status_code=404)
    S.requests[profile] += 1
    body = await request.body()

    secret = S.secrets.get(profile)
    if secret:
        try:
            verify(secret, body, dict(request.headers))
        except InvalidSignature:
            S.invalid_signatures[profile] += 1
            return Response(status_code=401)

    if profile == "outage" and time.time() < S.outage_until.get(profile, 0):
        return Response(status_code=503)
    if profile == "flaky" and random.random() < FLAKY_FAILURE_RATE:
        mode = random.choice(["500", "502", "503", "hang"])
        if mode == "hang":
            await asyncio.sleep(30)  # longer than Courier's timeout
        return Response(status_code=int(mode) if mode != "hang" else 504)
    if profile == "slow":
        await asyncio.sleep(random.uniform(1, 3))

    msg_id = request.headers.get("webhook-id", "")
    if msg_id in S.accepted[profile]:
        S.duplicate_accepts[profile] += 1  # a real receiver would just ignore it
    else:
        now = time.time()
        S.accepted[profile][msg_id] = now
        try:
            created = datetime.fromisoformat(json.loads(body)["timestamp"]).timestamp()
            S.latencies_ms[profile].append((now - created) * 1000)
        except (KeyError, ValueError):
            pass
    return {"ok": True}


@app.post("/admin/reset")
async def reset():
    global S
    S = State()
    return {"ok": True}


@app.post("/admin/secrets")
async def set_secrets(secrets: dict[str, str]):
    S.secrets.update(secrets)
    return {"ok": True}


@app.post("/admin/outage")
async def outage(profile: str = "outage", seconds: float = 60):
    S.outage_until[profile] = time.time() + seconds
    return {"profile": profile, "until": S.outage_until[profile]}


def _pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=100, method="inclusive")[int(q) - 1]


@app.get("/admin/stats")
async def stats():
    out = {}
    for p in sorted(PROFILES):
        lat = S.latencies_ms.get(p, [])
        out[p] = {
            "requests": S.requests.get(p, 0),
            "unique_accepted": len(S.accepted.get(p, {})),
            "duplicate_accepts": S.duplicate_accepts.get(p, 0),
            "invalid_signatures": S.invalid_signatures.get(p, 0),
            "latency_p50_ms": _pct(lat, 50), "latency_p95_ms": _pct(lat, 95),
            "latency_max_ms": max(lat) if lat else None,
        }
    return out


@app.get("/admin/accepted/{profile}")
async def accepted_ids(profile: str):
    return sorted(S.accepted.get(profile, {}))
