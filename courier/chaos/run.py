"""Chaos test: send N events through Courier to four misbehaving receivers and
prove that every one was delivered.

    python -m chaos.run --events 5000 --outage-seconds 60

Needs the API, at least one worker and the chaos receiver running
(`docker compose up` starts all of them).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

PROFILES = ["healthy", "flaky", "slow", "outage"]
# Slow and flaky receivers hold connections open for seconds, so they get more
# parallel requests; isolation still caps them so they can't starve the others.
MAX_IN_FLIGHT = {"healthy": 10, "flaky": 40, "slow": 40, "outage": 10}


async def main(args) -> int:
    headers = {"authorization": f"Bearer {args.api_key}"} if args.api_key else {}
    async with httpx.AsyncClient(base_url=args.api, headers=headers, timeout=30) as api, \
            httpx.AsyncClient(base_url=args.receiver, timeout=30) as rx:
        await rx.post("/admin/reset")
        # Fresh endpoints for this run, one per receiver profile.
        for ep in (await api.get("/v1/endpoints")).json():
            if "/hook/" in ep["url"]:
                await api.delete(f"/v1/endpoints/{ep['id']}")
        secrets, endpoint_ids = {}, {}
        for p in PROFILES:
            r = await api.post("/v1/endpoints", json={"url": f"{args.receiver_url_for_courier}/hook/{p}",
                                                      "description": f"chaos: {p}",
                                                      "max_in_flight": MAX_IN_FLIGHT[p]})
            r.raise_for_status()
            secrets[p], endpoint_ids[p] = r.json()["secret"], r.json()["id"]
        await rx.post("/admin/secrets", json=secrets)
        await rx.post("/admin/outage", params={"profile": "outage", "seconds": args.outage_seconds})

        print(f"Sending {args.events} events (x{len(PROFILES)} endpoints = "
              f"{args.events * len(PROFILES)} deliveries); 'outage' is down for {args.outage_seconds}s ...")
        sem = asyncio.Semaphore(args.concurrency)
        event_ids: set[str] = set()
        api_dupes = 0

        async def send(i: int):
            nonlocal api_dupes
            key = f"chaos-{args.run_id}-{i}"
            if args.rate:  # steady load: event i is sent at t0 + i/rate
                await asyncio.sleep(max(0.0, t0 + i / args.rate - time.perf_counter()))
            async with sem:
                r = await api.post("/v1/events", headers={"idempotency-key": key},
                                   json={"event_type": "order.paid", "payload": {"order": i, "amount": random.randint(100, 9999)}})
                r.raise_for_status()
                event_ids.add(r.json()["id"])
                # Simulate a sender that retries its own request (e.g. after a timeout).
                if random.random() < args.resend_rate:
                    again = await api.post("/v1/events", headers={"idempotency-key": key},
                                           json={"event_type": "order.paid", "payload": {"order": i}})
                    if again.json()["duplicate"]:
                        api_dupes += 1

        t0 = time.perf_counter()
        await asyncio.gather(*(send(i) for i in range(args.events)))
        ingest_s = time.perf_counter() - t0
        print(f"Ingested in {ingest_s:.1f}s ({args.events / ingest_s:.0f} events/s); "
              f"{api_dupes} sender retries correctly deduplicated by Idempotency-Key")

        deadline = time.time() + args.timeout
        remaining = dead = None
        while time.time() < deadline:
            d = (await api.get("/v1/stats")).json()["deliveries"]
            remaining, dead = d["pending"] + d["delivering"], d["dead"]
            print(f"  {time.perf_counter() - t0:6.0f}s  delivered={d['succeeded']:>6}  "
                  f"in progress={remaining:>6}  dead={dead}")
            if remaining == 0:
                break
            await asyncio.sleep(args.poll)
        total_s = time.perf_counter() - t0

        rstats = (await rx.get("/admin/stats")).json()
        rows, missing_total = [], 0
        for p in PROFILES:
            accepted = set((await rx.get(f"/admin/accepted/{p}")).json())
            missing = len(event_ids - accepted)
            missing_total += missing
            s = rstats[p]
            rows.append({"endpoint": p, "expected": len(event_ids), "delivered": len(event_ids & accepted),
                         "missing": missing, "http_requests": s["requests"],
                         "duplicates_seen": s["duplicate_accepts"], "bad_signatures": s["invalid_signatures"],
                         "p50_ms": s["latency_p50_ms"], "p95_ms": s["latency_p95_ms"]})

    if remaining:
        verdict = "INCOMPLETE"
        summary = f"{remaining} deliveries still in progress at the {args.timeout:.0f}s deadline (not lost; raise --timeout)"
    elif missing_total:
        verdict = "FAIL"
        summary = f"{missing_total} of {args.events * len(PROFILES)} deliveries never reached the receiver ({dead} dead-lettered)"
    else:
        verdict = "PASS"
        summary = f"all {args.events * len(PROFILES)} deliveries arrived, 0 lost"
    lost_total = missing_total if not remaining else None
    result = {
        "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "events": args.events, "deliveries": args.events * len(PROFILES),
        "outage_seconds": args.outage_seconds, "mode": f"steady {args.rate:g}/s" if args.rate else "burst",
        "ingest_events_per_s": round(args.events / ingest_s, 1),
        "total_seconds": round(total_s, 1),
        "sender_retries_deduplicated": api_dupes, "verdict": verdict, "lost": lost_total, "endpoints": rows,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2))

    fmt = lambda v: "-" if v is None else f"{v:,.0f}"  # noqa: E731
    print(f"\n{'endpoint':<9} {'expected':>8} {'delivered':>9} {'missing':>7} {'requests':>9} "
          f"{'dupes':>6} {'p50 ms':>8} {'p95 ms':>8}")
    for r in rows:
        print(f"{r['endpoint']:<9} {r['expected']:>8} {r['delivered']:>9} {r['missing']:>7} {r['http_requests']:>9} "
              f"{r['duplicates_seen']:>6} {fmt(r['p50_ms']):>8} {fmt(r['p95_ms']):>8}")
    print(f"\n{verdict}: {summary}, in {total_s:.0f}s. Results written to {out}")
    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--receiver", default="http://localhost:9000", help="receiver URL as seen by this script")
    ap.add_argument("--receiver-url-for-courier", default=None,
                    help="receiver URL as seen by Courier's workers (differs inside Docker)")
    ap.add_argument("--events", type=int, default=2000)
    ap.add_argument("--concurrency", type=int, default=50)
    ap.add_argument("--rate", type=float, default=None,
                    help="send at a steady N events/s instead of one burst (measures normal latency)")
    ap.add_argument("--outage-seconds", type=float, default=60)
    ap.add_argument("--resend-rate", type=float, default=0.05)
    ap.add_argument("--timeout", type=float, default=900)
    ap.add_argument("--poll", type=float, default=5)
    ap.add_argument("--out", default="chaos/results/latest.json")
    ap.add_argument("--run-id", default=str(int(time.time())))
    a = ap.parse_args()
    a.receiver_url_for_courier = a.receiver_url_for_courier or a.receiver
    sys.exit(asyncio.run(main(a)))
