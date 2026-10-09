# Courier

**Reliable webhook delivery.** Apps hand Courier an event ("payment succeeded"),
and Courier guarantees it reaches every subscriber's URL, even when those
servers are down, slow, flaky or returning errors.

It's the piece that Stripe, GitHub and Razorpay each build internally, and that
services like Svix sell: retries with backoff, signed payloads, a dead-letter
queue with replay, per-endpoint isolation and a full delivery log.

![Dashboard](docs/images/dashboard.png)

## Results

Measured with the included chaos test: four receivers that misbehave on
purpose, all receiving every event. Two workers, one PostgreSQL, one laptop-class
machine; retry timing compressed (1s, 2s, 4s…) so runs take minutes, not days.

| Scenario | Deliveries | Lost | Healthy endpoint p50 / p95 |
|---|---:|---:|---|
| Steady 50 events/s, 30 s full outage on one receiver | 12,000 | **0** | **6 ms / 26 ms** |
| Burst of 2,000 events at once, 40 s outage | 8,000 | **0** | 162 ms / 810 ms |
| Steady 100 events/s, a worker **killed with `kill -9`** mid-run | 12,000 | **0** | 6 ms / 514 ms |

The four receivers in every run:

| Receiver | Behaviour | What it proves |
|---|---|---|
| `healthy` | always 200 in ms | isolation: its latency stays low while the others struggle |
| `flaky` | 30% of requests fail (500/502/503 or hang past the timeout) | retries with backoff recover everything (≈1,300 extra attempts, 0 lost) |
| `slow` | 1–3 s per request | per-endpoint concurrency caps contain its backlog |
| `outage` | 503 for everything during the outage window | circuit breaker pauses it, probes, then drains the backlog |

Every request's HMAC signature was verified by the receiver (0 invalid), and
about 5% of events were deliberately sent twice with the same
`Idempotency-Key`: all were deduplicated.

The worker-crash run is the interesting one: the killed worker had already
sent 20 requests to the slow receiver but died before recording them. When
their leases expired, the surviving worker sent them again, so the receiver saw
**20 duplicates**. That is at-least-once delivery working as designed, and the
reason every request carries a stable `webhook-id` receivers can dedupe on
(see [why exactly-once is impossible](docs/DESIGN.md#why-at-least-once-and-not-exactly-once)).

Raw results: [`chaos/results/`](chaos/results). Reproduce them with
`docker compose --profile chaos run --rm chaos`.

## How it works

```
 sender ──POST /v1/events──►  API  ──one transaction──►  events + deliveries (PostgreSQL)
                                                                   │  NOTIFY
                        ┌──────────────────────────────────────────┘
                        ▼
        worker 1 … worker N   claim due deliveries:  FOR UPDATE SKIP LOCKED
                        │      (fair across endpoints, capped per endpoint, leased)
             sign (HMAC) + POST
                        ▼
                 receiver endpoint
                        │
     2xx ──► succeeded   │   failure ──► retry at 5s, 30s, 2m, 10m … (+ jitter)
                        │                    │ after N attempts / 410 Gone
                        │                    ▼
                        │                  dead  ──► replay from dashboard or API
     every attempt ──► delivery_attempts (status, error, duration, worker)
```

- **Durable before acknowledged.** `POST /v1/events` writes the event and one
  delivery per subscribed endpoint in a single transaction, then returns 202.
- **PostgreSQL is the queue.** Workers claim rows with `FOR UPDATE SKIP LOCKED`,
  so any number of workers share the work with no coordinator and no double
  claims. `LISTEN/NOTIFY` wakes them instantly when new events arrive.
- **Leases, not locks held during HTTP.** A claimed delivery is leased to one
  worker for 60 s. If the worker dies, the lease expires and another takes over.
  Results are fenced on the lease owner, so a worker that wakes up late can't
  overwrite a newer outcome.
- **Isolation.** Each endpoint has a cap on concurrent requests; claims are
  scheduled round-robin across endpoints; a circuit breaker stops sending to
  an endpoint after repeated failures and later lets a single probe through.
- **Signatures** follow the [Standard Webhooks](https://www.standardwebhooks.com/)
  spec: `webhook-id`, `webhook-timestamp`, `webhook-signature` (HMAC-SHA256).

Every one of these choices, and what it costs, is explained in
[docs/DESIGN.md](docs/DESIGN.md).

## Run it

Requires Docker.

```bash
docker compose up --build
```

- Dashboard: http://localhost:8000
- Interactive API docs: http://localhost:8000/docs
- Chaos receiver (to register as an endpoint): `http://receiver:9000/hook/healthy`, `/flaky`, `/slow`, `/outage`

Chaos test, Prometheus, and more workers:

```bash
docker compose --profile chaos run --rm chaos          # results in chaos/results/latest.json
docker compose --profile observability up -d           # Prometheus on http://localhost:9090
docker compose up -d --scale worker=4                  # add workers; nothing else to configure
docker compose kill worker                             # then watch nothing get lost
```

Tests (need the Compose PostgreSQL running, or any PostgreSQL 13+):

```bash
pip install -r requirements-dev.txt
docker compose up -d postgres
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/courier_test python -m pytest -q
```

## Using the API

```bash
# Register an endpoint. The secret is shown once; the receiver uses it to verify signatures.
curl -X POST localhost:8000/v1/endpoints -H 'content-type: application/json' \
  -d '{"url": "https://shop.example.com/webhooks", "event_types": ["payment.succeeded"]}'

# Send an event. Retrying with the same Idempotency-Key never creates a duplicate.
curl -X POST localhost:8000/v1/events -H 'content-type: application/json' \
  -H 'Idempotency-Key: order-123-paid' \
  -d '{"event_type": "payment.succeeded", "payload": {"order_id": 123, "amount": 500}}'
```

| Endpoint | Purpose |
|---|---|
| `POST /v1/endpoints` · `GET` · `PATCH /{id}` · `DELETE /{id}` | manage receivers |
| `POST /v1/endpoints/{id}/replay-dead` | re-queue all of an endpoint's dead deliveries |
| `POST /v1/events` · `GET /v1/events` · `GET /v1/events/{id}` | send events; full per-attempt history |
| `GET /v1/deliveries?status=dead` · `POST /v1/deliveries/{id}/replay` | inspect and replay |
| `GET /v1/stats` · `GET /metrics` · `GET /health` | dashboard data, Prometheus, liveness |

### Verifying on the receiving side

```python
from courier.signing import verify, InvalidSignature

def handle(request):
    try:
        verify(ENDPOINT_SECRET, request.body, request.headers)   # signature + 5-minute freshness
    except InvalidSignature:
        return 401
    if already_processed(request.headers["webhook-id"]):         # at-least-once: dedupe
        return 200
    process(request.json())
    return 200
```

## Configuration

All settings are environment variables with production-sensible defaults
(see [`courier/config.py`](courier/config.py) and [`.env.example`](.env.example)):
retry schedule, max attempts, request timeout, lease length, circuit breaker
threshold and cooldown, worker concurrency, payload size limit, API key, and the
SSRF guard (`COURIER_ALLOW_PRIVATE_URLS`, off by default).

## Project layout

```
courier/
  api.py          HTTP API, idempotent ingest, replay, stats, /metrics, dashboard
  worker.py       claim → sign → send → record; leases, retries, circuit breaker
  schema.sql      tables and the partial indexes the claim query relies on
  signing.py      Standard Webhooks signatures (sign + verify)
  retry.py        exponential backoff with jitter, Retry-After parsing
  urlguard.py     SSRF protection
  config.py       every tunable, from environment variables
  static/         dashboard (single file, no build step)
chaos/
  receiver.py     four deliberately unreliable receivers that verify every signature
  run.py          the chaos test: send, break things, prove nothing was lost
tests/            48 tests against a real PostgreSQL
docs/             DESIGN.md (decisions and trade-offs), LEARNING_GUIDE.md
```

## Deploying

`render.yaml` deploys the API, a worker and PostgreSQL on Render as a
Blueprint (set `COURIER_API_KEY`; it's generated for you). Any platform that
runs a Docker image and a PostgreSQL database works the same way: one web
process (`python -m courier api`) and one or more worker processes
(`python -m courier worker`) sharing `DATABASE_URL`.
