# Learning guide

How to go from "I can run it" to "I can explain and change any part of it".
Plan on 5–7 sessions of an hour or two. Don't skip the exercises: changing
the code yourself is what makes it yours in an interview.

## The courier office, mapped to code

| Courier office | What it does | Where it lives |
|---|---|---|
| Front desk | Accepts a message, gives a receipt | `api.py` → `create_event` |
| Register book | Every message written down before anything else | `schema.sql` → `events`, `deliveries` |
| Sorting bin | Messages waiting to go out | the `deliveries` table itself (`status = 'pending'`) |
| Delivery staff | Pick up messages, deliver, report back | `worker.py` → `claim`, `send`, `record` |
| Wax seal | Proves a message is genuine | `signing.py` |
| Coming back later | Retry with growing gaps | `retry.py`, `Worker._next_state` |
| "Shop closed, stop knocking" | Circuit breaker | `CLAIM_SQL` (capacity CTE) + `record` |
| Returns shelf | Dead-lettered deliveries + replay | `status = 'dead'`, `/replay` routes |
| Tracking page | Every attempt, visible | `delivery_attempts`, `static/dashboard.html` |
| Bell on the counter | New message, wake the staff | `NOTIFY` in `api.py`, `_listen` in `worker.py` |

## Session 1: run it and break it

1. `docker compose up --build`, open http://localhost:8000.
2. Register the four chaos receivers as endpoints (`http://receiver:9000/hook/healthy`,
   `/flaky`, `/slow`, `/outage`) and send a few events from the dashboard.
3. Click an event and read its attempt timeline.
4. `docker compose stop receiver`, send events, watch them wait and the
   circuits open. `docker compose start receiver`, watch them drain.
5. Run the chaos test: `docker compose --profile chaos run --rm chaos`.
6. During a chaos run, `docker compose kill worker` and restart it. Did
   anything get lost? Were there duplicates? Why?

**Check yourself:** explain, without notes, why the receiver can see the same
event twice.

## Session 2: the data model (`schema.sql`)

Read every table and column. Then answer:
- Why is there one `deliveries` row per (event, endpoint), and not a status on `events`?
- What's the `UNIQUE (event_id, endpoint_id)` constraint protecting against?
- Why are the indexes *partial* (`WHERE status = 'pending'`)?
- What does `locked_until` do that `status = 'delivering'` alone can't?

Then read `create_event` in `api.py`. Find the single transaction and the
`ON CONFLICT DO NOTHING`. Explain the idempotency key in one sentence.

## Session 3: the worker loop (`worker.py`)

Read in this order: `Worker.run` → `claim` → `process` → `send` → `record` → `_next_state`.
- Trace one delivery that fails twice then succeeds. Which columns change at each step?
- What wakes the loop up? There are four things; list them.
- Find the fencing condition (`locked_by = %s`). What bug would happen without it?

## Session 4: the claim query

This is the hardest part and the best interview material. Read the comment
above `CLAIM_SQL`, then the query, one CTE at a time. In `psql`, run each CTE
on its own against a database with some test data.
- What does a half-open endpoint get in `capacity`, and why 1?
- Why is `LIMIT` in `locked` and not `candidates`? (See DESIGN.md.)
- Why is the status condition written twice?
- Explain `ORDER BY c.rn` to someone non-technical.

## Session 5: signatures, retries, SSRF

- In a Python shell, sign a body with `signing.headers`, change one byte, and
  watch `verify` fail. Change the timestamp to 10 minutes ago and watch it fail.
- Call `backoff_seconds` 20 times for attempt 1. Why are the results different?
- Try `urlguard.check_url("http://169.254.169.254/")`. What is that address, and
  why would an attacker want Courier to call it?

## Session 6: tests

Run `python -m pytest -q`. Then read `tests/test_worker.py`; each test is one
failure scenario. Break the code on purpose and confirm a test catches it.
Each of these was checked to fail the named test:

| Change in `worker.py` | Test that catches it |
|---|---|
| `FOR UPDATE OF d SKIP LOCKED` → `FOR UPDATE OF d` | `test_claim_skips_rows_another_worker_is_locking` |
| In `record`, drop the `locked_by = %s` condition | `test_stale_worker_cannot_overwrite_new_lease_owner` |
| `ORDER BY c.rn, c.due_at` → `ORDER BY c.due_at` | `test_fair_scheduling_backlog_does_not_starve_others` |
| `ELSE e.max_in_flight END` → `ELSE 1000 END` | `test_per_endpoint_in_flight_cap` |
| Replace the backoff with `delay = 0.0` | `test_failure_schedules_retry_with_backoff` |
| Remove the expired-lease condition (`d.locked_until < now()`) | `test_crashed_worker_lease_expires_and_another_worker_finishes` |

Notice the first row. Removing `SKIP LOCKED` does **not** cause duplicate
claims. The status condition repeated in the `locked` step already prevents
those. What breaks is that workers *wait* on each other's locks instead of
moving on. So `SKIP LOCKED` is about throughput, and the re-check is about
correctness. Being able to say that precisely is a strong interview answer.

## Exercises (do at least two before you put this on your resume)

1. **Add a "rotate secret" endpoint.** `POST /v1/endpoints/{id}/rotate-secret`
   returns a new secret. Hard part: during rotation, sign with both old and new
   secrets so receivers don't break (the header already supports several signatures).
2. **Per-endpoint ordering mode.** Add `ordered boolean` to `endpoints`. For
   ordered endpoints, only claim a delivery if no older delivery for that
   endpoint is undelivered. Write a test. What does this do to throughput when
   one event keeps failing?
3. **Alerting.** Add a `GET /v1/alerts` that returns problems: queue lag over
   60 s, an endpoint dead for more than 10 minutes, the dead-letter count rising.
4. **Retention.** Write a job that deletes `delivery_attempts` older than 30
   days in batches of 10,000. Why batches?
5. **Rate limit in requests per second** per endpoint, in addition to the
   concurrency cap.

## The 60-second pitch

"Courier is a webhook delivery service: apps hand it events like 'payment
succeeded', and it guarantees they reach every subscriber, even when those
servers are down or flaky. Events are committed to PostgreSQL before the API
acknowledges them, and PostgreSQL is also the queue: workers claim deliveries
with `SKIP LOCKED`, under a lease, so I can run any number of them and a crashed
worker's work is picked up automatically. Failures retry with exponential
backoff and jitter, a circuit breaker stops hammering broken endpoints, and
fair scheduling keeps one slow customer from delaying the rest. Payloads are
HMAC-signed. I proved it with a chaos test: 12,000 deliveries to receivers that
fail 30% of the time, go fully offline, or take seconds to respond, including a
worker killed mid-run, with zero lost, and the healthy endpoint stayed at 26 ms
p95. The most interesting bug I found was that isolation didn't actually work
at first; under steady load the healthy endpoint's latency was 155 seconds,
and fixing it meant rewriting how workers claim work."
