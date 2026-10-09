# Design decisions

Each section answers a question an interviewer is likely to ask, with what
was chosen, why, and what it costs.

---

## Why PostgreSQL as the queue, and not Redis, RabbitMQ or Kafka?

**The problem it avoids: the dual write.** Accepting an event means two things
must happen: save it, and queue it for delivery. With a separate queue those
are two systems, and there's no transaction across them. If the API saves to
the database and then crashes before pushing to Redis, the event is stored but
never delivered. Push first and crash before saving, and a worker delivers an
event the database has no record of. Fixing that needs an "outbox" table and a
relay process, which is a queue in the database anyway.

With PostgreSQL as the queue, `POST /v1/events` writes the event and its
deliveries in **one transaction**. It either all happens or none of it does.

**How workers share the queue:** `SELECT ... FOR UPDATE SKIP LOCKED`. Each
worker locks the rows it's claiming; other workers skip locked rows instead of
waiting for them. Together with re-checking each row's status at the moment it
is locked (see the claim query section), no two workers get the same row, no
coordinator is needed, and adding workers needs no configuration.

Precisely: the re-check is what guarantees **correctness** (no double claims);
`SKIP LOCKED` is what gives **throughput** (no worker waits on another's lock).
The tests prove both separately: removing `SKIP LOCKED` doesn't create
duplicates, but it does make `test_claim_skips_rows_another_worker_is_locking` fail.

**What it costs:**
- PostgreSQL does all the coordination, so very high volume would eventually
  make it the bottleneck. A rough ceiling on one database is thousands of
  deliveries per second, far beyond what a single team's webhooks need.
- Polling for retries that become due. Mitigated by `LISTEN/NOTIFY`, so new
  events wake workers instantly, and by a 1-second poll for due retries.

**When I'd switch:** Kafka makes sense at millions of events per second, or
when many independent consumers need to replay the same stream. Neither
applies to webhook delivery, where each event goes to a known set of
endpoints and is then done. Adding Kafka here would add an extra system to
run, monitor and keep consistent, without solving a problem we have.

---

## Why at-least-once, and not exactly-once?

Exactly-once delivery over a network is impossible, because of one unavoidable
gap. Courier sends a request; the receiver processes it and replies 200; the
reply is lost (network drop, timeout, or the worker crashes before saving the
result). From Courier's side, "the receiver processed it and the reply was
lost" and "the request never arrived" look identical. There are only two choices:

- **Don't retry** → some events are lost (at-most-once).
- **Retry** → some events arrive twice (at-least-once).

For payments, losing an event is far worse than a duplicate, so Courier
retries. It then makes duplicates easy to handle: every attempt of an event
carries the same `webhook-id` and the same body, so the receiver keeps a list
of ids it has processed and ignores repeats. **Exactly-once processing** is
achieved by the pair: at-least-once delivery plus an idempotent receiver.

This shows up in the chaos results: when a worker was killed with `kill -9`
mid-run, 20 of its requests had reached the receiver but were never recorded.
They were sent again after the lease expired, and the receiver saw 20
duplicates, which it ignored. Nothing was lost.

**The sender side has the same problem.** If a sender's `POST /v1/events`
times out, it doesn't know whether Courier stored the event. So the API
accepts an `Idempotency-Key`: sending the same key again returns the original
event (HTTP 200, `"duplicate": true`) instead of creating a second one. This is
a `UNIQUE` constraint plus `INSERT ... ON CONFLICT DO NOTHING`, so it's
correct even when two identical requests arrive at the same moment.

---

## What happens if a worker crashes halfway through a delivery?

Claiming a delivery sets `status = 'delivering'` and a **lease**
(`locked_until = now() + 60s`, `locked_by = worker id`). Rows aren't kept
locked during the HTTP request; that would hold database connections open for
up to 10 seconds per request.

- **Worker dies:** the lease expires; the claim query treats an expired lease
  as due, and another worker picks the delivery up. Tested in
  `test_crashed_worker_lease_expires_and_another_worker_finishes`, and with a
  real `kill -9` in the chaos test.
- **Worker was only slow (a long GC pause, say) and comes back:** its result
  write includes `WHERE locked_by = <its own id>`. Since another worker now
  owns the row, the update matches nothing and the stale result can't
  overwrite the newer one. This is called **fencing**.
- **The lease must be longer than the request timeout** (60 s vs 10 s), or a
  slow but healthy request could be claimed twice.

---

## How do retries work, and why jitter?

After the n-th failure the delay follows a schedule: 5 s, 30 s, 2 min, 10 min,
30 min, 1 h, 3 h, 6 h, 12 h, which is about 22 hours in total over 10
attempts. Then the delivery is marked **dead**.

- **Growing delays** give a struggling receiver room to recover instead of
  hammering it.
- **±20% jitter**: if a receiver was down and 10,000 deliveries failed in the
  same minute, without jitter all 10,000 retries would land in the same second
  and knock it over again (the "thundering herd"). Jitter spreads them out.
- **`Retry-After`** on 429 and 503 responses is respected (capped at 24 h), so
  a receiver can ask for a pause.
- **What's retried:** timeouts, connection errors and every non-2xx response.
  **410 Gone** is the exception: it means the URL no longer exists, so the
  delivery goes straight to dead and the endpoint is disabled.
- Events older than 3 days are dead-lettered regardless of attempt count.

---

## What if the receiver is down for 6 hours?

1. The first few deliveries fail and are scheduled for retry.
2. After 5 consecutive failures the **circuit breaker opens**: for the next
   60 s no deliveries to that endpoint are even attempted. Workers spend their
   time on endpoints that work.
3. After the cooldown the circuit is **half-open**: the claim query allows
   exactly **one** in-flight request to that endpoint, a probe. If it fails,
   the circuit opens again. If it succeeds, the failure count resets and full
   traffic resumes.
4. Meanwhile new events keep arriving and are stored as pending.

Because only probes are attempted while the endpoint is down, most queued
deliveries don't use up their attempts during the outage. When the receiver
comes back, the backlog drains. The chaos tests' `outage` receiver shows this:
every delivery arrived after the outage ended, with almost no extra requests
(3,013 requests for 3,000 deliveries).

If the receiver is gone for days, deliveries reach max attempts or max age and
become dead. They stay in the database and can be replayed from the dashboard,
for one delivery or all of an endpoint's dead deliveries, once it's fixed.

---

## How does one bad endpoint not slow everyone else down?

This took two iterations, and the first version had a real flaw that the
steady-load chaos test exposed.

**Version 1:** a per-endpoint cap on concurrent requests (`max_in_flight`), and
claims ordered by oldest-due first. Under steady load, the healthy endpoint's
p50 latency was **155 seconds**. The slow and flaky endpoints' caps added up
to all of the workers' slots, so every delivery, healthy ones included, waited
in one shared first-in-first-out line.

**Version 2 (current):**
- **Fair scheduling.** The claim query ranks each endpoint's due deliveries
  separately, then takes every endpoint's 1st, then every endpoint's 2nd, and
  so on. A backlog on one endpoint no longer pushes others back.
- **Worker concurrency well above the sum of endpoint caps** (200 per worker).
  Waiting on network I/O in asyncio costs almost nothing, so this is cheap.
- **The circuit breaker** removes failing endpoints from the schedule entirely.

Result: healthy endpoint p50 **6 ms**, p95 **26 ms** under the same load, while
the slow endpoint built its own backlog without affecting anyone.

**Known limitation:** the per-endpoint cap is enforced when claiming, by
counting current leases. Two workers claiming at the same instant can each see
the same free capacity, so the cap is soft and can briefly be exceeded. A
strict cap needs a per-endpoint lock or a token table; for protecting
receivers, an occasional small overshoot is acceptable.

---

## Why does the claim query look so complicated?

```
capacity    free slots per endpoint (cap minus active leases; 1 if half-open; 0 if open)
candidates  per endpoint, its oldest due deliveries, at most `free` of them
locked      fair order, LIMIT, FOR UPDATE SKIP LOCKED
UPDATE      lease the rows to this worker, return everything needed to send
```

Three details, each found by testing:

1. **Per-endpoint index scans.** The first version sorted every due delivery
   to pick a few, so the claim got slower as the backlog grew; with a large
   backlog, one claim query was still running long after its worker had been stopped. Now each endpoint's candidates come from an
   index range scan (`LATERAL ... LIMIT free` on
   `(endpoint_id, next_attempt_at) WHERE status = 'pending'`), so cost depends
   on the number of endpoints, not the backlog. A test claims from a
   20,000-delivery backlog in well under 0.5 s.
2. **`LIMIT` at the locking step, not before.** Originally the candidates were
   limited first. Workers claiming at the same instant then all picked the same
   top N rows; `SKIP LOCKED` made the losers skip them, and they came back
   empty-handed. With `LIMIT` alongside `FOR UPDATE SKIP LOCKED`, a worker skips
   locked rows and keeps going.
3. **The status condition is repeated in the locking step.** Under READ
   COMMITTED, if another worker's claim commits between this query's snapshot
   and its lock, PostgreSQL re-checks the locking step's `WHERE` clause against
   the row's new version. Repeating the "is it still due?" condition there means
   an already-claimed row is dropped instead of claimed a second time.

`test_concurrent_workers_never_claim_the_same_delivery` runs 8 workers claiming
600 deliveries concurrently and checks every delivery is claimed exactly once.

---

## Security

- **HMAC-SHA256 signatures** (Standard Webhooks format) over
  `id.timestamp.body`. Receivers compare with a constant-time comparison.
  The timestamp is signed and must be within 5 minutes, so a captured request
  can't be replayed later. The header can carry several signatures, which
  supports rotating secrets without downtime.
- **SSRF protection.** Customers choose URLs Courier will call, so an attacker
  could register `http://169.254.169.254/` (cloud credentials) or
  `http://localhost:5432`. URLs resolving to private, loopback, link-local,
  multicast or reserved addresses are rejected at registration **and checked
  again before every delivery**, since DNS answers can change. Redirects aren't
  followed (a redirect could point inside the network). Credentials in URLs are
  rejected. *Remaining gap:* a DNS answer can change between Courier's check and
  the HTTP client's own lookup (DNS rebinding). Closing it fully means
  connecting to the already-checked IP address, which is a custom transport
  and a known next step.
- **Secrets** are shown once at creation. They're stored in plain text in the
  database because Courier needs them to sign; production would encrypt them
  at rest with a key from a secrets manager.
- **API authentication**: optional bearer key for every `/v1` route. Real
  multi-tenant use would need per-application keys and scoping.
- **Limits**: 256 KB payloads, validated event type format, URL length cap.

---

## Observability

- **Every attempt is recorded**: attempt number, time, duration, status code,
  error, a truncated response body, and which worker made it. The dashboard's
  per-event timeline comes straight from this.
- **`/metrics`** in Prometheus format: deliveries by status, attempts by
  outcome, latency quantiles, **queue lag** (age of the oldest due delivery,
  the best single signal that workers are falling behind), and open circuits.
  These are computed from the database, so they're correct with any number
  of API and worker processes.
- **What I'd alert on:** queue lag above 60 s, success rate below 90% across all
  endpoints, the dead-letter count rising.

---

## Things deliberately left out, and what I'd do next

| Not built | Why not yet | Next step |
|---|---|---|
| Ordering per endpoint | Strict order means one stuck event blocks every later one for that endpoint; most receivers don't need it | Optional per-endpoint FIFO mode: claim only the oldest undelivered delivery |
| Multi-tenancy | Single API key keeps the scope focused | `applications` table; keys and endpoints scoped per application |
| Data retention | Tables grow forever | Partition `delivery_attempts` by day; drop old partitions |
| DNS rebinding protection | See Security | Resolve once, connect to that IP |
| Encrypted secrets at rest | See Security | Envelope encryption with a KMS |
| Rate limits in requests/second | Concurrency caps protect most receivers | Token bucket per endpoint in the claim query |

---

## Questions to be ready for

- *"How do you guarantee an event isn't lost?"* It's committed to PostgreSQL,
  together with its deliveries, before the API returns 202. From then on it's
  retried until delivered or dead-lettered, and dead ones are kept for replay.
- *"Two workers, same event?"* `FOR UPDATE SKIP LOCKED` plus the repeated
  status check; tested with 8 concurrent workers.
- *"Worker crashes mid-delivery?"* Lease expires, another worker retries;
  fencing stops a late worker overwriting the result; the receiver may see a
  duplicate and dedupes on `webhook-id`.
- *"Why is exactly-once impossible?"* A lost reply is indistinguishable from a
  lost request; see above.
- *"Receiver down for 6 hours?"* Circuit breaker, probes, backlog drains on
  recovery, dead-letter and replay for longer outages.
- *"How would you scale to 100x?"* Add workers first (no config needed). Then
  partition `deliveries` by status/time, archive succeeded rows, and use a read
  replica for the dashboard. Beyond a single database: shard by endpoint id, or
  move the hot queue to a dedicated broker while keeping PostgreSQL as the
  source of truth through an outbox.
- *"What's the bottleneck today?"* Most likely the database, since every claim
  and every result is a write. I haven't profiled it in isolation: all the
  numbers come from one machine running the database, API, workers, receivers
  and the load generator together, so they understate what separate machines
  would do.
