-- Courier schema. Applied idempotently at startup.

-- Customers' receiving URLs ("the shops").
CREATE TABLE IF NOT EXISTS endpoints (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    url                   text NOT NULL,
    description           text,
    secret                text NOT NULL,              -- shared HMAC key, shown once at creation
    event_types           text[],                     -- NULL means "subscribe to everything"
    enabled               boolean NOT NULL DEFAULT true,
    disabled_reason       text,
    max_in_flight         int NOT NULL DEFAULT 10,    -- isolation: cap on concurrent requests to this endpoint
    consecutive_failures  int NOT NULL DEFAULT 0,     -- circuit breaker state
    circuit_open_until    timestamptz,
    created_at            timestamptz NOT NULL DEFAULT now()
);

-- What happened ("order #123 was paid"). Written once, never changed.
CREATE TABLE IF NOT EXISTS events (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type       text NOT NULL,
    payload          jsonb NOT NULL,
    idempotency_key  text UNIQUE,                     -- lets senders retry POST /events safely
    created_at       timestamptz NOT NULL DEFAULT now()
);

-- One row per (event, endpoint): the unit of work, and the queue itself.
CREATE TABLE IF NOT EXISTS deliveries (
    id                bigserial PRIMARY KEY,
    event_id          uuid NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    endpoint_id       uuid NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    status            text NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'delivering', 'succeeded', 'dead')),
    attempt_count     int NOT NULL DEFAULT 0,
    next_attempt_at   timestamptz NOT NULL DEFAULT now(),
    locked_until      timestamptz,                    -- lease: if the worker dies, this expires
    locked_by         text,
    last_error        text,
    last_status_code  int,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    delivered_at      timestamptz,
    UNIQUE (event_id, endpoint_id)
);
-- Partial indexes keep the worker's "what's due for this endpoint?" scan fast
-- however large the backlog or the pile of succeeded rows.
CREATE INDEX IF NOT EXISTS deliveries_due        ON deliveries (endpoint_id, next_attempt_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS deliveries_leased     ON deliveries (endpoint_id, locked_until) WHERE status = 'delivering';
CREATE INDEX IF NOT EXISTS deliveries_by_status  ON deliveries (status, endpoint_id);

-- Every HTTP attempt, for the dashboard's timeline and for debugging.
CREATE TABLE IF NOT EXISTS delivery_attempts (
    id             bigserial PRIMARY KEY,
    delivery_id    bigint NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt_no     int NOT NULL,
    started_at     timestamptz NOT NULL,
    duration_ms    int NOT NULL,
    status_code    int,
    error          text,
    response_body  text,
    worker         text
);
CREATE INDEX IF NOT EXISTS attempts_by_delivery ON delivery_attempts (delivery_id);
CREATE INDEX IF NOT EXISTS attempts_by_time     ON delivery_attempts (started_at);
