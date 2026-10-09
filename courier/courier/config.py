"""All tunable behaviour in one place, read from environment variables.

Every number that changes how Courier behaves (retry timing, timeouts,
circuit breaker thresholds) is a named setting here, so it can be explained,
tested and changed without touching the code.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, replace


def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def _schedule(name: str, default: str) -> tuple[float, ...]:
    return tuple(float(x) for x in os.getenv(name, default).split(",") if x.strip())


@dataclass(frozen=True)
class Settings:
    database_url: str = field(default_factory=lambda: os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/courier"))

    # API ------------------------------------------------------------------
    # When set, every /v1 request needs "Authorization: Bearer <key>".
    api_key: str | None = field(default_factory=lambda: os.getenv("COURIER_API_KEY") or None)
    max_payload_bytes: int = field(default_factory=lambda: _int("COURIER_MAX_PAYLOAD_BYTES", 256 * 1024))
    # Block endpoint URLs that resolve to private/internal addresses (SSRF).
    # Only turn this on for local development and the Docker demo.
    allow_private_urls: bool = field(default_factory=lambda: _bool("COURIER_ALLOW_PRIVATE_URLS", False))

    # Worker ---------------------------------------------------------------
    # Requests in flight per worker process. Async I/O makes waiting cheap, so
    # this is set well above the sum of typical endpoint caps: otherwise slow
    # endpoints can fill every slot and isolation breaks down.
    worker_concurrency: int = field(default_factory=lambda: _int("COURIER_WORKER_CONCURRENCY", 200))
    batch_size: int = field(default_factory=lambda: _int("COURIER_BATCH_SIZE", 100))
    poll_interval: float = field(default_factory=lambda: _float("COURIER_POLL_INTERVAL", 1.0))
    # A claimed delivery is "leased" to one worker for this long. If the worker
    # dies, the lease expires and another worker picks the delivery up.
    # Must be comfortably longer than request_timeout.
    lease_seconds: int = field(default_factory=lambda: _int("COURIER_LEASE_SECONDS", 60))
    request_timeout: float = field(default_factory=lambda: _float("COURIER_REQUEST_TIMEOUT", 10.0))
    response_body_limit: int = 1024

    # Retries --------------------------------------------------------------
    # Seconds to wait after the 1st, 2nd, 3rd... failed attempt (before jitter).
    # Default: 5s, 30s, 2m, 10m, 30m, 1h, 3h, 6h, 12h -> roughly a day in total.
    retry_schedule: tuple[float, ...] = field(default_factory=lambda: _schedule(
        "COURIER_RETRY_SCHEDULE", "5,30,120,600,1800,3600,10800,21600,43200"))
    max_attempts: int = field(default_factory=lambda: _int("COURIER_MAX_ATTEMPTS", 10))
    retry_jitter: float = 0.2  # +/-20%
    max_event_age_seconds: int = field(default_factory=lambda: _int("COURIER_MAX_EVENT_AGE_SECONDS", 3 * 24 * 3600))

    # Circuit breaker ------------------------------------------------------
    circuit_failure_threshold: int = field(default_factory=lambda: _int("COURIER_CIRCUIT_THRESHOLD", 5))
    circuit_cooldown_seconds: float = field(default_factory=lambda: _float("COURIER_CIRCUIT_COOLDOWN", 60.0))

    default_max_in_flight: int = field(default_factory=lambda: _int("COURIER_DEFAULT_MAX_IN_FLIGHT", 10))

    def with_overrides(self, **kwargs) -> "Settings":
        return replace(self, **kwargs)


def load_settings() -> Settings:
    return Settings()
