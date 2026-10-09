"""When to try again after a failed attempt."""
from __future__ import annotations

import random
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

MAX_RETRY_AFTER_SECONDS = 24 * 3600


def backoff_seconds(attempt_count: int, schedule: tuple[float, ...], jitter: float = 0.2,
                    rng: random.Random | None = None) -> float:
    """Delay after the `attempt_count`-th failure (1-based).

    Delays grow along the schedule so a struggling receiver gets breathing room,
    and jitter spreads retries out so thousands of them don't land on a
    recovering server in the same second (the "thundering herd").
    """
    if attempt_count < 1:
        raise ValueError("attempt_count is 1-based")
    base = schedule[min(attempt_count, len(schedule)) - 1]
    r = (rng or random).uniform(1 - jitter, 1 + jitter)
    return max(0.0, base * r)


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    """Seconds from a Retry-After header (either '120' or an HTTP date)."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return min(float(value), MAX_RETRY_AFTER_SECONDS)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    now = now or datetime.now(timezone.utc)
    return min(max(0.0, (when - now).total_seconds()), MAX_RETRY_AFTER_SECONDS)
