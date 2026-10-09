import random
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest

from courier import signing
from courier.retry import backoff_seconds, parse_retry_after
from courier.urlguard import UnsafeURL, check_url


# --- signatures -------------------------------------------------------------
def test_signature_round_trip():
    secret = signing.generate_secret()
    body = b'{"id":"evt_1","data":{"amount":500}}'
    h = signing.headers(secret, "evt_1", body)
    signing.verify(secret, body, h)  # no exception


@pytest.mark.parametrize("tamper", ["body", "secret", "id"])
def test_tampering_is_detected(tamper):
    secret, body = signing.generate_secret(), b'{"amount":500}'
    h = signing.headers(secret, "evt_1", body)
    if tamper == "body":
        body = b'{"amount":5000}'
    if tamper == "secret":
        secret = signing.generate_secret()
    if tamper == "id":
        h["webhook-id"] = "evt_2"
    with pytest.raises(signing.InvalidSignature):
        signing.verify(secret, body, h)


def test_replayed_old_request_is_rejected():
    secret, body = signing.generate_secret(), b"{}"
    h = signing.headers(secret, "evt_1", body, timestamp=1_000_000)
    with pytest.raises(signing.InvalidSignature, match="replay"):
        signing.verify(secret, body, h, now=1_000_000 + 301)


def test_multiple_signatures_accepted_for_secret_rotation():
    old, new, body = signing.generate_secret(), signing.generate_secret(), b"{}"
    h = signing.headers(new, "evt_1", body)
    h["webhook-signature"] = signing.sign(old, "evt_1", int(h["webhook-timestamp"]), body) + " " + h["webhook-signature"]
    signing.verify(new, body, h)


# --- backoff ----------------------------------------------------------------
def test_backoff_grows_and_caps_at_last_step():
    sched = (5, 30, 120)
    no_jitter = [backoff_seconds(n, sched, jitter=0) for n in (1, 2, 3, 4, 9)]
    assert no_jitter == [5, 30, 120, 120, 120]


def test_jitter_stays_in_bounds_and_actually_varies():
    rng = random.Random(1)
    vals = [backoff_seconds(1, (100,), 0.2, rng) for _ in range(500)]
    assert all(80 <= v <= 120 for v in vals)
    assert max(vals) - min(vals) > 20  # retries are spread out, not synchronised


def test_retry_after_seconds_and_date():
    assert parse_retry_after("120") == 120
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert parse_retry_after(format_datetime(now + timedelta(seconds=90), usegmt=True), now) == pytest.approx(90)
    assert parse_retry_after("garbage") is None
    assert parse_retry_after("999999999") == 24 * 3600  # capped


# --- SSRF guard ---------------------------------------------------------------
@pytest.mark.parametrize("url", [
    "http://127.0.0.1/hook", "http://localhost:5432/", "http://10.0.0.5/", "http://192.168.1.1/",
    "http://169.254.169.254/latest/meta-data/", "http://[::1]/", "http://0.0.0.0/",
    "file:///etc/passwd", "ftp://example.com/", "http://user:pass@8.8.8.8/",
])
def test_internal_and_bad_urls_are_blocked(url):
    with pytest.raises(UnsafeURL):
        check_url(url, allow_private=False)


def test_public_ip_allowed():
    check_url("https://8.8.8.8/hook", allow_private=False)


def test_dev_mode_allows_localhost():
    check_url("http://localhost:9000/hook", allow_private=True)
