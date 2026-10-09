"""Webhook signatures, following the Standard Webhooks spec (standardwebhooks.com).

Every request carries three headers:
    webhook-id         the event id, identical on every retry (receivers dedupe on it)
    webhook-timestamp  unix seconds when this attempt was sent
    webhook-signature  "v1,<base64 HMAC-SHA256 of '{id}.{timestamp}.{body}'>"

Signing the timestamp stops replay attacks: an attacker who captures a
request can't resend it later, because receivers reject old timestamps.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import time

SECRET_PREFIX = "whsec_"
DEFAULT_TOLERANCE_SECONDS = 300


class InvalidSignature(Exception):
    pass


def generate_secret() -> str:
    return SECRET_PREFIX + base64.b64encode(secrets.token_bytes(24)).decode()


def _key(secret: str) -> bytes:
    return base64.b64decode(secret.removeprefix(SECRET_PREFIX))


def sign(secret: str, msg_id: str, timestamp: int, body: bytes) -> str:
    to_sign = f"{msg_id}.{timestamp}.".encode() + body
    digest = hmac.new(_key(secret), to_sign, hashlib.sha256).digest()
    return "v1," + base64.b64encode(digest).decode()


def headers(secret: str, msg_id: str, body: bytes, timestamp: int | None = None) -> dict[str, str]:
    ts = int(time.time()) if timestamp is None else timestamp
    return {
        "webhook-id": msg_id,
        "webhook-timestamp": str(ts),
        "webhook-signature": sign(secret, msg_id, ts, body),
    }


def verify(secret: str, body: bytes, hdrs: dict[str, str],
           tolerance: int = DEFAULT_TOLERANCE_SECONDS, now: int | None = None) -> None:
    """What a receiver runs. Raises InvalidSignature unless the request is genuine and fresh."""
    h = {k.lower(): v for k, v in hdrs.items()}
    try:
        msg_id, ts, sig_header = h["webhook-id"], int(h["webhook-timestamp"]), h["webhook-signature"]
    except (KeyError, ValueError) as exc:
        raise InvalidSignature("missing or malformed webhook headers") from exc
    now = int(time.time()) if now is None else now
    if abs(now - ts) > tolerance:
        raise InvalidSignature("timestamp outside tolerance (possible replay)")
    expected = sign(secret, msg_id, ts, body)
    # The header may hold several space-separated signatures (during secret rotation).
    if not any(hmac.compare_digest(expected, s) for s in sig_header.split()):
        raise InvalidSignature("signature mismatch")
