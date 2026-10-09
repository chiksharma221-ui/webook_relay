import pytest
from fastapi.testclient import TestClient

from courier.api import create_app
from tests.conftest import make_settings


@pytest.fixture
def client(pool):  # pool fixture truncates tables first
    with TestClient(create_app(make_settings())) as c:
        yield c


def test_endpoint_returns_secret_once(client):
    r = client.post("/v1/endpoints", json={"url": "http://shop.test/hook"})
    assert r.status_code == 201 and r.json()["secret"].startswith("whsec_")
    assert "secret" not in client.get("/v1/endpoints").json()[0]


def test_ssrf_url_rejected():
    with TestClient(create_app(make_settings(allow_private_urls=False))) as c:
        r = c.post("/v1/endpoints", json={"url": "http://169.254.169.254/latest/meta-data"})
        assert r.status_code == 422


def test_event_fans_out_only_to_subscribers(client):
    client.post("/v1/endpoints", json={"url": "http://all.test/hook"})
    client.post("/v1/endpoints", json={"url": "http://pay.test/hook", "event_types": ["payment.succeeded"]})
    assert client.post("/v1/events", json={"event_type": "payment.succeeded", "payload": {}}).json()["deliveries"] == 2
    assert client.post("/v1/events", json={"event_type": "user.created", "payload": {}}).json()["deliveries"] == 1


def test_idempotency_key_prevents_duplicate_events(client):
    client.post("/v1/endpoints", json={"url": "http://shop.test/hook"})
    h = {"idempotency-key": "order-123-paid"}
    first = client.post("/v1/events", json={"event_type": "order.paid", "payload": {"a": 1}}, headers=h)
    second = client.post("/v1/events", json={"event_type": "order.paid", "payload": {"a": 1}}, headers=h)
    assert first.status_code == 202 and second.status_code == 200
    assert first.json()["id"] == second.json()["id"] and second.json()["duplicate"] is True
    assert client.get("/v1/stats").json()["deliveries"]["pending"] == 1


def test_validation_and_size_limits(client):
    assert client.post("/v1/events", json={"event_type": "bad type!", "payload": {}}).status_code == 422
    assert client.post("/v1/events", json={"event_type": "x", "payload": [1, 2]}).status_code == 422
    big = {"event_type": "x", "payload": {"blob": "a" * 300_000}}
    assert client.post("/v1/events", json=big).status_code == 413


def test_api_key_required_when_configured(pool):
    with TestClient(create_app(make_settings(api_key="s3cret"))) as c:
        assert c.get("/v1/endpoints").status_code == 401
        assert c.get("/v1/endpoints", headers={"authorization": "Bearer s3cret"}).status_code == 200
        assert c.get("/health").status_code == 200  # health stays public


def test_replay_only_allowed_for_finished_deliveries(client):
    client.post("/v1/endpoints", json={"url": "http://shop.test/hook"})
    client.post("/v1/events", json={"event_type": "x", "payload": {}})
    d = client.get("/v1/deliveries").json()[0]
    assert client.post(f"/v1/deliveries/{d['id']}/replay").status_code == 409  # still pending


def test_event_detail_and_stats_shape(client):
    client.post("/v1/endpoints", json={"url": "http://shop.test/hook"})
    ev = client.post("/v1/events", json={"event_type": "x", "payload": {"k": "v"}}).json()
    detail = client.get(f"/v1/events/{ev['id']}").json()
    assert detail["payload"] == {"k": "v"} and len(detail["deliveries"]) == 1
    s = client.get("/v1/stats").json()
    assert set(s) >= {"deliveries", "last_5_min", "per_minute", "oldest_due_seconds"}


def test_prometheus_metrics(client):
    client.post("/v1/endpoints", json={"url": "http://shop.test/hook"})
    client.post("/v1/events", json={"event_type": "x", "payload": {}})
    text = client.get("/metrics").text
    assert 'courier_deliveries{status="pending"} 1' in text
    assert "# TYPE courier_delivery_attempts_total counter" in text
    assert "courier_oldest_due_seconds" in text
