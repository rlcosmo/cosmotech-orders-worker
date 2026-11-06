import json
import urllib.error
import urllib.request
from unittest.mock import patch

from orders.runtime.health import WorkerHealth, start_health_server


def test_readiness_tracks_queue_and_drain_state():
    health = WorkerHealth()
    assert health.status("/health/live") == 200
    assert health.status("/health/ready") == 503
    health.record_poll(True)
    assert health.status("/health/ready") == 200
    health.record_poll(False)
    assert health.status("/health/ready") == 503
    health.record_poll(True)
    health.draining = True
    assert health.status("/health/ready") == 503
    assert health.status("/orders") == 404


def test_stalled_poll_fails_liveness():
    health = WorkerHealth()
    health.record_poll(True)
    with patch("orders.runtime.health.time.monotonic", return_value=health.last_poll + 91):
        assert health.status("/health/live") == 503
        assert health.status("/health/ready") == 503


def test_http_health_does_not_accept_business_requests():
    health = WorkerHealth()
    health.record_poll(True)
    server = start_health_server(health, port=0)
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with urllib.request.urlopen(base + "/health/ready", timeout=2) as response:
            assert response.status == 200
        with urllib.request.urlopen(base + "/version", timeout=2) as response:
            body = json.load(response)
            assert body["service"] == "orders-worker"
            assert body["channel"] == "stable"
            assert body["version"] == "1.0.4"
        try:
            urllib.request.urlopen(base + "/orders", data=b"{}", timeout=2)
        except urllib.error.HTTPError as error:
            assert error.code == 501
        else:
            raise AssertionError("Business writes must not be accepted")
    finally:
        server.shutdown()
        server.server_close()
