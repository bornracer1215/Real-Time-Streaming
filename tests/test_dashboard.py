"""Unit tests for dashboard/app.py's /api/latest endpoint, using fakeredis instead of a real
Redis server so these run with no external services at all.
"""
import json

import fakeredis
import pytest

import app as dashboard_app


@pytest.fixture
def client(monkeypatch):
    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(dashboard_app, "redis_client", fake)
    dashboard_app.app.config["TESTING"] = True
    with dashboard_app.app.test_client() as test_client:
        yield test_client, fake


def _seed(fake, device_id, sensor_type, **payload):
    fake.set(f"latest:{device_id}:{sensor_type}", json.dumps(payload))


def test_api_latest_returns_empty_when_no_data(client):
    test_client, _ = client
    resp = test_client.get("/api/latest")
    body = resp.get_json()

    assert resp.status_code == 200
    assert body["readings"] == []
    assert body["device_count"] == 0
    assert body["anomaly_count"] == 0


def test_api_latest_parses_and_aggregates_seeded_keys(client):
    test_client, fake = client
    _seed(fake, "sensor-001", "temperature", avg_value=22.5, min_value=21.0, max_value=24.0,
          reading_count=10, anomaly_count=0, window_start="2026-01-01T00:00:00", window_end="2026-01-01T00:05:00")
    _seed(fake, "sensor-002", "humidity", avg_value=55.0, min_value=50.0, max_value=60.0,
          reading_count=8, anomaly_count=2, window_start="2026-01-01T00:00:00", window_end="2026-01-01T00:05:00")

    body = test_client.get("/api/latest").get_json()

    assert body["device_count"] == 2
    assert body["anomaly_count"] == 2  # summed across both devices

    devices = {r["device_id"]: r for r in body["readings"]}
    assert devices["sensor-001"]["sensor_type"] == "temperature"
    assert devices["sensor-001"]["avg_value"] == 22.5
    assert devices["sensor-002"]["anomaly_count"] == 2


def test_api_latest_readings_sorted_by_sensor_type_then_device(client):
    test_client, fake = client
    _seed(fake, "sensor-b", "temperature", avg_value=1, min_value=1, max_value=1,
          reading_count=1, anomaly_count=0, window_start="x", window_end="x")
    _seed(fake, "sensor-a", "temperature", avg_value=1, min_value=1, max_value=1,
          reading_count=1, anomaly_count=0, window_start="x", window_end="x")
    _seed(fake, "sensor-a", "humidity", avg_value=1, min_value=1, max_value=1,
          reading_count=1, anomaly_count=0, window_start="x", window_end="x")

    body = test_client.get("/api/latest").get_json()
    ordering = [(r["sensor_type"], r["device_id"]) for r in body["readings"]]

    assert ordering == sorted(ordering)  # humidity/sensor-a, temperature/sensor-a, temperature/sensor-b
