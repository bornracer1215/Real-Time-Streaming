"""Unit tests for producer/devices.py — pure Python, no Kafka/network required."""
import random

from devices import ANOMALY_MULTIPLIER_RANGE, Device, SENSOR_PROFILES, build_fleet


def test_build_fleet_has_ten_devices_covering_all_sensor_types():
    fleet = build_fleet()
    assert len(fleet) == 10
    assert {d.sensor_type for d in fleet} == set(SENSOR_PROFILES.keys())
    assert len({d.device_id for d in fleet}) == 10  # all IDs unique


def test_reading_is_a_float():
    device = Device("sensor-test", "temperature")
    assert isinstance(device.next_reading(), float)


def test_non_anomalous_reading_always_stays_within_profile_bounds(monkeypatch):
    # Force random.random() to always return above ANOMALY_PROBABILITY, so the anomaly branch
    # never triggers — every reading should then be clamped inside [min, max] by the walk logic.
    monkeypatch.setattr(random, "random", lambda: 1.0)

    device = Device("sensor-test", "humidity")
    profile = SENSOR_PROFILES["humidity"]
    for _ in range(200):
        reading = device.next_reading()
        assert profile["min"] <= reading <= profile["max"]


def test_anomalous_reading_lands_far_outside_profile_bounds(monkeypatch):
    # Force the anomaly branch to always trigger, with a deterministic direction and multiplier,
    # so the resulting spike's magnitude is fully predictable.
    monkeypatch.setattr(random, "random", lambda: 0.0)  # always < ANOMALY_PROBABILITY
    monkeypatch.setattr(random, "choice", lambda seq: 1)  # spike direction: positive
    monkeypatch.setattr(random, "uniform", lambda a, b: b)  # take the upper bound every call

    device = Device("sensor-test", "pressure")
    profile = SENSOR_PROFILES["pressure"]
    reading = device.next_reading()

    span = profile["max"] - profile["min"]
    expected_min_spike = span * ANOMALY_MULTIPLIER_RANGE[0]
    assert reading > profile["max"] + expected_min_spike * 0.9  # comfortably outside, allowing rounding


def test_walk_drifts_rather_than_jumps(monkeypatch):
    # With the anomaly branch disabled, consecutive readings should differ by at most the
    # profile's walk_step — proving the "random walk" behavior (small drift, not independent
    # random noise) rather than just asserting on a single call.
    monkeypatch.setattr(random, "random", lambda: 1.0)

    device = Device("sensor-test", "temperature")
    walk_step = SENSOR_PROFILES["temperature"]["walk_step"]
    previous = device.next_reading()
    for _ in range(50):
        current = device.next_reading()
        assert abs(current - previous) <= walk_step + 1e-9  # +epsilon for float rounding
        previous = current
