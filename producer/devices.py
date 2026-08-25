"""Simulated fleet of IoT sensors: baseline ranges + random-walk state."""
import random

# Each device has one sensor type and a plausible operating range.
# (min, max, unit, walk_step) — walk_step bounds how much the value can drift per tick.
SENSOR_PROFILES = {
    "temperature": {"min": 15.0, "max": 30.0, "unit": "C", "walk_step": 0.4},
    "humidity": {"min": 30.0, "max": 70.0, "unit": "%", "walk_step": 1.5},
    "pressure": {"min": 990.0, "max": 1030.0, "unit": "hPa", "walk_step": 0.8},
}

DEVICE_IDS = [f"sensor-{i:03d}" for i in range(1, 11)]  # sensor-001 .. sensor-010

# Anomaly injection: small chance a reading spikes far outside the normal range.
ANOMALY_PROBABILITY = 0.02
ANOMALY_MULTIPLIER_RANGE = (2.5, 4.0)


class Device:
    """Tracks one simulated sensor's current value and emits the next reading."""

    def __init__(self, device_id: str, sensor_type: str):
        self.device_id = device_id
        self.sensor_type = sensor_type
        profile = SENSOR_PROFILES[sensor_type]
        self.unit = profile["unit"]
        self._min = profile["min"]
        self._max = profile["max"]
        self._walk_step = profile["walk_step"]
        self.value = random.uniform(self._min, self._max)

    def next_reading(self) -> float:
        # Random walk: drift a little from the last value, clamped to the normal range.
        delta = random.uniform(-self._walk_step, self._walk_step)
        self.value = max(self._min, min(self._max, self.value + delta))

        if random.random() < ANOMALY_PROBABILITY:
            direction = random.choice([-1, 1])
            multiplier = random.uniform(*ANOMALY_MULTIPLIER_RANGE)
            spike = (self._max - self._min) * multiplier * direction
            return round(self.value + spike, 2)

        return round(self.value, 2)


def build_fleet() -> list[Device]:
    sensor_types = list(SENSOR_PROFILES.keys())
    return [
        Device(device_id, sensor_types[i % len(sensor_types)])
        for i, device_id in enumerate(DEVICE_IDS)
    ]
