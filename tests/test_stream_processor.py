"""Unit tests for spark_streaming/stream_processor.py's transformation logic.

These build a local SparkSession and feed it a plain (batch) DataFrame — no Kafka connection, no
streaming context needed. withWatermark is a no-op outside of a real streaming query, so the same
functions under test here are exactly what the live pipeline runs against the Kafka stream.
"""
from datetime import datetime

import pytest
from pyspark.sql import Row, SparkSession

from stream_processor import ANOMALY_BOUNDS, add_anomaly_flag, build_windowed_aggregates


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("stream-processor-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_add_anomaly_flag_marks_out_of_range_readings(spark):
    lo, hi = ANOMALY_BOUNDS["temperature"]
    df = spark.createDataFrame([
        Row(device_id="d1", sensor_type="temperature", value=(lo + hi) / 2),  # well within range
        Row(device_id="d1", sensor_type="temperature", value=lo - 5.0),       # below range
        Row(device_id="d1", sensor_type="temperature", value=hi + 5.0),       # above range
    ])

    flagged = add_anomaly_flag(df).orderBy("value").collect()

    assert [row.is_anomaly for row in flagged] == [1, 0, 1]  # sorted by value: low-spike, normal, high-spike


def test_add_anomaly_flag_is_per_sensor_type(spark):
    # A value of 50 is a normal humidity reading but a wildly anomalous temperature reading —
    # confirms the bounds check is keyed off sensor_type, not applied globally.
    df = spark.createDataFrame([
        Row(device_id="d1", sensor_type="humidity", value=50.0),
        Row(device_id="d2", sensor_type="temperature", value=50.0),
    ])

    flagged = {row.sensor_type: row.is_anomaly for row in add_anomaly_flag(df).collect()}

    assert flagged["humidity"] == 0
    assert flagged["temperature"] == 1


def test_build_windowed_aggregates_computes_correct_stats_for_one_window(spark):
    t0 = datetime(2026, 1, 1, 12, 0, 0)
    readings = spark.createDataFrame([
        Row(device_id="d1", sensor_type="temperature", value=20.0, is_anomaly=0, event_time=t0),
        Row(device_id="d1", sensor_type="temperature", value=22.0, is_anomaly=0, event_time=t0),
        Row(device_id="d1", sensor_type="temperature", value=100.0, is_anomaly=1, event_time=t0),  # anomaly
    ])

    result = build_windowed_aggregates(
        readings, window_duration="5 minutes", slide_duration="5 minutes", watermark="1 minute"
    ).collect()

    assert len(result) == 1
    row = result[0]
    assert row.device_id == "d1"
    assert row.sensor_type == "temperature"
    assert row.reading_count == 3
    assert row.anomaly_count == 1
    assert row.min_value == 20.0
    assert row.max_value == 100.0
    assert row.avg_value == pytest.approx((20.0 + 22.0 + 100.0) / 3)


def test_build_windowed_aggregates_keeps_devices_and_sensor_types_separate(spark):
    t0 = datetime(2026, 1, 1, 12, 0, 0)
    readings = spark.createDataFrame([
        Row(device_id="d1", sensor_type="temperature", value=20.0, is_anomaly=0, event_time=t0),
        Row(device_id="d2", sensor_type="temperature", value=40.0, is_anomaly=0, event_time=t0),
        Row(device_id="d1", sensor_type="humidity", value=60.0, is_anomaly=0, event_time=t0),
    ])

    result = build_windowed_aggregates(
        readings, window_duration="5 minutes", slide_duration="5 minutes", watermark="1 minute"
    ).collect()

    # Three distinct (device_id, sensor_type) groups in, three rows out — nothing got merged.
    assert len(result) == 3
    groups = {(row.device_id, row.sensor_type) for row in result}
    assert groups == {("d1", "temperature"), ("d2", "temperature"), ("d1", "humidity")}
