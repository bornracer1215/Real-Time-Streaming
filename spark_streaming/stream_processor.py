"""Spark Structured Streaming job: Kafka -> windowed aggregation + anomaly flagging -> Postgres + Redis.

Usage:
    python stream_processor.py                          # spec defaults: 5-min window, 1-min slide
    python stream_processor.py --window-duration "30 seconds" --slide-duration "10 seconds"  # fast local demo
"""
from __future__ import annotations  # container's Python is 3.8; defers eval of newer type-hint syntax

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, max as spark_max, min as spark_min, sum as spark_sum, when, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

import sinks
from monitoring import ThroughputListener

# Defaults assume this runs inside the `spark` container on the Compose network (see
# docker-compose.yml), where services are reachable by name rather than localhost.
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
KAFKA_TOPIC = "iot-sensor-readings"

# Mirrors producer/devices.py's SENSOR_PROFILES min/max. Duplicated here rather than shared
# because the producer and this job are independent processes/languages-of-record in a real
# system (this Spark job doesn't import producer code) — see decisions.md.
ANOMALY_BOUNDS = {
    "temperature": (15.0, 30.0),
    "humidity": (30.0, 70.0),
    "pressure": (990.0, 1030.0),
}

READING_SCHEMA = StructType([
    StructField("device_id", StringType()),
    StructField("sensor_type", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("event_time", TimestampType()),
])


def anomaly_flag_column():
    # Builds: CASE WHEN sensor_type='temperature' AND (value<15 OR value>30) THEN 1 ... ELSE 0 END
    expr = None
    for sensor_type, (lo, hi) in ANOMALY_BOUNDS.items():
        condition = (col("sensor_type") == sensor_type) & ((col("value") < lo) | (col("value") > hi))
        expr = when(condition, 1) if expr is None else expr.when(condition, 1)
    return expr.otherwise(0)


def add_anomaly_flag(readings):
    """Tags each raw reading row with is_anomaly (0/1). Pure DataFrame transform — testable
    against a plain batch DataFrame, no Kafka/streaming context required."""
    return readings.withColumn("is_anomaly", anomaly_flag_column())


def build_windowed_aggregates(readings, window_duration, slide_duration, watermark):
    """The core windowing/aggregation logic, factored out of main() so it can be unit-tested
    directly against a static (batch) DataFrame — withWatermark is a no-op in batch mode, so the
    same code path works for both a real streaming query and a plain test DataFrame."""
    return (
        readings
        # Watermark: tolerate readings arriving up to `watermark` late (by event_time) before a
        # window is considered final. Without this, Spark would have to keep every window's
        # state in memory forever, since a late event could always still arrive.
        .withWatermark("event_time", watermark)
        .groupBy(
            window(col("event_time"), window_duration, slide_duration),
            col("device_id"),
            col("sensor_type"),
        )
        .agg(
            avg("value").alias("avg_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            count("*").alias("reading_count"),
            spark_sum("is_anomaly").alias("anomaly_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "device_id",
            "sensor_type",
            "avg_value",
            "min_value",
            "max_value",
            "reading_count",
            "anomaly_count",
        )
    )


def process_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        print(f"[batch {batch_id}] empty, skipping")
        return

    rows = [row.asDict() for row in batch_df.collect()]
    sinks.write_postgres(rows)
    sinks.write_redis(rows)
    print(f"[batch {batch_id}] wrote {len(rows)} window aggregate(s)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-duration", default="5 minutes")
    parser.add_argument("--slide-duration", default="1 minute")
    parser.add_argument("--watermark", default="1 minute")
    parser.add_argument("--trigger-interval", default="10 seconds")
    parser.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("iot-sensor-stream-processor")
        # The Kafka connector jar itself is supplied via `spark-submit --packages` (see
        # README), not here — that lets spark-submit resolve it before the JVM/context spins
        # up, which is the idiomatic place for it.
        # Default is 200 shuffle partitions, tuned for a real cluster. Locally, with 3 Kafka
        # partitions and a handful of devices, that's massive overkill and just adds overhead.
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ThroughputListener())

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    readings = add_anomaly_flag(
        raw.select(from_json(col("value").cast("string"), READING_SCHEMA).alias("data")).select("data.*")
    )

    windowed = build_windowed_aggregates(readings, args.window_duration, args.slide_duration, args.watermark)

    query = (
        windowed.writeStream
        .outputMode("update")  # emit a row every time a window's aggregate changes, not just when it closes
        .foreachBatch(process_batch)
        .option("checkpointLocation", "./checkpoint/sensor_aggregates")
        .trigger(processingTime=args.trigger_interval)
        .start()
    )

    print(f"Streaming query started. window={args.window_duration} slide={args.slide_duration} "
          f"watermark={args.watermark} trigger={args.trigger_interval}. Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
