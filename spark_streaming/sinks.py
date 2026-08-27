"""Writes one micro-batch of windowed aggregates to Postgres (history) and Redis (latest snapshot).

Called from stream_processor.py's foreachBatch — Spark has no built-in streaming sink for
Postgres or Redis, so this is the standard pattern: collect the (small) aggregated batch to
the driver and write it with ordinary client libraries.
"""
from __future__ import annotations  # container's Python is 3.8; defers eval of `list[dict]`-style hints

import json
import os

import psycopg2
import redis
from psycopg2.extras import execute_values

# Defaults assume this runs inside the `spark` container on the Compose network (see
# docker-compose.yml), where services are reachable by name rather than localhost.
PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_DSN = f"host={PG_HOST} port=5432 dbname=streaming user=streaming password=streaming"
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = 6379
REDIS_TTL_SECONDS = 300  # if a device stops reporting, its "latest" key expires instead of going stale forever

UPSERT_SQL = """
    INSERT INTO sensor_window_aggregates
        (window_start, window_end, device_id, sensor_type, avg_value, min_value, max_value,
         reading_count, anomaly_count)
    VALUES %s
    ON CONFLICT (window_start, device_id, sensor_type) DO UPDATE SET
        window_end = EXCLUDED.window_end,
        avg_value = EXCLUDED.avg_value,
        min_value = EXCLUDED.min_value,
        max_value = EXCLUDED.max_value,
        reading_count = EXCLUDED.reading_count,
        anomaly_count = EXCLUDED.anomaly_count,
        computed_at = now();
"""


def write_postgres(rows: list[dict]) -> None:
    # Fresh connection per batch: simplest thing that works at this scale (a handful of rows
    # every trigger interval). A long-running production job would pool connections instead.
    values = [
        (
            r["window_start"], r["window_end"], r["device_id"], r["sensor_type"],
            r["avg_value"], r["min_value"], r["max_value"], r["reading_count"], r["anomaly_count"],
        )
        for r in rows
    ]
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, values)
        conn.commit()


def write_redis(rows: list[dict]) -> None:
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        for r in rows:
            key = f"latest:{r['device_id']}:{r['sensor_type']}"

            # Guard against an out-of-order micro-batch overwriting a newer window with an
            # older one (can happen because sliding windows emit multiple overlapping rows).
            existing = client.get(key)
            if existing is not None:
                existing_window_end = json.loads(existing)["window_end"]
                if existing_window_end >= r["window_end"].isoformat():
                    continue

            payload = {
                "window_start": r["window_start"].isoformat(),
                "window_end": r["window_end"].isoformat(),
                "avg_value": r["avg_value"],
                "min_value": r["min_value"],
                "max_value": r["max_value"],
                "reading_count": r["reading_count"],
                "anomaly_count": r["anomaly_count"],
            }
            client.set(key, json.dumps(payload), ex=REDIS_TTL_SECONDS)
            # A maintained set of active devices so the dashboard can enumerate keys without
            # ever needing Redis's KEYS/SCAN over the whole keyspace.
            client.sadd("devices:active", r["device_id"])
    finally:
        client.close()
