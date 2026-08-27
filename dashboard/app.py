"""Minimal Flask dashboard: polls Redis for the latest per-device window aggregates.

Usage:
    python app.py     # serves http://localhost:5000
"""
import json
import os
from datetime import datetime, timezone

import redis
from flask import Flask, jsonify, render_template

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

app = Flask(__name__)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/latest")
def api_latest():
    readings = []
    # SCAN (cursor-based) rather than KEYS: doesn't block Redis even on a huge keyspace.
    # At our scale (dozens of keys) either would work fine — this is the habit that matters.
    for key in redis_client.scan_iter(match="latest:*"):
        _, device_id, sensor_type = key.split(":", 2)
        raw = redis_client.get(key)
        if raw is None:
            continue  # expired between SCAN and GET — device went quiet, just skip it
        payload = json.loads(raw)
        payload["device_id"] = device_id
        payload["sensor_type"] = sensor_type
        readings.append(payload)

    readings.sort(key=lambda r: (r["sensor_type"], r["device_id"]))

    return jsonify({
        "readings": readings,
        "device_count": len(readings),
        "anomaly_count": sum(r["anomaly_count"] for r in readings),
        "server_time": datetime.now(timezone.utc).isoformat(),
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
