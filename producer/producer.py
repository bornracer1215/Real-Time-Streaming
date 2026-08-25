"""Simulates a fleet of IoT sensors publishing readings to Kafka.

Usage:
    python producer.py --rate 10          # ~10 messages/sec total across the fleet
    python producer.py --rate 500         # throughput test
"""
import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from devices import build_fleet

TOPIC = "iot-sensor-readings"


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",              # wait for all in-sync replicas before ack — durability over raw speed
        "enable.idempotence": True,  # producer retries never create duplicate messages
        "linger.ms": 5,              # small batching window: trade a few ms of latency for throughput
    })


def delivery_report(err, msg):
    if err is not None:
        print(f"DELIVERY FAILED: {err}", file=sys.stderr)


def make_message(device) -> tuple[str, bytes]:
    reading = device.next_reading()
    payload = {
        "device_id": device.device_id,
        "sensor_type": device.sensor_type,
        "value": reading,
        "unit": device.unit,
        # event_time: when the reading was actually taken (vs. when Kafka/Spark processes it).
        # Spark's watermarking (Phase 3) keys off this field, not processing time.
        "event_time": datetime.now(timezone.utc).isoformat(),
    }
    return device.device_id, json.dumps(payload).encode("utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=10.0, help="messages/sec across the whole fleet")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--duration", type=float, default=None, help="seconds to run; omit to run forever")
    args = parser.parse_args()

    fleet = build_fleet()
    producer = build_producer(args.bootstrap_servers)

    running = True

    def handle_sigint(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_sigint)

    interval = 1.0 / args.rate
    sent = 0
    start = time.monotonic()

    print(f"Producing to '{TOPIC}' at ~{args.rate} msg/sec. Ctrl+C to stop.")

    while running:
        if args.duration is not None and (time.monotonic() - start) >= args.duration:
            break

        device = random.choice(fleet)
        key, value = make_message(device)

        # poll(0) drains the internal delivery-report queue without blocking — needed so
        # librdkafka can invoke delivery_report() and so its send buffer doesn't fill up.
        producer.poll(0)
        producer.produce(TOPIC, key=key, value=value, callback=delivery_report)

        sent += 1
        if sent % 100 == 0:
            elapsed = time.monotonic() - start
            print(f"sent={sent} elapsed={elapsed:.1f}s rate={sent / elapsed:.1f}/s")

        time.sleep(interval)

    print("Flushing remaining messages...")
    producer.flush(timeout=10)
    elapsed = time.monotonic() - start
    print(f"Done. Sent {sent} messages in {elapsed:.1f}s ({sent / elapsed:.1f}/s average).")


if __name__ == "__main__":
    main()
