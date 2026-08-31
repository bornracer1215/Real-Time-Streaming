# Throughput Benchmarks

## Environment

Everything here ran on a single Windows machine, via Docker Desktop:

- Kafka: single broker, KRaft mode, topic `iot-sensor-readings` with 3 partitions.
- Spark: single `apache/spark` container, `local[*]` master (one JVM, using whatever cores Docker
  Desktop's VM exposed to it) — not a real multi-node cluster.
- Producer: single-threaded Python process (`confluent-kafka`), also on the host.

These numbers describe *this implementation*, on *this machine* — not Kafka's or Spark's general
ceiling. A real cluster with more brokers, more partitions, and a multi-threaded/multi-process
producer would push all of these numbers up substantially. That's the honest framing for what
follows.

## Method

Four load tiers, each run for 30 seconds at an increasing `--rate` (target messages/sec):

- **Producer-side numbers** come straight from `producer.py`'s own accounting: messages actually
  sent divided by elapsed time.
- **Spark-side numbers** come from `spark_streaming/monitoring.py`'s `ThroughputListener`, which
  taps Spark's `StreamingQueryListener` API — the same mechanism a real production job would use to
  ship metrics to a monitoring system. Each 5-second trigger reports `inputRowsPerSecond`,
  `processedRowsPerSecond`, and the trigger's wall-clock duration.

## Results

| Target rate (msg/s) | Achieved producer rate (msg/s) | Spark input rate (msg/s) | Spark processed rate (msg/s) | Typical trigger duration | Fell behind? |
|---:|---:|---:|---:|---:|:---:|
| 100 | 93.8 | ~93 | ~270 | 1.7–1.9s | No |
| 500 | 385.4 | ~385 | ~1,000–1,200 | 1.6–2.0s | No (one transient spike) |
| 3,000 | 1,705.2 | ~1,700 | ~4,600–5,800 | 1.5–2.1s | No |
| 50,000 (uncapped) | 1,822.5 | ~1,800 | ~4,200–5,900 | 1.3–2.1s | No |

"Trigger duration" is measured against a 5-second trigger interval — so anything well under 5s
means Spark finished each micro-batch with room to spare before the next one arrived.

## Findings

1. **The producer script is the actual bottleneck, not Kafka or Spark.** Even with the rate limiter
   effectively disabled (`--rate 50000`), the single-threaded Python producer topped out around
   **~1,822 messages/sec**. That's per-message Python/JSON/librdkafka-call overhead in a plain
   loop, not the broker pushing back — Kafka never showed any sign of strain at any tier tested.
2. **Spark had significant headroom at every tier.** `processed_rows_per_sec` consistently ran
   2–3x higher than the actual input rate, meaning the streaming query itself was never close to
   the constraint — it was comfortably waiting on more data to arrive, not the other way around.
3. **No backlog ever accumulated.** Trigger duration stayed well under the 5-second budget the
   whole time (one cold-start outlier aside), so the "latest" data the dashboard reads from Redis
   was never more than one trigger interval — a few seconds — stale.
4. **The gap between target rate and achieved rate grows as the target increases.** At 100 msg/s
   the producer hit 94% of target; by 3,000 msg/s it was hitting a hard ceiling around 1,800 —
   Windows' sleep-timer granularity plus per-message Python overhead, not Kafka, is what's limiting
   this. A resume-style figure like "50,000 events/min" (~833/s) sits comfortably below this
   ceiling — verified achievable with real headroom to spare.

## Reproducing this

```bash
# 1. bring the stack up, make sure the Spark job is running (see README's Running Instructions)
# 2. from producer/, run each tier and watch producer.py's own rate log:
python producer.py --rate 100 --duration 30
python producer.py --rate 500 --duration 30
python producer.py --rate 3000 --duration 30
python producer.py --rate 50000 --duration 30   # rate limiter effectively disabled

# 3. in parallel, watch the Spark container's stdout for [throughput] lines from ThroughputListener
```

## Caveats

- Single broker, single partition-set, single Spark executor, single-process producer — this is a
  learning/demo environment, not a capacity-planning exercise for a production cluster.
- Docker Desktop on Windows runs everything inside a WSL2 VM, which adds its own overhead compared
  to a native Linux host — real numbers on Linux would likely be higher across the board.
