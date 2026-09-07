# Real-Time Streaming Pipeline

A small end-to-end streaming pipeline: a Python producer fakes IoT sensor readings, Kafka carries
them, Spark Structured Streaming aggregates them into rolling windows and flags anomalies, and the
results land in Postgres (history) and Redis (live), which a tiny Flask dashboard polls to show
what's happening right now.

It's a learning project first — the goal was to actually build and understand a streaming pipeline
end to end, not just wire together a tutorial. A few things went sideways along the way (Spark
flatly refuses to start natively on Windows without a native Hadoop shim, for one), and the
decisions made to work around that are part of what makes this a real pipeline rather than a
toy — see [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for how it actually performs under load.

## How data flows

```
producer.py  --->  Kafka topic          --->  Spark Structured Streaming  --->  Postgres (history)
(fake sensor      iot-sensor-readings        (windowed aggregation +            +
 readings)        3 partitions                anomaly detection)                Redis (latest snapshot)
                                                                                        |
                                                                                        v
                                                                               Flask dashboard (polls Redis)
```

Ten simulated sensors (temperature/humidity/pressure) publish JSON readings to Kafka, keyed by
device ID so each device's readings stay in order. Spark reads that stream, computes 5-minute
rolling averages (sliding every 1 minute) per device, flags readings that fall outside a normal
range as anomalies, and writes the results to both Postgres (a durable, queryable history) and
Redis (a fast "what's true right now" snapshot with a short TTL, so a dead sensor's last reading
doesn't linger forever). The dashboard just polls that Redis snapshot every couple of seconds.

## What's actually in each piece

- **`producer/`** — simulates the sensor fleet. `devices.py` models each sensor as a random walk
  (so readings drift realistically instead of jumping around) with a small chance of an injected
  spike. `producer.py` publishes those readings to Kafka via `confluent-kafka`.
- **`spark_streaming/`** — the actual streaming job. `stream_processor.py` reads from Kafka,
  windows and aggregates, and flags anomalies; `sinks.py` writes each micro-batch to Postgres and
  Redis; `monitoring.py` taps Spark's own progress-listener API to report live throughput. This
  runs *inside* a Docker container (`apache/spark`), not on the Windows host — see the note below.
- **`dashboard/`** — a minimal Flask app (`app.py`) that polls Redis and a single-page frontend
  (`templates/index.html`) that re-renders every 2 seconds.
- **`sql/init.sql`** — the Postgres schema, applied automatically on first container start.
- **`docker-compose.yml`** — Kafka (KRaft mode, no Zookeeper), Kafka UI (a web view into topics —
  handy for debugging, not part of the actual pipeline), Postgres, Redis, and the Spark container.

**Why Spark runs in Docker rather than as a normal Python process:** PySpark needs a Windows-only
Hadoop shim (`winutils.exe`) to start at all natively on Windows, and there's no trustworthy
official binary for it — the only sources are unsigned community builds on GitHub. Running Spark
inside a Linux container sidesteps that completely, and it's honestly closer to how Spark actually
gets deployed in the real world anyway (containers/clusters, not a bare laptop).

## Stack

Apache Kafka (KRaft mode) · Spark Structured Streaming · PostgreSQL · Redis · Flask · Docker Compose · Python

## Throughput

Full methodology and results in [docs/BENCHMARKS.md](docs/BENCHMARKS.md). Short version: the
pipeline was pushed up to ~1,822 messages/sec sustained (the producer script's own ceiling on this
machine — Kafka and Spark both had headroom to spare well beyond that), with Spark's processing
rate consistently running 2-3x ahead of the input rate and no backlog ever building up.

## Tests & CI

There's a small test suite (`tests/`) and a two-stage GitHub Actions pipeline
(`.github/workflows/ci.yml`) that runs on every push/PR to `main`:

- **Unit tests** (fast, no infrastructure): the sensor simulation logic (`devices.py`), the Spark
  anomaly-flagging and windowed-aggregation logic (run against a plain batch DataFrame — no Kafka
  needed), and the dashboard's `/api/latest` endpoint (using `fakeredis` instead of a real Redis).
  These run natively on Linux CI runners, where PySpark starts up without the Windows-specific
  `winutils.exe` issue this project hit locally (see above).
- **Integration smoke test** (slower, real infrastructure): brings up the actual Docker Compose
  stack, creates the Kafka topic, starts the real Spark job in its container, runs the real
  producer, and asserts that windowed aggregates actually landed in both Postgres and Redis. This
  is the test that answers "does the whole pipeline actually work," not just "is each piece's
  logic correct" — it only runs if the unit tests pass first.

Run the unit tests locally:

```bash
pip install -r requirements-dev.txt -r producer/requirements.txt -r spark_streaming/requirements.txt -r dashboard/requirements.txt
pytest tests/ -v
```

Note: on Windows, `tests/test_stream_processor.py` won't run locally for the same reason the Spark
job itself doesn't (no `winutils.exe`) — it's verified by copying it into the running `spark`
container and running `pytest` there, and by CI on every push.

## Running it

**Requirements:** Docker Desktop, Python 3.11+ on the host (for the producer and dashboard — Spark
itself runs entirely inside its container, no local PySpark install needed).

**1. Bring the infrastructure up**

```bash
docker compose up -d
```

This starts Kafka, Kafka UI (`http://localhost:8081`), Postgres, Redis, and an idling Spark
container. Give it a few seconds for the healthchecks to pass.

**2. Create the Kafka topic** (first time only)

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic iot-sensor-readings \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

*(On Windows Git Bash, prefix with `MSYS_NO_PATHCONV=1` — otherwise Git Bash mangles the `/opt/...`
path into a Windows path before Docker sees it.)*

**3. Set up a Python virtual environment for the producer and dashboard**

```bash
python -m venv .venv
.venv/Scripts/activate        # .venv/bin/activate on macOS/Linux
pip install -r producer/requirements.txt -r dashboard/requirements.txt
```

**4. Start the Spark streaming job** (runs inside its container)

```bash
docker exec spark bash -c \
  "pip3 install --target=/app/.python-packages psycopg2-binary redis"   # first time only

docker exec -e PYTHONPATH=/app/.python-packages spark /opt/spark/bin/spark-submit \
  --master local[*] --conf spark.jars.ivy=/app/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  stream_processor.py
```

This runs in the foreground and keeps streaming until you stop it. Useful flags for faster local
testing (the defaults are the spec-accurate 5-minute window / 1-minute slide, which takes a while
to show results):

```bash
--window-duration "30 seconds" --slide-duration "10 seconds" --watermark "10 seconds" --trigger-interval "5 seconds"
```

**5. Start the producer** (in another terminal, with the venv active)

```bash
cd producer
python producer.py --rate 15
```

`--rate` is target messages/sec across the whole simulated fleet; omit `--duration` to run
indefinitely, or add it to run for a fixed number of seconds.

**6. Start the dashboard** (in another terminal, with the venv active)

```bash
cd dashboard
python app.py
```

Open **`http://localhost:5000`** — you should see all 10 devices updating live, with anomalous
windows highlighted in red as they occur.

**Shutting down:**

```bash
docker compose down          # stops and removes the containers (data volumes persist)
```
