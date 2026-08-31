"""Throughput observability for the streaming query.

Spark reports per-micro-batch metrics (rows processed, input/processed rate, trigger duration)
through the StreamingQueryListener API. This is the same mechanism a real production job would
use to ship metrics to Prometheus/Datadog/etc. — here we just print them, since that's enough to
benchmark throughput locally.
"""
import json

from pyspark.sql.streaming import StreamingQueryListener


class ThroughputListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        # event.progress.json is Spark's own JSON serialization of the progress report — more
        # robust to read than the Python object's attributes, which vary a bit across versions.
        progress = json.loads(event.progress.json)
        num_input_rows = progress.get("numInputRows", 0)
        if num_input_rows == 0:
            return  # empty batch (no new Kafka messages this trigger) — nothing to report

        trigger_ms = progress.get("durationMs", {}).get("triggerExecution", 0)
        print(
            f"[throughput] batch={progress.get('batchId')} rows={num_input_rows} "
            f"input_rows_per_sec={progress.get('inputRowsPerSecond', 0.0):.1f} "
            f"processed_rows_per_sec={progress.get('processedRowsPerSecond', 0.0):.1f} "
            f"trigger_ms={trigger_ms}",
            flush=True,
        )

    def onQueryTerminated(self, event):
        pass
