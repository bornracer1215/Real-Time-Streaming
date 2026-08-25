-- Historical store for windowed aggregates computed by Spark.
-- Redis holds the "latest" snapshot for the dashboard; Postgres holds the full history.
CREATE TABLE IF NOT EXISTS sensor_window_aggregates (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    device_id       TEXT NOT NULL,
    sensor_type     TEXT NOT NULL,
    avg_value       DOUBLE PRECISION NOT NULL,
    min_value       DOUBLE PRECISION NOT NULL,
    max_value       DOUBLE PRECISION NOT NULL,
    reading_count   INTEGER NOT NULL,
    anomaly_count   INTEGER NOT NULL DEFAULT 0,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (window_start, device_id, sensor_type)
);

CREATE INDEX IF NOT EXISTS idx_sensor_window_device_time
    ON sensor_window_aggregates (device_id, window_start DESC);
