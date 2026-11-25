CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date DATE NOT NULL,
    metric_name TEXT NOT NULL,
    dimension TEXT NOT NULL DEFAULT '',
    value DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_date, metric_name, dimension)
);

CREATE INDEX IF NOT EXISTS daily_metrics_name_date_idx ON daily_metrics (metric_name, metric_date);
