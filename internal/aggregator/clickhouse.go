package aggregator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Metric struct {
	Date       time.Time `ch:"metric_date" json:"date"`
	Name       string    `ch:"metric_name" json:"metric_name"`
	Dimension  string    `ch:"dimension" json:"dimension"`
	Value      float64   `ch:"value" json:"value"`
	ComputedAt time.Time `ch:"computed_at" json:"computed_at"`
}

type ClickHouse struct {
	conn driver.Conn
}

func NewClickHouse(addr, db, user, password string) (*ClickHouse, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: db, Username: user, Password: password},
	})
	if err != nil {
		return nil, err
	}
	return &ClickHouse{conn: conn}, nil
}

func (c *ClickHouse) Ping(ctx context.Context) error { return c.conn.Ping(ctx) }

// Daily metrics for {d}. Duplicates from at-least-once delivery are removed by FINAL.
const dailyMetricsSQL = `
INSERT INTO daily_metrics (metric_date, metric_name, dimension, value, computed_at)
SELECT {d}, m.1, '', m.2, now64(3)
FROM
(
    SELECT
        uniqExact(user_id) AS dau,
        ifNotFinite(avgIf(progress_seconds, event_type = 'VIEW_FINISHED'), 0) AS avg_watch,
        countIf(event_type = 'VIEW_STARTED') AS started,
        countIf(event_type = 'VIEW_FINISHED') AS finished,
        uniqExact(session_id) AS sessions,
        countIf(event_type = 'LIKED') AS likes,
        countIf(event_type = 'SEARCHED') AS searches
    FROM movie_events FINAL
    WHERE event_date = {d}
)
ARRAY JOIN [
    ('dau', toFloat64(dau)),
    ('avg_watch_seconds', toFloat64(avg_watch)),
    ('views_started', toFloat64(started)),
    ('views_finished', toFloat64(finished)),
    ('view_conversion', if(started = 0, 0, finished / started)),
    ('sessions', toFloat64(sessions)),
    ('likes', toFloat64(likes)),
    ('searches', toFloat64(searches))
] AS m`

const dauByDeviceSQL = `
INSERT INTO daily_metrics (metric_date, metric_name, dimension, value, computed_at)
SELECT {d}, 'dau_by_device', toString(device_type), uniqExact(user_id), now64(3)
FROM movie_events FINAL
WHERE event_date = {d}
GROUP BY device_type`

const topMoviesSQL = `
INSERT INTO daily_top_movies (metric_date, movie_id, views, unique_viewers, rank, computed_at)
SELECT {d}, movie_id, views, viewers,
       row_number() OVER (ORDER BY views DESC, viewers DESC, movie_id), now64(3)
FROM
(
    SELECT movie_id,
           countIf(event_type = 'VIEW_STARTED') AS views,
           uniqExactIf(user_id, event_type = 'VIEW_STARTED') AS viewers
    FROM movie_events FINAL
    WHERE event_date = {d}
    GROUP BY movie_id
    HAVING views > 0
)`

// Day {d} fills one diagonal of the cohort matrix: every cohort first seen
// in [d-7, d] gets the cell day_number = d - cohort_date.
const cohortSQL = `
INSERT INTO cohort_retention (cohort_date, day_number, cohort_size, retained_users, retention_rate, computed_at)
WITH
    first_view AS
    (
        SELECT user_id, min(event_date) AS cohort_date
        FROM movie_events FINAL
        WHERE event_type = 'VIEW_STARTED' AND event_date <= {d}
        GROUP BY user_id
        HAVING cohort_date >= {d} - 7
    ),
    active AS
    (
        SELECT DISTINCT user_id
        FROM movie_events FINAL
        WHERE event_date = {d}
    )
SELECT
    f.cohort_date,
    toUInt8(dateDiff('day', f.cohort_date, {d})),
    count() AS cohort_size,
    countIf(a.user_id != '') AS retained,
    retained / cohort_size,
    now64(3)
FROM first_view AS f
LEFT JOIN active AS a ON a.user_id = f.user_id
GROUP BY f.cohort_date`

const retentionMetricsSQL = `
INSERT INTO daily_metrics (metric_date, metric_name, dimension, value, computed_at)
SELECT cohort_date, concat('retention_d', toString(day_number)), '', retention_rate, now64(3)
FROM cohort_retention FINAL
WHERE (cohort_date = {d} - 1 AND day_number = 1) OR (cohort_date = {d} - 7 AND day_number = 7)`

const readMetricsSQL = `
SELECT metric_date, metric_name, dimension, value, computed_at
FROM daily_metrics FINAL
WHERE (metric_date = {d} AND metric_name NOT LIKE 'retention_d%')
   OR (metric_date = {d} - 1 AND metric_name = 'retention_d1')
   OR (metric_date = {d} - 7 AND metric_name = 'retention_d7')
UNION ALL
SELECT metric_date, 'top_movie_views', movie_id, toFloat64(views), computed_at
FROM daily_top_movies FINAL
WHERE metric_date = {d} AND rank <= 10
UNION ALL
SELECT cohort_date, 'cohort_retention', concat('day_', toString(day_number)), retention_rate, computed_at
FROM cohort_retention FINAL
WHERE cohort_date >= {d} - 7 AND cohort_date <= {d} AND dateDiff('day', cohort_date, {d}) = day_number`

type ComputeResult struct {
	Records uint64
	Metrics []Metric
}

func (c *ClickHouse) Compute(ctx context.Context, day time.Time) (ComputeResult, error) {
	var res ComputeResult
	d := fmt.Sprintf("toDate('%s')", day.Format(time.DateOnly))
	q := func(sql string) string { return strings.ReplaceAll(sql, "{d}", d) }

	if err := c.conn.QueryRow(ctx, q("SELECT count() FROM movie_events FINAL WHERE event_date = {d}")).Scan(&res.Records); err != nil {
		return res, fmt.Errorf("count events: %w", err)
	}
	steps := []struct{ name, sql string }{
		{"daily metrics", dailyMetricsSQL},
		{"dau by device", dauByDeviceSQL},
		{"top movies", topMoviesSQL},
		{"cohort retention", cohortSQL},
		{"retention metrics", retentionMetricsSQL},
	}
	for _, s := range steps {
		if err := c.conn.Exec(ctx, q(s.sql)); err != nil {
			return res, fmt.Errorf("%s: %w", s.name, err)
		}
	}
	if err := c.conn.Select(ctx, &res.Metrics, q(readMetricsSQL)); err != nil {
		return res, fmt.Errorf("read metrics: %w", err)
	}
	return res, nil
}
