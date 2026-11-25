//go:build integration

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func clickhouseConn(t *testing.T) driver.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{env("CLICKHOUSE_ADDR", "localhost:9000")},
		Auth: clickhouse.Auth{Database: "cinema", Username: "default", Password: env("CLICKHOUSE_PASSWORD", "clickhouse")},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func postJSON(t *testing.T, url string, body any) (int, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	resp, err := http.Post(url, "application/json", r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func publish(t *testing.T, ev map[string]any) string {
	t.Helper()
	code, out := postJSON(t, env("PRODUCER_URL", "http://localhost:8080")+"/events", ev)
	if code != http.StatusCreated {
		t.Fatalf("publish: status %d, body %v", code, out)
	}
	return out["event_id"].(string)
}

func waitCount(t *testing.T, conn driver.Conn, want uint64, query string, args ...any) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(90 * time.Second)
	for {
		var n uint64
		if err := conn.QueryRow(ctx, query, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: got %d rows, want %d", n, want)
		}
		time.Sleep(time.Second)
	}
}

func TestEventReachesClickHouse(t *testing.T) {
	conn := clickhouseConn(t)
	user := "it-" + uuid.NewString()
	session := uuid.NewString()
	ts := time.Now().UTC().Truncate(time.Millisecond)
	t.Cleanup(func() {
		conn.Exec(context.Background(), "DELETE FROM movie_events WHERE user_id = ?", user)
	})

	id := publish(t, map[string]any{
		"user_id": user, "movie_id": "movie-it", "event_type": "VIEW_FINISHED",
		"timestamp": ts.Format(time.RFC3339Nano), "device_type": "TV",
		"session_id": session, "progress_seconds": 1234,
	})

	waitCount(t, conn, 1, "SELECT count() FROM movie_events WHERE event_id = toUUID(?)", id)

	var got struct {
		EventID    uuid.UUID `ch:"event_id"`
		UserID     string    `ch:"user_id"`
		MovieID    string    `ch:"movie_id"`
		EventType  string    `ch:"event_type"`
		Timestamp  time.Time `ch:"timestamp"`
		DeviceType string    `ch:"device_type"`
		SessionID  string    `ch:"session_id"`
		Progress   *uint32   `ch:"progress_seconds"`
	}
	err := conn.QueryRow(context.Background(), `SELECT event_id, user_id, movie_id, toString(event_type) AS event_type, timestamp,
		toString(device_type) AS device_type, session_id, progress_seconds FROM movie_events WHERE event_id = toUUID(?)`, id).ScanStruct(&got)
	if err != nil {
		t.Fatal(err)
	}
	if got.EventID.String() != id || got.UserID != user || got.MovieID != "movie-it" || got.EventType != "VIEW_FINISHED" ||
		got.DeviceType != "TV" || got.SessionID != session || got.Progress == nil || *got.Progress != 1234 ||
		!got.Timestamp.Equal(ts) {
		t.Fatalf("unexpected row: %+v (timestamp want %s)", got, ts)
	}
}

func TestInvalidEventRejected(t *testing.T) {
	url := env("PRODUCER_URL", "http://localhost:8080") + "/events"
	cases := map[string]map[string]any{
		"bad event_type": {"user_id": "u", "movie_id": "m", "event_type": "DANCED", "device_type": "TV", "session_id": "s", "progress_seconds": 1},
		"no progress":    {"user_id": "u", "movie_id": "m", "event_type": "VIEW_STARTED", "device_type": "TV", "session_id": "s"},
		"bad device":     {"user_id": "u", "movie_id": "m", "event_type": "LIKED", "device_type": "FRIDGE", "session_id": "s"},
		"no user":        {"movie_id": "m", "event_type": "LIKED", "device_type": "TV", "session_id": "s"},
	}
	for name, ev := range cases {
		t.Run(name, func(t *testing.T) {
			if code, out := postJSON(t, url, ev); code != http.StatusBadRequest {
				t.Fatalf("status %d, body %v", code, out)
			}
		})
	}
}

// Uses a date far in the past so generator traffic never touches it.
func TestAggregationAndExport(t *testing.T) {
	ctx := context.Background()
	conn := clickhouseConn(t)
	pg, err := pgx.Connect(ctx, env("POSTGRES_DSN", "postgres://metrics:metrics@localhost:5432/metrics"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pg.Close(ctx) })

	day0 := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	day1 := day0.AddDate(0, 0, 1)
	cleanup := func() {
		for _, q := range []string{
			"DELETE FROM movie_events WHERE event_date IN ('2001-01-01', '2001-01-02')",
			"DELETE FROM daily_metrics WHERE metric_date IN ('2001-01-01', '2001-01-02')",
			"DELETE FROM daily_top_movies WHERE metric_date IN ('2001-01-01', '2001-01-02')",
			"DELETE FROM cohort_retention WHERE cohort_date IN ('2001-01-01', '2001-01-02')",
		} {
			if err := conn.Exec(ctx, q); err != nil {
				t.Log(err)
			}
		}
		pg.Exec(ctx, "DELETE FROM daily_metrics WHERE metric_date IN ($1, $2)", day0, day1)
	}
	cleanup()
	t.Cleanup(cleanup)

	alice, bob := "it-"+uuid.NewString(), "it-"+uuid.NewString()
	ev := func(user, typ string, at time.Time, progress any) map[string]any {
		m := map[string]any{"user_id": user, "movie_id": "movie-it", "event_type": typ, "device_type": "MOBILE",
			"session_id": user + at.Format("0102"), "timestamp": at.Format(time.RFC3339)}
		if progress != nil {
			m["progress_seconds"] = progress
		}
		return m
	}
	publish(t, ev(alice, "VIEW_STARTED", day0.Add(10*time.Hour), 0))
	publish(t, ev(alice, "VIEW_FINISHED", day0.Add(11*time.Hour), 100))
	publish(t, ev(bob, "VIEW_STARTED", day0.Add(12*time.Hour), 0))
	publish(t, ev(alice, "VIEW_STARTED", day1.Add(9*time.Hour), 0))
	waitCount(t, conn, 4, "SELECT count() FROM movie_events WHERE event_date IN ('2001-01-01', '2001-01-02')")

	aggURL := env("AGGREGATOR_URL", "http://localhost:8090")
	recompute := func(date string) {
		if code, out := postJSON(t, aggURL+"/recompute?date="+date, nil); code != http.StatusOK {
			t.Fatalf("recompute %s: %d %v", date, code, out)
		}
	}
	metric := func(day time.Time, name, dim string) float64 {
		var v float64
		err := pg.QueryRow(ctx, "SELECT value FROM daily_metrics WHERE metric_date = $1 AND metric_name = $2 AND dimension = $3",
			day, name, dim).Scan(&v)
		if err != nil {
			t.Fatalf("metric %s/%s: %v", name, dim, err)
		}
		return v
	}
	rows := func() int {
		var n int
		pg.QueryRow(ctx, "SELECT count(*) FROM daily_metrics WHERE metric_date IN ($1, $2)", day0, day1).Scan(&n)
		return n
	}

	recompute("2001-01-01")
	recompute("2001-01-02")
	first := rows()
	recompute("2001-01-01")
	recompute("2001-01-02")
	if again := rows(); again != first {
		t.Fatalf("recompute is not idempotent: %d rows, then %d", first, again)
	}

	checks := []struct {
		name, dim string
		want      float64
	}{
		{"dau", "", 2},
		{"view_conversion", "", 0.5},
		{"avg_watch_seconds", "", 100},
		{"top_movie_views", "movie-it", 2},
		{"retention_d1", "", 0.5},
		{"cohort_retention", "day_0", 1},
		{"cohort_retention", "day_1", 0.5},
	}
	for _, c := range checks {
		if got := metric(day0, c.name, c.dim); got != c.want {
			t.Errorf("%s/%s = %v, want %v", c.name, c.dim, got, c.want)
		}
	}

	for range 2 {
		if code, out := postJSON(t, aggURL+"/export?date=2001-01-01", nil); code != http.StatusOK {
			t.Fatalf("export: %d %v", code, out)
		}
	}
	s3, err := minio.New(env("S3_ENDPOINT", "localhost:9000"), &minio.Options{
		Creds: credentials.NewStaticV4(env("S3_ACCESS_KEY", "minio"), env("S3_SECRET_KEY", "minio12345"), ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	bucket := env("S3_BUCKET", "movie-analytics")
	var objects int
	for o := range s3.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: "daily/2001-01-01/", Recursive: true}) {
		if o.Err != nil {
			t.Fatal(o.Err)
		}
		objects++
	}
	if objects != 1 {
		t.Fatalf("want exactly 1 exported object, got %d", objects)
	}
	obj, err := s3.GetObject(ctx, bucket, "daily/2001-01-01/aggregates.json", minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Date    string `json:"date"`
		Metrics []struct {
			Name  string  `json:"metric_name"`
			Value float64 `json:"value"`
		} `json:"metrics"`
	}
	if err := json.NewDecoder(obj).Decode(&file); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, m := range file.Metrics {
		names[m.Name] = true
	}
	for _, n := range []string{"dau", "view_conversion", "avg_watch_seconds", "retention_d1", "top_movie_views"} {
		if !names[n] {
			t.Errorf("export missing %s: %v", n, fmt.Sprint(names))
		}
	}
	s3.RemoveObject(ctx, bucket, "daily/2001-01-01/aggregates.json", minio.RemoveObjectOptions{})
}
