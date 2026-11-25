package aggregator

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewStore(ctx context.Context, dsn string, log *slog.Logger) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool, log: log}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, f := range files {
		sql, err := migrations.ReadFile(f)
		if err != nil {
			return err
		}
		err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			tag, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`, f)
			if err != nil || tag.RowsAffected() == 0 {
				return err
			}
			s.log.Info("applying migration", "file", f)
			_, err = tx.Exec(ctx, string(sql))
			return err
		})
		if err != nil {
			return fmt.Errorf("migration %s: %w", f, err)
		}
	}
	return nil
}

// Upsert writes rows idempotently on the primary key. For the top list, movies that
// dropped out of the top for that date are removed.
func (s *Store) Upsert(ctx context.Context, metrics []Metric) error {
	type key struct {
		date time.Time
		name string
	}
	dims := map[key][]string{}
	for _, m := range metrics {
		if m.Name != "top_movie_views" {
			continue
		}
		k := key{m.Date, m.Name}
		dims[k] = append(dims[k], m.Dimension)
	}

	return retry(ctx, s.log, "postgres upsert", func() error {
		return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			b := &pgx.Batch{}
			for k, d := range dims {
				b.Queue(`DELETE FROM daily_metrics WHERE metric_date = $1 AND metric_name = $2 AND dimension <> ALL($3)`, k.date, k.name, d)
			}
			for _, m := range metrics {
				b.Queue(`INSERT INTO daily_metrics (metric_date, metric_name, dimension, value, computed_at)
					VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (metric_date, metric_name, dimension)
					DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at`,
					m.Date, m.Name, m.Dimension, m.Value, m.ComputedAt)
			}
			return tx.SendBatch(ctx, b).Close()
		})
	})
}

func (s *Store) MetricsForDate(ctx context.Context, day time.Time) ([]Metric, error) {
	rows, err := s.pool.Query(ctx, `SELECT metric_date, metric_name, dimension, value, computed_at
		FROM daily_metrics WHERE metric_date = $1 ORDER BY metric_name, dimension`, day)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Metric, error) {
		var m Metric
		err := r.Scan(&m.Date, &m.Name, &m.Dimension, &m.Value, &m.ComputedAt)
		return m, err
	})
}

func retry(ctx context.Context, log *slog.Logger, what string, fn func() error) error {
	backoff := 500 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if attempt == 5 || ctx.Err() != nil {
			return fmt.Errorf("%s failed after %d attempts: %w", what, attempt, err)
		}
		log.Warn(what+" failed, retrying", "attempt", attempt, "backoff", backoff, "err", err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff *= 2
	}
}
