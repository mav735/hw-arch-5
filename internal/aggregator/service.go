package aggregator

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"
)

type Service struct {
	ch       *ClickHouse
	store    *Store
	exporter *Exporter
	log      *slog.Logger

	mu      sync.Mutex
	pending map[time.Time]bool
}

type CycleResult struct {
	Date     string `json:"date"`
	Records  uint64 `json:"records"`
	Metrics  int    `json:"metrics"`
	Duration string `json:"duration"`
}

func NewService(ch *ClickHouse, store *Store, exporter *Exporter, log *slog.Logger) *Service {
	return &Service{ch: ch, store: store, exporter: exporter, log: log, pending: map[time.Time]bool{}}
}

func (s *Service) Recompute(ctx context.Context, day time.Time) (CycleResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	date := day.Format(time.DateOnly)
	start := time.Now()
	s.log.Info("aggregation cycle started", "date", date)

	res, err := s.ch.Compute(ctx, day)
	if err != nil {
		s.log.Error("aggregation cycle failed", "date", date, "stage", "clickhouse", "err", err)
		return CycleResult{}, err
	}
	if err := s.store.Upsert(ctx, res.Metrics); err != nil {
		s.log.Error("aggregation cycle failed", "date", date, "stage", "postgres", "err", err)
		return CycleResult{}, err
	}
	out := CycleResult{Date: date, Records: res.Records, Metrics: len(res.Metrics), Duration: time.Since(start).Round(time.Millisecond).String()}
	s.log.Info("aggregation cycle finished", "date", date, "records", out.Records, "metrics", out.Metrics, "duration", out.Duration)
	return out, nil
}

func (s *Service) Export(ctx context.Context, day time.Time) (string, int, error) {
	date := day.Format(time.DateOnly)
	key, n, err := s.exporter.Export(ctx, day)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.pending[day] = true
		s.log.Error("s3 export failed, will retry on next run", "date", date, "err", err)
		return "", 0, err
	}
	delete(s.pending, day)
	s.log.Info("s3 export done", "date", date, "key", key, "metrics", n)
	return key, n, nil
}

// Run recomputes the last lookbackDays days and today every interval (late events
// and the cohort diagonals get refreshed), and exports yesterday plus failed dates
// every exportInterval.
func (s *Service) Run(ctx context.Context, interval, exportInterval time.Duration, lookbackDays int) {
	cycle := func() {
		today := Today()
		for i := lookbackDays; i >= 0; i-- {
			s.Recompute(ctx, today.AddDate(0, 0, -i))
		}
	}
	cycle()
	s.exportDue(ctx)

	aggTick := time.NewTicker(interval)
	expTick := time.NewTicker(exportInterval)
	defer aggTick.Stop()
	defer expTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-aggTick.C:
			cycle()
		case <-expTick.C:
			s.exportDue(ctx)
		}
	}
}

func (s *Service) exportDue(ctx context.Context) {
	s.mu.Lock()
	days := []time.Time{Today().AddDate(0, 0, -1)}
	for d := range s.pending {
		if !d.Equal(days[0]) {
			days = append(days, d)
		}
	}
	s.mu.Unlock()
	sort.Slice(days, func(i, j int) bool { return days[i].Before(days[j]) })
	for _, d := range days {
		s.Export(ctx, d)
	}
}

func Today() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}
