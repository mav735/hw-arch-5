package producer

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/mav735/hw-arch-5/internal/event"
)

const cohortWindow = 14

var idNamespace = uuid.MustParse("6f1c1d1e-4b8f-4a51-9a7e-3c2a8b0d5e11")

type movie struct {
	id       string
	duration int32
}

// Generator simulates users watching movies. Backfill replays past days
// deterministically (same ids on every run), live mode emits events in real time.
type Generator struct {
	pub          *Publisher
	rate         float64
	backfillDays int
	maxSessions  int
	movies       []movie
	log          *slog.Logger
}

func NewGenerator(pub *Publisher, rate float64, backfillDays int, log *slog.Logger) *Generator {
	r := rand.New(rand.NewPCG(42, 42))
	movies := make([]movie, 60)
	for i := range movies {
		movies[i] = movie{id: fmt.Sprintf("movie-%03d", i+1), duration: int32(20+r.IntN(130)) * 60}
	}
	return &Generator{pub: pub, rate: rate, backfillDays: backfillDays, maxSessions: 30, movies: movies, log: log}
}

func (g *Generator) Run(ctx context.Context) {
	if g.backfillDays > 0 {
		g.backfill(ctx)
	}
	g.live(ctx)
}

func (g *Generator) backfill(ctx context.Context) {
	today := truncDay(time.Now().UTC())
	total := 0
	for i := g.backfillDays; i >= 1; i-- {
		day := today.AddDate(0, 0, -i)
		evs := g.dayEvents(day)
		for start := 0; start < len(evs); start += 500 {
			end := min(start+500, len(evs))
			if err := g.pub.PublishBatch(ctx, evs[start:end]); err != nil {
				g.log.Error("backfill publish failed", "day", day.Format(time.DateOnly), "err", err)
				return
			}
		}
		total += len(evs)
		g.log.Info("backfill day published", "day", day.Format(time.DateOnly), "events", len(evs))
	}
	g.log.Info("backfill done", "days", g.backfillDays, "events", total)
}

func (g *Generator) dayEvents(day time.Time) []event.Event {
	r := dayRand(day, 1)
	seq := 0
	newID := func() string {
		seq++
		return uuid.NewSHA1(idNamespace, fmt.Appendf(nil, "%s/%d", day.Format(time.DateOnly), seq)).String()
	}
	var evs []event.Event
	for _, u := range activeUsers(day) {
		for range 1 + r.IntN(2) {
			start := day.Add(time.Duration(r.IntN(18*3600)) * time.Second)
			evs = append(evs, g.session(r, u, start, newID)...)
		}
	}
	return evs
}

func (g *Generator) live(ctx context.Context) {
	r := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 7))
	newID := func() string { return uuid.NewString() }
	tick := time.NewTicker(time.Duration(float64(time.Second) / g.rate))
	defer tick.Stop()

	var pool []string
	var poolDay time.Time
	var sessions [][]event.Event
	g.log.Info("live generator started", "rate", g.rate)

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		now := time.Now().UTC()
		if d := truncDay(now); !d.Equal(poolDay) {
			poolDay, pool = d, activeUsers(d)
		}
		if len(sessions) < g.maxSessions && (len(sessions) == 0 || r.Float64() < 0.3) {
			u := pool[r.IntN(len(pool))]
			if r.Float64() < 0.1 {
				u = fmt.Sprintf("user-%s-live-%04d", now.Format("20060102"), r.IntN(10000))
			}
			sessions = append(sessions, g.session(r, u, now, newID))
		}
		i := r.IntN(len(sessions))
		e := sessions[i][0]
		e.Timestamp = time.Now().UTC()
		if sessions[i] = sessions[i][1:]; len(sessions[i]) == 0 {
			sessions = append(sessions[:i], sessions[i+1:]...)
		}
		if err := g.pub.Publish(ctx, &e); err != nil && ctx.Err() == nil {
			g.log.Error("generator publish failed", "event_id", e.EventID, "err", err)
		}
	}
}

// session: [SEARCHED] -> VIEW_STARTED -> (PAUSED -> RESUMED)* -> FINISHED [-> LIKED],
// or abandoned after a pause. progress_seconds only grows.
func (g *Generator) session(r *rand.Rand, user string, t time.Time, newID func() string) []event.Event {
	m := g.movies[int(math.Pow(r.Float64(), 2.2)*float64(len(g.movies)))]
	device := userDevice(user, r)
	sid := newID()
	var evs []event.Event
	add := func(typ string, progress *int32) {
		evs = append(evs, event.Event{
			EventID: newID(), UserID: user, MovieID: m.id, EventType: typ, Timestamp: t,
			DeviceType: device, SessionID: sid, ProgressSeconds: progress,
		})
	}
	at := func(v int32) *int32 { return &v }

	if r.Float64() < 0.35 {
		add(event.Searched, nil)
		t = t.Add(time.Duration(5+r.IntN(60)) * time.Second)
	}
	var progress int32
	add(event.ViewStarted, at(0))
	for {
		step := int32(float64(m.duration) * (0.15 + 0.35*r.Float64()))
		t = t.Add(time.Duration(step) * time.Second)
		progress += step
		if progress >= m.duration {
			add(event.ViewFinished, at(m.duration))
			if r.Float64() < 0.4 {
				t = t.Add(time.Duration(2+r.IntN(30)) * time.Second)
				add(event.Liked, nil)
			}
			return evs
		}
		add(event.ViewPaused, at(progress))
		if r.Float64() < 0.2 {
			return evs
		}
		t = t.Add(time.Duration(30+r.IntN(900)) * time.Second)
		add(event.ViewResumed, at(progress))
	}
}

func cohortUsers(day time.Time) []string {
	n := 25 + dayRand(day, 2).IntN(20)
	users := make([]string, n)
	for i := range users {
		users[i] = fmt.Sprintf("user-%s-%03d", day.Format("20060102"), i)
	}
	return users
}

// activeUsers: the day's new cohort plus returning users from earlier cohorts,
// with a return probability that decays with cohort age.
func activeUsers(day time.Time) []string {
	r := dayRand(day, 3)
	users := cohortUsers(day)
	for age := 1; age <= cohortWindow; age++ {
		p := 0.1 + 0.4*math.Exp(-float64(age)/3)
		for _, u := range cohortUsers(day.AddDate(0, 0, -age)) {
			if r.Float64() < p {
				users = append(users, u)
			}
		}
	}
	return users
}

func userDevice(user string, r *rand.Rand) string {
	if r.Float64() < 0.2 {
		return event.Devices[r.IntN(len(event.Devices))]
	}
	h := fnv.New32a()
	h.Write([]byte(user))
	return event.Devices[h.Sum32()%uint32(len(event.Devices))]
}

func dayRand(day time.Time, salt uint64) *rand.Rand {
	return rand.New(rand.NewPCG(uint64(day.Unix()), salt))
}

func truncDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
