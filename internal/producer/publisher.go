package producer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mav735/hw-arch-5/internal/event"
)

type Publisher struct {
	client      *kgo.Client
	codec       *event.Codec
	topic       string
	maxAttempts int
	baseBackoff time.Duration
	maxBackoff  time.Duration
	log         *slog.Logger
}

func NewPublisher(brokers []string, topic string, codec *event.Codec, log *slog.Logger) (*Publisher, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.RecordDeliveryTimeout(10*time.Second),
	)
	if err != nil {
		return nil, err
	}
	return &Publisher{
		client:      cl,
		codec:       codec,
		topic:       topic,
		maxAttempts: 5,
		baseBackoff: 200 * time.Millisecond,
		maxBackoff:  5 * time.Second,
		log:         log,
	}, nil
}

func (p *Publisher) Close() { p.client.Close() }

func (p *Publisher) Publish(ctx context.Context, e *event.Event) error {
	return p.PublishBatch(ctx, []event.Event{*e})
}

// PublishBatch keys records by user_id so one user's events stay ordered in one
// partition, and retries failed records with exponential backoff.
func (p *Publisher) PublishBatch(ctx context.Context, events []event.Event) error {
	pending := make([]*event.Event, 0, len(events))
	values := make(map[string][]byte, len(events))
	for i := range events {
		e := &events[i]
		if err := e.Validate(); err != nil {
			return err
		}
		v, err := p.codec.Encode(e)
		if err != nil {
			return err
		}
		values[e.EventID] = v
		pending = append(pending, e)
	}

	backoff := p.baseBackoff
	for attempt := 1; ; attempt++ {
		recs := make([]*kgo.Record, len(pending))
		for i, e := range pending {
			recs[i] = &kgo.Record{Topic: p.topic, Key: []byte(e.UserID), Value: values[e.EventID]}
		}
		results := p.client.ProduceSync(ctx, recs...)

		var failed []*event.Event
		var lastErr error
		for i, r := range results {
			e := pending[i]
			if r.Err != nil {
				failed = append(failed, e)
				lastErr = r.Err
				continue
			}
			p.log.Info("event published",
				"event_id", e.EventID, "event_type", e.EventType, "timestamp", e.Timestamp.Format(time.RFC3339Nano),
				"partition", r.Record.Partition, "offset", r.Record.Offset)
		}
		if len(failed) == 0 {
			return nil
		}
		if attempt >= p.maxAttempts || ctx.Err() != nil {
			return fmt.Errorf("publish %d events after %d attempts: %w", len(failed), attempt, lastErr)
		}
		p.log.Warn("publish failed, retrying", "failed", len(failed), "attempt", attempt, "backoff", backoff, "err", lastErr)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff = min(backoff*2, p.maxBackoff)
		pending = failed
	}
}
