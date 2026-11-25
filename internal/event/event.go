package event

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
)

const (
	ViewStarted  = "VIEW_STARTED"
	ViewFinished = "VIEW_FINISHED"
	ViewPaused   = "VIEW_PAUSED"
	ViewResumed  = "VIEW_RESUMED"
	Liked        = "LIKED"
	Searched     = "SEARCHED"
)

var (
	eventTypes  = map[string]bool{ViewStarted: true, ViewFinished: true, ViewPaused: true, ViewResumed: true, Liked: true, Searched: true}
	viewEvents  = map[string]bool{ViewStarted: true, ViewFinished: true, ViewPaused: true, ViewResumed: true}
	deviceTypes = map[string]bool{"MOBILE": true, "DESKTOP": true, "TV": true, "TABLET": true}
	Devices     = []string{"MOBILE", "DESKTOP", "TV", "TABLET"}
)

type Event struct {
	EventID         string    `json:"event_id" avro:"event_id"`
	UserID          string    `json:"user_id" avro:"user_id"`
	MovieID         string    `json:"movie_id" avro:"movie_id"`
	EventType       string    `json:"event_type" avro:"event_type"`
	Timestamp       time.Time `json:"timestamp" avro:"timestamp"`
	DeviceType      string    `json:"device_type" avro:"device_type"`
	SessionID       string    `json:"session_id" avro:"session_id"`
	ProgressSeconds *int32    `json:"progress_seconds,omitempty" avro:"progress_seconds"`
}

var ErrInvalid = errors.New("invalid event")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

func (e *Event) Validate() error {
	if _, err := uuid.Parse(e.EventID); err != nil {
		return invalid("event_id must be a UUID")
	}
	switch {
	case e.UserID == "":
		return invalid("user_id is required")
	case e.MovieID == "":
		return invalid("movie_id is required")
	case e.SessionID == "":
		return invalid("session_id is required")
	case !eventTypes[e.EventType]:
		return invalid("unknown event_type %q", e.EventType)
	case !deviceTypes[e.DeviceType]:
		return invalid("unknown device_type %q", e.DeviceType)
	case e.Timestamp.IsZero():
		return invalid("timestamp is required")
	}
	if e.ProgressSeconds != nil && *e.ProgressSeconds < 0 {
		return invalid("progress_seconds must be >= 0")
	}
	if viewEvents[e.EventType] && e.ProgressSeconds == nil {
		return invalid("progress_seconds is required for %s", e.EventType)
	}
	return nil
}

// Codec writes the Confluent wire format: magic byte, schema id, Avro body.
type Codec struct {
	id     int
	schema avro.Schema
}

func NewCodec(id int, schemaJSON string) (*Codec, error) {
	s, err := avro.Parse(schemaJSON)
	if err != nil {
		return nil, err
	}
	return &Codec{id: id, schema: s}, nil
}

func (c *Codec) Encode(e *Event) ([]byte, error) {
	e.Timestamp = e.Timestamp.UTC()
	body, err := avro.Marshal(c.schema, e)
	if err != nil {
		return nil, invalid("%v", err)
	}
	out := make([]byte, 5, 5+len(body))
	binary.BigEndian.PutUint32(out[1:], uint32(c.id))
	return append(out, body...), nil
}
