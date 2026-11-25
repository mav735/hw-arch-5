package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/mav735/hw-arch-5/internal/env"
	"github.com/mav735/hw-arch-5/internal/registry"
	"github.com/mav735/hw-arch-5/schemas"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr := registry.New(env.String("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"))
	subject := schemas.MovieEventSubject

	if err := sr.SetCompatibility(ctx, subject, "BACKWARD"); err != nil {
		fail(err)
	}
	if _, err := sr.Register(ctx, subject, schemas.MovieEvent); err != nil {
		fail(err)
	}
	s, err := sr.Latest(ctx, subject)
	if err != nil {
		fail(err)
	}
	slog.Info("schema registered", "subject", s.Subject, "version", s.Version, "id", s.ID)
}

func fail(err error) {
	slog.Error("schema registration failed", "err", err)
	os.Exit(1)
}
