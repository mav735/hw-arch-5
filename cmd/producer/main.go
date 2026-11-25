package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/mav735/hw-arch-5/internal/env"
	"github.com/mav735/hw-arch-5/internal/event"
	"github.com/mav735/hw-arch-5/internal/producer"
	"github.com/mav735/hw-arch-5/internal/registry"
	"github.com/mav735/hw-arch-5/schemas"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sr := registry.New(env.String("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"))
	s, err := sr.Latest(ctx, schemas.MovieEventSubject)
	if err != nil {
		log.Error("load schema", "err", err)
		os.Exit(1)
	}
	codec, err := event.NewCodec(s.ID, s.Schema)
	if err != nil {
		log.Error("parse schema", "err", err)
		os.Exit(1)
	}
	log.Info("schema loaded", "subject", s.Subject, "version", s.Version, "id", s.ID)

	pub, err := producer.NewPublisher(
		env.List("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
		env.String("KAFKA_TOPIC", "movie-events"), codec, log)
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer pub.Close()

	if env.Bool("GENERATOR_ENABLED", true) {
		gen := producer.NewGenerator(pub,
			env.Float("GENERATOR_EVENTS_PER_SECOND", 5),
			env.Int("GENERATOR_BACKFILL_DAYS", 14), log)
		go gen.Run(ctx)
	}

	gin.SetMode(gin.ReleaseMode)
	srv := &http.Server{Addr: env.String("HTTP_ADDR", ":8080"), Handler: producer.Router(pub)}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Info("producer listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http server", "err", err)
		os.Exit(1)
	}
}
