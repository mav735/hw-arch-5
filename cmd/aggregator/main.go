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

	"github.com/mav735/hw-arch-5/internal/aggregator"
	"github.com/mav735/hw-arch-5/internal/env"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ch, err := aggregator.NewClickHouse(
		env.String("CLICKHOUSE_ADDR", "clickhouse:9000"),
		env.String("CLICKHOUSE_DB", "cinema"),
		env.String("CLICKHOUSE_USER", "default"),
		env.String("CLICKHOUSE_PASSWORD", ""))
	if err != nil {
		fatal(log, "clickhouse", err)
	}

	store, err := aggregator.NewStore(ctx, env.String("POSTGRES_DSN", "postgres://metrics:metrics@postgres:5432/metrics"), log)
	if err != nil {
		fatal(log, "postgres", err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		fatal(log, "postgres migrations", err)
	}

	exporter, err := aggregator.NewExporter(
		env.String("S3_ENDPOINT", "minio:9000"),
		env.String("S3_ACCESS_KEY", "minio"),
		env.String("S3_SECRET_KEY", "minio12345"),
		env.String("S3_BUCKET", "movie-analytics"), store)
	if err != nil {
		fatal(log, "s3", err)
	}

	svc := aggregator.NewService(ch, store, exporter, log)
	go svc.Run(ctx,
		env.Duration("AGGREGATION_INTERVAL", time.Minute),
		env.Duration("EXPORT_INTERVAL", time.Hour),
		env.Int("AGGREGATION_LOOKBACK_DAYS", 14))

	gin.SetMode(gin.ReleaseMode)
	srv := &http.Server{Addr: env.String("HTTP_ADDR", ":8080"), Handler: aggregator.Router(svc)}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Info("aggregator listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal(log, "http server", err)
	}
}

func fatal(log *slog.Logger, what string, err error) {
	log.Error(what, "err", err)
	os.Exit(1)
}
