package database

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var Pool *pgxpool.Pool

func Connect(ctx context.Context) error {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return fmt.Errorf("DATABASE_URL not set")
	}

	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return err
	}

	maxConns := int32(runtime.GOMAXPROCS(0)*2 + 5)

	cfg.MaxConns = maxConns
	cfg.MinConns = 5
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute
	cfg.HealthCheckPeriod = time.Minute
	cfg.ConnConfig.ConnectTimeout = 10 * time.Second

	Pool, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}

	if err = Pool.Ping(ctx); err != nil {
		return err
	}

	slog.LogAttrs(ctx, slog.LevelInfo, "database connected",
		slog.Int("max_conns", int(cfg.MaxConns)),
		slog.Int("min_conns", int(cfg.MinConns)))

	return nil
}

func Close() {
	if Pool != nil {
		Pool.Close()
	}
}
