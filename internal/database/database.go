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

	slog.Info("database connected",
		"max_conns", cfg.MaxConns,
		"min_conns", cfg.MinConns,
		"max_conn_lifetime", cfg.MaxConnLifetime,
		"max_conn_idle_time", cfg.MaxConnIdleTime,
	)

	return nil
}

func Close() {
	if Pool != nil {
		slog.Info("closing database connection pool")
		Pool.Close()
		slog.Info("database connection pool closed")
	}
}
