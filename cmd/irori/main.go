package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"golang.org/x/sync/errgroup"

	"irori/internal/database"
)

const (
	pollInterval = 5 * time.Second
	timeout      = 30 * time.Second
	maxRetries   = 5
	backoff      = 1 * time.Second
	maxBackoff   = 5 * time.Minute
)

type Item struct {
	ID         int64
	Payload    json.RawMessage
	RetryCount int
	MaxRetries int
}

func main() {
	_ = godotenv.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := database.Connect(ctx); err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "database connection failed", slog.Any("error", err))
		os.Exit(1)
	}
	defer database.Close()

	concurrency := runtime.GOMAXPROCS(0)
	slog.LogAttrs(ctx, slog.LevelInfo, "started", slog.Int("concurrency", concurrency))

	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				processBatch(ctx, concurrency*3, concurrency)
			}
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	cancel()
}

func processBatch(ctx context.Context, batchSize, concurrency int) {
	tx, err := database.Pool.Begin(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, payload, retry_count, max_retries
		FROM irori
		WHERE status = 'pending' AND next_retry_at <= NOW()
		ORDER BY next_retry_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, batchSize)
	if err != nil {
		return
	}

	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Item, error) {
		var item Item
		err := row.Scan(&item.ID, &item.Payload, &item.RetryCount, &item.MaxRetries)
		return item, err
	})
	if err != nil || len(items) == 0 {
		return
	}

	itemIDs := make([]int64, len(items))
	for i, item := range items {
		itemIDs[i] = item.ID
	}

	if _, err = tx.Exec(ctx, `UPDATE irori SET status = 'processing' WHERE id = ANY($1)`, itemIDs); err != nil {
		return
	}

	if err = tx.Commit(ctx); err != nil {
		return
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for _, item := range items {
		g.Go(func() error {
			processItem(gctx, item)
			return nil
		})
	}

	g.Wait()
}

func processItem(ctx context.Context, item Item) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := process(ctx, item); err != nil {
		retry(ctx, item, err)
		return
	}

	database.Pool.Exec(ctx, `UPDATE irori SET status = 'success', completed_at = NOW() WHERE id = $1`, item.ID)
}

func process(ctx context.Context, item Item) error {
	var payload map[string]any
	json.Unmarshal(item.Payload, &payload)

	// TODO: Implement your logic here

	return nil
}

func retry(ctx context.Context, item Item, processErr error) {
	newCount := item.RetryCount + 1
	errJSON, _ := json.Marshal(map[string]any{
		"time":    time.Now().UTC().Format(time.RFC3339),
		"error":   processErr.Error(),
		"attempt": newCount,
	})

	if newCount < item.MaxRetries {
		delay := min(time.Duration(float64(backoff)*math.Pow(2, float64(newCount))), maxBackoff)
		database.Pool.Exec(ctx, `
			UPDATE irori
			SET status = 'pending', retry_count = $2, next_retry_at = $3, errors = array_append(errors, $4)
			WHERE id = $1
		`, item.ID, newCount, time.Now().Add(delay), errJSON)
		slog.LogAttrs(ctx, slog.LevelWarn, "retry", slog.Int64("id", item.ID), slog.Int("attempt", newCount))
	} else {
		database.Pool.Exec(ctx, `
			UPDATE irori
			SET status = 'failed', retry_count = $2, completed_at = NOW(), errors = array_append(errors, $3)
			WHERE id = $1
		`, item.ID, newCount, errJSON)
		slog.LogAttrs(ctx, slog.LevelError, "failed", slog.Int64("id", item.ID), slog.String("error", processErr.Error()))
	}
}
