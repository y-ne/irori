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
		slog.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer database.Close()

	concurrency := runtime.GOMAXPROCS(0)
	batchSize := concurrency * 3

	slog.LogAttrs(ctx, slog.LevelInfo, "irori started",
		slog.Int("concurrency", concurrency),
		slog.Int("batch_size", batchSize))

	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				processBatch(ctx, batchSize, concurrency)
			}
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	slog.LogAttrs(ctx, slog.LevelInfo, "shutting down")
	cancel()
}

func processBatch(ctx context.Context, batchSize, concurrency int) {
	tx, err := database.Pool.Begin(ctx)
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "begin tx failed", slog.Any("error", err))
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
		slog.LogAttrs(ctx, slog.LevelError, "query failed", slog.Any("error", err))
		return
	}

	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Item, error) {
		var item Item
		err := row.Scan(&item.ID, &item.Payload, &item.RetryCount, &item.MaxRetries)
		return item, err
	})
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "scan failed", slog.Any("error", err))
		return
	}

	if len(items) == 0 {
		return
	}

	itemIDs := make([]int64, len(items))
	for i, item := range items {
		itemIDs[i] = item.ID
	}

	_, err = tx.Exec(ctx, `UPDATE irori SET status = 'processing' WHERE id = ANY($1)`, itemIDs)
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "update failed", slog.Any("error", err))
		return
	}

	if err = tx.Commit(ctx); err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "commit failed", slog.Any("error", err))
		return
	}

	slog.LogAttrs(ctx, slog.LevelInfo, "processing", slog.Int("count", len(items)))

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

	// TODO: Your business logic here
	err := process(ctx, item)

	if err == nil {
		if _, err := database.Pool.Exec(ctx, `UPDATE irori SET status = 'success', completed_at = NOW() WHERE id = $1`, item.ID); err != nil {
			slog.LogAttrs(ctx, slog.LevelError, "failed to mark success", slog.Int64("id", item.ID), slog.Any("error", err))
		} else {
			slog.LogAttrs(ctx, slog.LevelInfo, "success", slog.Int64("id", item.ID))
		}
	} else {
		retry(ctx, item, err)
	}
}

func process(ctx context.Context, item Item) error {
	var payload map[string]any
	if err := json.Unmarshal(item.Payload, &payload); err != nil {
		return err
	}

	slog.LogAttrs(ctx, slog.LevelInfo, "processing",
		slog.Int64("id", item.ID),
		slog.Any("payload", payload))

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
		delay := min(
			time.Duration(float64(backoff)*math.Pow(2, float64(newCount))),
			maxBackoff,
		)

		if _, err := database.Pool.Exec(ctx, `
			UPDATE irori
			SET status = 'pending', retry_count = $2, next_retry_at = $3, errors = array_append(errors, $4)
			WHERE id = $1
		`, item.ID, newCount, time.Now().Add(delay), errJSON); err != nil {
			slog.LogAttrs(ctx, slog.LevelError, "failed to schedule retry",
				slog.Int64("id", item.ID),
				slog.Any("error", err))
		} else {
			slog.LogAttrs(ctx, slog.LevelWarn, "retry",
				slog.Int64("id", item.ID),
				slog.Int("attempt", newCount),
				slog.Duration("next", delay))
		}
	} else {
		if _, err := database.Pool.Exec(ctx, `
			UPDATE irori
			SET status = 'failed', retry_count = $2, completed_at = NOW(), errors = array_append(errors, $3)
			WHERE id = $1
		`, item.ID, newCount, errJSON); err != nil {
			slog.LogAttrs(ctx, slog.LevelError, "failed to mark as failed",
				slog.Int64("id", item.ID),
				slog.Any("error", err))
		} else {
			slog.LogAttrs(ctx, slog.LevelError, "failed",
				slog.Int64("id", item.ID),
				slog.String("error", processErr.Error()))
		}
	}
}
