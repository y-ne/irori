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

	slog.Info("irori started", "concurrency", concurrency, "batch_size", batchSize)

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

	slog.Info("shutting down")
	cancel()
}

func processBatch(ctx context.Context, batchSize, concurrency int) {
	tx, err := database.Pool.Begin(ctx)
	if err != nil {
		slog.Error("begin tx failed", "error", err)
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
		slog.Error("query failed", "error", err)
		return
	}

	var items []Item
	for rows.Next() {
		var item Item
		rows.Scan(&item.ID, &item.Payload, &item.RetryCount, &item.MaxRetries)
		items = append(items, item)
	}
	rows.Close()

	if len(items) == 0 {
		return
	}

	itemIDs := make([]int64, len(items))
	for i, item := range items {
		itemIDs[i] = item.ID
	}

	_, err = tx.Exec(ctx, `UPDATE irori SET status = 'processing' WHERE id = ANY($1)`, itemIDs)
	if err != nil {
		slog.Error("update failed", "error", err)
		return
	}

	if err = tx.Commit(ctx); err != nil {
		slog.Error("commit failed", "error", err)
		return
	}

	slog.Info("processing", "count", len(items))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for _, item := range items {
		item := item
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
		database.Pool.Exec(ctx, `UPDATE irori SET status = 'success', completed_at = NOW() WHERE id = $1`, item.ID)
		slog.Info("success", "id", item.ID)
	} else {
		retry(ctx, item, err)
	}
}

func process(ctx context.Context, item Item) error {
	var payload map[string]any
	if err := json.Unmarshal(item.Payload, &payload); err != nil {
		return err
	}

	slog.Info("processing", "id", item.ID, "payload", payload)

	// TODO: Implement your logic here

	return nil
}

func retry(ctx context.Context, item Item, err error) {
	newCount := item.RetryCount + 1

	errJSON, _ := json.Marshal(map[string]any{
		"time":    time.Now().UTC().Format(time.RFC3339),
		"error":   err.Error(),
		"attempt": newCount,
	})

	if newCount < item.MaxRetries {
		delay := min(
			time.Duration(float64(backoff)*math.Pow(2, float64(newCount))),
			maxBackoff,
		)

		database.Pool.Exec(ctx, `
			UPDATE irori
			SET status = 'pending', retry_count = $2, next_retry_at = $3, errors = array_append(errors, $4)
			WHERE id = $1
		`, item.ID, newCount, time.Now().Add(delay), errJSON)

		slog.Warn("retry", "id", item.ID, "attempt", newCount, "next", delay)
	} else {
		database.Pool.Exec(ctx, `
			UPDATE irori
			SET status = 'failed', retry_count = $2, completed_at = NOW(), errors = array_append(errors, $3)
			WHERE id = $1
		`, item.ID, newCount, errJSON)

		slog.Error("failed", "id", item.ID, "error", err)
	}
}
