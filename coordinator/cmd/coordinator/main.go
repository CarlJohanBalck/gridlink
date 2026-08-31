// Command coordinator is the GridLink control plane: it accepts agent
// streams, tracks the node registry, and dispatches jobs (Phase 1: via a
// manual AdminService RPC).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"gridlink/coordinator/internal/deployments"
	"gridlink/coordinator/internal/registry"
	"gridlink/coordinator/internal/scheduler"
	"gridlink/coordinator/internal/server"
	"gridlink/coordinator/internal/store"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	addr := envOr("GRIDLINK_LISTEN", ":50051")
	token := os.Getenv("GRIDLINK_TOKEN")
	if token == "" {
		logger.Error("GRIDLINK_TOKEN is required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reg := registry.New(logger)
	sched := scheduler.New(reg, logger)

	deps := deployments.New(reg, logger)

	usageStore, err := openStore(ctx, logger)
	if err != nil {
		logger.Error("cannot open usage store", "err", err)
		os.Exit(1)
	}
	defer usageStore.Close()

	if err := server.Serve(ctx, server.Config{
		Addr:        addr,
		Token:       token,
		Registry:    reg,
		Scheduler:   sched,
		Deployments: deps,
		Store:       usageStore,
		Logger:      logger,
	}); err != nil && ctx.Err() == nil {
		logger.Error("coordinator exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("coordinator shut down cleanly")
}

// openStore picks the usage sink. GRIDLINK_DATABASE_URL is the real ledger;
// GRIDLINK_USAGE_LOG is the dev fallback. Refusing to start without either is
// deliberate: silently discarding usage would mean serving traffic that nobody
// gets paid for, and the failure would be invisible until someone asked where
// the money went.
func openStore(ctx context.Context, logger *slog.Logger) (store.Store, error) {
	if dsn := os.Getenv("GRIDLINK_DATABASE_URL"); dsn != "" {
		s, err := store.OpenPostgres(ctx, dsn)
		if err != nil {
			return nil, fmt.Errorf("postgres: %w", err)
		}
		logger.Info("usage ledger: postgres")
		return s, nil
	}
	path := os.Getenv("GRIDLINK_USAGE_LOG")
	if path == "" {
		return nil, fmt.Errorf("set GRIDLINK_DATABASE_URL (ledger) or GRIDLINK_USAGE_LOG (dev)")
	}
	logger.Warn("usage ledger: jsonl file; summaries unavailable — set GRIDLINK_DATABASE_URL for the real ledger",
		"path", path)
	return store.OpenJSONL(path), nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
