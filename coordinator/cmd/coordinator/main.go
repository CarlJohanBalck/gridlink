// Command coordinator is the GridLink control plane: it accepts agent
// streams, tracks the node registry, and dispatches jobs (Phase 1: via a
// manual AdminService RPC).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"gridlink/coordinator/internal/registry"
	"gridlink/coordinator/internal/scheduler"
	"gridlink/coordinator/internal/server"
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

	if err := server.Serve(ctx, server.Config{
		Addr:      addr,
		Token:     token,
		Registry:  reg,
		Scheduler: sched,
		Logger:    logger,
	}); err != nil && ctx.Err() == nil {
		logger.Error("coordinator exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("coordinator shut down cleanly")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
