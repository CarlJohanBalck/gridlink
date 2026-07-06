// Command agent is the GridLink provider daemon. It detects local GPU
// hardware, opens a single outbound bidirectional stream to the coordinator,
// heartbeats, and runs containerized jobs it is assigned.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"gridlink/agent/internal/client"
	"gridlink/agent/internal/gpu"
	"gridlink/agent/internal/runner"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	coordAddr := envOr("GRIDLINK_COORDINATOR", "localhost:50051")
	token := os.Getenv("GRIDLINK_TOKEN")
	if token == "" {
		logger.Error("GRIDLINK_TOKEN is required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// TODO(claude): implement gpu.Detect (nvidia-smi CSV parsing, NVML optional).
	info, err := gpu.Detect(ctx)
	if err != nil {
		logger.Warn("gpu detection failed, registering as cpu-only", "err", err)
	}

	// TODO(claude): implement runner.NewDockerRunner (Docker SDK, --gpus support).
	run, err := runner.NewDockerRunner(logger)
	if err != nil {
		logger.Error("docker unavailable", "err", err)
		os.Exit(1)
	}

	// TODO(claude): implement the connect/re-register/backoff loop in client.
	c := client.New(client.Config{
		CoordinatorAddr: coordAddr,
		Token:           token,
		NodeIDPath:      client.DefaultNodeIDPath(),
		GPU:             info,
		Runner:          run,
		Logger:          logger,
	})

	if err := c.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("agent exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("agent shut down cleanly")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
