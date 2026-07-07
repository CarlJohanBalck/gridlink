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
	computev1 "gridlink/contracts/gen/compute/v1"
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

	info, err := gpu.Detect(ctx)
	if err != nil {
		logger.Warn("gpu detection failed, registering as cpu-only", "err", err)
		info = &computev1.GpuInfo{Vendor: "none"}
	} else {
		logger.Info("gpu detected",
			"model", info.GetModel(), "vram_mb", info.GetVramTotalMb(), "count", info.GetGpuCount())
	}

	run, err := runner.NewDockerRunner(logger)
	if err != nil {
		logger.Error("docker unavailable", "err", err)
		os.Exit(1)
	}

	c := client.New(client.Config{
		CoordinatorAddr: coordAddr,
		Token:           token,
		NodeIDPath:      client.DefaultNodeIDPath(),
		GPU:             info,
		Utilization:     gpu.Utilization,
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
