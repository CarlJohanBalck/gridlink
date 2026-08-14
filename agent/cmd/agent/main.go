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
	"gridlink/agent/internal/sysinfo"
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

	system := sysinfo.Detect(ctx)

	// Docker is optional now: Mac providers have none by design, and a node
	// simply advertises what it can actually execute. Not being able to run
	// containers is no longer fatal — taking no work at all is what matters.
	var run runner.Runner
	if dr, err := runner.NewDockerRunner(logger); err != nil {
		logger.Info("no container runtime; not advertising container jobs", "err", err)
	} else {
		run = dr
		system.Runners = append(system.Runners, computev1.RunnerKind_RUNNER_KIND_DOCKER)
		// SystemInfo.docker_version stays empty: placement keys off `runners`,
		// and plumbing a version string would mean widening the dockerAPI test
		// seam for a field nothing reads yet.
	}

	// NOTE: RUNNER_KIND_NATIVE_METAL is deliberately NOT advertised yet. The
	// Metal engine lands in a later session; advertising it now would invite
	// deployments this agent cannot serve. Add it here alongside a non-nil
	// client.Config.Deployments, never before.
	if len(system.GetRunners()) == 0 {
		logger.Warn("node has no usable runner; it will register but take no work",
			"os", system.GetOs(), "arch", system.GetArch())
	}

	c := client.New(client.Config{
		CoordinatorAddr: coordAddr,
		Token:           token,
		NodeIDPath:      client.DefaultNodeIDPath(),
		GPU:             info,
		System:          system,
		Utilization:     gpu.Utilization,
		Runner:          run,
		DataPlaneAddr:   os.Getenv("GRIDLINK_DATA_ADDR"),
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
