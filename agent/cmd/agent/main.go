// Command agent is the GridLink provider daemon. It detects local GPU
// hardware, opens a single outbound bidirectional stream to the coordinator,
// heartbeats, and runs containerized jobs it is assigned.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"gridlink/agent/internal/client"
	"gridlink/agent/internal/deploy"
	"gridlink/agent/internal/engine"
	"gridlink/agent/internal/gpu"
	"gridlink/agent/internal/runner"
	"gridlink/agent/internal/sysinfo"
	computev1 "gridlink/contracts/gen/compute/v1"
)

func main() {
	// One binary, several roles. `engine` is spawned by the agent itself as a
	// sandboxed subprocess, so providers still install exactly one file.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "engine":
			if err := runEngine(os.Args[2:]); err != nil {
				slog.New(slog.NewTextHandler(os.Stderr, nil)).Error("engine failed", "err", err)
				os.Exit(1)
			}
			return
		case "doctor":
			if err := runDoctor(os.Args[2:]); err != nil {
				os.Exit(1)
			}
			return
		case "-h", "--help", "help":
			usage()
			return
		default:
			fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
			usage()
			os.Exit(2)
		}
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	coordAddr := envOr("GRIDLINK_COORDINATOR", "localhost:50051")
	// Where the gateway reaches this node's engines. Loopback works only when
	// the gateway runs on this machine.
	dataPlaneAddr := envOr("GRIDLINK_DATA_ADDR", "127.0.0.1")
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

	// The native Metal engine. Advertised only when it actually exists in this
	// build, so a node never attracts deployments it cannot serve.
	var deployments deploy.Manager
	if engine.Supported() {
		// The engine binds the same address the agent advertises, or the
		// gateway would be told to dial a port that only exists on loopback.
		nm, err := deploy.NewNativeManager(logger, dataPlaneAddr)
		if err != nil {
			logger.Error("metal engine unavailable", "err", err)
		} else {
			deployments = nm
			system.Runners = append(system.Runners, computev1.RunnerKind_RUNNER_KIND_NATIVE_METAL)
			// usable_vram_mb is what placement uses; total unified memory
			// overstates it by ~29% on Apple Silicon.
			if st, err := engine.GPUStats(); err == nil && info != nil {
				info.UsableVramMb = st.UsableVRAMMb
				logger.Info("metal engine ready",
					"gpu", st.GPUName, "usable_vram_mb", st.UsableVRAMMb)
			}
		}
	}

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
		Deployments:     deployments,
		DataPlaneAddr:   dataPlaneAddr,
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
