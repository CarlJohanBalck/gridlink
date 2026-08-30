package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"gridlink/agent/internal/engine"
	"gridlink/agent/internal/gpu"
	"gridlink/agent/internal/runner"
	"gridlink/agent/internal/sysinfo"
)

// runDoctor reports what this machine can contribute, and exits non-zero if
// the answer is "nothing".
//
// It exists for two audiences. A prospective provider runs it to find out
// whether their machine qualifies before signing up for anything. And it is
// the release smoke test: it exercises the cgo link and initialises Metal,
// which `go build` cannot check and which only fails when actually executed.
func runDoctor(_ []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Detection logs at info level; the report is the output here, so keep the
	// logger quiet. llama.cpp writes its own banner straight to stderr, which
	// would interleave with the report, so silence that too.
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine.Silence()

	sys := sysinfo.Detect(ctx)
	fmt.Printf("GridLink agent\n")
	fmt.Printf("  os/arch      %s/%s\n", sys.GetOs(), sys.GetArch())
	fmt.Printf("  cpu cores    %d\n", sys.GetCpuCores())
	fmt.Printf("  memory       %d MB\n", sys.GetRamTotalMb())

	if info, err := gpu.Detect(ctx); err == nil {
		fmt.Printf("  gpu          %s (%s)\n", info.GetModel(), info.GetVendor())
	} else {
		fmt.Printf("  gpu          none detected (%v)\n", err)
	}

	fmt.Printf("\nCapabilities\n")
	canWork := false

	// GPU inference. GPUStats is what actually touches Metal, so a broken cgo
	// link or a missing backend surfaces here rather than at first job.
	switch {
	case !engine.Supported():
		fmt.Printf("  gpu inference    no (needs an Apple Silicon Mac)\n")
	default:
		st, err := engine.GPUStats()
		if err != nil {
			fmt.Printf("  gpu inference    NO — engine present but the GPU is unusable: %v\n", err)
		} else {
			fmt.Printf("  gpu inference    yes — %s, %d MB usable\n", st.GPUName, st.UsableVRAMMb)
			canWork = true
		}
	}

	// Container jobs.
	if _, err := runner.NewDockerRunner(quiet); err != nil {
		fmt.Printf("  container jobs   no (no reachable Docker daemon)\n")
	} else {
		fmt.Printf("  container jobs   yes\n")
		canWork = true
	}

	fmt.Println()
	if !canWork {
		fmt.Println("This machine cannot take any work: it would register and sit idle.")
		return fmt.Errorf("no usable runner")
	}
	fmt.Println("Ready to provide. Start with:")
	fmt.Println("  GRIDLINK_TOKEN=<token> GRIDLINK_COORDINATOR=<host:50051> gridlink-agent")
	return nil
}

// usage is printed for an unrecognised subcommand.
func usage() {
	fmt.Fprintf(os.Stderr, `gridlink-agent — contribute this machine's GPU to GridLink

usage:
  gridlink-agent            run the agent (needs GRIDLINK_TOKEN, GRIDLINK_COORDINATOR)
  gridlink-agent doctor     report what this machine can contribute
  gridlink-agent engine     internal: serve one model (spawned by the agent)
`)
}
