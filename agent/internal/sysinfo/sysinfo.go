// Package sysinfo reports the host facts the coordinator needs for placement:
// OS, arch, cores, and total RAM. Runner capability is NOT detected here — it
// depends on what the agent actually managed to initialise, so main composes
// it (see SystemInfo.runners).
package sysinfo

import (
	"context"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
)

// probeTimeout bounds every shell-out so detection cannot hang agent startup.
const probeTimeout = 5 * time.Second

// execFunc runs a command and returns its stdout. Test seam.
type execFunc func(ctx context.Context, name string, args ...string) ([]byte, error)

func runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).Output()
}

// Detect gathers host facts. Best-effort by design: a field the platform will
// not tell us about is left zero rather than failing registration, because a
// node that cannot report its RAM is still a node that can take work.
func Detect(ctx context.Context) *computev1.SystemInfo {
	return detect(ctx, runCommand, runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
}

func detect(ctx context.Context, run execFunc, goos, goarch string, numCPU int) *computev1.SystemInfo {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	return &computev1.SystemInfo{
		Os:            goos,
		Arch:          goarch,
		CpuCores:      uint32(numCPU),
		RamTotalMb:    ramTotalMB(ctx, run, goos),
		DockerVersion: "", // set by main, which owns the Docker client
	}
}

// ramTotalMB reads total physical memory. 0 when unavailable.
func ramTotalMB(ctx context.Context, run execFunc, goos string) uint64 {
	switch goos {
	case "darwin":
		out, err := run(ctx, "sysctl", "-n", "hw.memsize")
		if err != nil {
			return 0
		}
		return parseUint(strings.TrimSpace(string(out))) / (1024 * 1024)
	case "linux":
		// MemTotal is in kB, e.g. "MemTotal:        8318216 kB".
		out, err := run(ctx, "cat", "/proc/meminfo")
		if err != nil {
			return 0
		}
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.HasPrefix(line, "MemTotal:") {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 2 {
				return 0
			}
			return parseUint(fields[1]) / 1024
		}
		return 0
	default:
		return 0
	}
}

func parseUint(s string) uint64 {
	v, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0
	}
	return v
}
