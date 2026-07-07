// Package gpu detects local GPU hardware and reports utilization.
package gpu

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
)

// ErrNoGPU means no supported GPU is usable on this machine (nvidia-smi
// missing or failing). Callers should register the node as CPU-only.
var ErrNoGPU = errors.New("no supported gpu detected")

// smiTimeout bounds every nvidia-smi invocation so detection can never hang
// agent startup or a heartbeat.
const smiTimeout = 5 * time.Second

// fakeGPUEnv turns on a synthetic GPU for dev setups without hardware
// (docker-compose agent). AMD/ROCm detection is out of scope for Phase 1.
const fakeGPUEnv = "GRIDLINK_FAKE_GPU"

// execFunc runs a command and returns its stdout. It is the seam tests use to
// fake nvidia-smi output.
type execFunc func(ctx context.Context, name string, args ...string) ([]byte, error)

func runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).Output()
}

// Detect inspects the local machine and returns GPU information.
func Detect(ctx context.Context) (*computev1.GpuInfo, error) {
	return detect(ctx, runCommand, os.Getenv)
}

// Utilization returns a point-in-time sample for heartbeats.
func Utilization(ctx context.Context) (*computev1.GpuUtilization, error) {
	return utilization(ctx, runCommand, os.Getenv)
}

func detect(ctx context.Context, run execFunc, getenv func(string) string) (*computev1.GpuInfo, error) {
	if getenv(fakeGPUEnv) == "1" {
		return &computev1.GpuInfo{
			Vendor:        "nvidia",
			Model:         "FAKE GPU (GRIDLINK_FAKE_GPU=1)",
			VramTotalMb:   24576,
			DriverVersion: "0.0-fake",
			CudaVersion:   "12.4",
			GpuCount:      1,
		}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, smiTimeout)
	defer cancel()

	out, err := run(ctx, "nvidia-smi",
		"--query-gpu=name,memory.total,driver_version",
		"--format=csv,noheader,nounits")
	if err != nil {
		// A missing binary and a failing driver mean the same thing to the
		// caller: nothing here can take GPU work.
		return nil, fmt.Errorf("nvidia-smi: %v: %w", err, ErrNoGPU)
	}

	lines := nonEmptyLines(string(out))
	if len(lines) == 0 {
		return nil, fmt.Errorf("nvidia-smi reported no devices: %w", ErrNoGPU)
	}
	fields := splitSmiLine(lines[0], 3)
	if fields == nil {
		return nil, fmt.Errorf("unexpected nvidia-smi output %q", lines[0])
	}

	return &computev1.GpuInfo{
		Vendor:        "nvidia",
		Model:         fields[0],
		VramTotalMb:   parseUint(fields[1]),
		DriverVersion: fields[2],
		CudaVersion:   cudaVersion(ctx, run),
		GpuCount:      uint32(len(lines)),
	}, nil
}

func utilization(ctx context.Context, run execFunc, getenv func(string) string) (*computev1.GpuUtilization, error) {
	if getenv(fakeGPUEnv) == "1" {
		return &computev1.GpuUtilization{}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, smiTimeout)
	defer cancel()

	out, err := run(ctx, "nvidia-smi",
		"--query-gpu=utilization.gpu,memory.used,temperature.gpu",
		"--format=csv,noheader,nounits")
	if err != nil {
		return nil, fmt.Errorf("nvidia-smi: %v: %w", err, ErrNoGPU)
	}

	lines := nonEmptyLines(string(out))
	if len(lines) == 0 {
		return nil, fmt.Errorf("nvidia-smi reported no devices: %w", ErrNoGPU)
	}
	fields := splitSmiLine(lines[0], 3)
	if fields == nil {
		return nil, fmt.Errorf("unexpected nvidia-smi output %q", lines[0])
	}

	return &computev1.GpuUtilization{
		GpuPercent:   uint32(parseUint(fields[0])),
		VramUsedMb:   parseUint(fields[1]),
		TemperatureC: uint32(parseUint(fields[2])),
	}, nil
}

// cudaRe matches the plain `nvidia-smi` banner; there is no --query-gpu field
// for the CUDA version.
var cudaRe = regexp.MustCompile(`CUDA Version:\s*([0-9][0-9.]*)`)

// cudaVersion is best-effort: empty when the banner is unavailable.
func cudaVersion(ctx context.Context, run execFunc) string {
	out, err := run(ctx, "nvidia-smi")
	if err != nil {
		return ""
	}
	m := cudaRe.FindSubmatch(out)
	if m == nil {
		return ""
	}
	return string(m[1])
}

func nonEmptyLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

// splitSmiLine splits one CSV line into exactly `want` trimmed fields. GPU
// names may themselves contain commas (e.g. "NVIDIA H100, PCIe"), so the
// fixed-format fields are taken from the right and the remainder is rejoined
// as the first field. Returns nil if there are fewer than `want` fields.
func splitSmiLine(line string, want int) []string {
	parts := strings.Split(line, ",")
	if len(parts) < want {
		return nil
	}
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	if len(parts) > want {
		head := strings.Join(parts[:len(parts)-(want-1)], ", ")
		parts = append([]string{head}, parts[len(parts)-(want-1):]...)
	}
	return parts
}

// parseUint reads a non-negative integer, returning 0 for anything else —
// nvidia-smi emits "[N/A]" for fields some GPUs don't report, and a zero is
// more useful than failing the whole sample.
func parseUint(s string) uint64 {
	v, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0
	}
	return v
}
