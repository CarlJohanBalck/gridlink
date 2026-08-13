// Package gpu detects local GPU hardware and reports utilization.
package gpu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
)

// ErrNoGPU means no supported GPU is usable on this machine (nvidia-smi
// missing or failing). Callers should register the node as CPU-only.
var ErrNoGPU = errors.New("no supported gpu detected")

// ErrUtilizationUnsupported means the GPU is detectable but its load cannot be
// sampled without extra privileges (Apple Silicon: powermetrics needs root).
// Heartbeats simply omit utilization in that case.
var ErrUtilizationUnsupported = errors.New("gpu utilization unsupported on this platform")

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
	return detect(ctx, runCommand, os.Getenv, runtime.GOOS)
}

// Utilization returns a point-in-time sample for heartbeats.
func Utilization(ctx context.Context) (*computev1.GpuUtilization, error) {
	return utilization(ctx, runCommand, os.Getenv, runtime.GOOS)
}

func detect(ctx context.Context, run execFunc, getenv func(string) string, goos string) (*computev1.GpuInfo, error) {
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

	// Apple Silicon has no nvidia-smi and no CUDA; its GPU is described by
	// system_profiler instead. Detection only reports the hardware — it says
	// nothing about whether containers can reach it (on macOS they cannot).
	if goos == "darwin" {
		return detectApple(ctx, run)
	}

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

func utilization(ctx context.Context, run execFunc, getenv func(string) string, goos string) (*computev1.GpuUtilization, error) {
	if getenv(fakeGPUEnv) == "1" {
		return &computev1.GpuUtilization{}, nil
	}

	// Sampling Apple Silicon GPU load needs powermetrics, which requires root.
	// The agent must not run privileged, so heartbeats go out without a sample.
	if goos == "darwin" {
		return nil, ErrUtilizationUnsupported
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

// ---- Apple Silicon ----

// appleGPUDeviceType is the sppci_device_type marking a GPU entry; the same
// report also lists non-GPU display hardware.
const appleGPUDeviceType = "spdisplays_gpu"

// spDisplays mirrors the subset of `system_profiler SPDisplaysDataType -json`
// the agent needs.
type spDisplays struct {
	GPUs []spGPU `json:"SPDisplaysDataType"`
}

type spGPU struct {
	Name       string `json:"_name"`
	Model      string `json:"sppci_model"`
	Cores      string `json:"sppci_cores"`
	DeviceType string `json:"sppci_device_type"`
}

// detectApple reads the integrated GPU out of system_profiler. VRAM is
// reported as total unified memory (see GpuInfo.vram_total_mb in the proto:
// it is shared with the CPU, not an allocatable budget).
func detectApple(ctx context.Context, run execFunc) (*computev1.GpuInfo, error) {
	out, err := run(ctx, "system_profiler", "SPDisplaysDataType", "-json")
	if err != nil {
		return nil, fmt.Errorf("system_profiler: %v: %w", err, ErrNoGPU)
	}

	var report spDisplays
	if err := json.Unmarshal(out, &report); err != nil {
		return nil, fmt.Errorf("parse system_profiler json: %w", err)
	}

	for _, g := range report.GPUs {
		// Older reports omit sppci_device_type; accept those rather than
		// dropping the only GPU on the machine.
		if g.DeviceType != "" && g.DeviceType != appleGPUDeviceType {
			continue
		}
		model := g.Model
		if model == "" {
			model = g.Name
		}
		if model == "" {
			continue
		}
		if cores := parseUint(g.Cores); cores > 0 {
			model = fmt.Sprintf("%s (%d-core GPU)", model, cores)
		}
		return &computev1.GpuInfo{
			Vendor:        "apple",
			Model:         model,
			VramTotalMb:   unifiedMemoryMB(ctx, run),
			DriverVersion: macOSVersion(ctx, run),
			// CudaVersion stays empty: Apple Silicon has no CUDA.
			GpuCount: 1,
		}, nil
	}
	return nil, fmt.Errorf("system_profiler reported no gpu: %w", ErrNoGPU)
}

// unifiedMemoryMB returns total system memory, which on Apple Silicon is also
// the memory the GPU draws from. Best-effort: 0 when sysctl is unavailable.
func unifiedMemoryMB(ctx context.Context, run execFunc) uint64 {
	out, err := run(ctx, "sysctl", "-n", "hw.memsize")
	if err != nil {
		return 0
	}
	return parseUint(strings.TrimSpace(string(out))) / (1024 * 1024)
}

// macOSVersion stands in for a driver version on Apple Silicon, where the GPU
// driver ships with the OS. Best-effort: empty when unavailable.
func macOSVersion(ctx context.Context, run execFunc) string {
	out, err := run(ctx, "sw_vers", "-productVersion")
	if err != nil {
		return ""
	}
	v := strings.TrimSpace(string(out))
	if v == "" {
		return ""
	}
	return "macOS " + v
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
