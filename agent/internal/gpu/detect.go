// Package gpu detects local GPU hardware and reports utilization.
package gpu

import (
	"context"

	computev1 "gridlink/contracts/gen/compute/v1"
)

// Detect inspects the local machine and returns GPU information.
//
// Implementation plan (Phase 1):
//  1. Try `nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader,nounits`.
//     Parse CSV; CUDA version via `nvidia-smi -q` or the smi header.
//  2. If nvidia-smi is absent, return a typed ErrNoGPU so main can log and
//     register as CPU-only (vendor "none").
//  3. AMD/ROCm detection is out of scope for Phase 1.
//
// Must never block > 5s; wrap exec calls with a context timeout. Keep
// exec.Command usage behind a small interface so tests can fake smi output.
func Detect(ctx context.Context) (*computev1.GpuInfo, error) {
	// TODO(claude): implement per the plan above.
	panic("not implemented")
}

// Utilization returns a point-in-time sample for heartbeats, via
// `nvidia-smi --query-gpu=utilization.gpu,memory.used,temperature.gpu`.
func Utilization(ctx context.Context) (*computev1.GpuUtilization, error) {
	// TODO(claude): implement.
	panic("not implemented")
}
