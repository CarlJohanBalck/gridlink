// Package deploy runs long-lived inference servers (Phase 2). Sibling of
// runner (one-shot jobs).
package deploy

import (
	"context"

	computev1 "gridlink/contracts/gen/compute/v1"
)

// Update mirrors runner.Update for deployment lifecycle.
type Update struct {
	DeploymentID string
	State        computev1.DeploymentState
	HostPort     uint32
	Err          string
	// Progress is 0-100 and meaningful only during PULLING, where GB-scale
	// weight downloads otherwise look identical to a hung deployment.
	Progress uint32
}

// Manager brings up deployments and streams their lifecycle. Interface (like
// runner.Runner) so tests use a fake instead of a real engine, a real download,
// or a real GPU.
//
// Stopping is by context cancellation, exactly as with jobs: the coordinator's
// StopDeployment cancels the deployment's context, and the Manager must then
// tear the engine down and emit STOPPED before closing the channel.
type Manager interface {
	// Start brings up the deployment and streams Updates until a terminal
	// state (FAILED / STOPPED), then closes the channel. READY is NOT
	// terminal — a healthy deployment sits in READY until stopped.
	// Cancelling ctx must tear down the engine and emit STOPPED.
	Start(ctx context.Context, spec *computev1.DeploymentSpec) (<-chan Update, error)
}

// Implementation plan (Phase 2) — NativeEngine (primary, macOS):
//   - resolve spec.Native.ModelFile in ModelRef@Revision; download to
//     ~/.gridlink/models/ if absent (state PULLING, report Progress; weights
//     are GB-scale on consumer uplinks)
//   - verify SHA-256 against spec.Native.Sha256 BEFORE loading; a mismatch is
//     FAILED, never a load attempt
//   - spawn the agent binary as `agent engine` under sandbox-exec, bound to a
//     free localhost port; state LOADING
//   - warm the engine once at agent startup: the first-ever Metal shader
//     compile costs ~6.5s, ~0.01s cached thereafter
//   - poll GET 127.0.0.1:<hostPort>/v1/models -> READY
//   - engine process exits or health fails 3x -> FAILED with its stderr tail
//   - report Metal's recommendedMaxWorkingSetSize back as
//     GpuInfo.usable_vram_mb; the engine has cgo, the detector does not
//
// Implementation plan (Phase 2) — ContainerEngine (Linux + NVIDIA only):
//   - pull spec.Container.Image (state PULLING)
//   - pick a free host port; publish hostPort -> spec.Container.ContainerPort,
//     GPU access, restart policy "no" (the COORDINATOR owns restarts and
//     re-placement, not Docker)
//   - vLLM args: ["--model", spec.Container.ModelRef,
//     "--served-model-name", spec.ServedModelName] + ExtraArgs
//   - mount a persistent named volume at /root/.cache/huggingface so weights
//     survive container restarts (NOT a host bind mount)
