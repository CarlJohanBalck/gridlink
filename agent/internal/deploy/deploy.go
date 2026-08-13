// Package deploy runs long-lived inference servers (Phase 2). Sibling of
// runner (one-shot jobs).
package deploy

import (
	"log/slog"

	computev1 "gridlink/contracts/gen/compute/v1"
)

// Update mirrors runner.Update for deployment lifecycle.
type Update struct {
	DeploymentID string
	State        computev1.DeploymentState
	HostPort     uint32
	Err          string
}

// Manager starts/stops deployments and health-checks them. DeploymentSpec
// carries a `engine` oneof, and the Manager dispatches on it: NativeEngine on
// macOS (the primary path), ContainerEngine on Linux + NVIDIA. A node only
// ever receives specs matching a RunnerKind it advertised.
//
// Implementation plan (Phase 2) — NativeEngine (primary):
//     - resolve spec.Native.ModelFile in ModelRef@Revision; download to
//       ~/.gridlink/models/ if absent (state PULLING, report
//       progress_percent; weights are GB-scale on consumer uplinks)
//     - verify SHA-256 against spec.Native.Sha256 BEFORE loading; mismatch
//       is FAILED, never a load attempt
//     - spawn the agent binary as `agent engine` under sandbox-exec, bound to
//       a free localhost port; state LOADING
//     - warm the engine once at agent startup: the first-ever Metal shader
//       compile costs ~6.5s, ~0.01s cached thereafter
//     - poll GET 127.0.0.1:<hostPort>/v1/models -> READY
//     - engine process exits or health fails 3x -> FAILED with its stderr tail
//
// Implementation plan (Phase 2) — ContainerEngine (Linux + NVIDIA only):
//     - pull spec.Container.Image (state PULLING)
//     - pick a free host port; publish hostPort -> spec.Container.ContainerPort,
//       GPU access, restart policy "no" (the COORDINATOR owns restarts and
//       re-placement, not Docker)
//     - vLLM args: ["--model", spec.Container.ModelRef,
//                   "--served-model-name", spec.ServedModelName] + ExtraArgs
//     - mount a persistent named volume at /root/.cache/huggingface so
//       weights survive container restarts (NOT a host bind mount)
//
//   Stop(deploymentID): stop the engine subprocess (or container, 30s grace)
//     and remove; state STOPPED.
//   List(): active deployment IDs, for Heartbeat.active_deployment_ids.
//
// Client wiring (client.go): handle StartDeployment / StopDeployment from
// the stream; forward Updates as DeploymentUpdate messages; include
// List() + data-plane addr (tailnet IP via `tailscale ip -4`, or
// GRIDLINK_DATA_ADDR override) in every Heartbeat.
type Manager struct {
	log *slog.Logger
	// TODO(claude): docker client, active map
}

func New(log *slog.Logger) (*Manager, error) {
	panic("not implemented")
}
