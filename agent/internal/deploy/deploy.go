// Package deploy runs long-lived inference containers (Phase 2). Sibling of
// runner (one-shot jobs); shares the Docker client.
package deploy

import (
	"context"
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

// Manager starts/stops deployment containers and health-checks them.
//
// Implementation plan (Phase 2):
//   Start(spec):
//     - pull spec.Image (state PULLING)
//     - pick a free host port; run container publishing
//       hostPort -> spec.ContainerPort, GPU access, restart policy "no"
//       (the COORDINATOR owns restarts/re-placement, not Docker)
//     - vLLM args: ["--model", spec.ModelRef,
//                   "--served-model-name", spec.ServedModelName] + ExtraArgs
//     - mount a persistent named volume at /root/.cache/huggingface so
//       weights survive container restarts (NOT a host bind mount)
//     - state LOADING; poll GET 127.0.0.1:<hostPort>/v1/models every 5s
//       (weights can take minutes on first pull) -> READY
//     - container exits or health fails 3x -> FAILED with container logs tail
//   Stop(deploymentID): docker stop (30s grace) + remove; state STOPPED.
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
