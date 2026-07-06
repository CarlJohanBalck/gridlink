// Package deployments is the coordinator's desired-state store + reconciler
// for model deployments (Phase 2). In-memory until Postgres (Phase 3).
package deployments

import (
	"log/slog"

	computev1 "gridlink/contracts/gen/compute/v1"
	"gridlink/coordinator/internal/registry"
)

// Manager owns the deployment table and drives nodes toward desired state.
//
// Implementation plan (Phase 2):
//   CreateDeployment(spec, replicas):
//     - assign deployment_id (UUIDv4)
//     - placement: pick ONLINE node with gpu.vram_total_mb >= spec.min_vram_mb
//       and no existing deployment (Phase 2: one deployment per node — vLLM
//       grabs ~90% of VRAM by default, so don't co-schedule)
//     - send StartDeployment via node.Send; state PULLING
//   OnDeploymentUpdate(nodeID, upd): record state + host_port; when READY,
//     compose data-plane addr = node.DataPlaneAddr (from heartbeat) + ":" +
//     host_port and store it on the replica.
//   Reconcile loop (every 15s):
//     - node OFFLINE > 60s with a deployment -> mark replica LOST and
//       re-place onto another eligible node (log loudly; this is the
//       consumer-hardware reality mitigation)
//     - deployment deleted -> send StopDeployment
//   ResolveModel(name): return replicas in READY state only.
//
// Invariant: this package never talks HTTP to nodes; all commands go over
// the control stream via registry.Node.Send.
type Manager struct {
	reg *registry.Registry
	log *slog.Logger
	// TODO(claude): tables: deployments (desired), replicas (observed)
}

func New(reg *registry.Registry, log *slog.Logger) *Manager {
	return &Manager{reg: reg, log: log}
}

// TODO(claude): methods per the plan above, plus wiring:
//   - server.go: AdminService Create/Delete/ListDeployments -> Manager
//   - server.go: GatewayService.ResolveModel -> Manager.ResolveModel
//   - server.go: GatewayService.ReportUsage -> append JSONL to
//     $GRIDLINK_USAGE_LOG (Phase 2), keep the interface Phase-3-ready
//   - agent stream handler: DeploymentUpdate -> Manager.OnDeploymentUpdate
var _ = computev1.DeploymentState_DEPLOYMENT_STATE_READY // keep import until implemented
