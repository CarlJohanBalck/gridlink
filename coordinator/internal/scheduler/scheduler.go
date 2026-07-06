// Package scheduler dispatches jobs to nodes. Phase 1 is deliberately dumb:
// the admin names a node, we generate a job ID, and forward the spec down
// that node's stream. Real matching/bidding arrives in Phase 2+.
package scheduler

import (
	"log/slog"

	computev1 "gridlink/contracts/gen/compute/v1"
	"gridlink/coordinator/internal/registry"
)

type Scheduler struct {
	reg *registry.Registry
	log *slog.Logger
	// TODO(claude): in-memory job table: job_id -> {node_id, state, last update}
}

func New(reg *registry.Registry, log *slog.Logger) *Scheduler {
	return &Scheduler{reg: reg, log: log}
}

var _ = computev1.JobState_JOB_STATE_UNSPECIFIED // keep import until implemented

// TODO(claude): implement:
//   RunJob(nodeID string, spec *computev1.JobSpec) (jobID string, err error)
//     - error if node unknown or OFFLINE
//     - fill spec.JobId with a UUIDv4, send RunJob via node.Send
//   OnJobUpdate(nodeID string, upd *computev1.JobUpdate)
//     - update job table, log state transitions
//   Reconcile(nodeID string, activeJobIDs []string)
//     - called from heartbeats after reconnects; mark jobs FAILED if the
//       agent no longer reports them but we think they're RUNNING
