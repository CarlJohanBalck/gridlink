// Package registry tracks connected nodes in memory (Phase 1; Postgres in
// Phase 3).
package registry

import (
	"log/slog"
	"sync"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
)

// Node is the coordinator's view of one agent.
type Node struct {
	ID        string
	Hostname  string
	GPU       *computev1.GpuInfo
	System    *computev1.SystemInfo
	LastSeen  time.Time
	Status    computev1.NodeStatus
	// Send delivers a message onto this node's live stream. Nil when offline.
	// Set by the server when the stream is established.
	Send func(*computev1.CoordinatorMessage) error
}

// Registry is a mutex-guarded map keyed by node ID.
type Registry struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	log   *slog.Logger
}

func New(log *slog.Logger) *Registry {
	return &Registry{nodes: make(map[string]*Node), log: log}
}

// TODO(claude): implement:
//   Upsert(reg *computev1.Register, send func(...) error) (nodeID string)
//     - assigns a UUIDv4 when reg.NodeId is empty
//   Touch(nodeID string, hb *computev1.Heartbeat)
//   MarkDisconnected(nodeID string)
//   Get(nodeID string) (*Node, bool)   — returned value must be a snapshot
//   List() []*computev1.NodeSummary
//   StartReaper(ctx, interval) — marks nodes OFFLINE after 3 missed heartbeats
