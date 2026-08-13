// Package registry tracks connected nodes in memory (Phase 1; Postgres in
// Phase 3).
package registry

import (
	"context"
	"log/slog"
	"sync"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"

	"github.com/google/uuid"
)

// Node is the coordinator's view of one agent.
type Node struct {
	ID       string
	Hostname string
	GPU      *computev1.GpuInfo
	System   *computev1.SystemInfo
	LastSeen time.Time
	Status   computev1.NodeStatus
	// Send delivers a message onto this node's live stream. Nil when offline.
	// Set by the server when the stream is established.
	Send func(*computev1.CoordinatorMessage) error
}

// Registry is a mutex-guarded map keyed by node ID.
type Registry struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	log   *slog.Logger

	// now is the clock; overridable in tests. Defaults to time.Now.
	now func() time.Time
	// newID mints node IDs; overridable in tests. Defaults to a UUIDv4.
	newID func() string
}

func New(log *slog.Logger) *Registry {
	return &Registry{
		nodes: make(map[string]*Node),
		log:   log,
		now:   time.Now,
		newID: uuid.NewString,
	}
}

// Upsert records a (re)connecting agent and returns its node ID. When
// reg.NodeId is empty (very first connect) a UUIDv4 is assigned; otherwise the
// existing node is refreshed and its stream Send func rebound. The node is
// marked ONLINE and LastSeen is stamped now.
func (r *Registry) Upsert(reg *computev1.Register, send func(*computev1.CoordinatorMessage) error) string {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := reg.GetNodeId()
	fresh := id == ""
	if fresh {
		id = r.newID()
	}

	n, ok := r.nodes[id]
	if !ok {
		n = &Node{ID: id}
		r.nodes[id] = n
	}
	n.Hostname = reg.GetHostname()
	n.GPU = reg.GetGpu()
	n.System = reg.GetSystem()
	n.LastSeen = r.now()
	n.Status = computev1.NodeStatus_NODE_STATUS_ONLINE
	n.Send = send

	// already_known is scoped to THIS coordinator process, not to the node's
	// history: the registry is in-memory, so a restarted coordinator logs
	// already_known=false for an agent that is very much reconnecting. Only
	// new_id=true means the coordinator had never seen this agent before.
	r.log.Info("node registered",
		"node_id", id, "hostname", n.Hostname, "new_id", fresh, "already_known", ok)
	return id
}

// Touch records a heartbeat: refreshes LastSeen and keeps the node ONLINE.
// A heartbeat for an unknown node is ignored.
func (r *Registry) Touch(nodeID string, _ *computev1.Heartbeat) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n, ok := r.nodes[nodeID]
	if !ok {
		return
	}
	n.LastSeen = r.now()
	n.Status = computev1.NodeStatus_NODE_STATUS_ONLINE
}

// MarkDisconnected flags a node OFFLINE and drops its stream handle. Called
// when the agent's stream ends.
func (r *Registry) MarkDisconnected(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if n, ok := r.nodes[nodeID]; ok {
		n.Status = computev1.NodeStatus_NODE_STATUS_OFFLINE
		n.Send = nil
	}
}

// Get returns a snapshot copy of a node (safe to read without the lock).
func (r *Registry) Get(nodeID string) (*Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	n, ok := r.nodes[nodeID]
	if !ok {
		return nil, false
	}
	cp := *n
	return &cp, true
}

// List returns a summary of every known node.
func (r *Registry) List() []*computev1.NodeSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]*computev1.NodeSummary, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, &computev1.NodeSummary{
			NodeId:         n.ID,
			Hostname:       n.Hostname,
			Status:         n.Status,
			Gpu:            n.GPU,
			LastSeenUnixMs: n.LastSeen.UnixMilli(),
		})
	}
	return out
}

// StartReaper runs a background loop that marks nodes OFFLINE once they miss 3
// heartbeat intervals. It ticks every interval and returns immediately; the
// loop stops when ctx is cancelled.
func (r *Registry) StartReaper(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				r.reap(interval)
			}
		}
	}()
}

// reap marks ONLINE nodes OFFLINE when their last heartbeat is older than 3
// intervals. Split out from StartReaper so tests can drive it deterministically.
func (r *Registry) reap(interval time.Duration) {
	threshold := 3 * interval
	now := r.now()

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, n := range r.nodes {
		if n.Status != computev1.NodeStatus_NODE_STATUS_ONLINE {
			continue
		}
		if now.Sub(n.LastSeen) > threshold {
			n.Status = computev1.NodeStatus_NODE_STATUS_OFFLINE
			n.Send = nil
			r.log.Warn("node offline: missed heartbeats",
				"node_id", n.ID, "last_seen", n.LastSeen, "threshold", threshold)
		}
	}
}
