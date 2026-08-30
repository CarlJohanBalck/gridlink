// Package deployments is the coordinator's desired-state store + reconciler
// for model deployments (Phase 2). In-memory until Postgres (Phase 3).
//
// Invariant: this package never talks HTTP to nodes. Every command goes over
// the control stream via registry.Node.Send, because agents dial out and the
// coordinator can never reach them directly.
package deployments

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
	"gridlink/coordinator/internal/registry"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

var (
	ErrNoCapacity      = errors.New("no eligible node")
	ErrUnknownDeploy   = errors.New("unknown deployment")
	ErrInvalidSpec     = errors.New("invalid deployment spec")
	ErrReplicasUnsupp  = errors.New("only replicas=1 is supported in Phase 2")
	errNodeNotSendable = errors.New("node has no live stream")
)

// reconcileInterval is how often desired state is compared to observed state.
const reconcileInterval = 15 * time.Second

// avoidAfterFailure is how long a node is skipped for a deployment that just
// failed on it. Whatever broke (bad hash, OOM, missing file) will usually
// break again immediately, so retrying the same node is a hot loop; but the
// cause can also be transient (a dropped download), so the ban expires rather
// than being permanent.
const avoidAfterFailure = 10 * time.Minute

// lostAfter is how long a placement survives its node going OFFLINE before it
// is re-placed. Long enough to absorb an agent restart (which reconnects in
// ~1s) without thrashing, short enough that a dead provider is not serving
// traffic for minutes.
const lostAfter = 60 * time.Second

// Replica is one placement of a deployment onto a node.
type Replica struct {
	DeploymentID string
	NodeID       string
	State        computev1.DeploymentState
	HostPort     uint32
	Progress     uint32
	Err          string
	// PlacedAt is when StartDeployment was sent; LastUpdate is the last time
	// the agent said anything about it.
	PlacedAt   time.Time
	LastUpdate time.Time
}

// Deployment is one desired model, plus its current placement.
type Deployment struct {
	ID   string
	Spec *computev1.DeploymentSpec
	// Replica is nil while unplaced (no eligible node yet).
	Replica *Replica
	// avoid holds nodes this deployment recently failed on, and until when.
	avoid map[string]time.Time
}

// Manager owns the deployment table and drives nodes toward desired state.
type Manager struct {
	reg *registry.Registry
	log *slog.Logger

	mu     sync.Mutex
	deploy map[string]*Deployment

	now   func() time.Time
	newID func() string
}

func New(reg *registry.Registry, log *slog.Logger) *Manager {
	return &Manager{
		reg:    reg,
		log:    log,
		deploy: make(map[string]*Deployment),
		now:    time.Now,
		newID:  uuid.NewString,
	}
}

// requiredRunner maps a spec's engine oneof to the runner a node must
// advertise. Returning an error for an unset engine is deliberate: a spec that
// names neither engine is a client bug, and defaulting would place work on a
// node that cannot run it.
func requiredRunner(spec *computev1.DeploymentSpec) (computev1.RunnerKind, error) {
	switch spec.GetEngine().(type) {
	case *computev1.DeploymentSpec_Native:
		return computev1.RunnerKind_RUNNER_KIND_NATIVE_METAL, nil
	case *computev1.DeploymentSpec_Container:
		return computev1.RunnerKind_RUNNER_KIND_DOCKER, nil
	default:
		return computev1.RunnerKind_RUNNER_KIND_UNSPECIFIED,
			fmt.Errorf("%w: exactly one of native/container must be set", ErrInvalidSpec)
	}
}

// Create validates, assigns an ID, and places the deployment.
func (m *Manager) Create(spec *computev1.DeploymentSpec, replicas uint32) (string, error) {
	if spec.GetServedModelName() == "" {
		return "", fmt.Errorf("%w: served_model_name is required", ErrInvalidSpec)
	}
	if replicas > 1 {
		// Horizontal scale-out is per-model-per-node; multiple replicas of the
		// SAME model is a later feature, not sharding (see CLAUDE.md).
		return "", ErrReplicasUnsupp
	}
	kind, err := requiredRunner(spec)
	if err != nil {
		return "", err
	}
	if native := spec.GetNative(); native != nil && native.GetModelFile() == "" {
		return "", fmt.Errorf("%w: native.model_file is required", ErrInvalidSpec)
	}

	id := m.newID()
	spec = proto.Clone(spec).(*computev1.DeploymentSpec)
	spec.DeploymentId = id

	d := &Deployment{ID: id, Spec: spec, avoid: make(map[string]time.Time)}

	m.mu.Lock()
	m.deploy[id] = d
	m.mu.Unlock()

	if err := m.place(d, kind); err != nil {
		// Keep the deployment: the reconciler retries, so a create issued
		// before any node connects succeeds as soon as one does. That is the
		// desired-state model, not a leak.
		m.log.Warn("deployment created but unplaced",
			"deployment_id", id, "model", spec.GetServedModelName(), "err", err)
	}
	return id, nil
}

// place selects a node and sends StartDeployment. Caller must NOT hold m.mu.
func (m *Manager) place(d *Deployment, kind computev1.RunnerKind) error {
	node, err := m.selectNode(d, kind)
	if err != nil {
		return err
	}

	msg := &computev1.CoordinatorMessage{
		Payload: &computev1.CoordinatorMessage_StartDeployment{
			StartDeployment: &computev1.StartDeployment{Spec: d.Spec},
		},
	}
	if err := node.Send(msg); err != nil {
		return fmt.Errorf("send StartDeployment to %s: %w", node.ID, err)
	}

	now := m.now()
	m.mu.Lock()
	d.Replica = &Replica{
		DeploymentID: d.ID,
		NodeID:       node.ID,
		State:        computev1.DeploymentState_DEPLOYMENT_STATE_PULLING,
		PlacedAt:     now,
		LastUpdate:   now,
	}
	m.mu.Unlock()

	m.log.Info("deployment placed",
		"deployment_id", d.ID, "model", d.Spec.GetServedModelName(),
		"node_id", node.ID, "runner", kind)
	return nil
}

// selectNode applies placement rules and explains a refusal, because "nothing
// happened" is the worst possible failure mode for an operator.
func (m *Manager) selectNode(d *Deployment, kind computev1.RunnerKind) (*registry.Node, error) {
	m.mu.Lock()
	occupied := make(map[string]bool)
	for _, other := range m.deploy {
		if other.ID != d.ID && other.Replica != nil {
			occupied[other.Replica.NodeID] = true
		}
	}
	m.mu.Unlock()

	m.mu.Lock()
	avoid := make(map[string]time.Time, len(d.avoid))
	for k, v := range d.avoid {
		avoid[k] = v
	}
	m.mu.Unlock()

	minVRAM := d.Spec.GetMinVramMb()
	now := m.now()
	var skipped []string

	for _, n := range m.reg.Snapshot() {
		if n.Status != computev1.NodeStatus_NODE_STATUS_ONLINE || n.Send == nil {
			continue
		}
		if until, banned := avoid[n.ID]; banned && now.Before(until) {
			skipped = append(skipped, n.ID+": recently failed here")
			continue
		}
		if !hasRunner(n.System.GetRunners(), kind) {
			skipped = append(skipped, n.ID+": no "+kind.String())
			continue
		}
		// One deployment per node: a loaded model dominates the GPU's memory,
		// so co-scheduling would push both into swap.
		if occupied[n.ID] {
			skipped = append(skipped, n.ID+": already hosts a deployment")
			continue
		}
		usable := n.GPU.GetUsableVramMb()
		if usable == 0 {
			// 0 means the node could not measure its GPU budget. Guessing from
			// vram_total_mb would overcommit by ~29% on Apple Silicon, so
			// refuse instead (see GpuInfo.usable_vram_mb in the proto).
			skipped = append(skipped, n.ID+": usable_vram_mb unknown")
			continue
		}
		if minVRAM > 0 && usable < minVRAM {
			skipped = append(skipped,
				fmt.Sprintf("%s: %d MB usable < %d MB required", n.ID, usable, minVRAM))
			continue
		}
		return n, nil
	}

	if len(skipped) == 0 {
		return nil, fmt.Errorf("%w: no ONLINE nodes", ErrNoCapacity)
	}
	return nil, fmt.Errorf("%w: %v", ErrNoCapacity, skipped)
}

func hasRunner(runners []computev1.RunnerKind, want computev1.RunnerKind) bool {
	for _, r := range runners {
		if r == want {
			return true
		}
	}
	return false
}

// Delete stops the deployment on its node and forgets it.
func (m *Manager) Delete(id string) error {
	m.mu.Lock()
	d, ok := m.deploy[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("%s: %w", id, ErrUnknownDeploy)
	}
	delete(m.deploy, id)
	replica := d.Replica
	m.mu.Unlock()

	if replica == nil {
		return nil
	}
	// Best effort: if the node is gone the deployment died with it, and the
	// desired state (absent) is already satisfied.
	if err := m.sendStop(replica.NodeID, id); err != nil {
		m.log.Warn("could not stop deployment on node",
			"deployment_id", id, "node_id", replica.NodeID, "err", err)
	}
	m.log.Info("deployment deleted", "deployment_id", id, "node_id", replica.NodeID)
	return nil
}

func (m *Manager) sendStop(nodeID, deploymentID string) error {
	node, ok := m.reg.Get(nodeID)
	if !ok || node.Send == nil {
		return errNodeNotSendable
	}
	return node.Send(&computev1.CoordinatorMessage{
		Payload: &computev1.CoordinatorMessage_StopDeployment{
			StopDeployment: &computev1.StopDeployment{DeploymentId: deploymentID},
		},
	})
}

// OnUpdate records what an agent reports about a deployment.
func (m *Manager) OnUpdate(nodeID string, upd *computev1.DeploymentUpdate) {
	id := upd.GetDeploymentId()

	m.mu.Lock()
	d, ok := m.deploy[id]
	if !ok || d.Replica == nil || d.Replica.NodeID != nodeID {
		m.mu.Unlock()
		// An update for something we do not want (or from a node we did not
		// place it on) means that agent is running an orphan: tell it to stop,
		// or it serves traffic nothing routes to and holds GPU memory.
		m.log.Warn("update for unknown deployment; stopping orphan",
			"deployment_id", id, "node_id", nodeID, "state", upd.GetState())
		if err := m.sendStop(nodeID, id); err != nil {
			m.log.Warn("could not stop orphan", "deployment_id", id, "err", err)
		}
		return
	}
	r := d.Replica
	r.State = upd.GetState()
	r.Progress = upd.GetProgressPercent()
	r.Err = upd.GetError()
	r.LastUpdate = m.now()
	if p := upd.GetHostPort(); p != 0 {
		r.HostPort = p
	}
	model := d.Spec.GetServedModelName()
	m.mu.Unlock()

	switch upd.GetState() {
	case computev1.DeploymentState_DEPLOYMENT_STATE_READY:
		m.log.Info("deployment ready",
			"deployment_id", id, "model", model, "node_id", nodeID, "port", upd.GetHostPort())
	case computev1.DeploymentState_DEPLOYMENT_STATE_FAILED:
		m.log.Error("deployment failed",
			"deployment_id", id, "model", model, "node_id", nodeID, "err", upd.GetError())
	default:
		m.log.Debug("deployment update",
			"deployment_id", id, "node_id", nodeID, "state", upd.GetState(),
			"progress", upd.GetProgressPercent())
	}
}

// OnHeartbeat reconciles what a node reports it is running against what we
// want it to run. A coordinator restart loses the table, so this is how
// orphans are found.
func (m *Manager) OnHeartbeat(nodeID string, activeIDs []string) {
	active := make(map[string]bool, len(activeIDs))
	for _, id := range activeIDs {
		active[id] = true
	}

	m.mu.Lock()
	wanted := make(map[string]bool)
	var missing []*Deployment
	for _, d := range m.deploy {
		if d.Replica == nil || d.Replica.NodeID != nodeID {
			continue
		}
		wanted[d.ID] = true
		// Only chase a replica the agent has had time to report. A deployment
		// placed moments ago is legitimately absent from a heartbeat already
		// in flight.
		if !active[d.ID] && m.now().Sub(d.Replica.PlacedAt) > lostAfter {
			missing = append(missing, d)
		}
	}
	m.mu.Unlock()

	for _, id := range activeIDs {
		if !wanted[id] {
			m.log.Warn("node runs a deployment we do not want; stopping",
				"deployment_id", id, "node_id", nodeID)
			if err := m.sendStop(nodeID, id); err != nil {
				m.log.Warn("could not stop orphan", "deployment_id", id, "err", err)
			}
		}
	}
	for _, d := range missing {
		m.log.Warn("deployment vanished from its node; re-placing",
			"deployment_id", d.ID, "node_id", nodeID)
		m.unplace(d)
	}
}

// unplace clears a replica so the reconciler will place it again.
func (m *Manager) unplace(d *Deployment) {
	m.mu.Lock()
	d.Replica = nil
	m.mu.Unlock()
}

// Resolve returns the READY replicas serving a model, for the gateway.
func (m *Manager) Resolve(modelName string) []*computev1.Replica {
	m.mu.Lock()
	type placed struct {
		id, nodeID string
		port       uint32
	}
	var candidates []placed
	for _, d := range m.deploy {
		if d.Spec.GetServedModelName() != modelName || d.Replica == nil {
			continue
		}
		// READY only: a PULLING or LOADING replica would 404 or hang, and a
		// FAILED one would black-hole traffic.
		if d.Replica.State != computev1.DeploymentState_DEPLOYMENT_STATE_READY {
			continue
		}
		candidates = append(candidates, placed{d.ID, d.Replica.NodeID, d.Replica.HostPort})
	}
	m.mu.Unlock()

	out := make([]*computev1.Replica, 0, len(candidates))
	for _, c := range candidates {
		node, ok := m.reg.Get(c.nodeID)
		if !ok || node.Status != computev1.NodeStatus_NODE_STATUS_ONLINE {
			continue
		}
		// Without a data-plane address the gateway has nowhere to dial, so
		// advertising the replica would produce a confusing connection error
		// instead of a clean "no replicas".
		if node.DataPlaneAddr == "" {
			m.log.Warn("READY replica has no data-plane address; not advertising",
				"deployment_id", c.id, "node_id", c.nodeID)
			continue
		}
		out = append(out, &computev1.Replica{
			NodeId:       c.nodeID,
			DeploymentId: c.id,
			Addr:         net.JoinHostPort(node.DataPlaneAddr, strconv.Itoa(int(c.port))),
		})
	}
	return out
}

// List summarises every deployment.
func (m *Manager) List() []*computev1.DeploymentSummary {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]*computev1.DeploymentSummary, 0, len(m.deploy))
	for _, d := range m.deploy {
		s := &computev1.DeploymentSummary{
			DeploymentId:    d.ID,
			ServedModelName: d.Spec.GetServedModelName(),
		}
		if d.Replica != nil {
			s.NodeId = d.Replica.NodeID
			s.State = d.Replica.State
		}
		out = append(out, s)
	}
	return out
}

// DeploymentIDsOn returns the deployments currently placed on a node, for
// ListNodes.
func (m *Manager) DeploymentIDsOn(nodeID string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out []string
	for _, d := range m.deploy {
		if d.Replica != nil && d.Replica.NodeID == nodeID {
			out = append(out, d.ID)
		}
	}
	return out
}

// StartReconciler drives desired state on a ticker until ctx is cancelled.
func (m *Manager) StartReconciler(ctx interface{ Done() <-chan struct{} }) {
	go func() {
		t := time.NewTicker(reconcileInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.Reconcile()
			}
		}
	}()
}

// Reconcile re-places deployments whose node died or that never placed. Node
// loss is not an edge case on consumer hardware — it is the product.
func (m *Manager) Reconcile() {
	now := m.now()

	m.mu.Lock()
	var toPlace []*Deployment
	for _, d := range m.deploy {
		if d.Replica == nil {
			toPlace = append(toPlace, d)
			continue
		}
		node, ok := m.reg.Get(d.Replica.NodeID)
		gone := !ok || node.Status != computev1.NodeStatus_NODE_STATUS_ONLINE
		if gone && now.Sub(d.Replica.LastUpdate) > lostAfter {
			m.log.Warn("node lost; re-placing deployment",
				"deployment_id", d.ID, "node_id", d.Replica.NodeID,
				"model", d.Spec.GetServedModelName())
			d.Replica = nil
			toPlace = append(toPlace, d)
			continue
		}
		// A FAILED replica is not retried in place: whatever broke (bad hash,
		// OOM, missing file) will break again on the same node. Re-placing
		// elsewhere is the only move that can succeed.
		if d.Replica.State == computev1.DeploymentState_DEPLOYMENT_STATE_FAILED &&
			now.Sub(d.Replica.LastUpdate) > lostAfter {
			m.log.Warn("deployment failed; re-placing",
				"deployment_id", d.ID, "node_id", d.Replica.NodeID, "err", d.Replica.Err)
			// Skip this node for a while, or the next placement lands right
			// back on the machine that just failed.
			if d.avoid == nil {
				d.avoid = make(map[string]time.Time)
			}
			d.avoid[d.Replica.NodeID] = now.Add(avoidAfterFailure)
			d.Replica = nil
			toPlace = append(toPlace, d)
		}
	}
	m.mu.Unlock()

	for _, d := range toPlace {
		kind, err := requiredRunner(d.Spec)
		if err != nil {
			continue
		}
		if err := m.place(d, kind); err != nil {
			m.log.Debug("still unplaced", "deployment_id", d.ID, "err", err)
		}
	}
}
