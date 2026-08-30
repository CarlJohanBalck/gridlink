package deployments

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
	"gridlink/coordinator/internal/registry"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeNode captures what the coordinator sends down a node's stream.
type fakeNode struct {
	mu   sync.Mutex
	sent []*computev1.CoordinatorMessage
	err  error
}

func (f *fakeNode) send(m *computev1.CoordinatorMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, m)
	return nil
}

func (f *fakeNode) starts() []*computev1.StartDeployment {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*computev1.StartDeployment
	for _, m := range f.sent {
		if s := m.GetStartDeployment(); s != nil {
			out = append(out, s)
		}
	}
	return out
}

func (f *fakeNode) stops() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, m := range f.sent {
		if s := m.GetStopDeployment(); s != nil {
			out = append(out, s.GetDeploymentId())
		}
	}
	return out
}

type nodeOpt func(*computev1.Register)

func withRunners(kinds ...computev1.RunnerKind) nodeOpt {
	return func(r *computev1.Register) { r.System.Runners = kinds }
}

func withUsableVRAM(mb uint64) nodeOpt {
	return func(r *computev1.Register) { r.Gpu.UsableVramMb = mb }
}

// addNode registers a node with sensible Mac-provider defaults.
func addNode(t *testing.T, reg *registry.Registry, id string, opts ...nodeOpt) *fakeNode {
	t.Helper()
	r := &computev1.Register{
		NodeId:   id,
		Hostname: id,
		Gpu: &computev1.GpuInfo{
			Vendor:       "apple",
			Model:        "Apple M4 (10-core GPU)",
			VramTotalMb:  16384,
			UsableVramMb: 12123,
		},
		System: &computev1.SystemInfo{
			Os:      "darwin",
			Arch:    "arm64",
			Runners: []computev1.RunnerKind{computev1.RunnerKind_RUNNER_KIND_NATIVE_METAL},
		},
	}
	for _, o := range opts {
		o(r)
	}
	fn := &fakeNode{}
	reg.Upsert(r, fn.send)
	return fn
}

func nativeSpec(model string, minVRAM uint64) *computev1.DeploymentSpec {
	return &computev1.DeploymentSpec{
		ServedModelName: model,
		MinVramMb:       minVRAM,
		Engine: &computev1.DeploymentSpec_Native{
			Native: &computev1.NativeEngine{
				ModelRef:  "org/repo-GGUF",
				ModelFile: "model-Q4_K_M.gguf",
			},
		},
	}
}

func newManager(t *testing.T) (*Manager, *registry.Registry) {
	t.Helper()
	reg := registry.New(testLogger())
	m := New(reg, testLogger())
	return m, reg
}

func TestCreatePlacesOnEligibleNode(t *testing.T) {
	m, reg := newManager(t)
	node := addNode(t, reg, "node-a")

	id, err := m.Create(nativeSpec("qwen-7b", 6000), 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	starts := node.starts()
	if len(starts) != 1 {
		t.Fatalf("StartDeployment sent %d times, want 1", len(starts))
	}
	got := starts[0].GetSpec()
	if got.GetDeploymentId() != id {
		t.Errorf("spec deployment_id = %q, want %q", got.GetDeploymentId(), id)
	}
	if got.GetNative().GetModelFile() != "model-Q4_K_M.gguf" {
		t.Errorf("model_file not forwarded: %+v", got.GetNative())
	}
}

// The spec's engine oneof must match a runner the node advertises. A Docker
// spec on a Mac would otherwise be accepted and never run.
func TestPlacementRequiresMatchingRunner(t *testing.T) {
	m, reg := newManager(t)
	node := addNode(t, reg, "mac", withRunners(computev1.RunnerKind_RUNNER_KIND_NATIVE_METAL))

	spec := &computev1.DeploymentSpec{
		ServedModelName: "llama-8b",
		Engine: &computev1.DeploymentSpec_Container{
			Container: &computev1.ContainerEngine{
				ModelRef: "meta/llama", Image: "vllm/vllm-openai:latest",
			},
		},
	}
	if _, err := m.Create(spec, 1); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if n := len(node.starts()); n != 0 {
		t.Errorf("sent %d StartDeployments to a node without RUNNER_KIND_DOCKER", n)
	}
}

// The whole point of usable_vram_mb: placement must never use the inflated
// total, and 0 means "unknown", which must refuse rather than guess.
func TestPlacementUsesUsableVRAMNotTotal(t *testing.T) {
	t.Run("unknown usable refuses even with huge total", func(t *testing.T) {
		m, reg := newManager(t)
		node := addNode(t, reg, "node-a", withUsableVRAM(0))
		if _, err := m.Create(nativeSpec("m", 4000), 1); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if n := len(node.starts()); n != 0 {
			t.Errorf("placed on a node with usable_vram_mb=0 (total was 16384)")
		}
	})

	t.Run("insufficient usable refuses", func(t *testing.T) {
		m, reg := newManager(t)
		// 12123 usable, but the model needs 14000 — which vram_total_mb
		// (16384) would have wrongly satisfied.
		node := addNode(t, reg, "node-a")
		if _, err := m.Create(nativeSpec("big", 14000), 1); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if n := len(node.starts()); n != 0 {
			t.Errorf("placed a 14000 MB model on a node with 12123 MB usable")
		}
	})

	t.Run("sufficient usable places", func(t *testing.T) {
		m, reg := newManager(t)
		node := addNode(t, reg, "node-a")
		if _, err := m.Create(nativeSpec("small", 12000), 1); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if n := len(node.starts()); n != 1 {
			t.Errorf("StartDeployment sent %d times, want 1", n)
		}
	})
}

func TestOneDeploymentPerNode(t *testing.T) {
	m, reg := newManager(t)
	a := addNode(t, reg, "node-a")
	b := addNode(t, reg, "node-b")

	if _, err := m.Create(nativeSpec("model-1", 0), 1); err != nil {
		t.Fatalf("Create 1: %v", err)
	}
	if _, err := m.Create(nativeSpec("model-2", 0), 1); err != nil {
		t.Fatalf("Create 2: %v", err)
	}

	// Each node hosts exactly one; a loaded model dominates GPU memory.
	if got := len(a.starts()) + len(b.starts()); got != 2 {
		t.Fatalf("total starts = %d, want 2", got)
	}
	if len(a.starts()) != 1 || len(b.starts()) != 1 {
		t.Errorf("deployments not spread: a=%d b=%d", len(a.starts()), len(b.starts()))
	}
}

func TestCreateRejectsBadSpecs(t *testing.T) {
	tests := []struct {
		name     string
		spec     *computev1.DeploymentSpec
		replicas uint32
		wantErr  error
	}{
		{
			name:    "no model name",
			spec:    &computev1.DeploymentSpec{Engine: &computev1.DeploymentSpec_Native{Native: &computev1.NativeEngine{ModelFile: "a.gguf"}}},
			wantErr: ErrInvalidSpec,
		},
		{
			name:    "no engine set",
			spec:    &computev1.DeploymentSpec{ServedModelName: "m"},
			wantErr: ErrInvalidSpec,
		},
		{
			name: "native without model_file",
			spec: &computev1.DeploymentSpec{
				ServedModelName: "m",
				Engine:          &computev1.DeploymentSpec_Native{Native: &computev1.NativeEngine{ModelRef: "org/repo"}},
			},
			wantErr: ErrInvalidSpec,
		},
		{
			name:     "multiple replicas",
			spec:     nativeSpec("m", 0),
			replicas: 3,
			wantErr:  ErrReplicasUnsupp,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reg := newManager(t)
			addNode(t, reg, "node-a")
			replicas := tt.replicas
			if replicas == 0 {
				replicas = 1
			}
			_, err := m.Create(tt.spec, replicas)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Create() error = %v, want errors.Is %v", err, tt.wantErr)
			}
		})
	}
}

// A create with no eligible node must be retried later, not dropped: that is
// what makes this a desired-state system.
func TestUnplacedDeploymentIsPlacedWhenANodeArrives(t *testing.T) {
	m, reg := newManager(t)

	id, err := m.Create(nativeSpec("qwen-7b", 0), 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := m.List(); len(got) != 1 || got[0].GetNodeId() != "" {
		t.Fatalf("expected one unplaced deployment, got %+v", got)
	}

	node := addNode(t, reg, "late-node")
	m.Reconcile()

	starts := node.starts()
	if len(starts) != 1 {
		t.Fatalf("StartDeployment sent %d times after reconcile, want 1", len(starts))
	}
	if starts[0].GetSpec().GetDeploymentId() != id {
		t.Errorf("placed the wrong deployment: %q", starts[0].GetSpec().GetDeploymentId())
	}
}

func TestReplacesDeploymentWhenNodeGoesOffline(t *testing.T) {
	m, reg := newManager(t)
	now := time.Now()
	m.now = func() time.Time { return now }

	dead := addNode(t, reg, "dying")
	id, err := m.Create(nativeSpec("qwen-7b", 0), 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if len(dead.starts()) != 1 {
		t.Fatalf("not placed initially")
	}

	// The node drops off and a healthy one appears.
	reg.MarkDisconnected("dying")
	healthy := addNode(t, reg, "healthy")

	// Before the grace window, nothing should move: an agent restart
	// reconnects in about a second and re-placing would thrash.
	m.Reconcile()
	if n := len(healthy.starts()); n != 0 {
		t.Fatalf("re-placed after %v, before the %v grace window", 0, lostAfter)
	}

	now = now.Add(lostAfter + time.Second)
	m.Reconcile()

	starts := healthy.starts()
	if len(starts) != 1 {
		t.Fatalf("re-placed %d times onto the healthy node, want 1", len(starts))
	}
	if starts[0].GetSpec().GetDeploymentId() != id {
		t.Errorf("re-placed the wrong deployment")
	}
}

// A FAILED replica must move rather than retry in place: whatever broke (bad
// hash, OOM) breaks again on the same node.
func TestReplacesFailedDeployment(t *testing.T) {
	m, reg := newManager(t)
	now := time.Now()
	m.now = func() time.Time { return now }

	bad := addNode(t, reg, "bad-node")
	id, err := m.Create(nativeSpec("qwen-7b", 0), 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	good := addNode(t, reg, "good-node")

	m.OnUpdate("bad-node", &computev1.DeploymentUpdate{
		DeploymentId: id,
		State:        computev1.DeploymentState_DEPLOYMENT_STATE_FAILED,
		Error:        "sha256 mismatch",
	})

	now = now.Add(lostAfter + time.Second)
	m.Reconcile()

	if len(good.starts()) != 1 {
		t.Errorf("failed deployment was not re-placed onto a healthy node")
	}
	if len(bad.starts()) != 1 {
		t.Errorf("bad node received %d starts, want only the original", len(bad.starts()))
	}
}

func TestResolveReturnsOnlyReadyReplicas(t *testing.T) {
	m, reg := newManager(t)
	addNode(t, reg, "node-a")
	id, err := m.Create(nativeSpec("qwen-7b", 0), 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// PULLING: not routable yet.
	if got := m.Resolve("qwen-7b"); len(got) != 0 {
		t.Errorf("resolved %d replicas while PULLING, want 0", len(got))
	}

	// READY but no data-plane address: still not routable, because the gateway
	// would have nowhere to dial.
	m.OnUpdate("node-a", &computev1.DeploymentUpdate{
		DeploymentId: id,
		State:        computev1.DeploymentState_DEPLOYMENT_STATE_READY,
		HostPort:     38111,
	})
	if got := m.Resolve("qwen-7b"); len(got) != 0 {
		t.Errorf("resolved %d replicas with no data-plane addr, want 0", len(got))
	}

	// Once the agent reports its address, the replica is routable.
	reg.Touch("node-a", &computev1.Heartbeat{DataPlaneAddr: "100.64.0.7"})
	got := m.Resolve("qwen-7b")
	if len(got) != 1 {
		t.Fatalf("resolved %d replicas, want 1", len(got))
	}
	if got[0].GetAddr() != "100.64.0.7:38111" {
		t.Errorf("addr = %q, want 100.64.0.7:38111", got[0].GetAddr())
	}
	if got[0].GetDeploymentId() != id || got[0].GetNodeId() != "node-a" {
		t.Errorf("replica identity wrong: %+v", got[0])
	}

	// A model nobody serves resolves to nothing rather than erroring.
	if got := m.Resolve("not-deployed"); len(got) != 0 {
		t.Errorf("resolved %d replicas for an unknown model", len(got))
	}
}

func TestDeleteStopsOnNode(t *testing.T) {
	m, reg := newManager(t)
	node := addNode(t, reg, "node-a")
	id, err := m.Create(nativeSpec("qwen-7b", 0), 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := m.Delete(id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	stops := node.stops()
	if len(stops) != 1 || stops[0] != id {
		t.Errorf("stops = %v, want [%s]", stops, id)
	}
	if got := m.List(); len(got) != 0 {
		t.Errorf("deployment still listed after delete: %+v", got)
	}
	if !errors.Is(m.Delete(id), ErrUnknownDeploy) {
		t.Error("deleting twice should report ErrUnknownDeploy")
	}
}

// After a coordinator restart the table is empty, so a node still running a
// deployment must be told to stop rather than left serving traffic nothing
// routes to.
func TestHeartbeatStopsOrphanedDeployments(t *testing.T) {
	m, reg := newManager(t)
	node := addNode(t, reg, "node-a")

	m.OnHeartbeat("node-a", []string{"orphan-from-a-previous-life"})

	stops := node.stops()
	if len(stops) != 1 || stops[0] != "orphan-from-a-previous-life" {
		t.Errorf("stops = %v, want the orphan to be stopped", stops)
	}
}

func TestUpdateForUnknownDeploymentStopsIt(t *testing.T) {
	m, reg := newManager(t)
	node := addNode(t, reg, "node-a")

	m.OnUpdate("node-a", &computev1.DeploymentUpdate{
		DeploymentId: "ghost",
		State:        computev1.DeploymentState_DEPLOYMENT_STATE_READY,
		HostPort:     1234,
	})
	if stops := node.stops(); len(stops) != 1 || stops[0] != "ghost" {
		t.Errorf("stops = %v, want [ghost]", stops)
	}
}

// A heartbeat that omits a just-placed deployment is a race, not a loss.
func TestHeartbeatDoesNotUnplaceRecentDeployment(t *testing.T) {
	m, reg := newManager(t)
	now := time.Now()
	m.now = func() time.Time { return now }
	node := addNode(t, reg, "node-a")

	if _, err := m.Create(nativeSpec("qwen-7b", 0), 1); err != nil {
		t.Fatalf("Create: %v", err)
	}
	m.OnHeartbeat("node-a", nil) // agent has not noticed it yet

	if got := m.List(); len(got) != 1 || got[0].GetNodeId() != "node-a" {
		t.Errorf("recent deployment was unplaced by a racing heartbeat: %+v", got)
	}
	if n := len(node.stops()); n != 0 {
		t.Errorf("sent %d stops for a deployment we want", n)
	}
}

func TestSelectNodeExplainsRefusal(t *testing.T) {
	m, reg := newManager(t)
	addNode(t, reg, "no-runner", withRunners())
	addNode(t, reg, "no-vram", withUsableVRAM(0))

	d := &Deployment{ID: "d1", Spec: nativeSpec("m", 0)}
	_, err := m.selectNode(d, computev1.RunnerKind_RUNNER_KIND_NATIVE_METAL)
	if err == nil {
		t.Fatal("expected a refusal")
	}
	// The message must name why each node was skipped; "nothing happened" is
	// the worst failure mode for an operator.
	msg := err.Error()
	for _, want := range []string{"no-runner", "no-vram", "usable_vram_mb unknown"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal %q does not mention %q", msg, want)
		}
	}
}

func TestDeploymentIDsOn(t *testing.T) {
	m, reg := newManager(t)
	addNode(t, reg, "node-a")
	id, err := m.Create(nativeSpec("qwen-7b", 0), 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	got := m.DeploymentIDsOn("node-a")
	if len(got) != 1 || got[0] != id {
		t.Errorf("DeploymentIDsOn = %v, want [%s]", got, id)
	}
	if n := len(m.DeploymentIDsOn("other")); n != 0 {
		t.Errorf("DeploymentIDsOn(other) = %d, want 0", n)
	}
}
