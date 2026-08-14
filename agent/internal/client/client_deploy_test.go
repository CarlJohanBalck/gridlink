package client

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gridlink/agent/internal/deploy"
	computev1 "gridlink/contracts/gen/compute/v1"
)

// ---- fake engine ----

// scriptManager emits a fixed sequence of updates, then holds the deployment
// "serving" until its context is cancelled — the shape a real engine has, where
// READY is not terminal.
type scriptManager struct {
	seq      []deploy.Update
	startErr error

	mu      sync.Mutex
	started []*computev1.DeploymentSpec
}

func (m *scriptManager) Start(ctx context.Context, spec *computev1.DeploymentSpec) (<-chan deploy.Update, error) {
	m.mu.Lock()
	m.started = append(m.started, spec)
	m.mu.Unlock()

	if m.startErr != nil {
		return nil, m.startErr
	}

	ch := make(chan deploy.Update, len(m.seq)+1)
	go func() {
		defer close(ch)
		for _, u := range m.seq {
			u.DeploymentID = spec.GetDeploymentId()
			select {
			case ch <- u:
			case <-ctx.Done():
				return
			}
		}
		// Serving: stay up until stopped, then report STOPPED like a real engine.
		<-ctx.Done()
		ch <- deploy.Update{
			DeploymentID: spec.GetDeploymentId(),
			State:        computev1.DeploymentState_DEPLOYMENT_STATE_STOPPED,
		}
	}()
	return ch, nil
}

func (m *scriptManager) startedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.started)
}

func (m *scriptManager) lastSpec() *computev1.DeploymentSpec {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.started) == 0 {
		return nil
	}
	return m.started[len(m.started)-1]
}

// ---- fakeCoord accessors ----

func (f *fakeCoord) deployStates(id string) []computev1.DeploymentState {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []computev1.DeploymentState
	for _, u := range f.deployUpdates {
		if u.GetDeploymentId() == id {
			out = append(out, u.GetState())
		}
	}
	return out
}

func (f *fakeCoord) deployUpdateFor(id string, state computev1.DeploymentState) *computev1.DeploymentUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.deployUpdates {
		if u.GetDeploymentId() == id && u.GetState() == state {
			return u
		}
	}
	return nil
}

func (f *fakeCoord) anyHeartbeatHasDeployment(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, hb := range f.heartbeats {
		for _, got := range hb.GetActiveDeploymentIds() {
			if got == id {
				return true
			}
		}
	}
	return false
}

// ---- helpers ----

func nativeSpec(id string) *computev1.DeploymentSpec {
	return &computev1.DeploymentSpec{
		DeploymentId:    id,
		ServedModelName: "llama-3.1-8b-instruct",
		MinVramMb:       6000,
		Engine: &computev1.DeploymentSpec_Native{
			Native: &computev1.NativeEngine{
				ModelRef:      "bartowski/Meta-Llama-3.1-8B-Instruct-GGUF",
				ModelFile:     "Meta-Llama-3.1-8B-Instruct-Q4_K_M.gguf",
				Sha256:        "abc123",
				ContextLength: 4096,
			},
		},
	}
}

func sendStart(t *testing.T, fc *fakeCoord, spec *computev1.DeploymentSpec) {
	t.Helper()
	fc.send <- &computev1.CoordinatorMessage{
		Payload: &computev1.CoordinatorMessage_StartDeployment{
			StartDeployment: &computev1.StartDeployment{Spec: spec},
		},
	}
}

func sendStop(t *testing.T, fc *fakeCoord, id string) {
	t.Helper()
	fc.send <- &computev1.CoordinatorMessage{
		Payload: &computev1.CoordinatorMessage_StopDeployment{
			StopDeployment: &computev1.StopDeployment{DeploymentId: id},
		},
	}
}

// ---- tests ----

func TestStartDeploymentForwardsUpdates(t *testing.T) {
	fc := &fakeCoord{assignID: "node-1", send: make(chan *computev1.CoordinatorMessage, 8)}
	mgr := &scriptManager{seq: []deploy.Update{
		{State: computev1.DeploymentState_DEPLOYMENT_STATE_PULLING, Progress: 42},
		{State: computev1.DeploymentState_DEPLOYMENT_STATE_LOADING},
		{State: computev1.DeploymentState_DEPLOYMENT_STATE_READY, HostPort: 38000},
	}}
	c := startFake(t, fc, Config{
		NodeIDPath:  filepath.Join(t.TempDir(), "node_id"),
		Deployments: mgr,
	})
	runClient(t, c)

	sendStart(t, fc, nativeSpec("dep-1"))

	if !eventually(t, 3*time.Second, func() bool {
		got := fc.deployStates("dep-1")
		return len(got) >= 3
	}) {
		t.Fatalf("deployment states = %v, want PULLING, LOADING, READY", fc.deployStates("dep-1"))
	}

	want := []computev1.DeploymentState{
		computev1.DeploymentState_DEPLOYMENT_STATE_PULLING,
		computev1.DeploymentState_DEPLOYMENT_STATE_LOADING,
		computev1.DeploymentState_DEPLOYMENT_STATE_READY,
	}
	got := fc.deployStates("dep-1")
	for i, w := range want {
		if got[i] != w {
			t.Errorf("state[%d] = %v, want %v", i, got[i], w)
		}
	}

	// progress_percent must survive the hop: it is the only signal that
	// distinguishes a slow multi-GB download from a hung deployment.
	if u := fc.deployUpdateFor("dep-1", computev1.DeploymentState_DEPLOYMENT_STATE_PULLING); u == nil {
		t.Fatal("no PULLING update received")
	} else if u.GetProgressPercent() != 42 {
		t.Errorf("progress_percent = %d, want 42", u.GetProgressPercent())
	}

	if u := fc.deployUpdateFor("dep-1", computev1.DeploymentState_DEPLOYMENT_STATE_READY); u == nil {
		t.Fatal("no READY update received")
	} else if u.GetHostPort() != 38000 {
		t.Errorf("host_port = %d, want 38000", u.GetHostPort())
	}

	// The spec must arrive intact, engine oneof included.
	if s := mgr.lastSpec(); s.GetNative().GetModelFile() == "" {
		t.Errorf("engine oneof lost in transit: %+v", s)
	}
}

func TestDeploymentAppearsInHeartbeat(t *testing.T) {
	fc := &fakeCoord{assignID: "node-1", send: make(chan *computev1.CoordinatorMessage, 8)}
	mgr := &scriptManager{seq: []deploy.Update{
		{State: computev1.DeploymentState_DEPLOYMENT_STATE_READY, HostPort: 38000},
	}}
	c := startFake(t, fc, Config{
		NodeIDPath:    filepath.Join(t.TempDir(), "node_id"),
		Deployments:   mgr,
		DataPlaneAddr: "100.64.0.7",
	})
	runClient(t, c)

	sendStart(t, fc, nativeSpec("dep-1"))

	if !eventually(t, 4*time.Second, func() bool {
		return fc.anyHeartbeatHasDeployment("dep-1")
	}) {
		t.Fatal("no heartbeat listed dep-1 in active_deployment_ids")
	}

	// data_plane_addr rides along, so the gateway can be told where to dial.
	fc.mu.Lock()
	defer fc.mu.Unlock()
	var sawAddr bool
	for _, hb := range fc.heartbeats {
		if hb.GetDataPlaneAddr() == "100.64.0.7" {
			sawAddr = true
		}
	}
	if !sawAddr {
		t.Error("no heartbeat carried data_plane_addr")
	}
}

func TestStopDeploymentCancels(t *testing.T) {
	fc := &fakeCoord{assignID: "node-1", send: make(chan *computev1.CoordinatorMessage, 8)}
	mgr := &scriptManager{seq: []deploy.Update{
		{State: computev1.DeploymentState_DEPLOYMENT_STATE_READY, HostPort: 38000},
	}}
	c := startFake(t, fc, Config{
		NodeIDPath:  filepath.Join(t.TempDir(), "node_id"),
		Deployments: mgr,
	})
	runClient(t, c)

	sendStart(t, fc, nativeSpec("dep-1"))
	if !eventually(t, 3*time.Second, func() bool {
		return fc.deployUpdateFor("dep-1", computev1.DeploymentState_DEPLOYMENT_STATE_READY) != nil
	}) {
		t.Fatal("deployment never reached READY")
	}

	sendStop(t, fc, "dep-1")

	if !eventually(t, 3*time.Second, func() bool {
		return fc.deployUpdateFor("dep-1", computev1.DeploymentState_DEPLOYMENT_STATE_STOPPED) != nil
	}) {
		t.Fatalf("no STOPPED update after StopDeployment; states = %v", fc.deployStates("dep-1"))
	}

	// Once stopped it must leave the active set, or the coordinator would
	// believe the model is still being served.
	if !eventually(t, 2*time.Second, func() bool {
		return len(c.activeDeploymentIDs()) == 0
	}) {
		t.Errorf("active deployments = %v, want empty", c.activeDeploymentIDs())
	}
}

func TestDuplicateStartDeploymentIgnored(t *testing.T) {
	fc := &fakeCoord{assignID: "node-1", send: make(chan *computev1.CoordinatorMessage, 8)}
	mgr := &scriptManager{seq: []deploy.Update{
		{State: computev1.DeploymentState_DEPLOYMENT_STATE_READY, HostPort: 38000},
	}}
	c := startFake(t, fc, Config{
		NodeIDPath:  filepath.Join(t.TempDir(), "node_id"),
		Deployments: mgr,
	})
	runClient(t, c)

	sendStart(t, fc, nativeSpec("dep-1"))
	if !eventually(t, 3*time.Second, func() bool {
		return fc.deployUpdateFor("dep-1", computev1.DeploymentState_DEPLOYMENT_STATE_READY) != nil
	}) {
		t.Fatal("deployment never reached READY")
	}

	// The coordinator re-sends StartDeployment to reconcile after its own
	// restart; that must not start a second engine for the same model.
	sendStart(t, fc, nativeSpec("dep-1"))
	time.Sleep(300 * time.Millisecond)

	if n := mgr.startedCount(); n != 1 {
		t.Errorf("engine started %d times, want 1", n)
	}
}

func TestStartDeploymentWithoutEngineFails(t *testing.T) {
	fc := &fakeCoord{assignID: "node-1", send: make(chan *computev1.CoordinatorMessage, 8)}
	// Deployments nil: a node with no engine, e.g. today's Mac agent.
	c := startFake(t, fc, Config{NodeIDPath: filepath.Join(t.TempDir(), "node_id")})
	runClient(t, c)

	sendStart(t, fc, nativeSpec("dep-1"))

	if !eventually(t, 3*time.Second, func() bool {
		return fc.deployUpdateFor("dep-1", computev1.DeploymentState_DEPLOYMENT_STATE_FAILED) != nil
	}) {
		t.Fatal("expected FAILED for a node with no engine")
	}
	if n := len(c.activeDeploymentIDs()); n != 0 {
		t.Errorf("active deployments = %d, want 0", n)
	}
}

func TestStartDeploymentEngineStartError(t *testing.T) {
	fc := &fakeCoord{assignID: "node-1", send: make(chan *computev1.CoordinatorMessage, 8)}
	mgr := &scriptManager{startErr: context.DeadlineExceeded}
	c := startFake(t, fc, Config{
		NodeIDPath:  filepath.Join(t.TempDir(), "node_id"),
		Deployments: mgr,
	})
	runClient(t, c)

	sendStart(t, fc, nativeSpec("dep-1"))

	if !eventually(t, 3*time.Second, func() bool {
		return fc.deployUpdateFor("dep-1", computev1.DeploymentState_DEPLOYMENT_STATE_FAILED) != nil
	}) {
		t.Fatal("expected FAILED when the engine refuses to start")
	}
	// A failed start must not leak into the active set.
	if !eventually(t, 2*time.Second, func() bool {
		return len(c.activeDeploymentIDs()) == 0
	}) {
		t.Errorf("active deployments = %v, want empty", c.activeDeploymentIDs())
	}
}

func TestRunJobWithoutRunnerFails(t *testing.T) {
	fc := &fakeCoord{assignID: "node-1", send: make(chan *computev1.CoordinatorMessage, 8)}
	// Runner nil: a Mac provider, which has no container runtime at all.
	c := startFake(t, fc, Config{NodeIDPath: filepath.Join(t.TempDir(), "node_id")})
	runClient(t, c)

	fc.send <- &computev1.CoordinatorMessage{
		Payload: &computev1.CoordinatorMessage_RunJob{
			RunJob: &computev1.RunJob{Spec: &computev1.JobSpec{JobId: "job-1", Image: "alpine:3.20"}},
		},
	}

	if !eventually(t, 3*time.Second, func() bool {
		for _, s := range fc.jobUpdateStates("job-1") {
			if s == computev1.JobState_JOB_STATE_FAILED {
				return true
			}
		}
		return false
	}) {
		t.Fatal("expected FAILED for a node with no container runtime")
	}
}

func TestRegisterCarriesSystemInfo(t *testing.T) {
	fc := &fakeCoord{assignID: "node-1"}
	sys := &computev1.SystemInfo{
		Os:         "darwin",
		Arch:       "arm64",
		CpuCores:   10,
		RamTotalMb: 16384,
		Runners:    []computev1.RunnerKind{computev1.RunnerKind_RUNNER_KIND_NATIVE_METAL},
	}
	c := startFake(t, fc, Config{
		NodeIDPath: filepath.Join(t.TempDir(), "node_id"),
		System:     sys,
	})
	runClient(t, c)

	if !eventually(t, 3*time.Second, func() bool {
		return fc.lastRegister() != nil
	}) {
		t.Fatal("no Register received")
	}
	got := fc.lastRegister().GetSystem()
	if got.GetOs() != "darwin" || got.GetArch() != "arm64" {
		t.Errorf("system os/arch = %q/%q, want darwin/arm64", got.GetOs(), got.GetArch())
	}
	if got.GetRamTotalMb() != 16384 || got.GetCpuCores() != 10 {
		t.Errorf("system ram/cores = %d/%d, want 16384/10", got.GetRamTotalMb(), got.GetCpuCores())
	}
	// runners is what placement keys off; losing it would make a capable node
	// look like it can take no work.
	if r := got.GetRunners(); len(r) != 1 || r[0] != computev1.RunnerKind_RUNNER_KIND_NATIVE_METAL {
		t.Errorf("system runners = %v, want [NATIVE_METAL]", r)
	}
}
