package client

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gridlink/agent/internal/runner"
	computev1 "gridlink/contracts/gen/compute/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ---- fake coordinator over bufconn ----

type fakeCoord struct {
	computev1.UnimplementedAgentServiceServer

	assignID  string
	dropFirst bool // close the first connection right after RegisterAck

	mu            sync.Mutex
	registers     []*computev1.Register
	heartbeats    []*computev1.Heartbeat
	jobUpdates    []*computev1.JobUpdate
	deployUpdates []*computev1.DeploymentUpdate
	connCount     int

	send chan *computev1.CoordinatorMessage // test -> agent, on the live stream
}

func (f *fakeCoord) Connect(stream grpc.BidiStreamingServer[computev1.AgentMessage, computev1.CoordinatorMessage]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	reg := first.GetRegister()
	if reg == nil {
		return status.Error(codes.InvalidArgument, "first message must be Register")
	}

	f.mu.Lock()
	f.registers = append(f.registers, reg)
	f.connCount++
	n := f.connCount
	id := reg.GetNodeId()
	if id == "" {
		id = f.assignID
	}
	f.mu.Unlock()

	if err := stream.Send(&computev1.CoordinatorMessage{
		Payload: &computev1.CoordinatorMessage_RegisterAck{
			RegisterAck: &computev1.RegisterAck{NodeId: id, HeartbeatIntervalS: 1},
		},
	}); err != nil {
		return err
	}

	if n == 1 && f.dropFirst {
		return status.Error(codes.Unavailable, "simulated disconnect")
	}

	recvErr := make(chan error, 1)
	go func() {
		for {
			m, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			f.mu.Lock()
			switch p := m.Payload.(type) {
			case *computev1.AgentMessage_Heartbeat:
				f.heartbeats = append(f.heartbeats, p.Heartbeat)
			case *computev1.AgentMessage_JobUpdate:
				f.jobUpdates = append(f.jobUpdates, p.JobUpdate)
			case *computev1.AgentMessage_DeploymentUpdate:
				f.deployUpdates = append(f.deployUpdates, p.DeploymentUpdate)
			}
			f.mu.Unlock()
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case err := <-recvErr:
			return err
		case m := <-f.send:
			if err := stream.Send(m); err != nil {
				return err
			}
		}
	}
}

func (f *fakeCoord) lastRegister() *computev1.Register {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.registers) == 0 {
		return nil
	}
	return f.registers[len(f.registers)-1]
}

func (f *fakeCoord) registerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.registers)
}

func (f *fakeCoord) jobUpdateStates(jobID string) []computev1.JobState {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []computev1.JobState
	for _, u := range f.jobUpdates {
		if u.GetJobId() == jobID {
			out = append(out, u.GetState())
		}
	}
	return out
}

// anyHeartbeatHasJob reports whether any received heartbeat listed jobID active.
func (f *fakeCoord) anyHeartbeatHasJob(jobID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, hb := range f.heartbeats {
		for _, id := range hb.GetActiveJobIds() {
			if id == jobID {
				return true
			}
		}
	}
	return false
}

// startFake wires a fakeCoord onto a bufconn listener and returns it plus a
// client pre-seeded with the matching in-memory dialer.
func startFake(t *testing.T, fc *fakeCoord, cfg Config) *Client {
	t.Helper()
	if fc.send == nil {
		fc.send = make(chan *computev1.CoordinatorMessage, 8)
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	computev1.RegisterAgentServiceServer(srv, fc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	if cfg.Logger == nil {
		cfg.Logger = testLogger()
	}
	if cfg.CoordinatorAddr == "" {
		cfg.CoordinatorAddr = "passthrough:///bufnet"
	}
	c := New(cfg)
	c.dialer = func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	return c
}

// ---- fake runner ----

// scriptRunner emits a fixed sequence of updates per job, then closes. If block
// is non-nil it waits on it after emitting `seq` (to hold a job "running").
type scriptRunner struct {
	seq   []runner.Update
	block chan struct{}

	mu      sync.Mutex
	started []*computev1.JobSpec
}

func (r *scriptRunner) Run(ctx context.Context, spec *computev1.JobSpec) (<-chan runner.Update, error) {
	r.mu.Lock()
	r.started = append(r.started, spec)
	r.mu.Unlock()

	ch := make(chan runner.Update, len(r.seq)+1)
	go func() {
		defer close(ch)
		for _, u := range r.seq {
			u.JobID = spec.GetJobId()
			select {
			case ch <- u:
			case <-ctx.Done():
				ch <- runner.Update{JobID: spec.GetJobId(), State: computev1.JobState_JOB_STATE_CANCELLED}
				return
			}
		}
		if r.block != nil {
			select {
			case <-r.block:
			case <-ctx.Done():
				ch <- runner.Update{JobID: spec.GetJobId(), State: computev1.JobState_JOB_STATE_CANCELLED}
			}
		}
	}()
	return ch, nil
}

func (r *scriptRunner) startedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.started)
}

// ---- helpers ----

func eventually(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func runClient(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("client.Run did not return after ctx cancel")
		}
	})
}

// ---- tests ----

func TestRegisterAndPersistNodeID(t *testing.T) {
	nodeIDPath := filepath.Join(t.TempDir(), "node_id")
	fc := &fakeCoord{assignID: "node-assigned-1"}
	r := &scriptRunner{}
	c := startFake(t, fc, Config{Token: "tok", NodeIDPath: nodeIDPath, Runner: r})
	runClient(t, c)

	if !eventually(t, 3*time.Second, func() bool { return fc.registerCount() >= 1 }) {
		t.Fatal("coordinator never received a Register")
	}
	reg := fc.lastRegister()
	if reg.GetNodeId() != "" {
		t.Errorf("first Register node_id = %q, want empty", reg.GetNodeId())
	}
	if reg.GetAgentVersion() == "" {
		t.Error("Register.agent_version is empty")
	}

	// The assigned node_id is persisted to disk (0600).
	if !eventually(t, 3*time.Second, func() bool {
		b, err := os.ReadFile(nodeIDPath)
		return err == nil && string(b) == "node-assigned-1\n"
	}) {
		t.Fatal("node_id was not persisted with the assigned value")
	}
	fi, err := os.Stat(nodeIDPath)
	if err != nil {
		t.Fatalf("stat node_id: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("node_id perms = %o, want 600", perm)
	}
}

func TestReconnectResendsPersistedNodeID(t *testing.T) {
	nodeIDPath := filepath.Join(t.TempDir(), "node_id")
	fc := &fakeCoord{assignID: "node-assigned-2", dropFirst: true}
	r := &scriptRunner{}
	c := startFake(t, fc, Config{Token: "tok", NodeIDPath: nodeIDPath, Runner: r})
	runClient(t, c)

	// After the first connection is dropped, the agent reconnects and its
	// second Register carries the persisted node_id.
	if !eventually(t, 5*time.Second, func() bool { return fc.registerCount() >= 2 }) {
		t.Fatalf("expected >=2 Registers after reconnect, got %d", fc.registerCount())
	}
	if got := fc.lastRegister().GetNodeId(); got != "node-assigned-2" {
		t.Errorf("reconnect Register node_id = %q, want node-assigned-2", got)
	}
}

func TestRunJobStreamsUpdates(t *testing.T) {
	fc := &fakeCoord{assignID: "node-x"}
	r := &scriptRunner{seq: []runner.Update{
		{State: computev1.JobState_JOB_STATE_PENDING},
		{State: computev1.JobState_JOB_STATE_RUNNING, LogChunk: "hello"},
		{State: computev1.JobState_JOB_STATE_SUCCEEDED, ExitCode: 0},
	}}
	c := startFake(t, fc, Config{Token: "tok", NodeIDPath: filepath.Join(t.TempDir(), "node_id"), Runner: r})
	runClient(t, c)

	if !eventually(t, 3*time.Second, func() bool { return fc.registerCount() >= 1 }) {
		t.Fatal("agent never registered")
	}

	fc.send <- &computev1.CoordinatorMessage{Payload: &computev1.CoordinatorMessage_RunJob{
		RunJob: &computev1.RunJob{Spec: &computev1.JobSpec{JobId: "job-1", Image: "busybox"}},
	}}

	if !eventually(t, 3*time.Second, func() bool { return r.startedCount() == 1 }) {
		t.Fatal("runner was never asked to run the job")
	}
	if !eventually(t, 3*time.Second, func() bool {
		got := fc.jobUpdateStates("job-1")
		return len(got) == 3 &&
			got[len(got)-1] == computev1.JobState_JOB_STATE_SUCCEEDED
	}) {
		t.Fatalf("did not receive PENDING->RUNNING->SUCCEEDED, got %v", fc.jobUpdateStates("job-1"))
	}
	states := fc.jobUpdateStates("job-1")
	want := []computev1.JobState{
		computev1.JobState_JOB_STATE_PENDING,
		computev1.JobState_JOB_STATE_RUNNING,
		computev1.JobState_JOB_STATE_SUCCEEDED,
	}
	for i, w := range want {
		if states[i] != w {
			t.Errorf("state[%d] = %v, want %v", i, states[i], w)
		}
	}
}

func TestHeartbeatReportsActiveJob(t *testing.T) {
	fc := &fakeCoord{assignID: "node-y"}
	block := make(chan struct{})
	r := &scriptRunner{
		seq:   []runner.Update{{State: computev1.JobState_JOB_STATE_RUNNING}},
		block: block,
	}
	c := startFake(t, fc, Config{Token: "tok", NodeIDPath: filepath.Join(t.TempDir(), "node_id"), Runner: r})
	runClient(t, c)

	if !eventually(t, 3*time.Second, func() bool { return fc.registerCount() >= 1 }) {
		t.Fatal("agent never registered")
	}
	fc.send <- &computev1.CoordinatorMessage{Payload: &computev1.CoordinatorMessage_RunJob{
		RunJob: &computev1.RunJob{Spec: &computev1.JobSpec{JobId: "job-hb", Image: "busybox"}},
	}}

	// While the job blocks, at least one heartbeat should report it active.
	if !eventually(t, 4*time.Second, func() bool { return fc.anyHeartbeatHasJob("job-hb") }) {
		t.Fatal("no heartbeat reported the active job")
	}
	close(block) // let the job finish
}

func TestCancelJobStopsRunner(t *testing.T) {
	fc := &fakeCoord{assignID: "node-z"}
	block := make(chan struct{})
	r := &scriptRunner{
		seq:   []runner.Update{{State: computev1.JobState_JOB_STATE_RUNNING}},
		block: block,
	}
	defer close(block) // safety net if the test fails before cancel propagates
	c := startFake(t, fc, Config{Token: "tok", NodeIDPath: filepath.Join(t.TempDir(), "node_id"), Runner: r})
	runClient(t, c)

	if !eventually(t, 3*time.Second, func() bool { return fc.registerCount() >= 1 }) {
		t.Fatal("agent never registered")
	}
	fc.send <- &computev1.CoordinatorMessage{Payload: &computev1.CoordinatorMessage_RunJob{
		RunJob: &computev1.RunJob{Spec: &computev1.JobSpec{JobId: "job-c", Image: "busybox"}},
	}}
	if !eventually(t, 3*time.Second, func() bool { return r.startedCount() == 1 }) {
		t.Fatal("runner never started")
	}

	fc.send <- &computev1.CoordinatorMessage{Payload: &computev1.CoordinatorMessage_CancelJob{
		CancelJob: &computev1.CancelJob{JobId: "job-c"},
	}}

	// Cancelling the job ctx makes the scriptRunner emit CANCELLED.
	if !eventually(t, 3*time.Second, func() bool {
		st := fc.jobUpdateStates("job-c")
		return len(st) > 0 && st[len(st)-1] == computev1.JobState_JOB_STATE_CANCELLED
	}) {
		t.Fatalf("job was not cancelled, states = %v", fc.jobUpdateStates("job-c"))
	}
}

func TestReadWriteNodeID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "node_id")

	if got := readNodeID(path, testLogger()); got != "" {
		t.Errorf("readNodeID on missing file = %q, want empty", got)
	}
	if err := writeNodeID(path, "abc-123"); err != nil {
		t.Fatalf("writeNodeID: %v", err)
	}
	if got := readNodeID(path, testLogger()); got != "abc-123" {
		t.Errorf("round-trip node_id = %q, want abc-123", got)
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	b := backoff{cur: backoffBase}
	var prevCur time.Duration
	for i := 0; i < 20; i++ {
		w := b.next()
		if w <= 0 {
			t.Fatalf("backoff wait %v must be positive", w)
		}
		if w > backoffCap {
			t.Fatalf("backoff wait %v exceeded cap %v", w, backoffCap)
		}
		prevCur = b.cur
	}
	if prevCur != backoffCap {
		t.Errorf("backoff cur settled at %v, want cap %v", prevCur, backoffCap)
	}

	b.reset()
	if b.cur != backoffBase {
		t.Errorf("after reset cur = %v, want %v", b.cur, backoffBase)
	}
}
