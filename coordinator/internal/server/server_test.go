package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
	"gridlink/coordinator/internal/registry"
	"gridlink/coordinator/internal/scheduler"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const testToken = "test-token"

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startTestServer spins up the coordinator over an in-memory bufconn listener
// and returns the node registry, the scheduler, plus a dial func for clients.
// No real network.
func startTestServer(t *testing.T) (*registry.Registry, *scheduler.Scheduler, func(ctx context.Context) *grpc.ClientConn) {
	t.Helper()

	reg := registry.New(testLogger())
	sched := scheduler.New(reg, testLogger())
	cfg := Config{
		Token:     testToken,
		Registry:  reg,
		Scheduler: sched,
		Logger:    testLogger(),
	}
	srv := buildServer(cfg)

	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dial := func(ctx context.Context) *grpc.ClientConn {
		t.Helper()
		conn, err := grpc.NewClient(
			"passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	return reg, sched, dial
}

func authCtx(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "bearer "+token)
}

func TestConnectRegisterAckHeartbeat(t *testing.T) {
	reg, _, dial := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := computev1.NewAgentServiceClient(dial(ctx))
	stream, err := client.Connect(authCtx(ctx, testToken))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Register -> expect a RegisterAck with an assigned node_id.
	if err := stream.Send(&computev1.AgentMessage{
		Payload: &computev1.AgentMessage_Register{
			Register: &computev1.Register{
				Hostname: "host-a",
				Gpu:      &computev1.GpuInfo{Vendor: "nvidia", Model: "RTX 4090"},
			},
		},
	}); err != nil {
		t.Fatalf("send register: %v", err)
	}

	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv ack: %v", err)
	}
	ack := msg.GetRegisterAck()
	if ack == nil {
		t.Fatalf("first reply is not a RegisterAck: %T", msg.Payload)
	}
	nodeID := ack.GetNodeId()
	if nodeID == "" {
		t.Fatal("RegisterAck.node_id is empty")
	}
	if ack.GetHeartbeatIntervalS() != HeartbeatIntervalS {
		t.Errorf("heartbeat interval = %d, want %d", ack.GetHeartbeatIntervalS(), HeartbeatIntervalS)
	}

	// The node is registered and visible via AdminService.ListNodes.
	admin := computev1.NewAdminServiceClient(dial(ctx))
	resp, err := admin.ListNodes(authCtx(ctx, testToken), &computev1.ListNodesRequest{})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(resp.GetNodes()) != 1 {
		t.Fatalf("ListNodes returned %d nodes, want 1", len(resp.GetNodes()))
	}
	if n := resp.GetNodes()[0]; n.GetNodeId() != nodeID ||
		n.GetHostname() != "host-a" ||
		n.GetStatus() != computev1.NodeStatus_NODE_STATUS_ONLINE ||
		n.GetGpu().GetModel() != "RTX 4090" {
		t.Errorf("unexpected node summary: %+v", n)
	}

	// Heartbeat -> LastSeen advances. Sleep past a millisecond tick first so the
	// Touch timestamp is observably later than the register timestamp.
	before, ok := reg.Get(nodeID)
	if !ok {
		t.Fatal("node missing from registry")
	}
	time.Sleep(2 * time.Millisecond)
	if err := stream.Send(&computev1.AgentMessage{
		Payload: &computev1.AgentMessage_Heartbeat{Heartbeat: &computev1.Heartbeat{}},
	}); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}

	if !eventually(t, 2*time.Second, func() bool {
		n, ok := reg.Get(nodeID)
		return ok && n.LastSeen.After(before.LastSeen)
	}) {
		t.Fatal("heartbeat did not advance LastSeen")
	}
}

func TestConnectFirstMessageMustBeRegister(t *testing.T) {
	_, _, dial := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := computev1.NewAgentServiceClient(dial(ctx))
	stream, err := client.Connect(authCtx(ctx, testToken))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// A Heartbeat as the first message must be rejected with InvalidArgument.
	if err := stream.Send(&computev1.AgentMessage{
		Payload: &computev1.AgentMessage_Heartbeat{Heartbeat: &computev1.Heartbeat{}},
	}); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("recv err = %v (code %v), want InvalidArgument", err, status.Code(err))
	}
}

func TestConnectRejectsBadToken(t *testing.T) {
	cases := []struct {
		name string
		ctx  func(context.Context) context.Context
	}{
		{"wrong token", func(c context.Context) context.Context { return authCtx(c, "nope") }},
		{"no token", func(c context.Context) context.Context { return c }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, dial := startTestServer(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			client := computev1.NewAgentServiceClient(dial(ctx))
			stream, err := client.Connect(tc.ctx(ctx))
			if err != nil {
				t.Fatalf("Connect: %v", err)
			}
			_ = stream.Send(&computev1.AgentMessage{
				Payload: &computev1.AgentMessage_Register{Register: &computev1.Register{Hostname: "h"}},
			})
			_, err = stream.Recv()
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("recv err = %v (code %v), want Unauthenticated", err, status.Code(err))
			}
		})
	}
}

func TestListNodesRejectsBadToken(t *testing.T) {
	_, _, dial := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	admin := computev1.NewAdminServiceClient(dial(ctx))
	_, err := admin.ListNodes(authCtx(ctx, "nope"), &computev1.ListNodesRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("ListNodes err = %v (code %v), want Unauthenticated", err, status.Code(err))
	}
}

// TestRunJobEndToEnd drives the whole Phase-1 job path over the wire: an
// agent connects, AdminService.RunJob dispatches to it, and its JobUpdates
// land in the scheduler's job table.
func TestRunJobEndToEnd(t *testing.T) {
	_, sched, dial := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Agent connects and registers.
	stream, err := computev1.NewAgentServiceClient(dial(ctx)).Connect(authCtx(ctx, testToken))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := stream.Send(&computev1.AgentMessage{
		Payload: &computev1.AgentMessage_Register{Register: &computev1.Register{Hostname: "host-a"}},
	}); err != nil {
		t.Fatalf("send register: %v", err)
	}
	ack, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv ack: %v", err)
	}
	nodeID := ack.GetRegisterAck().GetNodeId()

	// Admin pushes a job to that node.
	admin := computev1.NewAdminServiceClient(dial(ctx))
	resp, err := admin.RunJob(authCtx(ctx, testToken), &computev1.RunJobRequest{
		NodeId: nodeID,
		Spec: &computev1.JobSpec{
			Image:   "nvidia/cuda:12.4.1-base-ubuntu22.04",
			Command: []string{"nvidia-smi"},
			Gpu:     true,
		},
	})
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	jobID := resp.GetJobId()
	if jobID == "" {
		t.Fatal("RunJob returned empty job_id")
	}

	// The agent receives the dispatched spec on its stream.
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv job: %v", err)
	}
	rj := msg.GetRunJob()
	if rj == nil {
		t.Fatalf("agent received %T, want RunJob", msg.Payload)
	}
	if rj.GetSpec().GetJobId() != jobID || rj.GetSpec().GetImage() != "nvidia/cuda:12.4.1-base-ubuntu22.04" {
		t.Errorf("dispatched spec = %+v, want job %s", rj.GetSpec(), jobID)
	}

	// The agent streams PENDING -> RUNNING -> SUCCEEDED back; the scheduler's
	// job table follows.
	for _, st := range []computev1.JobState{
		computev1.JobState_JOB_STATE_PENDING,
		computev1.JobState_JOB_STATE_RUNNING,
		computev1.JobState_JOB_STATE_SUCCEEDED,
	} {
		if err := stream.Send(&computev1.AgentMessage{
			Payload: &computev1.AgentMessage_JobUpdate{JobUpdate: &computev1.JobUpdate{
				JobId: jobID, State: st, LogChunk: "line\n",
			}},
		}); err != nil {
			t.Fatalf("send update %v: %v", st, err)
		}
	}
	if !eventually(t, 2*time.Second, func() bool {
		j, ok := sched.Get(jobID)
		return ok && j.State == computev1.JobState_JOB_STATE_SUCCEEDED
	}) {
		j, _ := sched.Get(jobID)
		t.Fatalf("job never reached SUCCEEDED, last state %v", j.State)
	}
}

func TestRunJobValidation(t *testing.T) {
	reg, _, dial := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	admin := computev1.NewAdminServiceClient(dial(ctx))

	offlineID := reg.Upsert(&computev1.Register{Hostname: "host-off"}, nil)
	reg.MarkDisconnected(offlineID)

	spec := &computev1.JobSpec{Image: "test/image"}
	tests := []struct {
		name string
		req  *computev1.RunJobRequest
		want codes.Code
	}{
		{"missing node_id", &computev1.RunJobRequest{Spec: spec}, codes.InvalidArgument},
		{"missing image", &computev1.RunJobRequest{NodeId: "n", Spec: &computev1.JobSpec{}}, codes.InvalidArgument},
		{"unknown node", &computev1.RunJobRequest{NodeId: "nope", Spec: spec}, codes.NotFound},
		{"offline node", &computev1.RunJobRequest{NodeId: offlineID, Spec: spec}, codes.FailedPrecondition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := admin.RunJob(authCtx(ctx, testToken), tt.req)
			if status.Code(err) != tt.want {
				t.Fatalf("RunJob err = %v (code %v), want %v", err, status.Code(err), tt.want)
			}
		})
	}
}

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
