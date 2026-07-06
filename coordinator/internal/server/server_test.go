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
// and returns the node registry plus a dial func for clients. No real network.
func startTestServer(t *testing.T) (*registry.Registry, func(ctx context.Context) *grpc.ClientConn) {
	t.Helper()

	reg := registry.New(testLogger())
	cfg := Config{
		Token:     testToken,
		Registry:  reg,
		Scheduler: scheduler.New(reg, testLogger()),
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
	return reg, dial
}

func authCtx(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "bearer "+token)
}

func TestConnectRegisterAckHeartbeat(t *testing.T) {
	reg, dial := startTestServer(t)

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
	_, dial := startTestServer(t)

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
			_, dial := startTestServer(t)
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
	_, dial := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	admin := computev1.NewAdminServiceClient(dial(ctx))
	_, err := admin.ListNodes(authCtx(ctx, "nope"), &computev1.ListNodesRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("ListNodes err = %v (code %v), want Unauthenticated", err, status.Code(err))
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
