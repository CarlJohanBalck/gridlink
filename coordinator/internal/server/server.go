// Package server hosts the gRPC endpoints: AgentService (bidi stream per
// agent) and AdminService (CLI convenience).
package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
	"gridlink/coordinator/internal/registry"
	"gridlink/coordinator/internal/scheduler"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// HeartbeatIntervalS is the cadence the coordinator asks agents to heartbeat
// at; the reaper marks a node OFFLINE after 3 missed intervals.
const HeartbeatIntervalS uint32 = 10

type Config struct {
	Addr      string
	Token     string
	Registry  *registry.Registry
	Scheduler *scheduler.Scheduler
	Logger    *slog.Logger
}

// Serve blocks until ctx is cancelled.
func Serve(ctx context.Context, cfg Config) error {
	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.Addr, err)
	}

	srv := buildServer(cfg)
	cfg.Registry.StartReaper(ctx, time.Duration(HeartbeatIntervalS)*time.Second)

	go func() {
		<-ctx.Done()
		cfg.Logger.Info("shutting down gRPC server")
		srv.GracefulStop()
	}()

	cfg.Logger.Info("coordinator listening", "addr", cfg.Addr)
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// buildServer wires interceptors and services onto a *grpc.Server without
// binding a listener, so tests can drive it over an in-memory bufconn.
func buildServer(cfg Config) *grpc.Server {
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(unaryAuthInterceptor(cfg.Token)),
		grpc.StreamInterceptor(streamAuthInterceptor(cfg.Token)),
	)
	computev1.RegisterAgentServiceServer(srv, &agentServer{
		reg:   cfg.Registry,
		sched: cfg.Scheduler,
		log:   cfg.Logger,
	})
	computev1.RegisterAdminServiceServer(srv, &adminServer{
		reg: cfg.Registry,
		log: cfg.Logger,
	})
	// Reflection is a Phase-1 dev convenience so grpcurl works without proto
	// paths. Note: the auth interceptor also guards the reflection service.
	reflection.Register(srv)
	return srv
}

// ---- auth: kept in ONE place so mTLS/per-node keys can replace it later ----

// checkToken validates `authorization: bearer <token>` stream/RPC metadata.
func checkToken(ctx context.Context, want string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return status.Error(codes.Unauthenticated, "missing authorization")
	}
	const prefix = "bearer "
	got := vals[0]
	if len(got) < len(prefix) || !strings.EqualFold(got[:len(prefix)], prefix) || got[len(prefix):] != want {
		return status.Error(codes.Unauthenticated, "invalid token")
	}
	return nil
}

func unaryAuthInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := checkToken(ctx, token); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func streamAuthInterceptor(token string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := checkToken(ss.Context(), token); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// ---- AgentService ----

type agentServer struct {
	computev1.UnimplementedAgentServiceServer
	reg   *registry.Registry
	sched *scheduler.Scheduler // TODO: OnJobUpdate/Reconcile wiring when scheduler lands
	log   *slog.Logger
}

// Connect handles one agent's lifetime on a single bidi stream.
func (s *agentServer) Connect(stream grpc.BidiStreamingServer[computev1.AgentMessage, computev1.CoordinatorMessage]) error {
	ctx := stream.Context()

	// The first message MUST be a Register.
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	reg := first.GetRegister()
	if reg == nil {
		return status.Error(codes.InvalidArgument, "first message must be Register")
	}

	// One writer owns stream.Send; guard it with a per-stream mutex.
	var sendMu sync.Mutex
	send := func(m *computev1.CoordinatorMessage) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(m)
	}

	nodeID := s.reg.Upsert(reg, send)
	log := s.log.With("node_id", nodeID)
	defer s.reg.MarkDisconnected(nodeID)

	if err := send(&computev1.CoordinatorMessage{
		Payload: &computev1.CoordinatorMessage_RegisterAck{
			RegisterAck: &computev1.RegisterAck{
				NodeId:             nodeID,
				HeartbeatIntervalS: HeartbeatIntervalS,
			},
		},
	}); err != nil {
		return err
	}
	log.Info("agent connected", "hostname", reg.GetHostname())

	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}
			return err
		}
		switch p := msg.Payload.(type) {
		case *computev1.AgentMessage_Heartbeat:
			s.reg.Touch(nodeID, p.Heartbeat)
		case *computev1.AgentMessage_JobUpdate:
			// TODO: scheduler.OnJobUpdate when scheduler is implemented.
			log.Debug("job update", "job_id", p.JobUpdate.GetJobId(), "state", p.JobUpdate.GetState())
		case *computev1.AgentMessage_Register:
			// Redundant re-register on a live stream; refresh in place.
			s.reg.Upsert(p.Register, send)
		default:
			log.Debug("ignoring agent message", "type", fmt.Sprintf("%T", p))
		}
	}
}

// ---- AdminService ----

type adminServer struct {
	computev1.UnimplementedAdminServiceServer
	reg *registry.Registry
	log *slog.Logger
}

func (s *adminServer) ListNodes(_ context.Context, _ *computev1.ListNodesRequest) (*computev1.ListNodesResponse, error) {
	return &computev1.ListNodesResponse{Nodes: s.reg.List()}, nil
}

// RunJob and the deployment RPCs stay on UnimplementedAdminServiceServer
// (returning Unimplemented) until the scheduler (Phase 1) / deployments
// (Phase 2) land.
