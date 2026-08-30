// Package server hosts the gRPC endpoints: AgentService (bidi stream per
// agent) and AdminService (CLI convenience).
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
	"gridlink/coordinator/internal/deployments"
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

// shutdownGrace bounds GracefulStop on shutdown. Agent streams are infinite,
// so a bare GracefulStop would wait forever; after the grace period the
// server is stopped hard and agents reconnect to the next coordinator.
const shutdownGrace = 5 * time.Second

type Config struct {
	Addr        string
	Token       string
	Registry    *registry.Registry
	Scheduler   *scheduler.Scheduler
	Deployments *deployments.Manager
	Logger      *slog.Logger
	// UsageLogPath receives ReportUsage records as JSONL (Phase 2). Empty
	// disables usage logging.
	UsageLogPath string
}

// Serve blocks until ctx is cancelled.
func Serve(ctx context.Context, cfg Config) error {
	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.Addr, err)
	}

	srv := buildServer(cfg)
	cfg.Registry.StartReaper(ctx, time.Duration(HeartbeatIntervalS)*time.Second)
	cfg.Deployments.StartReconciler(ctx)

	go func() {
		<-ctx.Done()
		cfg.Logger.Info("shutting down gRPC server")
		done := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(shutdownGrace):
			cfg.Logger.Warn("graceful stop timed out; closing agent streams")
			srv.Stop()
		}
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
		reg:    cfg.Registry,
		sched:  cfg.Scheduler,
		deploy: cfg.Deployments,
		log:    cfg.Logger,
	})
	computev1.RegisterAdminServiceServer(srv, &adminServer{
		reg:    cfg.Registry,
		sched:  cfg.Scheduler,
		deploy: cfg.Deployments,
		log:    cfg.Logger,
	})
	computev1.RegisterGatewayServiceServer(srv, &gatewayServer{
		deploy:   cfg.Deployments,
		usageLog: cfg.UsageLogPath,
		log:      cfg.Logger,
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
	reg    *registry.Registry
	sched  *scheduler.Scheduler
	deploy *deployments.Manager
	log    *slog.Logger
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
			s.sched.Reconcile(nodeID, p.Heartbeat.GetActiveJobIds())
			s.deploy.OnHeartbeat(nodeID, p.Heartbeat.GetActiveDeploymentIds())
		case *computev1.AgentMessage_JobUpdate:
			s.sched.OnJobUpdate(nodeID, p.JobUpdate)
		case *computev1.AgentMessage_DeploymentUpdate:
			s.deploy.OnUpdate(nodeID, p.DeploymentUpdate)
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
	reg    *registry.Registry
	sched  *scheduler.Scheduler
	deploy *deployments.Manager
	log    *slog.Logger
}

func (s *adminServer) ListNodes(_ context.Context, _ *computev1.ListNodesRequest) (*computev1.ListNodesResponse, error) {
	nodes := s.reg.List()
	// Attach placements so `why was nothing scheduled here` is answerable from
	// one call rather than by correlating logs.
	for _, n := range nodes {
		n.DeploymentIds = s.deploy.DeploymentIDsOn(n.GetNodeId())
	}
	return &computev1.ListNodesResponse{Nodes: nodes}, nil
}

func (s *adminServer) RunJob(_ context.Context, req *computev1.RunJobRequest) (*computev1.RunJobResponse, error) {
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if req.GetSpec().GetImage() == "" {
		return nil, status.Error(codes.InvalidArgument, "spec.image is required")
	}

	jobID, err := s.sched.RunJob(req.GetNodeId(), req.GetSpec())
	switch {
	case errors.Is(err, scheduler.ErrUnknownNode):
		return nil, status.Error(codes.NotFound, err.Error())
	case errors.Is(err, scheduler.ErrNodeOffline):
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	case err != nil:
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &computev1.RunJobResponse{JobId: jobID}, nil
}

// ---- AdminService: deployments ----

func (s *adminServer) CreateDeployment(_ context.Context, req *computev1.CreateDeploymentRequest) (*computev1.CreateDeploymentResponse, error) {
	id, err := s.deploy.Create(req.GetSpec(), req.GetReplicas())
	switch {
	case errors.Is(err, deployments.ErrInvalidSpec), errors.Is(err, deployments.ErrReplicasUnsupp):
		return nil, status.Error(codes.InvalidArgument, err.Error())
	case err != nil:
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &computev1.CreateDeploymentResponse{DeploymentId: id}, nil
}

func (s *adminServer) DeleteDeployment(_ context.Context, req *computev1.DeleteDeploymentRequest) (*computev1.DeleteDeploymentResponse, error) {
	err := s.deploy.Delete(req.GetDeploymentId())
	switch {
	case errors.Is(err, deployments.ErrUnknownDeploy):
		return nil, status.Error(codes.NotFound, err.Error())
	case err != nil:
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &computev1.DeleteDeploymentResponse{}, nil
}

func (s *adminServer) ListDeployments(_ context.Context, _ *computev1.ListDeploymentsRequest) (*computev1.ListDeploymentsResponse, error) {
	return &computev1.ListDeploymentsResponse{Deployments: s.deploy.List()}, nil
}

// ---- GatewayService ----

type gatewayServer struct {
	computev1.UnimplementedGatewayServiceServer
	deploy   *deployments.Manager
	usageLog string
	log      *slog.Logger

	usageMu sync.Mutex
}

func (s *gatewayServer) ResolveModel(_ context.Context, req *computev1.ResolveModelRequest) (*computev1.ResolveModelResponse, error) {
	if req.GetServedModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "served_model_name is required")
	}
	return &computev1.ResolveModelResponse{
		Replicas: s.deploy.Resolve(req.GetServedModelName()),
	}, nil
}

// ReportUsage appends one JSONL record per request. Phase 2 only logs; Phase 3
// turns this into the ledger feed, so the record shape is the thing that has
// to be right, not the sink.
func (s *gatewayServer) ReportUsage(_ context.Context, req *computev1.ReportUsageRequest) (*computev1.ReportUsageResponse, error) {
	if s.usageLog == "" {
		return &computev1.ReportUsageResponse{}, nil
	}
	rec := map[string]any{
		"ts_unix_ms":        req.GetTimestampUnixMs(),
		"served_model_name": req.GetServedModelName(),
		"node_id":           req.GetNodeId(),
		"deployment_id":     req.GetDeploymentId(),
		"api_key_id":        req.GetApiKeyId(),
		"prompt_tokens":     req.GetPromptTokens(),
		"completion_tokens": req.GetCompletionTokens(),
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Serialized so concurrent gateway reports cannot interleave partial lines
	// into the JSONL.
	s.usageMu.Lock()
	defer s.usageMu.Unlock()
	f, err := os.OpenFile(s.usageLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		// Usage is billing data: failing loudly beats silently losing it.
		s.log.Error("cannot open usage log", "path", s.usageLog, "err", err)
		return nil, status.Error(codes.Unavailable, "usage log unavailable")
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		s.log.Error("cannot append usage record", "err", err)
		return nil, status.Error(codes.Unavailable, "usage log write failed")
	}
	return &computev1.ReportUsageResponse{}, nil
}
