// Package server hosts the gRPC endpoints: AgentService (bidi stream per
// agent) and AdminService (CLI convenience).
package server

import (
	"context"
	"log/slog"

	"gridlink/coordinator/internal/registry"
	"gridlink/coordinator/internal/scheduler"
)

type Config struct {
	Addr      string
	Token     string
	Registry  *registry.Registry
	Scheduler *scheduler.Scheduler
	Logger    *slog.Logger
}

// Serve blocks until ctx is cancelled.
//
// Implementation plan (Phase 1):
//  - grpc.NewServer with a stream+unary interceptor validating
//    `authorization: bearer <Config.Token>` metadata. Keep auth in ONE place.
//  - AgentService.Connect handler:
//      1. First recv MUST be Register (else close with InvalidArgument).
//      2. registry.Upsert -> nodeID; reply RegisterAck{node_id, 10s}.
//         The Send func wraps stream.Send behind a per-stream mutex.
//      3. Loop recv: Heartbeat -> registry.Touch + scheduler.Reconcile;
//         JobUpdate -> scheduler.OnJobUpdate.
//      4. On recv error / ctx done: registry.MarkDisconnected(nodeID).
//  - AdminService: ListNodes -> registry.List; RunJob -> scheduler.RunJob.
//  - Start registry.StartReaper(ctx, 10s).
//  - Graceful stop on ctx.Done().
func Serve(ctx context.Context, cfg Config) error {
	// TODO(claude): implement per the plan above.
	panic("not implemented")
}
