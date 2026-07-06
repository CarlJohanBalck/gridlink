// Package router resolves a served model name to a live replica.
package router

import (
	"context"
	"log/slog"

	computev1 "gridlink/contracts/gen/compute/v1"
)

type Router struct {
	log *slog.Logger
	// TODO(claude): grpc conn to coordinator GatewayService
	// TODO(claude): tiny cache: model -> {replicas, fetchedAt}, TTL ~2s
	// TODO(claude): round-robin counter per model
}

func New(coordAddr, token string, log *slog.Logger) *Router {
	// TODO(claude): dial coordinator with bearer-token per-RPC credentials.
	return &Router{log: log}
}

// Pick returns the replica to serve this request, or an error that the proxy
// maps to 404 (unknown model) / 503 (no READY replicas).
func (r *Router) Pick(ctx context.Context, model string) (*computev1.Replica, error) {
	// TODO(claude): ResolveModel (cached), round-robin, error mapping.
	panic("not implemented")
}
