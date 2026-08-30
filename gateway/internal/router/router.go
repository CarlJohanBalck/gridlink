// Package router resolves a served model name to a live replica.
package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	// ErrUnknownModel means nothing is deployed under that name (404).
	ErrUnknownModel = errors.New("unknown model")
	// ErrNoReplicas means the model exists but has no READY replica (503).
	ErrNoReplicas = errors.New("no ready replicas")
)

// cacheTTL bounds how stale routing may be. Resolving per request would make
// the coordinator a hot path on every token stream; two seconds is short
// enough that a dead replica is dropped quickly, since a failed dial also
// invalidates the entry immediately.
const cacheTTL = 2 * time.Second

type entry struct {
	replicas  []*computev1.Replica
	fetchedAt time.Time
	next      int // round-robin cursor
}

type Router struct {
	client computev1.GatewayServiceClient
	conn   *grpc.ClientConn
	log    *slog.Logger
	now    func() time.Time

	mu    sync.Mutex
	cache map[string]*entry
}

// bearerCreds attaches the shared bootstrap token to every RPC. Kept here and
// in the agent's client only; replacing it with mTLS means touching both.
type bearerCreds struct{ token string }

func (b bearerCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "bearer " + b.token}, nil
}

// RequireTransportSecurity reports false because Phase 1/2 run over plaintext
// gRPC on a trusted network; TLS arrives with per-node credentials.
func (b bearerCreds) RequireTransportSecurity() bool { return false }

func New(coordAddr, token string, log *slog.Logger) *Router {
	conn, err := grpc.NewClient(coordAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearerCreds{token: token}),
	)
	if err != nil {
		// NewClient only fails on malformed targets; connection happens lazily,
		// so a coordinator that is down does not stop the gateway from booting.
		log.Error("invalid coordinator address", "addr", coordAddr, "err", err)
		return &Router{log: log, now: time.Now, cache: map[string]*entry{}}
	}
	return &Router{
		client: computev1.NewGatewayServiceClient(conn),
		conn:   conn,
		log:    log,
		now:    time.Now,
		cache:  map[string]*entry{},
	}
}

func (r *Router) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

// Pick returns the replica to serve this request. Errors map to 404 (unknown
// model) or 503 (no READY replicas) in the proxy.
func (r *Router) Pick(ctx context.Context, model string) (*computev1.Replica, error) {
	if model == "" {
		return nil, fmt.Errorf("%w: empty model name", ErrUnknownModel)
	}
	reps, err := r.replicas(ctx, model)
	if err != nil {
		return nil, err
	}

	// Round-robin across replicas of one model. This spreads load between
	// nodes each serving the whole model — it is not sharding (CLAUDE.md).
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.cache[model]
	if !ok || len(e.replicas) == 0 {
		return reps[0], nil
	}
	rep := e.replicas[e.next%len(e.replicas)]
	e.next++
	return rep, nil
}

// PickExcluding returns a replica other than the one that just failed, so a
// retry does not land on the same broken node.
func (r *Router) PickExcluding(ctx context.Context, model, excludeDeploymentID string) (*computev1.Replica, error) {
	reps, err := r.replicas(ctx, model)
	if err != nil {
		return nil, err
	}
	for _, rep := range reps {
		if rep.GetDeploymentId() != excludeDeploymentID {
			return rep, nil
		}
	}
	return nil, fmt.Errorf("%w for %q", ErrNoReplicas, model)
}

// replicas returns the cached replica set, refreshing when stale.
func (r *Router) replicas(ctx context.Context, model string) ([]*computev1.Replica, error) {
	r.mu.Lock()
	if e, ok := r.cache[model]; ok && r.now().Sub(e.fetchedAt) < cacheTTL {
		reps := e.replicas
		r.mu.Unlock()
		if len(reps) == 0 {
			return nil, fmt.Errorf("%w for %q", ErrNoReplicas, model)
		}
		return reps, nil
	}
	r.mu.Unlock()

	if r.client == nil {
		return nil, fmt.Errorf("%w: no coordinator connection", ErrNoReplicas)
	}
	resp, err := r.client.ResolveModel(ctx, &computev1.ResolveModelRequest{ServedModelName: model})
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", model, err)
	}

	reps := resp.GetReplicas()
	r.mu.Lock()
	e, ok := r.cache[model]
	if !ok {
		e = &entry{}
		r.cache[model] = e
	}
	e.replicas = reps
	e.fetchedAt = r.now()
	r.mu.Unlock()

	if len(reps) == 0 {
		// The coordinator answered, so the name may simply have nothing READY
		// yet. The proxy distinguishes 404 from 503 via ListModels.
		return nil, fmt.Errorf("%w for %q", ErrNoReplicas, model)
	}
	return reps, nil
}

// Invalidate drops a model's cache entry after a failed dial, so the next
// request re-resolves instead of retrying a replica known to be unreachable.
func (r *Router) Invalidate(model string) {
	r.mu.Lock()
	delete(r.cache, model)
	r.mu.Unlock()
}

// Models lists servable model names, for GET /v1/models.
func (r *Router) Models(ctx context.Context) ([]string, error) {
	return r.listModels(ctx, false)
}

// DeployedModels includes models with no READY replica right now, so the proxy
// can tell "unknown model" apart from "temporarily unavailable".
func (r *Router) DeployedModels(ctx context.Context) ([]string, error) {
	return r.listModels(ctx, true)
}

func (r *Router) listModels(ctx context.Context, includeUnready bool) ([]string, error) {
	if r.client == nil {
		return nil, errors.New("no coordinator connection")
	}
	resp, err := r.client.ListModels(ctx, &computev1.ListModelsRequest{IncludeUnready: includeUnready})
	if err != nil {
		return nil, fmt.Errorf("list models: %w", err)
	}
	return resp.GetServedModelNames(), nil
}
