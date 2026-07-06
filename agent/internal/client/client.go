// Package client owns the agent's single bidirectional stream to the
// coordinator: connect, register, heartbeat, receive jobs, report updates,
// reconnect with backoff.
package client

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	computev1 "gridlink/contracts/gen/compute/v1"
	"gridlink/agent/internal/runner"
)

type Config struct {
	CoordinatorAddr string
	Token           string
	NodeIDPath      string // persisted node identity
	GPU             *computev1.GpuInfo
	Runner          runner.Runner
	Logger          *slog.Logger
}

type Client struct {
	cfg Config
}

func New(cfg Config) *Client { return &Client{cfg: cfg} }

// DefaultNodeIDPath is ~/.gridlink/node_id.
func DefaultNodeIDPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".gridlink/node_id"
	}
	return filepath.Join(home, ".gridlink", "node_id")
}

// Run blocks until ctx is cancelled, maintaining the coordinator connection.
//
// Implementation plan (Phase 1):
//  1. Outer loop: dial gRPC (insecure for Phase 1; TODO TLS), open
//     AgentService.Connect with `authorization: bearer <token>` metadata.
//  2. Send Register (node_id from NodeIDPath if present, else empty).
//     Wait for RegisterAck; persist assigned node_id (0600 perms).
//  3. Fan-in goroutines:
//       - heartbeat ticker (interval from ack) -> Heartbeat with a
//         gpu.Utilization sample + active job IDs
//       - stream recv loop -> dispatch RunJob / CancelJob
//       - per-job goroutine consuming runner Updates -> send JobUpdate
//     ONE writer goroutine owns stream.Send; everyone else writes to its
//     send channel.
//  4. On stream error: tear down, backoff (1s..60s with jitter), goto 1.
//     Running jobs keep running across reconnects; the next Heartbeat's
//     active_job_ids lets the coordinator reconcile.
func (c *Client) Run(ctx context.Context) error {
	// TODO(claude): implement per the plan above.
	panic("not implemented")
}
