// Package dialer abstracts HOW the gateway reaches a replica's data plane.
// This interface is the seam that lets Phase 2a (Tailscale: plain TCP dial to
// a tailnet IP) be replaced by Phase 2b (reverse tunnel: agents dial out to
// the gateway, yamux-multiplexed streams) with zero changes elsewhere.
package dialer

import (
	"context"
	"net"
)

type Dialer interface {
	// DialReplica opens a TCP-like connection to the replica's addr
	// (as returned by ResolveModel).
	DialReplica(ctx context.Context, addr string) (net.Conn, error)
}

// Direct dials addr with net.Dialer. Works when gateway and nodes share a
// network (Tailscale tailnet, or localhost dev).
type Direct struct{ d net.Dialer }

func NewDirect() *Direct { return &Direct{} }

func (x *Direct) DialReplica(ctx context.Context, addr string) (net.Conn, error) {
	return x.d.DialContext(ctx, "tcp", addr)
}

// TODO(phase 2b, claude): TunnelDialer + a TunnelServer the agents dial into:
//   - agent opens ONE outbound TLS TCP conn to the gateway, runs yamux server
//   - gateway keeps a map[node_id]*yamux.Session
//   - DialReplica(addr "node_id/deployment_id") -> session.Open() stream,
//     first bytes = small header naming the deployment; agent side connects
//     the stream to 127.0.0.1:<host_port> of that deployment's container.
