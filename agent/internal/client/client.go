// Package client owns the agent's single bidirectional stream to the
// coordinator: connect, register, heartbeat, receive jobs, report updates,
// reconnect with backoff.
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gridlink/agent/internal/runner"
	computev1 "gridlink/contracts/gen/compute/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	agentVersion       = "phase1"
	defaultHeartbeatS  = 10
	backoffBase        = 1 * time.Second
	backoffCap         = 60 * time.Second
	outboundBufferSize = 32
)

type Config struct {
	CoordinatorAddr string
	Token           string
	NodeIDPath      string // persisted node identity
	GPU             *computev1.GpuInfo
	// Utilization samples GPU load for heartbeats. Optional; nil (or a
	// sampling error) sends heartbeats without utilization.
	Utilization func(context.Context) (*computev1.GpuUtilization, error)
	Runner      runner.Runner
	Logger      *slog.Logger
}

type Client struct {
	cfg Config

	// dialer is a test seam for in-memory transports (e.g. bufconn). Nil in
	// production, where the real gRPC dialer is used.
	dialer func(context.Context, string) (net.Conn, error)

	mu   sync.Mutex
	jobs map[string]context.CancelFunc // active jobs -> their cancel; survives reconnects
	out  chan *computev1.AgentMessage  // current session's outbound queue; nil when disconnected
}

func New(cfg Config) *Client {
	return &Client{cfg: cfg, jobs: make(map[string]context.CancelFunc)}
}

// DefaultNodeIDPath is ~/.gridlink/node_id.
func DefaultNodeIDPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".gridlink/node_id"
	}
	return filepath.Join(home, ".gridlink", "node_id")
}

// Run blocks until ctx is cancelled, maintaining the coordinator connection.
// Each iteration is one stream lifetime; on failure it backs off (with jitter)
// and reconnects, resending Register. Running jobs are owned by the Client and
// keep running across reconnects.
func (c *Client) Run(ctx context.Context) error {
	b := backoff{cur: backoffBase}
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := c.runOnce(ctx, b.reset)
		if ctx.Err() != nil {
			return nil
		}
		wait := b.next()
		c.cfg.Logger.Warn("coordinator connection lost; reconnecting",
			"err", err, "backoff", wait)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

// runOnce maintains a single stream from dial to teardown. onConnected is
// invoked once RegisterAck arrives, so the caller can reset its backoff.
func (c *Client) runOnce(ctx context.Context, onConnected func()) (err error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if c.dialer != nil {
		opts = append(opts, grpc.WithContextDialer(c.dialer))
	}
	conn, err := grpc.NewClient(c.cfg.CoordinatorAddr, opts...)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// sessCtx bounds this stream's helper goroutines (writer, heartbeat).
	// It is a child of ctx, so a global shutdown tears the session down too.
	sessCtx, sessCancel := context.WithCancel(ctx)
	defer sessCancel()

	streamCtx := metadata.AppendToOutgoingContext(sessCtx, "authorization", "bearer "+c.cfg.Token)
	stream, err := computev1.NewAgentServiceClient(conn).Connect(streamCtx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	out := make(chan *computev1.AgentMessage, outboundBufferSize)

	// Single writer goroutine: the ONLY sender on this stream. Everyone else
	// enqueues to `out`. Concurrent stream.Send would be a data race.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-sessCtx.Done():
				return
			case m := <-out:
				if err := stream.Send(m); err != nil {
					sessCancel() // unblock recv loop and heartbeat
					return
				}
			}
		}
	}()
	defer wg.Wait()

	// Register MUST be the first frame on every new stream. Enqueue it before
	// publishing `out`, so no in-flight job update can jump ahead of it.
	nodeID := readNodeID(c.cfg.NodeIDPath, c.cfg.Logger)
	out <- &computev1.AgentMessage{Payload: &computev1.AgentMessage_Register{
		Register: &computev1.Register{
			NodeId:       nodeID,
			Hostname:     hostname(),
			AgentVersion: agentVersion,
			Gpu:          c.cfg.GPU,
		},
	}}

	// First reply must be RegisterAck.
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("awaiting register ack: %w", err)
	}
	ack := first.GetRegisterAck()
	if ack == nil {
		return fmt.Errorf("first coordinator message was %T, want RegisterAck", first.Payload)
	}
	if id := ack.GetNodeId(); id != "" && id != nodeID {
		if werr := writeNodeID(c.cfg.NodeIDPath, id); werr != nil {
			c.cfg.Logger.Error("failed to persist node_id", "err", werr)
		}
		nodeID = id
	}
	log := c.cfg.Logger.With("node_id", nodeID)
	log.Info("registered with coordinator")
	onConnected()

	// Publish the outbound queue so job goroutines can emit updates, then
	// start heartbeating.
	c.mu.Lock()
	c.out = out
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.out = nil
		c.mu.Unlock()
	}()

	interval := time.Duration(defaultHeartbeatS) * time.Second
	if ack.GetHeartbeatIntervalS() > 0 {
		interval = time.Duration(ack.GetHeartbeatIntervalS()) * time.Second
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.heartbeatLoop(sessCtx, out, interval)
	}()

	// Recv loop: the ONLY receiver on this stream. Dispatches coordinator
	// commands until the stream fails or ctx is cancelled.
	for {
		msg, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("recv: %w", err)
		}
		switch p := msg.Payload.(type) {
		case *computev1.CoordinatorMessage_RunJob:
			c.startJob(ctx, p.RunJob.GetSpec(), log)
		case *computev1.CoordinatorMessage_CancelJob:
			c.cancelJob(p.CancelJob.GetJobId(), log)
		default:
			log.Debug("ignoring coordinator message", "type", fmt.Sprintf("%T", p))
		}
	}
}

func (c *Client) heartbeatLoop(ctx context.Context, out chan<- *computev1.AgentMessage, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var util *computev1.GpuUtilization
			if c.cfg.Utilization != nil {
				u, err := c.cfg.Utilization(ctx)
				if err == nil {
					util = u
				}
			}
			hb := &computev1.AgentMessage{Payload: &computev1.AgentMessage_Heartbeat{
				Heartbeat: &computev1.Heartbeat{
					GpuUtilization: util,
					ActiveJobIds:   c.activeJobIDs(),
				},
			}}
			select {
			case out <- hb:
			case <-ctx.Done():
				return
			}
		}
	}
}

// ---- jobs (owned by the Client, so they survive reconnects) ----

func (c *Client) startJob(rootCtx context.Context, spec *computev1.JobSpec, log *slog.Logger) {
	if spec == nil {
		return
	}
	id := spec.GetJobId()
	c.mu.Lock()
	if _, dup := c.jobs[id]; dup {
		c.mu.Unlock()
		log.Debug("ignoring duplicate RunJob", "job_id", id)
		return
	}
	// jobCtx derives from the long-lived rootCtx, NOT the session, so a
	// reconnect does not kill a running container.
	jobCtx, cancel := context.WithCancel(rootCtx)
	c.jobs[id] = cancel
	c.mu.Unlock()

	log.Info("starting job", "job_id", id, "image", spec.GetImage())
	go c.runJob(jobCtx, spec, log)
}

func (c *Client) runJob(ctx context.Context, spec *computev1.JobSpec, log *slog.Logger) {
	id := spec.GetJobId()
	defer c.removeJob(id)

	updates, err := c.cfg.Runner.Run(ctx, spec)
	if err != nil {
		log.Error("runner failed to start job", "job_id", id, "err", err)
		c.emit(&computev1.JobUpdate{
			JobId: id,
			State: computev1.JobState_JOB_STATE_FAILED,
			Error: err.Error(),
		})
		return
	}
	for u := range updates {
		c.emit(&computev1.JobUpdate{
			JobId:    id,
			State:    u.State,
			LogChunk: u.LogChunk,
			ExitCode: u.ExitCode,
			Error:    u.Err,
		})
	}
}

func (c *Client) cancelJob(id string, log *slog.Logger) {
	c.mu.Lock()
	cancel, ok := c.jobs[id]
	c.mu.Unlock()
	if !ok {
		log.Debug("cancel for unknown job", "job_id", id)
		return
	}
	log.Info("cancelling job", "job_id", id)
	cancel()
}

func (c *Client) removeJob(id string) {
	c.mu.Lock()
	delete(c.jobs, id)
	c.mu.Unlock()
}

func (c *Client) activeJobIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.jobs) == 0 {
		return nil
	}
	ids := make([]string, 0, len(c.jobs))
	for id := range c.jobs {
		ids = append(ids, id)
	}
	return ids
}

// emit best-effort delivers a JobUpdate on the current stream. If disconnected
// (or the queue is full), it drops the update; the coordinator reconciles from
// the next Heartbeat's active_job_ids.
func (c *Client) emit(u *computev1.JobUpdate) {
	c.mu.Lock()
	out := c.out
	c.mu.Unlock()
	if out == nil {
		return
	}
	msg := &computev1.AgentMessage{Payload: &computev1.AgentMessage_JobUpdate{JobUpdate: u}}
	select {
	case out <- msg:
	default:
	}
}

// ---- node_id persistence ----

func readNodeID(path string, log *slog.Logger) string {
	b, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Warn("could not read node_id file", "path", path, "err", err)
		}
		return ""
	}
	return strings.TrimSpace(string(b))
}

func writeNodeID(path, id string) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

// ---- backoff: exponential with half-range jitter, capped ----

type backoff struct {
	cur time.Duration
}

func (b *backoff) reset() { b.cur = backoffBase }

// next returns the wait before the next attempt and advances the schedule.
// The wait is in [cur/2, cur] to spread reconnect storms; cur doubles up to cap.
func (b *backoff) next() time.Duration {
	if b.cur < backoffBase {
		b.cur = backoffBase
	}
	half := b.cur / 2
	wait := half + time.Duration(rand.Int63n(int64(half)+1))
	if b.cur < backoffCap {
		b.cur *= 2
		if b.cur > backoffCap {
			b.cur = backoffCap
		}
	}
	return wait
}
