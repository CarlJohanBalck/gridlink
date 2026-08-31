// Package usage ships token counts to the coordinator. Phase 2: coordinator
// just logs them (JSONL). Phase 3 turns this into the metering ledger — the
// report schema is already shaped for that.
package usage

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// queueSize bounds memory when the coordinator is slow or down. Reports are
// dropped past this rather than queued forever: usage must never apply
// backpressure to inference.
const queueSize = 1024

// reportTimeout bounds one ReportUsage call so a hung coordinator cannot stall
// the drain goroutine indefinitely.
const reportTimeout = 5 * time.Second

type Record struct {
	// RequestID is the idempotency key. The coordinator rejects a report
	// without one, because a retry would otherwise be billed twice.
	RequestID        string
	Model            string
	NodeID           string
	DeploymentID     string
	APIKeyID         string
	PromptTokens     uint64
	CompletionTokens uint64
	TimestampUnixMs  int64
}

type Reporter struct {
	client computev1.GatewayServiceClient
	conn   *grpc.ClientConn
	log    *slog.Logger

	ch   chan Record
	stop chan struct{}
	wg   sync.WaitGroup

	// dropped counts records lost to a full queue. Phase 3 bills on this data,
	// so silent loss would be a revenue bug; it is logged periodically.
	dropped atomic.Uint64
}

type bearerCreds struct{ token string }

func (b bearerCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "bearer " + b.token}, nil
}

func (b bearerCreds) RequireTransportSecurity() bool { return false }

func NewReporter(coordAddr, token string, log *slog.Logger) *Reporter {
	r := &Reporter{
		log:  log,
		ch:   make(chan Record, queueSize),
		stop: make(chan struct{}),
	}
	conn, err := grpc.NewClient(coordAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearerCreds{token: token}),
	)
	if err != nil {
		log.Error("usage reporting disabled: bad coordinator address",
			"addr", coordAddr, "err", err)
		return r
	}
	r.conn = conn
	r.client = computev1.NewGatewayServiceClient(conn)

	r.wg.Add(1)
	go r.drain()
	return r
}

// Report enqueues a record without blocking. A full queue drops the record:
// the alternative is stalling a user's inference response on a metering write.
func (r *Reporter) Report(rec Record) {
	if r.client == nil {
		return
	}
	select {
	case r.ch <- rec:
	default:
		if n := r.dropped.Add(1); n%100 == 1 {
			r.log.Warn("usage queue full; dropping records", "dropped_total", n)
		}
	}
}

func (r *Reporter) drain() {
	defer r.wg.Done()
	for {
		select {
		case <-r.stop:
			// Flush what is already queued so a clean shutdown does not lose
			// usage that was successfully captured.
			for {
				select {
				case rec := <-r.ch:
					r.send(rec)
				default:
					return
				}
			}
		case rec := <-r.ch:
			r.send(rec)
		}
	}
}

func (r *Reporter) send(rec Record) {
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()

	_, err := r.client.ReportUsage(ctx, &computev1.ReportUsageRequest{
		RequestId:        rec.RequestID,
		ServedModelName:  rec.Model,
		NodeId:           rec.NodeID,
		DeploymentId:     rec.DeploymentID,
		ApiKeyId:         rec.APIKeyID,
		PromptTokens:     rec.PromptTokens,
		CompletionTokens: rec.CompletionTokens,
		TimestampUnixMs:  rec.TimestampUnixMs,
	})
	if err != nil {
		// Nothing better to do than log: the response already went out, and
		// retrying indefinitely would grow the queue without bound.
		r.log.Error("usage report failed",
			"model", rec.Model, "deployment_id", rec.DeploymentID, "err", err)
	}
}

// Close stops the reporter, flushing whatever is queued.
func (r *Reporter) Close() error {
	if r.client == nil {
		return nil
	}
	close(r.stop)
	r.wg.Wait()
	if n := r.dropped.Load(); n > 0 {
		r.log.Warn("usage records dropped over this session", "dropped_total", n)
	}
	return r.conn.Close()
}

// Dropped reports how many records were lost to a full queue.
func (r *Reporter) Dropped() uint64 { return r.dropped.Load() }
