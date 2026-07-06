// Package usage ships token counts to the coordinator. Phase 2: coordinator
// just logs them (JSONL). Phase 3 turns this into the metering ledger — the
// report schema is already shaped for that.
package usage

import (
	"log/slog"
)

type Record struct {
	Model            string
	NodeID           string
	DeploymentID     string
	APIKeyID         string
	PromptTokens     uint64
	CompletionTokens uint64
	TimestampUnixMs  int64
}

type Reporter struct {
	log *slog.Logger
	// TODO(claude): buffered channel + background goroutine calling
	// GatewayService.ReportUsage; drop-with-metric on overflow. Usage
	// reporting must NEVER block or fail an inference response.
}

func NewReporter(coordAddr, token string, log *slog.Logger) *Reporter {
	return &Reporter{log: log}
}

func (r *Reporter) Report(rec Record) {
	// TODO(claude): non-blocking enqueue.
}
