package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// JSONL appends usage records to a file. It is the fallback when no database
// is configured, so a laptop, `make test` and scripts/demo.sh work with no
// Postgres. It deliberately cannot aggregate: summarising by reading the whole
// file back would invite treating it as a real ledger, which it is not.
type JSONL struct {
	path string

	// mu serialises appends so concurrent reports cannot interleave partial
	// lines into what is meant to be one JSON object per line.
	mu sync.Mutex
	// seen makes Insert idempotent within one process lifetime. It cannot
	// survive a restart, which is one more reason this is not the real ledger.
	seen map[string]bool
}

var _ Store = (*JSONL)(nil)

func OpenJSONL(path string) *JSONL {
	return &JSONL{path: path, seen: make(map[string]bool)}
}

func (j *JSONL) Insert(_ context.Context, rec UsageRecord) error {
	if err := rec.validate(); err != nil {
		return err
	}
	ts := rec.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	if j.seen[rec.RequestID] {
		return nil
	}

	line, err := json.Marshal(map[string]any{
		"request_id":        rec.RequestID,
		"ts_unix_ms":        ts.UnixMilli(),
		"served_model_name": rec.ServedModelName,
		"node_id":           rec.NodeID,
		"deployment_id":     rec.DeploymentID,
		"api_key_id":        rec.APIKeyID,
		"prompt_tokens":     rec.PromptTokens,
		"completion_tokens": rec.CompletionTokens,
	})
	if err != nil {
		return fmt.Errorf("marshal usage: %w", err)
	}

	f, err := os.OpenFile(j.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open usage log: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("append usage: %w", err)
	}
	j.seen[rec.RequestID] = true
	return nil
}

// Summarize is not supported: this sink exists to keep dev working, not to
// answer billing questions. Callers should report the error rather than
// present a zeroed summary as if it were real.
func (j *JSONL) Summarize(context.Context, time.Time, time.Time) (*Summary, error) {
	return nil, fmt.Errorf("usage summaries require a database (set GRIDLINK_DATABASE_URL)")
}

func (j *JSONL) Close() error { return nil }
