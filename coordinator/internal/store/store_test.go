package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testPostgres opens the database named by GRIDLINK_TEST_DATABASE_URL and
// truncates it. Skipped when unset, so `make test` needs no database.
func testPostgres(t *testing.T) *Postgres {
	t.Helper()
	dsn := os.Getenv("GRIDLINK_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set GRIDLINK_TEST_DATABASE_URL to run store tests against Postgres")
	}
	ctx := context.Background()
	p, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPostgres: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	if _, err := p.pool.Exec(ctx, `TRUNCATE usage_records`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return p
}

func rec(id, node, key string, prompt, completion uint64, ts time.Time) UsageRecord {
	return UsageRecord{
		RequestID:        id,
		Timestamp:        ts,
		ServedModelName:  "qwen-0.5b",
		NodeID:           node,
		DeploymentID:     "dep-1",
		APIKeyID:         key,
		PromptTokens:     prompt,
		CompletionTokens: completion,
	}
}

func TestPostgresInsertAndSummarize(t *testing.T) {
	p := testPostgres(t)
	ctx := context.Background()
	now := time.Now().UTC()

	records := []UsageRecord{
		rec("r1", "node-a", "key-0", 10, 5, now.Add(-30*time.Minute)),
		rec("r2", "node-a", "key-0", 20, 7, now.Add(-20*time.Minute)),
		rec("r3", "node-b", "key-1", 1, 1, now.Add(-10*time.Minute)),
	}
	for _, r := range records {
		if err := p.Insert(ctx, r); err != nil {
			t.Fatalf("Insert %s: %v", r.RequestID, err)
		}
	}

	s, err := p.Summarize(ctx, now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatalf("Summarize: %v", err)
	}
	if s.Total.Requests != 3 || s.Total.PromptTokens != 31 || s.Total.CompletionTokens != 13 {
		t.Errorf("total = %+v, want 3 requests / 31 prompt / 13 completion", s.Total)
	}

	// node-a did the most work, so it sorts first.
	if len(s.ByNode) != 2 {
		t.Fatalf("by node = %+v, want 2 groups", s.ByNode)
	}
	if s.ByNode[0].Key != "node-a" || s.ByNode[0].Requests != 2 ||
		s.ByNode[0].PromptTokens != 30 || s.ByNode[0].CompletionTokens != 12 {
		t.Errorf("node-a totals = %+v", s.ByNode[0])
	}
	if s.ByNode[1].Key != "node-b" || s.ByNode[1].Requests != 1 {
		t.Errorf("node-b totals = %+v", s.ByNode[1])
	}

	if len(s.ByAPIKey) != 2 || s.ByAPIKey[0].Key != "key-0" || s.ByAPIKey[0].Requests != 2 {
		t.Errorf("by api key = %+v", s.ByAPIKey)
	}
}

// The money-critical property: reporting the same request twice must not
// double-bill the customer or double-credit the provider.
func TestPostgresInsertIsIdempotent(t *testing.T) {
	p := testPostgres(t)
	ctx := context.Background()
	now := time.Now().UTC()

	r := rec("same-request", "node-a", "key-0", 10, 5, now)
	for i := 0; i < 3; i++ {
		if err := p.Insert(ctx, r); err != nil {
			t.Fatalf("Insert attempt %d: %v", i+1, err)
		}
	}
	// A retry could carry different numbers if the gateway recomputed them;
	// the first write must still win rather than silently updating a billed
	// record.
	tampered := rec("same-request", "node-a", "key-0", 999, 999, now)
	if err := p.Insert(ctx, tampered); err != nil {
		t.Fatalf("Insert tampered: %v", err)
	}

	s, err := p.Summarize(ctx, now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatalf("Summarize: %v", err)
	}
	if s.Total.Requests != 1 {
		t.Errorf("requests = %d, want 1 after repeated reports", s.Total.Requests)
	}
	if s.Total.PromptTokens != 10 || s.Total.CompletionTokens != 5 {
		t.Errorf("totals = %+v, want the first write to stand (10/5)", s.Total)
	}
}

func TestPostgresSummarizeWindowExcludesOutside(t *testing.T) {
	p := testPostgres(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := p.Insert(ctx, rec("old", "node-a", "key-0", 100, 100, now.Add(-48*time.Hour))); err != nil {
		t.Fatalf("Insert old: %v", err)
	}
	if err := p.Insert(ctx, rec("recent", "node-a", "key-0", 1, 2, now.Add(-time.Minute))); err != nil {
		t.Fatalf("Insert recent: %v", err)
	}

	s, err := p.Summarize(ctx, now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatalf("Summarize: %v", err)
	}
	if s.Total.Requests != 1 || s.Total.PromptTokens != 1 {
		t.Errorf("total = %+v, want only the recent record", s.Total)
	}

	// An empty window must report zeros, not fail.
	empty, err := p.Summarize(ctx, now.Add(24*time.Hour), now.Add(25*time.Hour))
	if err != nil {
		t.Fatalf("Summarize empty window: %v", err)
	}
	if empty.Total.Requests != 0 || len(empty.ByNode) != 0 {
		t.Errorf("empty window = %+v", empty)
	}
}

func TestPostgresMigrationsAreIdempotent(t *testing.T) {
	dsn := os.Getenv("GRIDLINK_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set GRIDLINK_TEST_DATABASE_URL to run store tests against Postgres")
	}
	ctx := context.Background()
	// Opening twice must not fail or re-apply: startup runs migrations every
	// time, so a second coordinator start would break otherwise.
	for i := 0; i < 2; i++ {
		p, err := OpenPostgres(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenPostgres attempt %d: %v", i+1, err)
		}
		p.Close()
	}
}

func TestPostgresRejectsUnbillableRecords(t *testing.T) {
	p := testPostgres(t)
	ctx := context.Background()

	tests := []struct {
		name string
		rec  UsageRecord
	}{
		{"no request id", UsageRecord{NodeID: "node-a"}},
		{"no node id", UsageRecord{RequestID: "r1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.Insert(ctx, tt.rec)
			if !errors.Is(err, ErrInvalidRecord) {
				t.Errorf("Insert() error = %v, want ErrInvalidRecord", err)
			}
		})
	}
}

// ---- JSONL fallback (always runs; no database) ----

func TestJSONLAppendsAndDeduplicates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.jsonl")
	j := OpenJSONL(path)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := j.Insert(ctx, rec("r1", "node-a", "key-0", 10, 5, now)); err != nil {
		t.Fatalf("Insert r1: %v", err)
	}
	if err := j.Insert(ctx, rec("r2", "node-a", "key-0", 1, 1, now)); err != nil {
		t.Fatalf("Insert r2: %v", err)
	}
	// Same request twice: one line, matching the Postgres behaviour.
	if err := j.Insert(ctx, rec("r1", "node-a", "key-0", 10, 5, now)); err != nil {
		t.Fatalf("Insert duplicate: %v", err)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 2 {
		t.Errorf("wrote %d lines, want 2:\n%s", len(lines), body)
	}
	if !strings.Contains(lines[0], `"request_id":"r1"`) {
		t.Errorf("first line missing request_id: %s", lines[0])
	}
}

// Summaries must fail loudly rather than return zeros that look like "no usage".
func TestJSONLCannotSummarize(t *testing.T) {
	j := OpenJSONL(filepath.Join(t.TempDir(), "usage.jsonl"))
	s, err := j.Summarize(context.Background(), time.Now(), time.Now())
	if err == nil {
		t.Fatalf("Summarize() = %+v, want an error", s)
	}
	if !strings.Contains(err.Error(), "GRIDLINK_DATABASE_URL") {
		t.Errorf("error %q should name the setting that fixes it", err)
	}
}

func TestJSONLRejectsUnbillableRecords(t *testing.T) {
	j := OpenJSONL(filepath.Join(t.TempDir(), "usage.jsonl"))
	if err := j.Insert(context.Background(), UsageRecord{NodeID: "n"}); !errors.Is(err, ErrInvalidRecord) {
		t.Errorf("Insert() error = %v, want ErrInvalidRecord", err)
	}
}
