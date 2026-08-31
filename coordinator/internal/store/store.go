// Package store persists usage records (Phase 3). Hand-written SQL against
// Postgres; migrations are embedded .sql files applied in order at startup.
//
// The interface exists so the coordinator can run against a JSONL file when no
// database is configured, which keeps `make test`, scripts/demo.sh, and a
// laptop with no Postgres working unchanged.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// UsageRecord is one billed inference request.
type UsageRecord struct {
	// RequestID is the idempotency key, minted by the gateway. Reporting the
	// same one twice must not produce a second row.
	RequestID        string
	Timestamp        time.Time
	ServedModelName  string
	NodeID           string
	DeploymentID     string
	APIKeyID         string
	PromptTokens     uint64
	CompletionTokens uint64
}

// Totals is an aggregate over some slice of usage.
type Totals struct {
	// Key is the node ID or API key ID the totals belong to; empty for a
	// grand total.
	Key              string
	Requests         uint64
	PromptTokens     uint64
	CompletionTokens uint64
}

// Summary answers "who did what over this window".
type Summary struct {
	From, To time.Time
	Total    Totals
	ByNode   []Totals
	ByAPIKey []Totals
}

// Store records and aggregates usage.
type Store interface {
	// Insert records one request. Inserting an already-recorded RequestID is a
	// no-op, not an error: the caller cannot tell a duplicate from a first
	// attempt after a timeout.
	Insert(ctx context.Context, rec UsageRecord) error
	// Summarize aggregates usage in [from, to).
	Summarize(ctx context.Context, from, to time.Time) (*Summary, error)
	Close() error
}

// Postgres is the durable Store.
type Postgres struct {
	pool *pgxpool.Pool
}

var _ Store = (*Postgres)(nil)

// OpenPostgres connects, verifies the connection, and applies migrations.
func OpenPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	// pgxpool connects lazily; ping so a bad DSN fails at startup rather than
	// on the first billed request.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	p := &Postgres{pool: pool}
	if err := p.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return p, nil
}

func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}

// migrate applies every embedded migration that has not run yet, in filename
// order. Each runs in its own transaction alongside the row recording it, so a
// failure cannot leave a migration half-applied but marked done.
func (p *Postgres) migrate(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			name       TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	// Filename order is the migration order, so names are zero-padded.
	sort.Strings(names)

	for _, name := range names {
		var exists bool
		if err := p.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)`, name,
		).Scan(&exists); err != nil {
			return fmt.Errorf("check %s: %w", name, err)
		}
		if exists {
			continue
		}

		body, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}

		tx, err := p.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (name) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit %s: %w", name, err)
		}
	}
	return nil
}

// ErrInvalidRecord means the record could never be billed correctly.
var ErrInvalidRecord = errors.New("invalid usage record")

func (r UsageRecord) validate() error {
	if r.RequestID == "" {
		// Without it there is no idempotency, so a retry would double-bill.
		return fmt.Errorf("%w: request_id is required", ErrInvalidRecord)
	}
	if r.NodeID == "" {
		// Nobody could be credited for this work.
		return fmt.Errorf("%w: node_id is required", ErrInvalidRecord)
	}
	return nil
}

func (p *Postgres) Insert(ctx context.Context, rec UsageRecord) error {
	if err := rec.validate(); err != nil {
		return err
	}
	ts := rec.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	// ON CONFLICT DO NOTHING, not an error: a duplicate means the reporter
	// retried after a timeout, which is expected rather than exceptional.
	_, err := p.pool.Exec(ctx, `
		INSERT INTO usage_records
			(request_id, ts, served_model_name, node_id, deployment_id,
			 api_key_id, prompt_tokens, completion_tokens)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (request_id) DO NOTHING`,
		rec.RequestID, ts, rec.ServedModelName, rec.NodeID, rec.DeploymentID,
		rec.APIKeyID, int64(rec.PromptTokens), int64(rec.CompletionTokens))
	if err != nil {
		return fmt.Errorf("insert usage: %w", err)
	}
	return nil
}

func (p *Postgres) Summarize(ctx context.Context, from, to time.Time) (*Summary, error) {
	s := &Summary{From: from, To: to}

	if err := p.pool.QueryRow(ctx, `
		SELECT COUNT(*), COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(completion_tokens), 0)
		FROM usage_records WHERE ts >= $1 AND ts < $2`, from, to,
	).Scan(&s.Total.Requests, &s.Total.PromptTokens, &s.Total.CompletionTokens); err != nil {
		return nil, fmt.Errorf("total: %w", err)
	}

	byNode, err := p.groupBy(ctx, "node_id", from, to)
	if err != nil {
		return nil, fmt.Errorf("by node: %w", err)
	}
	s.ByNode = byNode

	byKey, err := p.groupBy(ctx, "api_key_id", from, to)
	if err != nil {
		return nil, fmt.Errorf("by api key: %w", err)
	}
	s.ByAPIKey = byKey
	return s, nil
}

// groupBy aggregates over one column. The column name is not user input — it
// comes from two call sites in this file — but it is still restricted to a
// known set rather than interpolated blindly, so this cannot become an
// injection point if it is ever wired to a request.
func (p *Postgres) groupBy(ctx context.Context, column string, from, to time.Time) ([]Totals, error) {
	switch column {
	case "node_id", "api_key_id":
	default:
		return nil, fmt.Errorf("cannot group by %q", column)
	}

	rows, err := p.pool.Query(ctx, fmt.Sprintf(`
		SELECT %s, COUNT(*),
		       COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(completion_tokens), 0)
		FROM usage_records
		WHERE ts >= $1 AND ts < $2
		GROUP BY %s
		ORDER BY SUM(prompt_tokens) + SUM(completion_tokens) DESC, %s`,
		column, column, column), from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Totals
	for rows.Next() {
		var t Totals
		if err := rows.Scan(&t.Key, &t.Requests, &t.PromptTokens, &t.CompletionTokens); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// Ping reports whether the database is still reachable, for health checks.
func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}
