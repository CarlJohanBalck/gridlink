# Phase 3 spec — metering + ledger

Read CLAUDE.md first, then docs/STATUS.md. Phase 2 is done and verified across
two machines.

## Why

Usage is currently appended to a JSONL file by the coordinator. That is enough
to prove the numbers are right, and not enough for anything else: it is not
queryable, a restart on another host leaves it behind, and nothing aggregates
it. Phase 4 settles payments against these numbers, so they have to become
durable and summable first.

## What Phase 3 adds

1. **Postgres persistence** (`coordinator/internal/store`): usage records
   written to a real database, with hand-written SQL and embedded `.sql`
   migrations applied in order at startup. No ORM.
2. **Idempotent reporting**: `ReportUsageRequest` gains a `request_id`, and the
   store enforces uniqueness on it (see the decision below).
3. **Aggregation**: earnings per provider node and spend per API key over a
   time window, exposed on `AdminService`.
4. **Dashboard**: a read-only page served by the coordinator showing nodes,
   deployments, and usage totals.

Postgres is a **coordinator** dependency. Providers still install one binary
and nothing else.

## Decision: usage reporting must be idempotent

Today `ReportUsage` has no request identity, so a retry writes a second row and
bills the customer twice while crediting the provider twice. The gateway
already retries inference against another replica, and the usage reporter
retries nothing only because it drops on failure — neither property is one to
rely on when the output is money.

Every usage record therefore carries a `request_id` minted by the gateway per
inference request, with a unique index in Postgres. Re-reporting is a no-op
(`ON CONFLICT DO NOTHING`) rather than an error, because the reporter cannot
tell a duplicate from a first attempt after a timeout.

## Decision: the JSONL sink stays

`GRIDLINK_USAGE_LOG` continues to work when `GRIDLINK_DATABASE_URL` is unset,
so `make test`, `scripts/demo.sh`, and a laptop with no database keep working
unchanged. The store is chosen at startup; nothing above it knows which sink
is in use.

## Decision: no money in the schema yet

The ledger records **tokens**, not currency. Pricing (per-model rates, provider
revenue share) is a Phase 4 concern and would be guesswork now; token counts
are facts and prices are policy, so keeping them apart means a pricing change
does not rewrite history.

## Schema (v1)

```sql
CREATE TABLE usage_records (
  request_id        TEXT PRIMARY KEY,       -- idempotency key from the gateway
  ts                TIMESTAMPTZ NOT NULL,
  served_model_name TEXT   NOT NULL,
  node_id           TEXT   NOT NULL,
  deployment_id     TEXT   NOT NULL,
  api_key_id        TEXT   NOT NULL,
  prompt_tokens     BIGINT NOT NULL CHECK (prompt_tokens >= 0),
  completion_tokens BIGINT NOT NULL CHECK (completion_tokens >= 0)
);
CREATE INDEX usage_records_ts_idx      ON usage_records (ts);
CREATE INDEX usage_records_node_ts_idx ON usage_records (node_id, ts);
CREATE INDEX usage_records_key_ts_idx  ON usage_records (api_key_id, ts);
```

## Definition of done

- With `GRIDLINK_DATABASE_URL` set, a request through the gateway lands one row
  in `usage_records`, and reporting the same `request_id` twice still leaves
  exactly one row.
- `AdminService.GetUsageSummary` returns per-node and per-key totals over a
  time window that match the rows.
- A coordinator restart loses nothing: the totals are the same afterwards.
- With `GRIDLINK_DATABASE_URL` unset, everything behaves exactly as it does
  today (JSONL), and `make test` passes with no database running.
- The dashboard shows nodes, deployments and usage totals, and needs no
  database of its own.

## Running Postgres for development

```sh
docker run -d --name gridlink-pg -e POSTGRES_PASSWORD=dev -e POSTGRES_DB=gridlink \
  -p 55432:5432 postgres:16-alpine

export GRIDLINK_TEST_DATABASE_URL="postgres://postgres:dev@localhost:55432/gridlink?sslmode=disable"
cd coordinator && go test ./internal/store/ -v
```

Migrations apply themselves at startup, so there is no separate step.

## Suggested sessions

1. ~~`store` package~~ — done: migrations, `Insert`, idempotency, and a
   `Summary` query, plus a JSONL fallback implementing the same interface.
   Postgres tests are skipped unless `GRIDLINK_TEST_DATABASE_URL` is set.
2. ~~Proto + coordinator wiring~~ — done: `request_id` on ReportUsage,
   `GetUsageSummary` on AdminService, store selected at startup from
   `GRIDLINK_DATABASE_URL` / `GRIDLINK_USAGE_LOG`.
3. ~~Gateway mints a `request_id`~~ — done, one per inference request and
   reused across a retry so a retried request is still billed once.
4. Dashboard: a read-only page on the coordinator showing nodes, deployments
   and usage totals.

Verified end to end against Postgres: three requests through the gateway
produced three rows, `GetUsageSummary` reported 3 requests / 35 prompt /
37 completion tokens broken down by node and by API key, and the totals were
identical after restarting the coordinator.
