-- Usage records: one row per billed inference request.
--
-- request_id is the primary key rather than a surrogate id, because it is the
-- idempotency key: the gateway mints one per request and may report the same
-- one twice after a timeout. Making it the key means a duplicate cannot become
-- a second row, which would double-bill the customer and double-credit the
-- provider.
--
-- Token counts are stored; money is not. Counts are facts, prices are policy
-- (Phase 4), so a pricing change must not rewrite history.
CREATE TABLE IF NOT EXISTS usage_records (
    request_id        TEXT PRIMARY KEY,
    ts                TIMESTAMPTZ NOT NULL,
    served_model_name TEXT   NOT NULL,
    node_id           TEXT   NOT NULL,
    deployment_id     TEXT   NOT NULL,
    api_key_id        TEXT   NOT NULL,
    prompt_tokens     BIGINT NOT NULL CHECK (prompt_tokens >= 0),
    completion_tokens BIGINT NOT NULL CHECK (completion_tokens >= 0)
);

-- Every query is "totals over a time window", optionally narrowed to one node
-- (what a provider earned) or one key (what a customer spent).
CREATE INDEX IF NOT EXISTS usage_records_ts_idx      ON usage_records (ts);
CREATE INDEX IF NOT EXISTS usage_records_node_ts_idx ON usage_records (node_id, ts);
CREATE INDEX IF NOT EXISTS usage_records_key_ts_idx  ON usage_records (api_key_id, ts);
