# GridLink — Distributed GPU Compute Platform

## What this is

A marketplace platform where personal computers contribute GPU compute and get paid
for it. Providers run a lightweight **agent**; a central **coordinator** schedules
containerized AI workloads onto them; later phases add an inference **gateway**,
metering, and payouts.

## Current phase: PHASE 1

**Goal:** Two machines. An agent that (a) detects its GPU, (b) registers with the
coordinator over a single outbound gRPC stream, (c) heartbeats, and (d) runs a
containerized job on command and streams status back. No payments, no web UI,
no inference gateway, no persistence beyond in-memory (Postgres comes in Phase 3).

Do NOT build ahead of the current phase unless explicitly asked. If a task seems
to require Phase 2+ functionality, stop and flag it instead of building it.

### Phase roadmap (context only — do not implement future phases)
1. **Plumbing (now):** register / heartbeat / run container job over one stream
2. Inference endpoints: vLLM containers + OpenAI-compatible gateway
   (spec: docs/PHASE2.md — scaffolding exists but stays unimplemented in Phase 1)
3. Metering + ledger (Postgres), dashboard
4. Payments (Stripe Connect / USDC), verification & reputation

## Architecture decisions (settled — do not relitigate)

- **Agents dial OUT.** Provider machines sit behind home NATs. The agent opens ONE
  long-lived bidirectional gRPC stream (`AgentService.Connect`) to the coordinator
  and everything (registration, heartbeats, job commands, job status) flows over it.
  The coordinator NEVER dials the agent.
- **All workloads run in Docker containers** via the local Docker daemon with the
  NVIDIA Container Toolkit (`--gpus`). The agent never executes raw commands from
  the coordinator on the host. Job spec = image + env + resource limits only.
- **Protobuf contracts are the source of truth.** Both binaries build against
  generated code from `contracts/proto/`. Change the proto first, regenerate
  (`make proto`), then update both sides in the same commit.
- **Language:** Go 1.22+ for both agent and coordinator. Standard library where
  reasonable; approved deps: `google.golang.org/grpc`, `google.golang.org/protobuf`,
  `github.com/docker/docker` (client), `github.com/NVIDIA/go-nvml` (optional,
  fall back to parsing `nvidia-smi --query-gpu=... --format=csv`).
- **IDs:** coordinator assigns `node_id` (UUIDv4) at first registration; the agent
  persists it in `~/.gridlink/node_id` and reuses it on reconnect.
- **Auth (Phase 1):** shared bootstrap token via `GRIDLINK_TOKEN` env var sent in
  stream metadata. mTLS/per-node keys come later; keep the token check isolated in
  one interceptor so it's easy to replace.

## Repo layout

```
contracts/proto/compute/v1/   protobuf definitions (agent.proto, common.proto)
agent/                        provider daemon (Go module: gridlink/agent)
  cmd/agent/                  main entrypoint
  internal/gpu/               GPU detection & benchmarking
  internal/runner/            Docker job execution
  internal/client/            coordinator stream client + reconnect loop
coordinator/                  control plane (Go module: gridlink/coordinator)
  cmd/coordinator/            main entrypoint
  internal/server/            gRPC server + stream handling
  internal/registry/          in-memory node registry (Phase 1)
  internal/scheduler/         job dispatch (Phase 1: manual via admin RPC)
  internal/deployments/       PHASE 2: deployment desired-state + reconciler
gateway/                      PHASE 2: OpenAI-compatible inference front door
  internal/dialer/            data-plane transport seam (Tailscale now, tunnel later)
  internal/router/            model -> replica resolution (via coordinator)
  internal/proxy/             HTTP server, SSE passthrough, usage capture
  internal/usage/             async usage reporting
docs/PHASE2.md                Phase 2 spec + definition of done
scripts/                      dev helpers
```

## Conventions

- Errors: wrap with `fmt.Errorf("context: %w", err)`; no panics outside `main`.
- Logging: `log/slog`, structured, `logger.With("node_id", id)` style. No fmt.Println.
- Context: every blocking call takes a `context.Context`; honor cancellation.
- Concurrency: prefer channels + one owner goroutine per stream over shared mutexed
  maps, except the registry which is a mutex-guarded map (it's fine).
- Tests: table-driven, `_test.go` next to the code. The Docker runner must have an
  interface (`runner.Runner`) so tests can use a fake; never require a real Docker
  daemon or GPU in unit tests.
- Reconnects: agent uses exponential backoff with jitter (1s → 60s cap), resends
  Register on every new stream.
- Heartbeat: every 10s from agent; coordinator marks a node OFFLINE after 30s silence.

## Commands

```
make proto          # regenerate Go code from contracts/proto (buf generate)
make build          # build both binaries into ./bin
make test           # go test ./... in both modules
make run-coord      # run coordinator locally on :50051
make run-agent      # run agent against localhost coordinator
docker compose up   # coordinator + a fake-GPU agent for local dev
```

## Definition of done for Phase 1

- `make run-coord` + `make run-agent` on two machines: agent appears in the
  coordinator's node list with real GPU info and stays ONLINE via heartbeats.
- An admin RPC (`AdminService.RunJob`) can push a job (e.g. image
  `nvidia/cuda:12.4.1-base-ubuntu22.04`, cmd `nvidia-smi`) to a named node; the
  agent runs it and streams PENDING → RUNNING → SUCCEEDED/FAILED + logs back.
- Agent survives coordinator restarts (reconnect + re-register, same node_id).
- `make test` passes with no Docker daemon and no GPU present.
