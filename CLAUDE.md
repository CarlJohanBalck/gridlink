# GridLink — Distributed GPU Compute Platform

## What this is

A marketplace platform where personal computers contribute GPU compute and get paid
for it. Providers run a lightweight **agent**; a central **coordinator** schedules
containerized AI workloads onto them; later phases add an inference **gateway**,
metering, and payouts.

## Current phase: PHASE 3

**Goal:** Durable metering. Usage currently lands in a JSONL file that a
coordinator restart leaves behind and nothing can query. Phase 3 puts it in
Postgres, aggregates it per provider and per API key, and exposes it — which is
what payments in Phase 4 will settle against.

Do NOT build ahead of the current phase unless explicitly asked. If a task seems
to require Phase 4+ functionality, stop and flag it instead of building it.

**Postgres is a COORDINATOR dependency only.** Providers still install one
binary and nothing else; the zero-preinstall rule is about provider machines.

### Phase roadmap (context only — do not implement future phases)
1. ~~Plumbing:~~ register / heartbeat / run container job over one stream — done
2. ~~Inference endpoints:~~ native Metal engine on Mac providers +
   OpenAI-compatible gateway (spec + measured results: docs/PHASE2.md) — done,
   verified across two machines
3. **Metering + ledger (now):** Postgres, aggregation, dashboard
   (spec: docs/PHASE3.md)
4. Payments (Stripe Connect / USDC), verification & reputation

## Architecture decisions (settled — do not relitigate)

- **Agents dial OUT.** Provider machines sit behind home NATs. The agent opens ONE
  long-lived bidirectional gRPC stream (`AgentService.Connect`) to the coordinator
  and everything (registration, heartbeats, job commands, job status) flows over it.
  The coordinator NEVER dials the agent.
- **The coordinator never supplies executable code.** This is the invariant;
  containers were only ever the mechanism. A job spec names an image or a model
  plus parameters — never a command to run on the host.
- **Execution is platform-dependent** behind the `runner.Runner` interface:
  - *Linux + NVIDIA:* Docker via the local daemon with the NVIDIA Container
    Toolkit (`--gpus`). Job spec = image + env + resource limits.
  - *macOS (Apple Silicon):* a native inference engine (llama.cpp built with
    embedded Metal shaders), run as a **subprocess of the agent binary itself**
    (`agent engine`) under `sandbox-exec`. Docker is NOT used on macOS: Docker
    Desktop is an app install, and its Linux VM cannot reach Metal at all —
    containers on macOS see no GPU device. Job spec = model + sampling params.
- **Mac providers must need zero preinstalled apps.** One signed, notarized
  binary, no Docker, no Python, no Homebrew. Anything a provider would have to
  install first is a non-starter. (Build-machine tooling like cmake is fine.)
- **Protobuf contracts are the source of truth.** Both binaries build against
  generated code from `contracts/proto/`. Change the proto first, regenerate
  (`make proto`), then update both sides in the same commit.
- **Language:** Go 1.22+ for both agent and coordinator. Standard library where
  reasonable; approved deps: `google.golang.org/grpc`, `google.golang.org/protobuf`,
  `github.com/docker/docker` (client), `github.com/NVIDIA/go-nvml` (optional,
  fall back to parsing `nvidia-smi --query-gpu=... --format=csv`).
  GPU detection is per-platform: `nvidia-smi` CSV on Linux, `system_profiler
  SPDisplaysDataType -json` on macOS (vendor `apple`). Phase 2 adds llama.cpp
  vendored and linked via cgo — the only cgo in the tree, macOS-only. Phase 3
  adds `github.com/jackc/pgx/v5` (coordinator only). No ORM: SQL is written by
  hand, and migrations are plain embedded .sql files applied in order.
- **Horizontal scale-out only — never shard a model across nodes.** One node
  serves one whole model; the coordinator routes each request to exactly one
  node. No tensor or pipeline parallelism, no cross-node hop in the request
  path. Rejected because providers are strangers' machines on separate home
  networks: tensor parallelism needs an all-reduce per layer (~80 per token for
  a 70B) against ~7ms LAN / 30-80ms WAN round-trips, and pipeline parallelism
  leaves every node but one idle per request while making any single node's
  departure fail every in-flight request through it. Capacity scales with the
  number of providers, not with the size of one model.
- **Model size is bounded by one node's memory.** ~7-8B quantized on a 16GB M4,
  larger models only on higher-memory Macs. Advertise and route accordingly.
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
  internal/engine/            Metal inference engine (cgo, macOS only) + its
                              OpenAI-compatible HTTP server
  internal/deploy/            long-running deployments: weight download,
                              sandboxed engine subprocess, readiness
  internal/sysinfo/           OS/arch/CPU/RAM detection
coordinator/                  control plane (Go module: gridlink/coordinator)
  cmd/coordinator/            main entrypoint
  internal/server/            gRPC server + stream handling
  internal/registry/          in-memory node registry
  internal/scheduler/         one-shot job dispatch (manual via admin RPC)
  internal/deployments/       deployment desired-state + placement + reconciler
  internal/store/             PHASE 3: Postgres persistence for usage/ledger
gateway/                      OpenAI-compatible inference front door
  internal/dialer/            data-plane transport seam (Tailscale now, tunnel later)
  internal/router/            model -> replica resolution (via coordinator)
  internal/proxy/             HTTP server, SSE passthrough, usage capture
  internal/usage/             async usage reporting
docs/STATUS.md                where things stand + what is next (read first)
docs/PHASE2.md                Phase 2 spec + measured engine-spike results
docs/PHASE3.md                Phase 3 spec + definition of done
scripts/                      dev helpers, installer, release + demo scripts
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
make engine         # fetch + build llama.cpp for the Metal engine (macOS, once)
make build          # build all binaries into ./bin
make test           # go test ./... in every module
make release        # provider binaries + SHA256SUMS into ./dist
make publish VERSION=v0.1.0   # test, build, tag and upload a release
make run-coord      # run coordinator locally on :50051
make run-agent      # run agent against localhost coordinator
./scripts/demo.sh start       # coordinator + agent + gateway, serving a model
docker compose up   # coordinator + a fake-GPU agent for local dev
```

## Definition of done

Phases 1 and 2 are done; see docs/STATUS.md for what was verified and how to
reproduce it. The current phase's definition of done lives in docs/PHASE3.md.

Unchanged across every phase: **`make test` passes with no Docker daemon, no
GPU, and no database present.** Tests that need real hardware or a real
Postgres are opt-in behind an env var and skipped by default.
