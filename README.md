# GridLink

Distributed GPU compute platform: personal computers contribute GPU power via
a lightweight agent; a coordinator places AI models onto them and an
OpenAI-compatible gateway routes inference to whichever node is serving.
Apple Silicon Macs run models natively on Metal, installing nothing.

**Status: Phase 2 functionally complete** — a request goes client → gateway →
node → Metal engine and back, metered, and a deployment re-places itself when
a node dies. Not yet shippable to real providers: the binary needs Apple
notarization, and the cross-machine (tailnet) path is untested.

See `docs/STATUS.md` for where things stand and what is next, `CLAUDE.md` for
architecture decisions and conventions, and `docs/PHASE2.md` for the Phase 2
spec and measured engine-spike results.

## Try it on one Mac

Requires an Apple Silicon Mac. `make engine` fetches and builds llama.cpp
(build machine only — providers install nothing).

```bash
make engine && make build
./scripts/demo.sh start                      # ~1 min; downloads a 469 MB model once
./scripts/demo.sh ask "Name three colours."
./scripts/demo.sh stream "Count to five."
./scripts/demo.sh status                     # nodes, deployments, usage records
./scripts/demo.sh stop
```

This runs the coordinator, an agent, and the gateway on one machine and
serves a small model on the Mac's GPU. The gateway listens on :8099 because
Docker Desktop commonly occupies :8080.

## Quick start (dev)

```bash
make proto        # requires buf (https://buf.build)
make build
make run-coord    # terminal 1
make run-agent    # terminal 2 (same machine, or set GRIDLINK_COORDINATOR)
./scripts/smoke.sh <node_id>
```

## Two-machine runbook (Phase 1 definition of done)

Coordinator on one machine, agent on another, over the LAN. Works without a
GPU: the agent registers CPU-only and jobs run as plain containers (`gpu` mode
in the smoke script needs an NVIDIA Linux box).

**1. Coordinator machine** — pick a shared secret and start it:

```bash
GRIDLINK_TOKEN=<token> make run-coord
# note this machine's LAN IP; allow inbound :50051 if the OS firewall asks
```

**2. Provider machine** — needs Docker running. On the same arch, build with
`make build`; for a linux/arm64 provider (e.g. Raspberry Pi 5) cross-compile
and copy:

```bash
make build-linux-arm64
scp bin/agent-linux-arm64 <provider>:agent
```

Then on the provider:

```bash
GRIDLINK_TOKEN=<token> GRIDLINK_COORDINATOR=<coordinator-ip>:50051 ./agent
```

Without an NVIDIA GPU, expect `gpu detection failed, registering as cpu-only`
followed by `registered with coordinator` — that's the intended fallback.

**3. Verify** (from any machine with grpcurl):

```bash
export GRIDLINK_COORDINATOR=<coordinator-ip>:50051 GRIDLINK_TOKEN=<token>
./scripts/smoke.sh <node_id> cpu     # alpine uname -a; use "gpu" on NVIDIA nodes
```

The coordinator log shows pull progress, `RUNNING`, the job's output as log
chunks, then `SUCCEEDED`. Job output is only visible in the coordinator log in
Phase 1.

**4. Restart survival** — Ctrl-C the coordinator and start it again. The agent
logs `coordinator connection lost; reconnecting`, then re-registers within
seconds with the same `node_id` (persisted in `~/.gridlink/node_id`).

## Layout

- `contracts/` — protobuf source of truth (`make proto` regenerates)
- `agent/` — provider daemon (GPU detect, Docker runner, coordinator stream)
- `coordinator/` — control plane (registry, scheduler, gRPC server)
