# GridLink

Distributed GPU compute platform: personal computers contribute GPU power via
a lightweight agent; a coordinator schedules containerized AI workloads onto
them. Later phases add inference endpoints, metering, and payouts.

**Status: Phase 1** — plumbing only (register / heartbeat / run one container
job). See `CLAUDE.md` for architecture decisions, conventions, and the
definition of done. That file is the source of truth for contributors and for
Claude Code sessions.

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
