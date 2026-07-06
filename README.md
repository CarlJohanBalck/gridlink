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

## Layout

- `contracts/` — protobuf source of truth (`make proto` regenerates)
- `agent/` — provider daemon (GPU detect, Docker runner, coordinator stream)
- `coordinator/` — control plane (registry, scheduler, gRPC server)
