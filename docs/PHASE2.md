# Phase 2 spec — inference endpoints

Read CLAUDE.md first. Do not start Phase 2 until the Phase 1 definition of
done passes on two real machines.

Phase 2 targets **Apple Silicon Macs as providers**: one signed binary, no
preinstalled apps, one whole model per node, no cross-node sharding. Docker is
not part of this path (see CLAUDE.md for why).

## What Phase 2 adds

1. **Deployments** (control plane): long-running model servers, driven by the
   coordinator over the existing agent stream. New proto messages:
   DeploymentSpec/State, Start/StopDeployment, DeploymentUpdate, plus
   Heartbeat gains active_deployment_ids and data_plane_addr.
2. **Engine** (provider side): llama.cpp vendored and linked via cgo, built
   with `GGML_METAL_EMBED_LIBRARY` so the Metal shaders live inside the binary
   and the provider installs nothing. The agent runs it as a subprocess of
   itself (`agent engine`) under `sandbox-exec`, so an engine crash cannot take
   down the agent, its stream, or other jobs. Weights are downloaded to a cache
   (`~/.gridlink/models/`), never shipped by the coordinator.
3. **Gateway** (data plane): new binary, OpenAI-compatible
   (/v1/models, /v1/chat/completions, /v1/completions), API-key auth,
   SSE streaming passthrough, per-request usage capture reported to the
   coordinator (GatewayService.ReportUsage — logged as JSONL in Phase 2,
   becomes the Phase 3 ledger feed).
4. **Placement + reconciliation**: coordinator/internal/deployments places
   replicas on eligible ONLINE nodes (one deployment per node — a loaded model
   dominates unified memory), and re-places when a node dies. Node loss is
   NORMAL on consumer hardware; the reconciler is not an edge case, it's the
   product.

## Distribution decision

The agent must be **Developer ID signed and notarized**. An ad-hoc signed
binary is quarantined by Gatekeeper on any Mac that downloads it, so without
notarization there is no zero-install story at all. Start enrollment early —
it is calendar time, not work time. Simulate a downloaded provider with
`xattr -w com.apple.quarantine "0081;0;GridLink;" ./agent`.

## Data plane transport decision

- **Phase 2a (build this):** all nodes + gateway join a Tailscale tailnet.
  Gateway dials `<tailnet-ip>:<host_port>` directly. Zero tunnel code.
- **Phase 2b (later):** replace with a reverse tunnel (agent dials out,
  yamux). The ONLY code allowed to know about transport is
  gateway/internal/dialer — everything else must stay transport-agnostic.

## Definition of done

- `CreateDeployment` for a small model (e.g. Qwen2.5-0.5B-Instruct for fast
  iteration, then an 8B quantized) reaches READY on a real Mac node, with the
  engine using the GPU (verify: not a CPU-only fallback).
- `curl gateway:8080/v1/chat/completions -d '{"model":"...","stream":true,...}'`
  streams tokens end-to-end through the tailnet.
- Kill the node mid-stream: request fails, reconciler re-places the
  deployment on another node, model returns to READY without operator action.
- Usage JSONL contains correct prompt/completion token counts for both
  streaming and non-streaming requests.
- A notarized binary downloaded (not scp'd) to a clean Mac runs without any
  install step and without Gatekeeper intervention.

## Suggested Claude Code sessions

0. **Engine spike first** — before any proto work. Prove a self-contained Go
   binary drives llama.cpp on Metal at acceptable tokens/sec, since the job
   contract's shape depends on the answer. Throwaway code; measure, then
   delete.
1. Proto extensions compile; regenerate; both Phase 1 binaries still build.
2. agent/internal/deploy + client wiring (fake engine in tests).
3. coordinator/internal/deployments + server wiring (Admin + GatewayService).
4. gateway: router + proxy non-streaming path.
5. gateway: SSE streaming + stream_options usage capture.
6. Reconciler + kill-a-node end-to-end test.
