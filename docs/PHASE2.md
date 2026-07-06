# Phase 2 spec — inference endpoints

Read CLAUDE.md first. Do not start Phase 2 until the Phase 1 definition of
done passes on two real machines.

## What Phase 2 adds

1. **Deployments** (control plane): long-running vLLM containers, driven by
   the coordinator over the existing agent stream. New proto messages:
   DeploymentSpec/State, Start/StopDeployment, DeploymentUpdate, plus
   Heartbeat gains active_deployment_ids and data_plane_addr.
2. **Gateway** (data plane): new binary, OpenAI-compatible
   (/v1/models, /v1/chat/completions, /v1/completions), API-key auth,
   SSE streaming passthrough, per-request usage capture reported to the
   coordinator (GatewayService.ReportUsage — logged as JSONL in Phase 2,
   becomes the Phase 3 ledger feed).
3. **Placement + reconciliation**: coordinator/internal/deployments places
   replicas on eligible ONLINE nodes (one deployment per node — vLLM takes
   ~90% of VRAM), and re-places when a node dies. Node loss is NORMAL on
   consumer hardware; the reconciler is not an edge case, it's the product.

## Data plane transport decision

- **Phase 2a (build this):** all nodes + gateway join a Tailscale tailnet.
  Gateway dials `<tailnet-ip>:<host_port>` directly. Zero tunnel code.
- **Phase 2b (later):** replace with a reverse tunnel (agent dials out,
  yamux). The ONLY code allowed to know about transport is
  gateway/internal/dialer — everything else must stay transport-agnostic.

## Definition of done

- `CreateDeployment` for a small model (e.g. Qwen2.5-0.5B-Instruct for fast
  iteration, then Llama 3.1 8B) reaches READY on a real node.
- `curl gateway:8080/v1/chat/completions -d '{"model":"...","stream":true,...}'`
  streams tokens end-to-end through the tailnet.
- Kill the node mid-stream: request fails, reconciler re-places the
  deployment on another node, model returns to READY without operator action.
- Usage JSONL contains correct prompt/completion token counts for both
  streaming and non-streaming requests.

## Suggested Claude Code sessions

1. Proto extensions compile; regenerate; both Phase 1 binaries still build.
2. agent/internal/deploy + client wiring (fake Docker in tests).
3. coordinator/internal/deployments + server wiring (Admin + GatewayService).
4. gateway: router + proxy non-streaming path.
5. gateway: SSE streaming + stream_options usage capture.
6. Reconciler + kill-a-node end-to-end test.
