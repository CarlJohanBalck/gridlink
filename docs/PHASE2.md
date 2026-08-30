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

## Engine spike results (measured, Mac mini M4 base / 16GB / macOS 26.5.2)

Session 0 ran. Verdict: **viable, build it.** A Go binary with llama.cpp linked
via cgo drove Metal with no installed dependencies.

- **Self-contained: confirmed.** `otool -L` on the linked binary lists only
  `/usr/lib` and `/System/Library` — nothing from Homebrew, no OpenSSL despite
  cmake finding it at configure time. `GGML_METAL_EMBED_LIBRARY=ON` works:
  the log says `using embedded metal library`, so no `.metallib` ships
  alongside. Binary was 7.5 MB with static llama.cpp/ggml.
- **Throughput (Llama 3.1 8B Q4_K_M, 33/33 layers on GPU):** 19.4 tok/s
  generation, 147.6 tok/s prefill. Comfortably above reading speed, so it is a
  usable product on a *base* M4. Qwen2.5 0.5B: ~145 tok/s generation.
- **Memory:** 8B Q4 occupies ~4.6 GB weights + 256 MB KV + 267 MB compute
  ≈ 5.2 GB. Fits with room to spare.
- **Scheduling must NOT use total RAM.** Metal reports
  `recommendedMaxWorkingSetSize = 12713 MB` on this 16 GB machine — the usable
  GPU budget is ~78% of RAM. The agent currently registers 16384 MB (from
  `hw.memsize`), overstating by 29%. Placement decisions must use the Metal
  figure; the engine has cgo and can query it, so it should report the real
  budget rather than the detector guessing.
- **Cold start:** the first-ever run compiles Metal shaders (6.5 s); every run
  after is 0.01 s (OS-cached). Warm the engine once at agent startup so a
  provider's first job does not eat the penalty.
- **`sandbox-exec` is viable.** Metal works under a `(deny default)` profile
  given `iokit-open`, `mach-lookup`, `file-read*` and a writable scratch
  subpath — full speed, GPU still `MTL0 (Apple M4)`.
- **Gatekeeper kills an unnotarized agent ONLY when the file is quarantined.**
  With a quarantine xattr set, the ad-hoc-signed binary was **SIGKILLed
  (exit 137)** with no output, and `spctl -a` returned `rejected`. But the
  quarantine attribute is applied by browsers and LaunchServices — **not** by
  `curl`, `brew`, or `scp`. Verified: a curl-downloaded copy of the agent has
  no quarantine attribute and runs unsigned. So notarization gates a
  double-clickable .app/.dmg from a web page; it does NOT gate a terminal
  install (`curl ... | sh`, Homebrew), which is how this ships.

## Distribution decision

Ship as a **terminal install** (`curl -fsSL <url>/install.sh | sh`, and later
a Homebrew formula). Terminal downloads do not carry the quarantine
attribute, so an unsigned binary runs fine and no Apple Developer
subscription is required.

Developer ID signing + notarization ($99/yr) buys a double-clickable app
downloaded from a web page without warnings. That is a polish decision for
when there are users to polish for, not a prerequisite. Revisit it if
providers turn out to need a GUI installer.

To test the quarantined path anyway:
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
