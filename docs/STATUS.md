# Where GridLink stands

Last updated: **2026-08-14**, branch `main`, HEAD `c5a6a98`, working tree clean.

Start here, then read CLAUDE.md (settled decisions) and docs/PHASE2.md (spec +
measured spike results). This file is the "what now"; those two are the "what
and why".

## Done

**Phase 1 is complete and validated on real hardware.** Coordinator on the Mac
mini (192.168.0.225), agent on the Raspberry Pi 5 (192.168.0.99), over the LAN:

- Pi registered, stayed ONLINE on heartbeats, ran an `alpine` job end to end
  (PULLING → RUNNING → SUCCEEDED, logs streamed back).
- Coordinator restart mid-session: agent reconnected in ~1s with the same
  `node_id` from `~/.gridlink/node_id`, and a **second job dispatched over the
  rebound stream succeeded** — which is the check that proves the registry
  rebinds the stream's Send func rather than holding a dead one.
- `make test` passes with no Docker and no GPU (6 packages).

**The one Phase 1 DoD item never exercised:** a `gpu`-mode job on real NVIDIA
hardware. The `--gpus` path and the `nvidia-smi` CSV parser are unit-tested
against fakes only. Under the current direction this matters less (Macs are
the target), but it has never run.

**Direction changed this session.** Three decisions, all now in CLAUDE.md:
Mac providers with zero preinstalled apps; no Docker on macOS (its Linux VM
cannot reach Metal — verified: containers there see no `/dev/dri`, no
`/dev/nvidia*`); and horizontal scale-out only, never sharding a model across
nodes. Rationale is recorded alongside each, because these are exactly the
decisions that get relitigated later.

**Engine spike ran — verdict: viable, build it.** A Go binary with llama.cpp
linked via cgo drove Metal with zero installed dependencies. Llama 3.1 8B
Q4_K_M: **19.4 tok/s** generate, 147.6 tok/s prefill, 33/33 layers on GPU,
~5.2 GB. Full numbers and the two findings that change planned work
(Metal's usable-memory budget, and Gatekeeper killing unnotarized binaries)
are in docs/PHASE2.md.

**Proto extended for the native path** (`6459c50`): `DeploymentSpec` now has an
`engine` oneof (ContainerEngine | NativeEngine), plus `GpuInfo.usable_vram_mb`,
`SystemInfo.runners`, and `DeploymentUpdate.progress_percent`.

**Session 2 (agent-side wiring) is done** — `d2885c9`. The agent reports
`SystemInfo` (it previously sent none at all) including a `runners` capability
list, handles StartDeployment/StopDeployment against a `deploy.Manager`
interface, and reports `active_deployment_ids` + `data_plane_addr` in
heartbeats. Docker is no longer required to start: an unreachable daemon
downgrades advertised capability instead of exiting, and RunJob fails cleanly
on a runner-less node. Registration is logged with os/arch/runners/gpu.

**Session 3 (the Metal engine) is done** — `c5a6a98`. A Mac provider can now
actually serve a model:

- `make engine` fetches + builds llama.cpp (pinned commit) via
  `scripts/fetch-llama.sh`; `make build` fails fast with that instruction if
  it is missing. Not vendored into git.
- `agent engine` loads a GGUF and serves `/v1/models`, `/v1/chat/completions`
  (streaming and not), `/health`. The agent re-execs itself to spawn it under
  `sandbox-exec`.
- `deploy.NativeManager` downloads weights with progress, verifies SHA-256
  before loading, spawns the engine, polls to READY, and reports STOPPED on
  cancellation. Verified end to end on the M4 under the real sandbox.
- `usable_vram_mb` is populated by the engine (12123 MB on this 16 GB M4) and
  `RUNNER_KIND_NATIVE_METAL` is advertised only when the engine exists in the
  build. The Mac now registers `runners=[RUNNER_KIND_NATIVE_METAL]`.
- The Pi cross-build is still pure Go via a non-cgo stub.

Two opt-in tests exercise real hardware and are skipped by default, so
`make test` stays GPU-free:

```bash
S=/path/to/models
GRIDLINK_TEST_MODEL=$S/qwen0.5b.gguf go test ./internal/engine/ -run TestManualGenerate -v
GRIDLINK_TEST_MODEL=$S/qwen0.5b.gguf GRIDLINK_TEST_AGENT_BIN=$PWD/bin/agent \
  go test ./internal/deploy/ -run TestManualNativeDeployment -v
```

## Next: Phase 2, session 4 — coordinator placement

The agent side is complete; nothing can drive it yet. `AdminService.CreateDeployment`
and `coordinator/internal/deployments` are still stubs, so a deployment can
only be started from a Go test.

1. **Implement the deployments store + reconciler** in
   `coordinator/internal/deployments`: desired state, placement onto an ONLINE
   node whose `SystemInfo.runners` includes the spec's engine kind and whose
   `GpuInfo.usable_vram_mb` >= `min_vram_mb` (never `vram_total_mb`; and 0
   means unknown, so refuse to place).
2. **Wire `CreateDeployment` / `DeleteDeployment` / `ListDeployments`** on
   AdminService, and route `StartDeployment` / `StopDeployment` down the
   existing agent stream.
3. **Consume `DeploymentUpdate`** and `Heartbeat.active_deployment_ids` to
   reconcile after a coordinator restart — re-sending StartDeployment for a
   deployment the agent already runs is expected and already ignored agent-side.
4. **Re-place on node loss**, which is the actual product: consumer nodes
   disappear constantly.

Then session 5 is the gateway.

## Open items that need you, not code

- **Apple Developer enrollment ($99/yr).** This is the hard blocker for the
  whole distribution story and it is calendar time, not work time. An
  unnotarized binary carrying a quarantine xattr was **SIGKILLed (exit 137)**
  with no output; `spctl` returned `rejected`. Start enrollment before the
  engine work lands, or there is nothing to ship it in.
- **`NodeSummary` does not expose `SystemInfo`**, so `ListNodes` cannot show a
  node's runners, RAM, or `usable_vram_mb` — only the coordinator's
  registration log does. Worth adding in session 4, since placement decisions
  will be impossible to debug from the outside otherwise.
- **No token accounting yet.** The engine does not report prompt/completion
  token counts, which Phase 2's definition of done requires for usage JSONL.
  llama.cpp knows them; they need plumbing through `engine.Token` and the
  chat response's `usage` field.
- **Engine concurrency is one request at a time**, serialized by a mutex. That
  is correct for one llama.cpp context, but it means a slow generation blocks
  the next request rather than queueing with any fairness. Fine for now; worth
  revisiting when the gateway can drive real load.
- **Pre-existing `go vet` failures in `agent/internal/runner/docker_test.go`**
  (a range variable copying a mutex, and an unused cancel on some paths). Not
  introduced by the Phase 2 work, but `go vet ./...` is not clean in the agent
  module until they are fixed.
- **cgo enters the tree with the engine.** The agent's macOS build will then
  need Xcode CLT and lose trivial cross-compilation. `make build-linux-arm64`
  (the Pi target) must keep working — it is pure Go and should stay that way.

## Rebuilding the spike

The spike lived in a session scratchpad and is gone. To reproduce (needs
`cmake`, already installed via Homebrew):

```bash
git clone --depth 1 https://github.com/ggml-org/llama.cpp
cmake -S llama.cpp -B build -DCMAKE_BUILD_TYPE=Release \
  -DGGML_METAL=ON -DGGML_METAL_EMBED_LIBRARY=ON -DBUILD_SHARED_LIBS=OFF \
  -DLLAMA_BUILD_TESTS=OFF -DLLAMA_BUILD_EXAMPLES=OFF -DLLAMA_BUILD_SERVER=OFF
cmake --build build --config Release -j $(sysctl -n hw.ncpu)
```

`llama-app` fails to link with `LLAMA_BUILD_SERVER=OFF` (it wants
`llama-server-impl`). Harmless — every library needed builds first, and the
error is at 100%.

Link from Go with the static libs in `build/src`, `build/ggml/src`,
`build/ggml/src/ggml-metal`, `build/ggml/src/ggml-blas`, plus `-lc++` and the
Metal / MetalKit / Foundation / Accelerate / CoreFoundation frameworks. Verify
self-containment with `otool -L` — only `/usr/lib` and `/System/Library`
entries are acceptable.

## Re-running the two-machine test

```bash
# Mac (coordinator)
GRIDLINK_TOKEN=dev-token make run-coord

# Pi (agent) — key-based ssh is set up as host alias `pi5`
make build-linux-arm64 && scp bin/agent-linux-arm64 pi5:agent
ssh pi5 'GRIDLINK_TOKEN=dev-token GRIDLINK_COORDINATOR=192.168.0.225:50051 ./agent'

# Mac (drive it)
GRIDLINK_TOKEN=dev-token ./scripts/smoke.sh <node_id> cpu
```

The agent exits immediately if the Docker daemon is unreachable, so "the node
never appeared" is almost always Docker rather than networking. Note this is
the behaviour the macOS pivot removes: Mac providers will not have Docker at
all, so that startup check has to become platform-aware.
