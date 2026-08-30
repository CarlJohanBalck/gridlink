# Where GridLink stands

Last updated: **2026-08-30**, branch `main`, HEAD `b49ee72`, working tree clean.

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

**Session 4 (coordinator placement) is done** — `82351f0`. A deployment now
runs end to end from the admin API:

```bash
grpcurl ... AdminService/CreateDeployment   # -> deployment_id
grpcurl ... AdminService/ListDeployments    # PULLING -> LOADING -> READY
grpcurl ... GatewayService/ResolveModel     # -> {node_id, deployment_id, addr}
curl $addr/v1/chat/completions              # real tokens
grpcurl ... AdminService/DeleteDeployment   # engine subprocess exits
```

Verified live on the M4, including a real Hugging Face download with SHA-256
verification, and coordinator-restart reconciliation (the restarted process
has an empty table, spots the still-running deployment from the next
heartbeat, and stops it).

- Placement matches `spec.engine` against `SystemInfo.runners` and sizes
  against `usable_vram_mb`; `0` refuses rather than guessing.
- A refusal names why each node was skipped, and `ListNodes` now returns
  `SystemInfo` plus `deployment_ids`.
- Unplaced deployments are retried by the reconciler, so creating one before
  any node connects is fine.
- A lost or FAILED replica is re-placed after a 60s grace window, onto a
  *different* node (the failed one is skipped for 10 min).
- `ResolveModel` returns only READY replicas that have a data-plane address.
- `ReportUsage` appends JSONL to `$GRIDLINK_USAGE_LOG`.

**Session 5 (the gateway) is done** — `cadfed6`. **Phase 2's definition of
done is met.** A request now goes client → gateway → node → Metal engine and
back, metered:

```bash
GRIDLINK_API_KEYS=sk-1 GRIDLINK_GATEWAY_LISTEN=:8099 ./bin/gateway
curl -H "Authorization: Bearer sk-1" localhost:8099/v1/models
curl -H "Authorization: Bearer sk-1" localhost:8099/v1/chat/completions -d '{...}'
```

- Token counts come from the tokenizer, identical streaming and not
  (11/9/20 for the same prompt), and land in `$GRIDLINK_USAGE_LOG` as JSONL
  with the calling key's ID.
- SSE streams through unbuffered (`FlushInterval=-1`); usage is scraped in
  passing rather than by reading the response.
- 401 without a valid key, 404 for an unknown model, 503 for a deployed model
  with no READY replica, 502 after a failed retry on another replica.
- **Node loss verified live**: killed the node hosting a deployment, the
  reconciler re-placed it on a second node, and inference resumed with no
  operator action.

Two nodes on one Mac, for testing re-placement without a second machine:

```bash
mkdir -p /tmp/node2/.gridlink/models
ln -f ~/.gridlink/models/*.gguf /tmp/node2/.gridlink/models/   # skip re-download
HOME=/tmp/node2 GRIDLINK_TOKEN=... GRIDLINK_DATA_ADDR=127.0.0.1 ./bin/agent
```

## Next

Phase 2 is functionally complete. What remains before calling it shipped:

1. ~~Notarization~~ — **done differently**: `scripts/install.sh` ships the
   agent as a terminal install, which needs no Apple account (see the
   corrected note below). `make release` builds the artifacts and a
   SHA256SUMS the installer verifies. What is still missing is somewhere to
   publish them: the installer points at a GitHub releases URL that does not
   exist yet.
2. **Tailscale end-to-end.** Everything so far ran with
   `GRIDLINK_DATA_ADDR=127.0.0.1` on one machine. The tailnet path
   (`dialer.Direct` to a tailnet IP) has never been exercised across two
   real machines.
3. **`/v1/completions` is routed but unimplemented by the native engine** —
   it forwards and 404s. Either implement it or stop advertising the route.
4. **The gateway is a single point of failure** and holds request bodies in
   memory (8 MiB cap) to allow retries. Fine at this scale; worth revisiting
   before real traffic.

Then Phase 3: metering + ledger (Postgres), which is what the usage JSONL
was shaped for.

## Open items that need you, not code

- **Publish the release artifacts.** `make release` produces them and
  `scripts/install.sh` expects them at `GRIDLINK_BASE_URL`, but nothing is
  hosted yet, so the documented curl command does not work for anyone else.
  Tested end to end against a local HTTP server.
- **Apple Developer enrollment ($99/yr) is OPTIONAL, not a blocker.** Earlier
  notes here overstated it. Gatekeeper only kills binaries carrying the
  quarantine attribute, which browsers set and `curl`/`brew`/`scp` do not —
  verified on this machine. A terminal install therefore runs unsigned.
  Signing only buys a warning-free double-clickable app from a web page.
- **No token accounting yet.** The engine does not report prompt/completion
  token counts, which Phase 2's definition of done requires for usage JSONL.
  llama.cpp knows them; they need plumbing through `engine.Token` and the
  chat response's `usage` field. This now blocks the gateway.
- **Agent startup costs ~9s on a cold Metal cache**, because `GPUStats()`
  initialises the backend and compiles shaders. Subsequent starts are
  instant. It delays first registration, so do not mistake it for a hang.
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
