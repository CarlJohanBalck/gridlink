# Where GridLink stands

Last updated: **2026-08-13**, branch `main`, HEAD `6459c50`, working tree clean.

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

## Next: Phase 2, session 2 — agent-side wiring

1. **Advertise capability.** Populate `SystemInfo.runners` in `Register`:
   `RUNNER_KIND_NATIVE_METAL` on darwin/arm64, `RUNNER_KIND_DOCKER` where a
   Docker daemon is reachable. Without this the coordinator cannot place
   anything on a mixed fleet.
2. **Handle StartDeployment / StopDeployment** on the existing stream in
   `agent/internal/client`, dispatching to `agent/internal/deploy`. The
   implementation plan is written out in the `Manager` doc comment in
   [deploy.go](../agent/internal/deploy/deploy.go).
3. **Fake engine in tests.** Same pattern as `runner.Runner`: never require a
   real GPU, a real download, or a real Docker daemon in unit tests.
4. Emit `DeploymentUpdate` (with `progress_percent` during PULLING) and include
   `active_deployment_ids` in every Heartbeat.

Deliberately NOT in session 2: the real llama.cpp engine (session 3+), the
gateway, and placement/reconciliation on the coordinator.

## Open items that need you, not code

- **Apple Developer enrollment ($99/yr).** This is the hard blocker for the
  whole distribution story and it is calendar time, not work time. An
  unnotarized binary carrying a quarantine xattr was **SIGKILLed (exit 137)**
  with no output; `spctl` returned `rejected`. Start enrollment before the
  engine work lands, or there is nothing to ship it in.
- **`usable_vram_mb` has no producer yet.** The field exists and the contract
  says 0 means "refuse to place". Querying Metal needs cgo, which arrives with
  the engine — so the engine should report it rather than the detector
  guessing. Until then placement must not run.
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
