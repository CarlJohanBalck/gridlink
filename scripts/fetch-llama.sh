#!/usr/bin/env bash
# Fetch and build llama.cpp for the agent's Metal engine (macOS only).
#
# Not vendored into git: llama.cpp is large and we only need its static libs.
# Pinned to a commit so builds are reproducible — bump LLAMA_COMMIT
# deliberately, never float.
#
# Build-machine only. Providers install nothing: GGML_METAL_EMBED_LIBRARY
# compiles the Metal shaders INTO the binary, so no .metallib ships alongside.
set -euo pipefail

LLAMA_COMMIT="${LLAMA_COMMIT:-a94d563}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="$ROOT/agent/internal/engine/third_party/llama.cpp"
BUILD="$DEST/build"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "fetch-llama.sh: macOS only (the Metal engine does not build elsewhere)" >&2
  exit 1
fi
if ! command -v cmake >/dev/null 2>&1; then
  echo "fetch-llama.sh: cmake not found (brew install cmake)" >&2
  exit 1
fi

if [[ ! -d "$DEST/.git" ]]; then
  echo "==> cloning llama.cpp @ $LLAMA_COMMIT"
  mkdir -p "$(dirname "$DEST")"
  git clone --filter=blob:none "https://github.com/ggml-org/llama.cpp" "$DEST"
fi

echo "==> checking out $LLAMA_COMMIT"
git -C "$DEST" fetch --depth 1 origin "$LLAMA_COMMIT" 2>/dev/null || git -C "$DEST" fetch origin
git -C "$DEST" checkout --quiet "$LLAMA_COMMIT"

# libllama.a and friends are all we need; skip the tools, which additionally
# fail to link when the server is disabled.
echo "==> configuring"
cmake -S "$DEST" -B "$BUILD" \
  -DCMAKE_BUILD_TYPE=Release \
  -DGGML_METAL=ON \
  -DGGML_METAL_EMBED_LIBRARY=ON \
  -DBUILD_SHARED_LIBS=OFF \
  -DLLAMA_BUILD_TESTS=OFF \
  -DLLAMA_BUILD_EXAMPLES=OFF \
  -DLLAMA_BUILD_TOOLS=OFF \
  -DLLAMA_BUILD_SERVER=OFF \
  -DLLAMA_CURL=OFF >/dev/null

# Build ONLY the llama library target. Building "all" drags in llama-app,
# which fails to compile its downloader when LLAMA_CURL=OFF and which we do
# not need: the agent is the only front end for this engine.
echo "==> building (this takes a few minutes the first time)"
cmake --build "$BUILD" --config Release -j "$(sysctl -n hw.ncpu)" --target llama

echo "==> done: $BUILD"
