#!/usr/bin/env bash
# One-command local demo: coordinator + agent + gateway on this Mac, serving a
# small model over an OpenAI-compatible API.
#
#   ./scripts/demo.sh start   # bring it all up and deploy a model
#   ./scripts/demo.sh ask "your question"
#   ./scripts/demo.sh stream "your question"
#   ./scripts/demo.sh status  # nodes, deployments, usage so far
#   ./scripts/demo.sh stop    # shut everything down
#
# Requires: make build && make engine (macOS, Apple Silicon).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN="${GRIDLINK_DEMO_DIR:-/tmp/gridlink-demo}"
TOKEN="${GRIDLINK_TOKEN:-dev-token}"
KEY="${GRIDLINK_DEMO_KEY:-sk-demo}"
# Port 8080 is often taken by Docker Desktop, so the demo uses 8099.
GW_PORT="${GRIDLINK_DEMO_PORT:-8099}"
COORD="localhost:50051"

MODEL_NAME="qwen-0.5b"
MODEL_REPO="Qwen/Qwen2.5-0.5B-Instruct-GGUF"
MODEL_FILE="qwen2.5-0.5b-instruct-q4_k_m.gguf"
# Verified digest of that exact file. The agent refuses to load anything else,
# so a corrupted or swapped download fails instead of silently serving.
MODEL_SHA="74a4da8c9fdbcd15bd1f6d01d621410d31c6fc00986f5eb687824e7b93d7a9db"

grpc() { grpcurl -plaintext -H "authorization: bearer ${TOKEN}" "$@"; }

start() {
  mkdir -p "$RUN"
  if [[ ! -x "$ROOT/bin/gateway" ]]; then
    echo "binaries missing; run: make build" >&2; exit 1
  fi

  echo "==> starting coordinator"
  GRIDLINK_TOKEN="$TOKEN" GRIDLINK_USAGE_LOG="$RUN/usage.jsonl" \
    "$ROOT/bin/coordinator" > "$RUN/coordinator.log" 2>&1 &
  echo $! > "$RUN/coordinator.pid"
  sleep 2

  echo "==> starting agent (first run takes ~10s: Metal compiles its shaders)"
  GRIDLINK_TOKEN="$TOKEN" GRIDLINK_COORDINATOR="$COORD" GRIDLINK_DATA_ADDR=127.0.0.1 \
    "$ROOT/bin/agent" > "$RUN/agent.log" 2>&1 &
  echo $! > "$RUN/agent.pid"

  echo "==> starting gateway on :${GW_PORT}"
  GRIDLINK_TOKEN="$TOKEN" GRIDLINK_COORDINATOR="$COORD" \
    GRIDLINK_GATEWAY_LISTEN=":${GW_PORT}" GRIDLINK_API_KEYS="$KEY" \
    "$ROOT/bin/gateway" > "$RUN/gateway.log" 2>&1 &
  echo $! > "$RUN/gateway.pid"

  echo -n "==> waiting for the node to register"
  for _ in $(seq 1 40); do
    if grpc "$COORD" compute.v1.AdminService/ListNodes 2>/dev/null | grep -q NODE_STATUS_ONLINE; then
      echo " ok"; break
    fi
    echo -n "."; sleep 1
  done

  echo "==> deploying ${MODEL_NAME}"
  grpc -d "{\"spec\":{\"served_model_name\":\"${MODEL_NAME}\",\"min_vram_mb\":2000,
      \"native\":{\"model_ref\":\"${MODEL_REPO}\",\"model_file\":\"${MODEL_FILE}\",
      \"sha256\":\"${MODEL_SHA}\",\"context_length\":2048}},\"replicas\":1}" \
    "$COORD" compute.v1.AdminService/CreateDeployment >/dev/null

  echo -n "==> waiting for it to be READY (first run downloads ~469 MB)"
  for _ in $(seq 1 300); do
    if curl -sf -H "Authorization: Bearer ${KEY}" "localhost:${GW_PORT}/v1/models" \
         2>/dev/null | grep -q "$MODEL_NAME"; then
      echo " ok"
      echo
      echo "Ready. Try:"
      echo "  $0 ask \"Name three colours.\""
      echo "  $0 stream \"Count to five.\""
      echo "  $0 status"
      return 0
    fi
    echo -n "."; sleep 2
  done
  echo
  echo "timed out; see $RUN/agent.log" >&2
  exit 1
}

ask() {
  local q="${1:-Say hello.}"
  curl -s -H "Authorization: Bearer ${KEY}" -H 'content-type: application/json' \
    -d "{\"model\":\"${MODEL_NAME}\",\"messages\":[{\"role\":\"user\",\"content\":$(json_str "$q")}],\"max_tokens\":200}" \
    "localhost:${GW_PORT}/v1/chat/completions" |
    python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["choices"][0]["message"]["content"]); print("\n[usage]", d["usage"])'
}

stream() {
  local q="${1:-Count to five.}"
  curl -sN -H "Authorization: Bearer ${KEY}" -H 'content-type: application/json' \
    -d "{\"model\":\"${MODEL_NAME}\",\"messages\":[{\"role\":\"user\",\"content\":$(json_str "$q")}],\"max_tokens\":200,\"stream\":true}" \
    "localhost:${GW_PORT}/v1/chat/completions" |
    python3 -u -c '
import sys, json
for line in sys.stdin:
    if not line.startswith("data: "): continue
    p = line[6:].strip()
    if p == "[DONE]": print(); break
    try: c = json.loads(p)
    except ValueError: continue
    if c.get("usage"): print("\n[usage]", c["usage"]); continue
    for ch in c.get("choices", []):
        sys.stdout.write(ch.get("delta", {}).get("content", ""))
'
}

status() {
  echo "== nodes =="
  grpc "$COORD" compute.v1.AdminService/ListNodes
  echo "== deployments =="
  grpc "$COORD" compute.v1.AdminService/ListDeployments
  echo "== usage so far =="
  cat "$RUN/usage.jsonl" 2>/dev/null || echo "(none yet)"
}

stop() {
  for p in gateway agent coordinator; do
    if [[ -f "$RUN/$p.pid" ]]; then
      kill "$(cat "$RUN/$p.pid")" 2>/dev/null || true
      rm -f "$RUN/$p.pid"
    fi
  done
  # The engine is a child of the agent; make sure it goes too.
  pkill -f "agent engine" 2>/dev/null || true
  echo "stopped (logs remain in $RUN)"
}

# json_str quotes a string safely, so a question containing quotes or newlines
# cannot break the request body.
json_str() { python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1"; }

case "${1:-}" in
  start)  start ;;
  ask)    shift; ask "${1:-}" ;;
  stream) shift; stream "${1:-}" ;;
  status) status ;;
  stop)   stop ;;
  *) echo "usage: $0 {start|ask <q>|stream <q>|status|stop}" >&2; exit 1 ;;
esac
