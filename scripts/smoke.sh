#!/usr/bin/env bash
# Phase 1 smoke test: list nodes, then run a job on the named one.
#   smoke.sh <node_id> [gpu|cpu]   (default: gpu)
# gpu: nvidia-smi in a CUDA container (needs an NVIDIA Linux node)
# cpu: uname -a in alpine (works on any node, incl. arm64)
# Requires grpcurl (https://github.com/fullstorydev/grpcurl).
set -euo pipefail
ADDR="${GRIDLINK_COORDINATOR:-localhost:50051}"
TOKEN="${GRIDLINK_TOKEN:-dev-token}"
H=(-H "authorization: bearer ${TOKEN}")

echo "== nodes =="
grpcurl -plaintext "${H[@]}" "$ADDR" compute.v1.AdminService/ListNodes

NODE_ID="${1:?usage: smoke.sh <node_id> [gpu|cpu]}"
MODE="${2:-gpu}"
case "$MODE" in
  gpu) IMAGE="nvidia/cuda:12.4.1-base-ubuntu22.04"; CMD='["nvidia-smi"]'; GPU=true ;;
  cpu) IMAGE="alpine:3.20"; CMD='["uname", "-a"]'; GPU=false ;;
  *) echo "unknown mode: $MODE (want gpu or cpu)" >&2; exit 1 ;;
esac

echo "== running ${MODE} job on ${NODE_ID} =="
grpcurl -plaintext "${H[@]}" -d "{
  \"node_id\": \"${NODE_ID}\",
  \"spec\": {
    \"image\": \"${IMAGE}\",
    \"command\": ${CMD},
    \"gpu\": ${GPU},
    \"timeout_s\": 120
  }
}" "$ADDR" compute.v1.AdminService/RunJob
echo "job dispatched; watch the coordinator log for state changes and output"
