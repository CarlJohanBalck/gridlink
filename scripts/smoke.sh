#!/usr/bin/env bash
# Phase 1 smoke test: list nodes, run nvidia-smi on the first one, tail logs.
# Requires grpcurl (https://github.com/fullstorydev/grpcurl).
set -euo pipefail
ADDR="${GRIDLINK_COORDINATOR:-localhost:50051}"
TOKEN="${GRIDLINK_TOKEN:-dev-token}"
H=(-H "authorization: bearer ${TOKEN}")

echo "== nodes =="
grpcurl -plaintext "${H[@]}" "$ADDR" compute.v1.AdminService/ListNodes

NODE_ID="${1:?usage: smoke.sh <node_id>}"
echo "== running nvidia-smi on ${NODE_ID} =="
grpcurl -plaintext "${H[@]}" -d "{
  \"node_id\": \"${NODE_ID}\",
  \"spec\": {
    \"image\": \"nvidia/cuda:12.4.1-base-ubuntu22.04\",
    \"command\": [\"nvidia-smi\"],
    \"gpu\": true,
    \"timeout_s\": 120
  }
}" "$ADDR" compute.v1.AdminService/RunJob
