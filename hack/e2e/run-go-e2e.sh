#!/usr/bin/env bash
#
# Copyright 2026 Ben Coxford.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# run-go-e2e.sh — Run the upstream Kubernetes Go e2e/conformance suite locally
# in a privileged container, mirroring the k8s-go-e2e.yml CI workflow.
#
# Uses the same CI build image as GitHub Actions (ghcr.io/bencoxforddev/kubeair/build)
# via podman (or docker).
#
# Requirements: podman (preferred) or docker.
#
# What this script does:
#   1. Pulls the CI build image.
#   2. Launches a privileged container with the repo bind-mounted.
#   3. Inside the container: builds the kubelet, provisions a single-node k3s
#      cluster, then runs hack/e2e/run-upstream-go-e2e.sh.
#
# Environment overrides:
#   CONTAINER_RUNTIME      podman or docker. Default: podman
#   BUILD_IMAGE            Build image to use.
#                          Default: ghcr.io/bencoxforddev/kubeair/build:1.33
#   RUN_CONFORMANCE        "1" to run conformance suite. Default: 1
#   RUN_E2E                "1" to run non-conformance e2e suite. Default: 0
#   CONFORMANCE_FOCUS      Focus regex. Default: \[Conformance\]
#   CONFORMANCE_SKIP       Skip regex. Default: \[Serial\]|\[Slow\]|\[Disruptive\]|\[Flaky\]
#   E2E_FOCUS              Focus regex for e2e suite. Default: \[sig-node\]
#   E2E_SKIP               Skip regex for e2e suite. Default: \[Serial\]|\[Slow\]|\[Disruptive\]|\[Flaky\]
#   GINKGO_NODES           Parallel ginkgo nodes. Default: 4
#   RESET_EXISTING         "1" to uninstall existing k3s before init. Default: 0
#   SKIP_BUILD             "1" to reuse an existing binary (skip bazel build). Default: 0
set -euo pipefail

CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-podman}"
BUILD_IMAGE="${BUILD_IMAGE:-ghcr.io/bencoxforddev/kubeair/build:1.33}"
RUN_CONFORMANCE="${RUN_CONFORMANCE:-1}"
RUN_E2E="${RUN_E2E:-0}"
CONFORMANCE_FOCUS="${CONFORMANCE_FOCUS:-\\[Conformance\\]}"
CONFORMANCE_SKIP="${CONFORMANCE_SKIP:-\\[Serial\\]|\\[Slow\\]|\\[Disruptive\\]|\\[Flaky\\]}"
E2E_FOCUS="${E2E_FOCUS:-\\[sig-node\\]}"
E2E_SKIP="${E2E_SKIP:-\\[Serial\\]|\\[Slow\\]|\\[Disruptive\\]|\\[Flaky\\]}"
GINKGO_NODES="${GINKGO_NODES:-4}"
RESET_EXISTING="${RESET_EXISTING:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

log()  { printf '[go-e2e-run] %s\n' "$*"; }
die()  { printf '[go-e2e-run] ERROR: %s\n' "$*" >&2; exit 1; }
step() { printf '\n[go-e2e-run] ══ %s ══\n' "$*"; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1 — install $2"
}

require_cmd "$CONTAINER_RUNTIME" "$CONTAINER_RUNTIME"

step "Running upstream Kubernetes Go e2e suite"
log "Runtime:  $CONTAINER_RUNTIME"
log "Image:    $BUILD_IMAGE"
log "Repo:     $REPO_ROOT"

# ── Pull image ─────────────────────────────────────────────────────────────────

if [[ "$BUILD_IMAGE" == *:local || "$BUILD_IMAGE" == localhost/* ]]; then
  log "Skipping pull for local image: $BUILD_IMAGE"
else
  step "Pulling build image"
  "$CONTAINER_RUNTIME" pull "$BUILD_IMAGE" || log "Pull failed — trying with cached image"
fi

# ── Artifact dirs ──────────────────────────────────────────────────────────────

ARTIFACT_DIR="${HOME}/.kubeair/go-e2e-artifacts/$(date +%Y%m%dT%H%M%S)"
mkdir -p "$ARTIFACT_DIR"

RESULTS_DIR="$REPO_ROOT/tests/results"
copy_results() {
  mkdir -p "$RESULTS_DIR"
  cp -r "$ARTIFACT_DIR/." "$RESULTS_DIR/" 2>/dev/null || true
  log "Results copied to: $RESULTS_DIR"
  ls -lh "$RESULTS_DIR" || true
}
trap copy_results EXIT

step "Launching container (--privileged)"

mkdir -p "${HOME}/.cache/bazel"

"$CONTAINER_RUNTIME" run \
  --rm \
  --privileged \
  --network=host \
  --ulimit nofile=65536:65536 \
  -v "$REPO_ROOT:/workspace:z" \
  -v "$ARTIFACT_DIR:/artifacts:z" \
  -v "${HOME}/.cache/bazel:/root/.cache/bazel:z" \
  -e RESET_EXISTING="$RESET_EXISTING" \
  -e SKIP_BUILD="$SKIP_BUILD" \
  -e RUN_CONFORMANCE="$RUN_CONFORMANCE" \
  -e RUN_E2E="$RUN_E2E" \
  -e CONFORMANCE_FOCUS="$CONFORMANCE_FOCUS" \
  -e CONFORMANCE_SKIP="$CONFORMANCE_SKIP" \
  -e E2E_FOCUS="$E2E_FOCUS" \
  -e E2E_SKIP="$E2E_SKIP" \
  -e GINKGO_NODES="$GINKGO_NODES" \
  -e KUBEAIR_REPO_PATH=/workspace \
  -e ARTIFACT_DIR=/artifacts \
  "$BUILD_IMAGE" \
  bash /workspace/hack/e2e/run-go-e2e-in-container.sh

log "Artifacts saved to: $ARTIFACT_DIR"
ls -lh "$ARTIFACT_DIR" || true

log "run-go-e2e.sh complete"
