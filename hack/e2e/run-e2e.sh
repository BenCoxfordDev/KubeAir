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
# run-e2e.sh — Run the kube-air e2e suite in a privileged container.
#
# Uses the same CI build image as GitHub Actions (ghcr.io/bencoxforddev/kubeair/build)
# via podman (or docker). Replaces the previous Colima VM approach for faster
# local iteration on macOS.
#
# Requirements: podman (preferred) or docker.
#
# What this script does:
#   1. Pulls the CI build image.
#   2. Launches a privileged container with the repo bind-mounted.
#   3. Inside the container: builds the kubelet with Bazel, provisions a
#      single-node k3s cluster (no systemd needed), and runs all test suites.
#
# Environment overrides:
#   CONTAINER_RUNTIME    podman or docker. Default: podman
#   BUILD_IMAGE          Build image to use.
#                        Default: ghcr.io/bencoxforddev/kubeair/build:1.33
#   RUN_UNIT_TESTS       "1" to run unit/conformance/smoke. Default: 1
#   RUN_E2E_TESTS        "1" to run live cluster e2e. Default: 1
#   RESET_EXISTING       "1" to uninstall existing k3s before init. Default: 0
#   SKIP_BUILD           "1" to reuse an existing binary (skip bazel build). Default: 0
set -euo pipefail

CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-podman}"
BUILD_IMAGE="${BUILD_IMAGE:-ghcr.io/bencoxforddev/kubeair/build:1.33}"
RUN_UNIT_TESTS="${RUN_UNIT_TESTS:-1}"
RUN_E2E_TESTS="${RUN_E2E_TESTS:-1}"
RESET_EXISTING="${RESET_EXISTING:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

log()  { printf '[colima-run] %s\n' "$*"; }
die()  { printf '[colima-run] ERROR: %s\n' "$*" >&2; exit 1; }
step() { printf '\n[colima-run] ══ %s ══\n' "$*"; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1 — install $2"
}

require_cmd "$CONTAINER_RUNTIME" "$CONTAINER_RUNTIME"

step "Running kube-air e2e suite"
log "Runtime:  $CONTAINER_RUNTIME"
log "Image:    $BUILD_IMAGE"
log "Repo:     $REPO_ROOT"

# ── Pull image ─────────────────────────────────────────────────────────────────

step "Pulling build image"
"$CONTAINER_RUNTIME" pull "$BUILD_IMAGE" || log "Pull failed — trying with cached image"

# ── Run container ──────────────────────────────────────────────────────────────

ARTIFACT_DIR="${HOME}/.kubeair/e2e-artifacts/$(date +%Y%m%dT%H%M%S)"
mkdir -p "$ARTIFACT_DIR"

step "Launching container (--privileged)"

"$CONTAINER_RUNTIME" run \
  --rm \
  --privileged \
  --network=host \
  -v "$REPO_ROOT:/workspace:z" \
  -v "$ARTIFACT_DIR:/artifacts:z" \
  -e RUN_UNIT_TESTS="$RUN_UNIT_TESTS" \
  -e RUN_E2E_TESTS="$RUN_E2E_TESTS" \
  -e RESET_EXISTING="$RESET_EXISTING" \
  -e SKIP_BUILD="$SKIP_BUILD" \
  -e KUBEAIR_REPO_PATH=/workspace \
  -e ARTIFACT_DIR=/artifacts \
  "$BUILD_IMAGE" \
  bash /workspace/hack/e2e/run-in-container.sh

log "Artifacts saved to: $ARTIFACT_DIR"
ls -lh "$ARTIFACT_DIR" || true

log "colima-run.sh complete"
