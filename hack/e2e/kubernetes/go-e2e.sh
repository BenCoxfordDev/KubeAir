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
# go-e2e.sh — Run upstream Kubernetes Go e2e/conformance tests against
# the kubeadm cluster provisioned inside the e2e podman container.
#
# The cluster runs inside the container launched by hack/e2e/run-e2e.sh.
# This script execs into that container and runs run-upstream-go-e2e.sh there
# so test binaries are linux/arm64 and the API server endpoint is reachable.
#
# Prerequisites (macOS host):
#   podman   (brew install podman)
#   curl, tar
#
# The e2e container must already be running (i.e. `just e2e` is in progress
# or you launched the container manually).
#
# Usage:
#   bash hack/e2e/kubernetes/go-e2e.sh
#
# Environment variables:
#   CONTAINER_RUNTIME     podman or docker. Default: podman
#   UPSTREAM_K8S_VERSION  Kubernetes release for test binaries. Default: detect.
#   RUN_CONFORMANCE       "1" to run conformance suite. Default 1.
#   RUN_E2E               "1" to run non-conformance e2e suite. Default 0.
#   CONFORMANCE_FOCUS     Focus regex for conformance. Default \[Conformance\].
#   CONFORMANCE_SKIP      Skip regex for conformance.
#                         Default \[Serial\]|\[Slow\]|\[Disruptive\]|\[Flaky\].
#   E2E_FOCUS             Focus regex for e2e. Default \[sig-node\].
#   E2E_SKIP              Skip regex for e2e.
#                         Default \[Serial\]|\[Slow\]|\[Disruptive\]|\[Flaky\].
#   GINKGO_NODES          Parallel ginkgo nodes. Default 4.
#   LOCAL_ARTIFACT_DIR    Where to copy artifacts on the host after the run.
#                         Default /tmp/k8s-upstream-go-e2e-artifacts.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_SCRIPT="${SCRIPT_DIR}/run-upstream-go-e2e.sh"

CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-podman}"
LOCAL_ARTIFACT_DIR="${LOCAL_ARTIFACT_DIR:-/tmp/k8s-upstream-go-e2e-artifacts}"

UPSTREAM_K8S_VERSION="${UPSTREAM_K8S_VERSION:-}"
RUN_CONFORMANCE="${RUN_CONFORMANCE:-1}"
RUN_E2E="${RUN_E2E:-0}"
CONFORMANCE_FOCUS="${CONFORMANCE_FOCUS:-\\[Conformance\\]}"
CONFORMANCE_SKIP="${CONFORMANCE_SKIP:-\\[Serial\\]|\\[Slow\\]|\\[Disruptive\\]|\\[Flaky\\]}"
E2E_FOCUS="${E2E_FOCUS:-\\[sig-node\\]}"
E2E_SKIP="${E2E_SKIP:-\\[Serial\\]|\\[Slow\\]|\\[Disruptive\\]|\\[Flaky\\]}"
GINKGO_NODES="${GINKGO_NODES:-4}"

log()  { printf '[go-e2e] %s\n' "$*"; }
die()  { printf '[go-e2e] ERROR: %s\n' "$*" >&2; exit 1; }
step() { printf '\n[go-e2e] == %s ==\n' "$*"; }
require_cmd() { command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"; }

require_cmd "$CONTAINER_RUNTIME"
require_cmd curl
require_cmd tar

# ── Find running e2e container ────────────────────────────────────────────────

step "Locating e2e container"
CONTAINER_ID=$("$CONTAINER_RUNTIME" ps --filter ancestor=ghcr.io/bencoxforddev/kubeair/build --format '{{.ID}}' 2>/dev/null | head -1)
if [[ -z "$CONTAINER_ID" ]]; then
  die "No running kubeair build container found. Start one with 'just e2e' first."
fi
log "Container: $CONTAINER_ID"

container_exec() { "$CONTAINER_RUNTIME" exec "$CONTAINER_ID" "$@"; }

# ── Verify cluster is up inside the container ─────────────────────────────────

step "Verifying cluster inside container"
container_exec kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml cluster-info >/dev/null \
  || die "Cluster not reachable inside container. Ensure setup-node-k3s.sh completed."
log "Cluster reachable"

# ── Copy runner script into container ─────────────────────────────────────────

step "Copying run-upstream-go-e2e.sh into container"
VM_RUNNER="/tmp/run-upstream-go-e2e.sh"
"$CONTAINER_RUNTIME" cp "$RUNNER_SCRIPT" "${CONTAINER_ID}:${VM_RUNNER}"
container_exec chmod +x "$VM_RUNNER"
log "Runner copied to ${VM_RUNNER}"

# ── Run test suite inside container ───────────────────────────────────────────

VMARTIFACT_DIR="/tmp/k8s-upstream-go-e2e-artifacts"

step "Running upstream Kubernetes Go e2e inside container"
container_exec env \
  KUBECONFIG=/etc/rancher/k3s/k3s.yaml \
  ARTIFACT_DIR="$VMARTIFACT_DIR" \
  UPSTREAM_K8S_VERSION="$UPSTREAM_K8S_VERSION" \
  RUN_CONFORMANCE="$RUN_CONFORMANCE" \
  RUN_E2E="$RUN_E2E" \
  CONFORMANCE_FOCUS="$CONFORMANCE_FOCUS" \
  CONFORMANCE_SKIP="$CONFORMANCE_SKIP" \
  E2E_FOCUS="$E2E_FOCUS" \
  E2E_SKIP="$E2E_SKIP" \
  GINKGO_NODES="$GINKGO_NODES" \
  bash "$VM_RUNNER"

# ── Pull artifacts back to host ───────────────────────────────────────────────

step "Collecting artifacts from container"
mkdir -p "$LOCAL_ARTIFACT_DIR"
"$CONTAINER_RUNTIME" exec "$CONTAINER_ID" \
  tar czf - -C /tmp k8s-upstream-go-e2e-artifacts 2>/dev/null \
  | tar xzf - -C "$LOCAL_ARTIFACT_DIR" || true
log "Artifacts saved to: ${LOCAL_ARTIFACT_DIR}"

log "go-e2e.sh complete"
