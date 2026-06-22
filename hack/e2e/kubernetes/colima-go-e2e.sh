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
# colima-go-e2e.sh — Run upstream Kubernetes Go e2e/conformance tests against
# the kubeadm cluster provisioned by hack/e2e/colima-run.sh.
#
# The cluster runs entirely inside the Colima VM (kubeadm, endpoint
# 127.0.0.1:6443 inside the VM). This script copies run-upstream-go-e2e.sh
# into the VM and executes it there so test binaries are linux and the API
# server endpoint is reachable.
#
# Prerequisites (macOS host):
#   colima     (brew install colima)
#   curl, tar
#
# The Colima VM must already be provisioned by colima-run.sh (or equivalent).
#
# Usage:
#   bash hack/e2e/kubernetes/colima-go-e2e.sh
#
# Environment variables:
#   COLIMA_PROFILE        Colima profile name. Default: kubeair-e2e
#                         (matches colima-run.sh default).
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
#                         Default /tmp/k8s-upstream-go-e2e-colima-artifacts.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_SCRIPT="${SCRIPT_DIR}/run-upstream-go-e2e.sh"

COLIMA_PROFILE="${COLIMA_PROFILE:-kubeair-e2e}"
LOCAL_ARTIFACT_DIR="${LOCAL_ARTIFACT_DIR:-/tmp/k8s-upstream-go-e2e-colima-artifacts}"

UPSTREAM_K8S_VERSION="${UPSTREAM_K8S_VERSION:-}"
RUN_CONFORMANCE="${RUN_CONFORMANCE:-1}"
RUN_E2E="${RUN_E2E:-0}"
CONFORMANCE_FOCUS="${CONFORMANCE_FOCUS:-\\[Conformance\\]}"
CONFORMANCE_SKIP="${CONFORMANCE_SKIP:-\\[Serial\\]|\\[Slow\\]|\\[Disruptive\\]|\\[Flaky\\]}"
E2E_FOCUS="${E2E_FOCUS:-\\[sig-node\\]}"
E2E_SKIP="${E2E_SKIP:-\\[Serial\\]|\\[Slow\\]|\\[Disruptive\\]|\\[Flaky\\]}"
GINKGO_NODES="${GINKGO_NODES:-4}"

log()  { printf '[colima-go-e2e] %s\n' "$*"; }
die()  { printf '[colima-go-e2e] ERROR: %s\n' "$*" >&2; exit 1; }
step() { printf '\n[colima-go-e2e] == %s ==\n' "$*"; }
require_cmd() { command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1 (brew install $1)"; }

require_cmd colima

COLIMA_RUN_SCRIPT="${SCRIPT_DIR}/../colima-run.sh"

vm_exec() { colima ssh --profile "$COLIMA_PROFILE" -- "$@"; }

# ── Ensure the VM is running (provision if needed) ────────────────────────────

step "Checking Colima VM (profile: ${COLIMA_PROFILE})"
if ! colima ssh --profile "$COLIMA_PROFILE" -- true >/dev/null 2>&1; then
  log "VM not running — launching hack/e2e/colima-run.sh to provision it"
  [[ -f "$COLIMA_RUN_SCRIPT" ]] || die "colima-run.sh not found at ${COLIMA_RUN_SCRIPT}"
  COLIMA_PROFILE="$COLIMA_PROFILE" bash "$COLIMA_RUN_SCRIPT"
  # Verify it came up
  colima ssh --profile "$COLIMA_PROFILE" -- true \
    || die "Colima VM still not reachable after colima-run.sh"
fi
log "VM reachable via SSH"

# ── Verify cluster is up inside the VM ───────────────────────────────────────

step "Verifying cluster inside VM"
VM_KUBECONFIG="$(vm_exec bash -c 'echo $HOME')/.kube/config"
vm_exec kubectl --kubeconfig "$VM_KUBECONFIG" cluster-info >/dev/null \
  || die "Cluster not reachable inside VM. Ensure setup-node.sh completed successfully."
log "Cluster reachable"

# ── Copy runner script into VM ────────────────────────────────────────────────

step "Copying run-upstream-go-e2e.sh into VM"
VM_RUNNER="/tmp/run-upstream-go-e2e.sh"
colima ssh --profile "$COLIMA_PROFILE" -- bash -c \
  "cat > ${VM_RUNNER} && chmod +x ${VM_RUNNER}" \
  < "$RUNNER_SCRIPT"
log "Runner copied to ${VM_RUNNER}"

# ── Run test suite inside VM ──────────────────────────────────────────────────

VM_ARTIFACT_DIR="/tmp/k8s-upstream-go-e2e-artifacts"

step "Running upstream Kubernetes Go e2e inside VM"
vm_exec env \
  KUBECONFIG="$VM_KUBECONFIG" \
  ARTIFACT_DIR="$VM_ARTIFACT_DIR" \
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

step "Collecting artifacts from VM"
mkdir -p "$LOCAL_ARTIFACT_DIR"
colima ssh --profile "$COLIMA_PROFILE" -- \
  tar czf - -C /tmp k8s-upstream-go-e2e-artifacts 2>/dev/null \
  | tar xzf - -C "$LOCAL_ARTIFACT_DIR" || true
log "Artifacts saved to: ${LOCAL_ARTIFACT_DIR}"

log "colima-go-e2e.sh complete"
