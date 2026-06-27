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
# run-in-container.sh — Entry point executed inside the CI build container.
#
# Builds the kube-air kubelet with Bazel, provisions an upstream Kubernetes
# cluster using setup-node.sh, then runs run-cluster-tests.sh against it.
#
# This script is used by both:
#   - hack/e2e/run-e2e.sh  (local macOS dev via podman/docker)
#   - .github/workflows/k8s-rust-e2e.yml  (GitHub Actions container job)
#
# Environment:
#   KUBEAIR_REPO_PATH   Path to the kube-air source tree. Default: /workspace
#   KUBERNETES_VERSION  Kubernetes version. Default: read from .version at repo root
#   SKIP_BUILD          "1" to skip bazel build (reuse existing binary). Default: 0
#   RUN_UNIT_TESTS      "1" to run unit/conformance/smoke tests. Default: 1
#   RUN_E2E_TESTS       "1" to run live cluster e2e tests. Default: 1
#   ARTIFACT_DIR        Directory to write test output. Default: /tmp/kubeair-artifacts
set -euo pipefail

KUBEAIR_REPO_PATH="${KUBEAIR_REPO_PATH:-/workspace}"
SKIP_BUILD="${SKIP_BUILD:-0}"
RUN_UNIT_TESTS="${RUN_UNIT_TESTS:-1}"
RUN_E2E_TESTS="${RUN_E2E_TESTS:-1}"
ARTIFACT_DIR="${ARTIFACT_DIR:-/tmp/kubeair-artifacts}"

log()  { printf '[container-entry] %s\n' "$*"; }
die()  { printf '[container-entry] ERROR: %s\n' "$*" >&2; exit 1; }
step() { printf '\n[container-entry] ══ %s ══\n' "$*"; }

[[ -d "$KUBEAIR_REPO_PATH" ]] || die "Repo not found at $KUBEAIR_REPO_PATH"

cd "$KUBEAIR_REPO_PATH"

# ── Ensure bazel resolves to bazelisk ─────────────────────────────────────────

if ! command -v bazel >/dev/null 2>&1 && command -v bazelisk >/dev/null 2>&1; then
  ln -sf "$(command -v bazelisk)" /usr/local/bin/bazel
  log "Linked bazel -> bazelisk"
fi

# ── Fix missing unversioned gcc symlinks ──────────────────────────────────────
# rules_distroless extracts .deb contents without running postinst scripts, so
# update-alternatives is never called and /usr/bin/gcc is not created.
# We also handle dangling symlinks left by image layers that point to gcc-14.
GCC=$(find /usr/bin -maxdepth 1 -name 'gcc-[0-9]*' 2>/dev/null | sort | head -1)
if [[ -n "$GCC" ]]; then
  ln -sf "$GCC" /usr/bin/gcc
  ln -sf "$GCC" /usr/bin/cc
  log "Linked /usr/bin/gcc -> $GCC"
else
  die "No versioned gcc binary (gcc-NN) found in /usr/bin — check build image packages"
fi
GPP=$(find /usr/bin -maxdepth 1 -name 'g++-[0-9]*' 2>/dev/null | sort | head -1)
if [[ -n "$GPP" ]]; then
  ln -sf "$GPP" /usr/bin/g++
  ln -sf "$GPP" /usr/bin/c++
  log "Linked /usr/bin/g++ -> $GPP"
fi

# ── Build kubelet ──────────────────────────────────────────────────────────────

if [[ "$SKIP_BUILD" != "1" ]]; then
  step "Building kube-air kubelet"
  BAZEL_EXTRA_FLAGS="--config=container" just build native
else
  log "Skipping build (SKIP_BUILD=1)"
fi

KUBELET_BIN="$KUBEAIR_REPO_PATH/bazel-bin/src/main"
[[ -f "$KUBELET_BIN" ]] || die "kubelet binary not found at $KUBELET_BIN — run without SKIP_BUILD=1 first"

log "kubelet binary: $KUBELET_BIN ($(du -sh "$KUBELET_BIN" | cut -f1))"

# ── Provision cluster ────────────────────────────────────────────────────────

step "Provisioning cluster"

KUBELET_BIN="$KUBELET_BIN" \
bash "$KUBEAIR_REPO_PATH/hack/e2e/setup-node.sh"

# ── Monitor kubelet memory ─────────────────────────────────────────────────────

step "Starting kubelet memory monitor"
bash "$KUBEAIR_REPO_PATH/hack/e2e/memory-monitor.sh" /tmp/kubelet.pid "$ARTIFACT_DIR/kubelet-memory.csv" 2 &
MONITOR_PID=$!

# ── Run tests ─────────────────────────────────────────────────────────────────

step "Running cluster tests"

KUBEAIR_REPO_PATH="$KUBEAIR_REPO_PATH" \
KUBECONFIG=/etc/kubernetes/admin.conf \
RUN_UNIT_TESTS="$RUN_UNIT_TESTS" \
RUN_E2E_TESTS="$RUN_E2E_TESTS" \
ARTIFACT_DIR="$ARTIFACT_DIR" \
bash "$KUBEAIR_REPO_PATH/hack/e2e/run-cluster-tests.sh"

# ── Memory report ─────────────────────────────────────────────────────────────

kill "$MONITOR_PID" 2>/dev/null || true
bash "$KUBEAIR_REPO_PATH/hack/e2e/memory-report.sh" "$ARTIFACT_DIR/kubelet-memory.csv"
