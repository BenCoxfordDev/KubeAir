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
# run-upstream-go-e2e.sh — Run upstream Kubernetes Go e2e/conformance tests
# against an already-provisioned cluster.
#
# Environment variables:
#   KUBECONFIG            Path to kubeconfig (required).
#   ARTIFACT_DIR          Log/artifact output path. Default /tmp/k8s-upstream-e2e-artifacts.
#   UPSTREAM_K8S_VERSION  Kubernetes release to fetch test binaries for.
#                         Accepts v1.33.0, 1.33.0, stable, stable-1.33.
#                         Default: detect from cluster server version.
#   RUN_CONFORMANCE       "1" to run conformance suite. Default 1.
#   RUN_E2E               "1" to run non-conformance e2e suite. Default 0.
#   CONFORMANCE_FOCUS     Focus regex for conformance run. Default \[Conformance\].
#   CONFORMANCE_SKIP      Skip regex for conformance run.
#                         Default \[Serial\]|\[Slow\]|\[Disruptive\]|\[Flaky\].
#   E2E_FOCUS             Focus regex for e2e run. Default \[sig-node\].
#   E2E_SKIP              Skip regex for e2e run.
#                         Default \[Serial\]|\[Slow\]|\[Disruptive\]|\[Flaky\].
#   GINKGO_NODES          Parallel ginkgo nodes. Default 4.
set -euo pipefail

KUBECONFIG="${KUBECONFIG:-}"
ARTIFACT_DIR="${ARTIFACT_DIR:-/tmp/k8s-upstream-e2e-artifacts}"
UPSTREAM_K8S_VERSION="${UPSTREAM_K8S_VERSION:-}"
RUN_CONFORMANCE="${RUN_CONFORMANCE:-1}"
RUN_E2E="${RUN_E2E:-0}"
CONFORMANCE_FOCUS="${CONFORMANCE_FOCUS:-\\[Conformance\\]}"
CONFORMANCE_SKIP="${CONFORMANCE_SKIP:-\\[Serial\\]|\\[Slow\\]|\\[Disruptive\\]|\\[Flaky\\]}"
E2E_FOCUS="${E2E_FOCUS:-\\[sig-node\\]}"
E2E_SKIP="${E2E_SKIP:-\\[Serial\\]|\\[Slow\\]|\\[Disruptive\\]|\\[Flaky\\]}"
GINKGO_NODES="${GINKGO_NODES:-4}"

log()  { printf '[k8s-upstream] %s\n' "$*"; }
die()  { printf '[k8s-upstream] ERROR: %s\n' "$*" >&2; exit 1; }
step() { printf '\n[k8s-upstream] == %s ==\n' "$*"; }
require_cmd() { command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"; }

require_cmd kubectl
require_cmd curl
require_cmd tar

[[ -n "$KUBECONFIG" ]] || die "KUBECONFIG is required"
[[ -f "$KUBECONFIG" ]] || die "kubeconfig not found at $KUBECONFIG"

mkdir -p "$ARTIFACT_DIR"

run_suite() {
  local label=$1; shift
  local log_file="$ARTIFACT_DIR/${label}.log"
  log "Running: $label"
  if "$@" 2>&1 | tee "$log_file"; then
    log "PASS: $label"
    return 0
  fi
  log "FAIL: $label (see $log_file)"
  return 1
}

normalize_version() {
  local raw="$1"
  if [[ "$raw" == stable* ]]; then
    printf '%s' "$raw"
    return 0
  fi
  if [[ "$raw" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf '%s' "$raw"
    return 0
  fi
  if [[ "$raw" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf 'v%s' "$raw"
    return 0
  fi
  return 1
}

resolve_release_version() {
  local requested="$1"
  if [[ -n "$requested" ]]; then
    local norm
    norm="$(normalize_version "$requested")" || die "Invalid UPSTREAM_K8S_VERSION: $requested"
    if [[ "$norm" == stable* ]]; then
      curl -fsSL "https://dl.k8s.io/release/${norm}.txt"
    else
      printf '%s\n' "$norm"
    fi
    return 0
  fi

  local detected
  # Try JSON output first (kubectl ≥1.28 --output=json), fall back to
  # --short (deprecated but present in older releases), then plain text parsing.
  detected="$(kubectl --kubeconfig "$KUBECONFIG" version --output=json 2>/dev/null \
    | grep -o '"gitVersion"[[:space:]]*:[[:space:]]*"[^"]*"' \
    | awk -F'"' 'NR==1{print $4}' \
    || true)"
  if [[ -z "$detected" ]]; then
    # Older kubectl: "Server Version: v1.xx.y"
    detected="$(kubectl --kubeconfig "$KUBECONFIG" version --short 2>/dev/null \
      | awk '/Server Version:/{print $NF}' \
      || true)"
  fi
  [[ -n "$detected" ]] || die "Could not detect cluster server version; set UPSTREAM_K8S_VERSION"
  normalize_version "$detected" || die "Cluster version is not a supported semver: $detected"
}

step "Checking cluster connectivity"
kubectl --kubeconfig "$KUBECONFIG" cluster-info >/dev/null
kubectl --kubeconfig "$KUBECONFIG" get nodes -o wide | tee "$ARTIFACT_DIR/nodes.txt"

step "Resolving upstream Kubernetes test binaries"
K8S_RELEASE="$(resolve_release_version "$UPSTREAM_K8S_VERSION")"
log "Using upstream release: $K8S_RELEASE"

WORK_DIR="/tmp/k8s-upstream-e2e-${K8S_RELEASE}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"

# Detect host OS and arch for the correct test binary archive.
_os="$(uname -s | tr '[:upper:]' '[:lower:]')"
_arch="$(uname -m)"
case "$_arch" in
  x86_64)  _arch="amd64" ;;
  aarch64|arm64) _arch="arm64" ;;
  *) die "Unsupported host architecture: $_arch" ;;
esac
TARBALL_NAME="kubernetes-test-${_os}-${_arch}.tar.gz"
TARBALL="$WORK_DIR/${TARBALL_NAME}"

TEST_URL="https://dl.k8s.io/release/${K8S_RELEASE}/${TARBALL_NAME}"
log "Downloading: $TEST_URL"
curl -fL --retry 5 --retry-delay 2 "$TEST_URL" -o "$TARBALL"

tar -xzf "$TARBALL" -C "$WORK_DIR"
E2E_BIN="$WORK_DIR/kubernetes/test/bin/e2e.test"
GINKGO_BIN="$WORK_DIR/kubernetes/test/bin/ginkgo"

[[ -x "$E2E_BIN" ]] || die "e2e.test not found in extracted archive"
[[ -x "$GINKGO_BIN" ]] || die "ginkgo not found in extracted archive"

OVERALL_PASS=0

if [[ "$RUN_CONFORMANCE" == "1" ]]; then
  step "Running upstream Kubernetes conformance"
  REPORT_DIR="$ARTIFACT_DIR/conformance"
  mkdir -p "$REPORT_DIR"

  if ! run_suite "upstream_conformance" \
    "$GINKGO_BIN" \
    "--nodes=${GINKGO_NODES}" \
    "$E2E_BIN" \
    -- \
    "--provider=skeleton" \
    "--kubeconfig=${KUBECONFIG}" \
    "--report-dir=${REPORT_DIR}" \
    "--disable-log-dump=true" \
    "--ginkgo.focus=${CONFORMANCE_FOCUS}" \
    "--ginkgo.skip=${CONFORMANCE_SKIP}"; then
    OVERALL_PASS=1
  fi
fi

if [[ "$RUN_E2E" == "1" ]]; then
  step "Running upstream Kubernetes e2e"
  REPORT_DIR="$ARTIFACT_DIR/e2e"
  mkdir -p "$REPORT_DIR"

  if ! run_suite "upstream_e2e" \
    "$GINKGO_BIN" \
    "--nodes=${GINKGO_NODES}" \
    "$E2E_BIN" \
    -- \
    "--provider=skeleton" \
    "--kubeconfig=${KUBECONFIG}" \
    "--report-dir=${REPORT_DIR}" \
    "--disable-log-dump=true" \
    "--ginkgo.focus=${E2E_FOCUS}" \
    "--ginkgo.skip=${E2E_SKIP}"; then
    OVERALL_PASS=1
  fi
fi

if [[ "$RUN_CONFORMANCE" != "1" && "$RUN_E2E" != "1" ]]; then
  die "Nothing to run: set RUN_CONFORMANCE=1 and/or RUN_E2E=1"
fi

if [[ "$OVERALL_PASS" -ne 0 ]]; then
  die "One or more upstream Kubernetes suites failed"
fi

log "All requested upstream Kubernetes Go suites passed"
