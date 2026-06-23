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
# run-cluster-tests.sh — Run kube-air Rust conformance, smoke, and live e2e
#                         tests against an already-provisioned cluster.
#
# Runs ON the Linux node (CI container or GitHub runner) after setup-node.sh.
#
# Environment variables:
#   KUBEAIR_REPO_PATH   Path to the kube-air source tree. Default /opt/kubeair.
#   KUBECONFIG          Path to admin kubeconfig. Default /etc/kubernetes/admin.conf.
#   CARGO_BUILD_JOBS    Parallel Cargo jobs. Default 2.
#   RUN_UNIT_TESTS      Run lib/conformance/smoke unit tests. Default "1".
#   RUN_E2E_TESTS       Run live cluster e2e tests. Default "1".
#   TEST_FILTER         Optional cargo test filter (name substring) for e2e tests.
#   ARTIFACT_DIR        Directory to write test output/logs. Default /tmp/kubeair-test-artifacts.
set -euo pipefail

# Ensure cargo/rustc are on PATH when run as a non-login shell.
# shellcheck source=/dev/null
[[ -f "${HOME}/.cargo/env" ]] && source "${HOME}/.cargo/env"

KUBEAIR_REPO_PATH="${KUBEAIR_REPO_PATH:-/opt/kubeair}"
# Prefer the user-readable kubeconfig; fall back to k3s then kubeadm paths.
KUBECONFIG="${KUBECONFIG:-${HOME}/.kube/config}"
[[ -f "$KUBECONFIG" ]] || KUBECONFIG="/etc/rancher/k3s/k3s.yaml"
[[ -f "$KUBECONFIG" ]] || KUBECONFIG="/etc/kubernetes/admin.conf"
CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
RUN_UNIT_TESTS="${RUN_UNIT_TESTS:-1}"
RUN_E2E_TESTS="${RUN_E2E_TESTS:-1}"
TEST_FILTER="${TEST_FILTER:-}"
ARTIFACT_DIR="${ARTIFACT_DIR:-/tmp/kubeair-test-artifacts}"

log()  { printf '[run-tests] %s\n' "$*"; }
die()  { printf '[run-tests] ERROR: %s\n' "$*" >&2; exit 1; }
step() { printf '\n[run-tests] ══ %s ══\n' "$*"; }

[[ -d "$KUBEAIR_REPO_PATH" ]] || die "kube-air repo not found at $KUBEAIR_REPO_PATH"
[[ -f "$KUBECONFIG" ]] || die "kubeconfig not found at $KUBECONFIG"

mkdir -p "$ARTIFACT_DIR"
cd "$KUBEAIR_REPO_PATH"

export KUBECONFIG

OVERALL_PASS=0  # 0 = pass, non-zero = fail
FAILED_SUITES=()  # names of suites that failed

run_suite() {
  local label=$1; shift
  local log_file="$ARTIFACT_DIR/${label}.log"
  log "Running: $label"
  if "$@" 2>&1 | tee "$log_file"; then
    log "PASS: $label"
  else
    log "FAIL: $label (see $log_file)"
    OVERALL_PASS=1
    FAILED_SUITES+=("$label")
  fi
}

# ── Unit / conformance / smoke tests (no live cluster needed) ─────────────────

if [[ "$RUN_UNIT_TESTS" == "1" ]]; then
  step "Unit, conformance, and smoke tests"

  run_suite "crate-unit-tests" \
    bazel test //crates/kubelet-core/src:core_test \
                //crates/kubelet-adapters/src:adapters_test \
                //crates/kubelet-app/src:app_test \
                //crates/kubelet-cri/src:cri_test \
                //crates/kubelet-ports/src:ports_test \
                --build_tests_only

  run_suite "conformance" \
    bazel test //tests/conformance:conformance_test

  run_suite "smoke" \
    bazel test //tests/smoke:smoke_test

  run_suite "integration" \
    bazel test //tests/integration:all
fi

# ── Live cluster e2e tests ────────────────────────────────────────────────────

if [[ "$RUN_E2E_TESTS" == "1" ]]; then
  step "Live cluster e2e tests"

  # Verify cluster is reachable before running tests
  log "Verifying cluster connectivity..."
  kubectl --kubeconfig "$KUBECONFIG" cluster-info \
    || die "Cannot reach cluster. Is the cluster running?"

  FILTER_ARG=""
  [[ -n "$TEST_FILTER" ]] && FILTER_ARG="--test_filter=$TEST_FILTER"

  run_suite "e2e_cluster_health" \
    bazel test //tests/e2e:cluster_health_test $FILTER_ARG

  run_suite "e2e_containerd_status" \
    bazel test //tests/e2e:containerd_api_status_test $FILTER_ARG

  run_suite "e2e_kubectl_ops" \
    bazel test //tests/e2e:kubectl_ops_test $FILTER_ARG

  run_suite "e2e_workload_features" \
    bazel test //tests/e2e:workload_features_test $FILTER_ARG

  run_suite "e2e_container_cleanup" \
    bazel test //tests/e2e:container_cleanup_test $FILTER_ARG
fi

# ── Summary ───────────────────────────────────────────────────────────────────

step "Test summary"
log "Artifacts written to: $ARTIFACT_DIR"
ls -lh "$ARTIFACT_DIR" || true

if [[ "$OVERALL_PASS" -ne 0 ]]; then
  log "Failed suites (${#FAILED_SUITES[@]}):"
  for suite in "${FAILED_SUITES[@]}"; do
    log "  FAIL  $suite"
    # Print the last 20 lines of the log for quick triage
    log "  --- last 20 lines of ${suite}.log ---"
    tail -20 "$ARTIFACT_DIR/${suite}.log" | sed 's/^/    /' >&2
  done
  die "${#FAILED_SUITES[@]} suite(s) FAILED: ${FAILED_SUITES[*]}"
fi

log "All test suites PASSED"
exit 0
