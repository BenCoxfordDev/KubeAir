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
# colima-run.sh — macOS Colima wrapper for the v2 kubeadm e2e suite.
#
# Mirrors the GitHub Actions workflow (kubeadm-e2e.yml) so local macOS
# developers can run the identical setup — just different base image
# delivery (Colima VM vs. GitHub Ubuntu runner).
#
# What this script does:
#   1.  Ensures a Colima VM is running with the containerd runtime.
#   2.  Cross-compiles the kube-air kubelet for the VM's architecture.
#   3.  Copies the repo and the compiled binary into the VM.
#   4.  Runs setup-node.sh inside the VM to provision the cluster.
#   5.  Runs run-cluster-tests.sh inside the VM against the live cluster.
#
# Environment overrides:
#   COLIMA_PROFILE              Colima profile name. Default: kubeair-e2e
#   COLIMA_CPU                  vCPUs. Default: 4
#   COLIMA_MEMORY               GiB RAM. Default: 8
#   COLIMA_DISK                 GiB disk. Default: 40
#   COLIMA_ARCH                 x86_64 | aarch64. Default: auto-detect.
#   COLIMA_RECREATE             "1" to delete and recreate profile. Default: 0
#   KUBERNETES_VERSION          kubeadm version. Default: 1.33.0
#   CALICO_VERSION              Calico / Tigera version. Default: v3.29.1
#   CALICO_EBPF                 "1" (default) to use eBPF dataplane.
#   CARGO_BUILD_JOBS            Parallel cargo jobs inside VM. Default: 2
#   RUN_UNIT_TESTS              "1" (default) to run unit/conformance/smoke.
#   RUN_E2E_TESTS               "1" (default) to run live cluster e2e.
#   RESET_EXISTING_CLUSTER      "1" to kubeadm reset before init. Default: 0
#   VM_REPO_PATH                Where to place the repo in the VM.
#                               Default: /opt/kubeair
#   SKIP_SYNC_REPO              "1" to skip tar-pipe sync (use existing VM copy). Default: 0
#   SKIP_BUILD_BINARY           "1" to skip cargo build inside VM. Default: 0
set -euo pipefail

COLIMA_PROFILE="${COLIMA_PROFILE:-kubeair-e2e}"
COLIMA_CPU="${COLIMA_CPU:-4}"
COLIMA_MEMORY="${COLIMA_MEMORY:-8}"
COLIMA_DISK="${COLIMA_DISK:-40}"
COLIMA_RECREATE="${COLIMA_RECREATE:-0}"
KUBERNETES_VERSION="${KUBERNETES_VERSION:-1.33.0}"
CALICO_VERSION="${CALICO_VERSION:-v3.29.1}"
CALICO_EBPF="${CALICO_EBPF:-1}"
CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
RUN_UNIT_TESTS="${RUN_UNIT_TESTS:-1}"
RUN_E2E_TESTS="${RUN_E2E_TESTS:-1}"
RESET_EXISTING_CLUSTER="${RESET_EXISTING_CLUSTER:-0}"
VM_REPO_PATH="${VM_REPO_PATH:-/opt/kubeair}"
SKIP_SYNC_REPO="${SKIP_SYNC_REPO:-0}"
SKIP_BUILD_BINARY="${SKIP_BUILD_BINARY:-0}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

log()  { printf '[colima-e2e] %s\n' "$*"; }
die()  { printf '[colima-e2e] ERROR: %s\n' "$*" >&2; exit 1; }
step() { printf '\n[colima-e2e] ══ %s ══\n' "$*"; }

require_cmd() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }

require_cmd colima

# ── Step 1: Ensure Colima VM is running ───────────────────────────────────────

# COLIMA_ARCH defaults to host arch; Colima also defaults to this, so omitting
# the flag is equivalent. Expose it for cases where the user wants x86_64 emulation.
HOST_ARCH=$(uname -m)
case "$HOST_ARCH" in
  arm64|aarch64) COLIMA_ARCH="${COLIMA_ARCH:-aarch64}" ;;
  x86_64)        COLIMA_ARCH="${COLIMA_ARCH:-x86_64}"  ;;
  *)             COLIMA_ARCH="${COLIMA_ARCH:-}" ;;
esac

step "Ensuring Colima VM is running (profile: $COLIMA_PROFILE, arch: $COLIMA_ARCH)"

colima_is_running() {
  colima ssh --profile "$COLIMA_PROFILE" -- true >/dev/null 2>&1
}

colima_runtime() {
  colima status --profile "$COLIMA_PROFILE" 2>&1 \
    | awk -F': ' '/runtime:/ {print $2; exit}' \
    | tr -d '[:space:]",'
}

if colima_is_running; then
  RUNTIME=$(colima_runtime || echo "unknown")
  log "Colima profile '$COLIMA_PROFILE' is running (runtime: $RUNTIME)"

  if [[ "$RUNTIME" != "containerd" && "$COLIMA_RECREATE" != "1" ]]; then
    die "Profile runtime is '$RUNTIME', not 'containerd'. Set COLIMA_RECREATE=1 to recreate."
  fi

  if [[ "$COLIMA_RECREATE" == "1" ]]; then
    log "Deleting and recreating profile as requested"
    colima delete --profile "$COLIMA_PROFILE" --force 2>/dev/null || true
    colima start \
      --profile   "$COLIMA_PROFILE" \
      --runtime   containerd \
      --cpu        "$COLIMA_CPU" \
      --memory     "$COLIMA_MEMORY" \
      --disk       "$COLIMA_DISK" \
      --arch       "$COLIMA_ARCH"
  fi
else
  log "Starting Colima profile '$COLIMA_PROFILE' with containerd"
  colima start \
    --profile   "$COLIMA_PROFILE" \
    --runtime   containerd \
    --cpu        "$COLIMA_CPU" \
    --memory     "$COLIMA_MEMORY" \
    --disk       "$COLIMA_DISK" \
    --arch       "$COLIMA_ARCH"
fi

# Verify SSH connectivity
colima ssh --profile "$COLIMA_PROFILE" -- true \
  || die "Cannot SSH into Colima profile '$COLIMA_PROFILE'"
log "Colima VM SSH: OK"

# ── Step 3: Sync repo into VM ─────────────────────────────────────────────────
# Build happens natively inside the VM — no cross-compilation needed.
# The VM is already the correct architecture (aarch64 or x86_64 Linux).

step "Syncing repo into Colima VM at $VM_REPO_PATH"

vm_exec() {
  colima ssh --profile "$COLIMA_PROFILE" -- "$@"
}

vm_exec sudo mkdir -p "$VM_REPO_PATH"
vm_exec sudo chown "$(vm_exec id -un):$(vm_exec id -gn)" "$VM_REPO_PATH"

if [[ "$SKIP_SYNC_REPO" != "1" ]]; then
  # Sync repo into VM via tar pipe — avoids rsync SSH transport complexity with Colima.
  # Excludes .git and target/ to keep the transfer fast.
  log "Syncing repo via tar pipe to $VM_REPO_PATH ..."
  COPYFILE_DISABLE=1 tar czf - \
    -C "$REPO_ROOT" \
    --exclude='.git' \
    --exclude='target' \
    --exclude='.cache' \
    --exclude='.Trash' \
    --exclude='*.log' \
    . \
  | colima ssh --profile "$COLIMA_PROFILE" -- \
      bash -c "sudo mkdir -p '$VM_REPO_PATH' && sudo tar xzf - --warning=no-unknown-keyword -C '$VM_REPO_PATH'"
  log "Repo synced"
else
  log "Skipping repo sync (SKIP_SYNC_REPO=1)"
fi

# ── Step 4: Install Rust in VM and build kubelet natively ─────────────────────
# Building natively inside the VM avoids all cross-compilation toolchain issues
# (particularly aws-lc-sys / ring which require a glibc-capable C cross-compiler).

step "Building kubelet natively inside VM"

if [[ "$SKIP_BUILD_BINARY" == "1" ]]; then
  log "Skipping binary build (SKIP_BUILD_BINARY=1)"
else
  colima ssh --profile "$COLIMA_PROFILE" -- bash -s -- \
      "$VM_REPO_PATH" "$CARGO_BUILD_JOBS" <<'BUILD'
set -euo pipefail
VM_REPO_PATH="$1"
CARGO_BUILD_JOBS="$2"

# Install build deps
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq \
  build-essential \
  pkg-config \
  libssl-dev \
  protobuf-compiler \
  curl \
  git

# Install rustup / cargo if not present (Colima images don't ship Rust)
if ! command -v cargo &>/dev/null; then
  echo "Installing Rust via rustup..."
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --no-modify-path --profile minimal 2>&1
fi

# Source cargo env (rustup install does not modify current shell PATH)
# shellcheck disable=SC1090
source "$HOME/.cargo/env" 2>/dev/null \
  || export PATH="$HOME/.cargo/bin:$PATH"

rustc --version
cargo --version

cd "$VM_REPO_PATH"
cargo build --release --bin kubelet --locked --jobs "$CARGO_BUILD_JOBS"
BUILD
fi

VM_KUBELET_BIN="$VM_REPO_PATH/target/release/kubelet"
log "Kubelet built at $VM_KUBELET_BIN (inside VM)"

# ── Step 6: Run setup-node.sh inside VM ───────────────────────────────────────

step "Running setup-node.sh inside VM"

# 6a: Patch containerd cgroup driver first (Colima manages containerd, not systemd).
log "Patching containerd cgroup driver for kubeadm compatibility..."
colima ssh --profile "$COLIMA_PROFILE" -- bash -s <<'PATCH'
set -euo pipefail
sudo mkdir -p /etc/containerd
if [[ ! -f /etc/containerd/config.toml ]]; then
  containerd config default | sudo tee /etc/containerd/config.toml >/dev/null
fi
sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/g' /etc/containerd/config.toml
sudo sed -i 's|sandbox_image = ".*"|sandbox_image = "registry.k8s.io/pause:3.10"|g' \
  /etc/containerd/config.toml
# Colima does not use systemd for containerd — send SIGHUP to reload config
sudo pkill -HUP containerd 2>/dev/null || true
sleep 2
PATCH

# 6b: Run setup-node.sh (SKIP_CONTAINERD_CONFIG=1 since Colima owns containerd)
vm_exec env \
  KUBELET_BIN="$VM_KUBELET_BIN" \
  KUBERNETES_VERSION="$KUBERNETES_VERSION" \
  CALICO_VERSION="$CALICO_VERSION" \
  CALICO_EBPF="$CALICO_EBPF" \
  SKIP_CONTAINERD_CONFIG="1" \
  RESET_EXISTING_CLUSTER="$RESET_EXISTING_CLUSTER" \
  KUBEAIR_REPO_PATH="$VM_REPO_PATH" \
  bash "$VM_REPO_PATH/hack/e2e/setup-node.sh"

# ── Step 7: Run tests inside VM ───────────────────────────────────────────────

step "Running cluster tests inside VM"

VM_KUBECONFIG="$(vm_exec bash -c 'echo $HOME')/.kube/config"

vm_exec env \
  KUBEAIR_REPO_PATH="$VM_REPO_PATH" \
  KUBECONFIG="$VM_KUBECONFIG" \
  CARGO_BUILD_JOBS="$CARGO_BUILD_JOBS" \
  RUN_UNIT_TESTS="$RUN_UNIT_TESTS" \
  RUN_E2E_TESTS="$RUN_E2E_TESTS" \
  ARTIFACT_DIR="/tmp/kubeair-test-artifacts" \
  bash "$VM_REPO_PATH/hack/e2e/run-cluster-tests.sh"

# ── Step 8: Collect artifacts ─────────────────────────────────────────────────

step "Collecting artifacts from VM"

LOCAL_ARTIFACTS="/tmp/kubeair-colima-artifacts-$(date +%Y%m%dT%H%M%S)"
mkdir -p "$LOCAL_ARTIFACTS"

# Pull artifact directory from VM
colima ssh --profile "$COLIMA_PROFILE" -- \
  tar czf - -C /tmp kubeair-test-artifacts 2>/dev/null \
  | tar xzf - -C "$LOCAL_ARTIFACTS" || true

log "Artifacts saved to: $LOCAL_ARTIFACTS"
ls -lh "$LOCAL_ARTIFACTS" 2>/dev/null || true

log "colima-run.sh complete"
