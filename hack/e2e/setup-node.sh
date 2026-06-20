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
# setup-node.sh — Shared single-node Kubernetes cluster provisioner.
#
# Idempotent: safe to re-run (most steps are no-ops if already done).
#
# Runs ON the Linux node (Colima VM or GitHub runner).
# Called by colima-run.sh (inside the VM via SSH) and by the GH Actions
# workflow step (directly on the runner).
#
# Environment variables (all optional unless noted):
#   KUBELET_BIN             Path to the kube-air kubelet binary to deploy.
#                           Defaults to /tmp/kubelet-bin/kubelet.
#   KUBERNETES_VERSION      kubeadm/kubectl/kubelet package version.
#                           Defaults to 1.33.0.
#   CALICO_VERSION          Tigera operator / Calico version.
#                           Defaults to v3.29.1.
#   CALICO_EBPF             Set to "1" (default) to configure Calico eBPF mode.
#   POD_CIDR                Kubernetes pod CIDR. Default 192.168.0.0/16.
#   SERVICE_CIDR            Kubernetes service CIDR. Default 10.96.0.0/12.
#   SKIP_CONTAINERD_CONFIG  Set to "1" to skip containerd reconfiguration
#                           (useful when Colima already manages containerd).
#   NODE_NAME               Override the node hostname registered with kubeadm.
#                           Defaults to $(hostname).
#   KUBEAIR_REPO_PATH       Path to the kube-air source tree inside the VM.
#                           Defaults to /opt/kubeair.
#   RESET_EXISTING_CLUSTER  Set to "1" to kubeadm reset before init.
#                           Default "0".
set -euo pipefail

KUBELET_BIN="${KUBELET_BIN:-/tmp/kubelet-bin/kubelet}"
KUBERNETES_VERSION="${KUBERNETES_VERSION:-1.33.0}"
CALICO_VERSION="${CALICO_VERSION:-v3.29.1}"
CALICO_EBPF="${CALICO_EBPF:-1}"
POD_CIDR="${POD_CIDR:-192.168.0.0/16}"
SERVICE_CIDR="${SERVICE_CIDR:-10.96.0.0/12}"
SKIP_CONTAINERD_CONFIG="${SKIP_CONTAINERD_CONFIG:-0}"
NODE_NAME="${NODE_NAME:-$(hostname)}"
KUBEAIR_REPO_PATH="${KUBEAIR_REPO_PATH:-/opt/kubeair}"
RESET_EXISTING_CLUSTER="${RESET_EXISTING_CLUSTER:-0}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

log()  { printf '[setup-node] %s\n' "$*"; }
die()  { printf '[setup-node] ERROR: %s\n' "$*" >&2; exit 1; }
step() { printf '\n[setup-node] ══ %s ══\n' "$*"; }

# ── helpers ───────────────────────────────────────────────────────────────────

has_systemd() {
  [[ "$(cat /proc/1/comm 2>/dev/null || echo unknown)" == "systemd" ]]
}

restart_service() {
  local svc=$1
  if has_systemd; then
    sudo systemctl daemon-reload
    sudo systemctl restart "$svc"
  else
    # Colima / runit / openrc: kill and re-spawn
    sudo pkill -x "$svc" 2>/dev/null || true
    sleep 1
    sudo "$svc" &>/tmp/"$svc".log &
    disown
    sleep 2
  fi
}

wait_for_socket() {
  local socket=$1 max=${2:-60}
  for ((i=0; i<max; i++)); do
    [[ -S "$socket" ]] && return 0
    sleep 1
  done
  die "Timed out waiting for socket: $socket"
}

wait_for_pods_running() {
  local namespace=$1 label=$2 expected_min=${3:-1} max_attempts=${4:-120}
  local kc="${KUBECONFIG:-/etc/kubernetes/admin.conf}"
  log "Waiting for pods in namespace=$namespace label=$label to be Running (min=$expected_min)..."
  for ((i=0; i<max_attempts; i++)); do
    local count
    count=$(kubectl --kubeconfig "$kc" \
      get pods -n "$namespace" -l "$label" \
      --field-selector=status.phase=Running \
      --no-headers 2>/dev/null | wc -l 2>/dev/null) || count=0
    count="${count//[[:space:]]/}"
    if [[ "$count" -ge "$expected_min" ]]; then
      log "  -> $count/$expected_min pods Running"
      return 0
    fi
    sleep 5
  done
  log "WARNING: pods not all Running after $(( max_attempts * 5 ))s — current state:"
  kubectl --kubeconfig "$kc" \
    get pods -n "$namespace" -l "$label" --no-headers 2>/dev/null || true
}

# ── Step 0: optional reset ────────────────────────────────────────────────────

if [[ "$RESET_EXISTING_CLUSTER" == "1" ]]; then
  step "Resetting existing cluster"
  sudo kubeadm reset -f --cri-socket unix:///run/containerd/containerd.sock 2>/dev/null || true
  sudo rm -rf /etc/kubernetes /var/lib/etcd /var/lib/kubelet/pods
fi

# ── Step 1: system prerequisites ─────────────────────────────────────────────

step "Installing system prerequisites"
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg \
  lsb-release \
  socat \
  conntrack \
  ipset \
  ipvsadm \
  jq \
  ebtables \
  ethtool

# ── Step 2: containerd ────────────────────────────────────────────────────────

if [[ "$SKIP_CONTAINERD_CONFIG" != "1" ]]; then
  step "Configuring containerd"

  # Install containerd if not present
  if ! command -v containerd &>/dev/null; then
    log "Installing containerd from apt..."
    sudo apt-get install -y -qq containerd
  fi

  CONTAINERD_VERSION=$(containerd --version | awk '{print $3}' || echo "unknown")
  log "Containerd version: $CONTAINERD_VERSION"

  # Generate default config and enable SystemdCgroup
  sudo mkdir -p /etc/containerd
  containerd config default | sudo tee /etc/containerd/config.toml >/dev/null

  # Enable systemd cgroup driver — required for Kubernetes
  sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/g' /etc/containerd/config.toml

  # Ensure sandbox image is from registry.k8s.io
  sudo sed -i \
    's|sandbox_image = ".*"|sandbox_image = "registry.k8s.io/pause:3.10"|g' \
    /etc/containerd/config.toml

  restart_service containerd
  wait_for_socket /run/containerd/containerd.sock
  log "containerd socket ready"
else
  step "Skipping containerd configuration (SKIP_CONTAINERD_CONFIG=1)"
  wait_for_socket /run/containerd/containerd.sock 30
fi

# Allow the current (non-root) user to reach the containerd socket so that
# e2e test helpers (ctr / crictl) work without sudo.  This matters on GitHub
# Actions runners where the socket is root-owned.
#
# Strategy: create a 'containerd' group, add the current user to it, then
# grant the socket to that group.  Because usermod changes don't take effect
# in the running shell, we also chmod the socket directly so tools work
# immediately in this session without re-login.
CURRENT_USER="$(id -un)"
if ! getent group containerd >/dev/null 2>&1; then
  sudo groupadd --system containerd
fi
sudo usermod -aG containerd "$CURRENT_USER" || true
# Write a systemd drop-in so the socket gets the right group on every restart.
if has_systemd; then
  sudo mkdir -p /etc/systemd/system/containerd.service.d
  sudo tee /etc/systemd/system/containerd.service.d/socket-group.conf >/dev/null <<'DROPIN'
[Service]
ExecStartPost=/bin/sh -c 'chgrp containerd /run/containerd/containerd.sock && chmod g+rw /run/containerd/containerd.sock'
DROPIN
  sudo systemctl daemon-reload
fi
# Immediately fix the socket for this session (the drop-in only fires on restart).
if [[ -S /run/containerd/containerd.sock ]]; then
  sudo chgrp containerd /run/containerd/containerd.sock
  sudo chmod g+rw    /run/containerd/containerd.sock
  log "containerd socket group set to 'containerd' and made group-writable"
fi

# Install crictl — prefer apt package, fall back to GitHub release with retries
if ! command -v crictl &>/dev/null; then
  step "Installing crictl"
  # Try apt first (Ubuntu 24.04 ships cri-tools)
  if apt-cache show cri-tools &>/dev/null 2>&1; then
    sudo apt-get install -y -qq cri-tools
  else
    CRICTL_VERSION="v1.33.0"
    ARCH=$(uname -m)
    case "$ARCH" in
      x86_64)  CRICTL_ARCH="linux-amd64" ;;
      aarch64) CRICTL_ARCH="linux-arm64" ;;
      *)       die "Unsupported arch: $ARCH" ;;
    esac
    CRICTL_URL="https://github.com/kubernetes-sigs/cri-tools/releases/download/${CRICTL_VERSION}/crictl-${CRICTL_VERSION}-${CRICTL_ARCH}.tar.gz"
    for attempt in 1 2 3; do
      log "Downloading crictl (attempt $attempt)..."
      if curl -sSL --retry 3 --retry-delay 5 --max-time 120 "$CRICTL_URL" \
          | sudo tar -xz -C /usr/local/bin; then
        break
      fi
      [[ $attempt -eq 3 ]] && die "Failed to download crictl after 3 attempts"
      sleep 10
    done
  fi
fi

sudo tee /etc/crictl.yaml >/dev/null <<'EOF'
runtime-endpoint: unix:///run/containerd/containerd.sock
image-endpoint: unix:///run/containerd/containerd.sock
timeout: 10
EOF

# ── Step 3: kubeadm / kubectl / kubelet (stock) ───────────────────────────────

step "Installing kubeadm, kubectl, kubelet"

# Only install if not already at the exact target version
KUBE_PKG_VERSION="${KUBERNETES_VERSION}-1.1"
if ! dpkg-query -W kubeadm 2>/dev/null | grep -qF "${KUBE_PKG_VERSION}"; then
  KUBE_MAJOR_MINOR=$(echo "$KUBERNETES_VERSION" | cut -d. -f1-2)

  # Add Kubernetes apt repo
  KUBE_KEY_URL="https://pkgs.k8s.io/core:/stable:/v${KUBE_MAJOR_MINOR}/deb/Release.key"
  KUBE_KEYRING="/etc/apt/keyrings/kubernetes-apt-keyring.gpg"
  KUBE_SOURCES="/etc/apt/sources.list.d/kubernetes.list"

  sudo mkdir -p /etc/apt/keyrings

  # Download the signing key with retries (pkgs.k8s.io can transiently 403 on CI)
  KEY_DOWNLOADED=0
  for attempt in 1 2 3 4 5; do
    log "Downloading Kubernetes apt key (attempt $attempt/5): $KUBE_KEY_URL"
    HTTP_CODE=$(curl -sSL --retry 3 --retry-delay 5 --retry-connrefused \
      --max-time 60 -w "%{http_code}" -o /tmp/k8s-apt.key "$KUBE_KEY_URL" 2>&1) || true
    if [[ "$HTTP_CODE" == "200" ]] && [[ -s /tmp/k8s-apt.key ]]; then
      KEY_DOWNLOADED=1
      break
    fi
    log "  -> HTTP $HTTP_CODE — retrying in 15s..."
    sleep 15
  done

  if [[ "$KEY_DOWNLOADED" -ne 1 ]]; then
    die "Failed to download Kubernetes apt signing key from $KUBE_KEY_URL after 5 attempts (last HTTP $HTTP_CODE)"
  fi

  sudo gpg --dearmor --yes -o "$KUBE_KEYRING" < /tmp/k8s-apt.key
  rm -f /tmp/k8s-apt.key

  echo "deb [signed-by=${KUBE_KEYRING}] \
https://pkgs.k8s.io/core:/stable:/v${KUBE_MAJOR_MINOR}/deb/ /" \
    | sudo tee "$KUBE_SOURCES"

  sudo apt-get update -qq
  # --allow-downgrades: GitHub runners ship a newer kube* version by default.
  # --allow-change-held-packages: in case a previous run left them held.
  sudo apt-get install -y -qq \
    --allow-downgrades \
    --allow-change-held-packages \
    "kubelet=${KUBERNETES_VERSION}-1.1" \
    "kubeadm=${KUBERNETES_VERSION}-1.1" \
    "kubectl=${KUBERNETES_VERSION}-1.1"

  sudo apt-mark hold kubelet kubeadm kubectl
fi

# ── Step 4: kernel modules and sysctl ────────────────────────────────────────

step "Loading kernel modules"
sudo modprobe overlay
sudo modprobe br_netfilter

sudo tee /etc/modules-load.d/k8s.conf >/dev/null <<'EOF'
overlay
br_netfilter
EOF

sudo tee /etc/sysctl.d/99-k8s.conf >/dev/null <<'EOF'
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
EOF
sudo sysctl --system -q

# BPF filesystem (required for Calico eBPF)
if [[ "$CALICO_EBPF" == "1" ]]; then
  if ! mountpoint -q /sys/fs/bpf; then
    sudo mount bpffs /sys/fs/bpf -t bpf
    log "Mounted BPF filesystem at /sys/fs/bpf"
  fi
  # Persist across reboots
  grep -q '/sys/fs/bpf' /etc/fstab 2>/dev/null || \
    echo 'none /sys/fs/bpf bpf defaults 0 0' | sudo tee -a /etc/fstab >/dev/null
fi

# ── Step 5: kubeadm init ──────────────────────────────────────────────────────

KUBECONFIG="/etc/kubernetes/admin.conf"

if [[ -f "$KUBECONFIG" ]]; then
  log "Kubernetes admin.conf already exists — skipping kubeadm init"
  # Ensure user-readable copy exists
  mkdir -p "$HOME/.kube"
  sudo cp "$KUBECONFIG" "$HOME/.kube/config"
  sudo chown "$(id -u):$(id -g)" "$HOME/.kube/config"
else
  step "Running kubeadm init"

  # Build kubeadm configuration
  # Always include kube-proxy: Calico eBPF bootstraps by talking to the
  # API server via ClusterIP, which requires kube-proxy or eBPF to already
  # be running — a classic chicken-and-egg.  kube-proxy handles ClusterIP
  # routing until Calico is fully up; Calico eBPF will replace it once live.
  SKIP_PHASES=""

  KUBEADM_CONFIG_FILE="$(mktemp /tmp/kubeadm-config.XXXXXX.yaml)"
  cat > "$KUBEADM_CONFIG_FILE" <<EOF
---
apiVersion: kubeadm.k8s.io/v1beta3
kind: ClusterConfiguration
kubernetesVersion: "v${KUBERNETES_VERSION}"
networking:
  podSubnet: "${POD_CIDR}"
  serviceSubnet: "${SERVICE_CIDR}"
---
apiVersion: kubeadm.k8s.io/v1beta3
kind: InitConfiguration
nodeRegistration:
  name: "${NODE_NAME}"
  criSocket: "unix:///run/containerd/containerd.sock"
---
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
cgroupDriver: systemd
failSwapOn: false
EOF

  SKIP_ARG=""
  [[ -n "$SKIP_PHASES" ]] && SKIP_ARG="--skip-phases=${SKIP_PHASES}"

  # shellcheck disable=SC2086
  sudo kubeadm init \
    --config="$KUBEADM_CONFIG_FILE" \
    $SKIP_ARG \
    --ignore-preflight-errors=NumCPU \
    2>&1 | tee /tmp/kubeadm-init.log

  rm -f "$KUBEADM_CONFIG_FILE"

  # Make kubeconfig accessible
  mkdir -p "$HOME/.kube"
  sudo cp "$KUBECONFIG" "$HOME/.kube/config"
  sudo chown "$(id -u):$(id -g)" "$HOME/.kube/config"
fi

# Use the user-readable copy so kubectl works without root
KUBECONFIG="$HOME/.kube/config"
export KUBECONFIG

# Remove taint so single-node cluster can schedule pods
kubectl --kubeconfig "$KUBECONFIG" taint nodes --all \
  node-role.kubernetes.io/control-plane- 2>/dev/null || true

# ── Step 6: Replace kubelet binary with kube-air ──────────────────────────────

step "Installing kube-air kubelet"

[[ -f "$KUBELET_BIN" ]] || die "kubelet binary not found at KUBELET_BIN=$KUBELET_BIN"

STOCK_KUBELET_PATH=$(command -v kubelet)

# Determine the path systemd will actually execute.  The ExecStart in the
# kubelet service unit may differ from `command -v kubelet` due to PATH
# ordering (e.g. /usr/bin/kubelet vs /usr/local/bin/kubelet).  Read it
# directly from the unit so we always deploy to the right location.
SYSTEMD_KUBELET_PATH=$(systemctl cat kubelet 2>/dev/null \
  | sed -n 's/^ExecStart=\([^ ]*\).*/\1/p' | head -1)
if [[ -z "$SYSTEMD_KUBELET_PATH" ]]; then
  SYSTEMD_KUBELET_PATH="$STOCK_KUBELET_PATH"
fi
log "systemd ExecStart kubelet path: $SYSTEMD_KUBELET_PATH"
log "PATH kubelet path: $STOCK_KUBELET_PATH"

# Quick sanity-check: confirm the binary can execute before deploying it.
"$KUBELET_BIN" --version 2>&1 | head -1 || die "kube-air binary self-check failed — binary cannot execute"

# Deploy to the PATH-resolved location.
log "Replacing $STOCK_KUBELET_PATH with kube-air binary"
sudo install -m 755 "$KUBELET_BIN" "$STOCK_KUBELET_PATH"

# Also deploy to the systemd ExecStart path if it differs.
if [[ "$SYSTEMD_KUBELET_PATH" != "$STOCK_KUBELET_PATH" && -f "$SYSTEMD_KUBELET_PATH" ]]; then
  log "Also deploying to $SYSTEMD_KUBELET_PATH (systemd ExecStart path)"
  sudo install -m 755 "$KUBELET_BIN" "$SYSTEMD_KUBELET_PATH"
fi

# Create a systemd service unit for kubelet (kubeadm drop-in expects it)
if has_systemd; then
  step "Enabling and starting kube-air kubelet"
  sudo systemctl daemon-reload
  sudo systemctl enable kubelet
  # Use restart: kubeadm init may have left the stock kubelet running.
  # 'restart' stops any running instance and starts the kube-air binary.
  sudo systemctl restart kubelet

  # Wait up to 60 s for the service to be active.
  # A fast exit (e.g. unrecognised flag) shows up within a few seconds.
  log "Waiting for kubelet service to become active..."
  KUBELET_ACTIVE=0
  for ((i=0; i<60; i++)); do
    if systemctl is-active kubelet --quiet 2>/dev/null; then
      KUBELET_ACTIVE=1
      break
    fi
    sleep 1
  done

  if [[ "$KUBELET_ACTIVE" -eq 0 ]]; then
    log "ERROR: kubelet service did not become active within 60 s"
    log "=== systemctl status kubelet ==="
    sudo systemctl status kubelet --no-pager -l 2>/dev/null || true
    log "=== journalctl -u kubelet (last 100 lines) ==="
    sudo journalctl -u kubelet --no-pager -n 100 2>/dev/null || true
    log "=== kubeadm-flags.env ==="
    cat /var/lib/kubelet/kubeadm-flags.env 2>/dev/null || true
    die "kubelet failed to start — check logs above"
  fi

  log "kubelet service is active"
  sudo systemctl status kubelet --no-pager -l 2>/dev/null || true
fi

# ── Step 7: Install Calico CNI ────────────────────────────────────────────────

# Wait for the API server to be reachable before any kubectl apply.
# The kube-air kubelet was just (re)started; the API server static pod can
# take 30-60 s to come up on a cold GitHub Actions runner.
# Require 5 consecutive successful checks (10 s apart) before proceeding —
# a single success is not enough because the stock-kubelet API server pod
# may still be alive while the kube-air kubelet is restarting it.
step "Waiting for API server to become stable"
APISERVER_READY=0
CONSECUTIVE=0
for ((i=0; i<150; i++)); do
  if kubectl --kubeconfig "$KUBECONFIG" get --raw /healthz &>/dev/null 2>&1; then
    CONSECUTIVE=$((CONSECUTIVE+1))
    log "  API server /healthz OK (consecutive: $CONSECUTIVE/5, poll $((i+1)))"
    if [[ "$CONSECUTIVE" -ge 5 ]]; then
      APISERVER_READY=1
      log "API server is stable"
      break
    fi
  else
    if [[ "$CONSECUTIVE" -gt 0 ]]; then
      log "  API server /healthz failed after $CONSECUTIVE successes — resetting counter"
    fi
    CONSECUTIVE=0
    [[ $((i % 10)) -eq 0 ]] && log "  still waiting for API server... ($((i*2))s elapsed)"
  fi
  sleep 2
done
if [[ "$APISERVER_READY" -eq 0 ]]; then
  log "ERROR: API server not stable after 300 s"
  sudo journalctl -u kubelet --no-pager -n 50 2>/dev/null || true
  die "API server never became stable — cannot install Calico"
fi

step "Installing Calico ${CALICO_VERSION}"

# Download Tigera operator manifest locally first so kubectl doesn't need to
# stream it from GitHub and make per-resource API calls in parallel.
TIGERA_OPERATOR_URL="https://raw.githubusercontent.com/projectcalico/calico/${CALICO_VERSION}/manifests/tigera-operator.yaml"
TIGERA_MANIFEST="/tmp/tigera-operator.yaml"
log "Downloading Tigera operator manifest..."
for attempt in 1 2 3; do
  if curl -sSL --retry 3 --retry-delay 5 --max-time 60 \
      -o "$TIGERA_MANIFEST" "$TIGERA_OPERATOR_URL"; then
    [[ -s "$TIGERA_MANIFEST" ]] && break
  fi
  [[ $attempt -eq 3 ]] && die "Failed to download Tigera operator manifest after 3 attempts"
  sleep 10
done

# Apply with retries — the API server can briefly blip right after becoming stable.
# Use --server-side to avoid the kubectl.kubernetes.io/last-applied-configuration
# annotation, which exceeds the 262144-byte limit on Calico's large CRDs.
log "Applying Tigera operator from local manifest (server-side apply)"
TIGERA_APPLIED=0
for attempt in 1 2 3; do
  if kubectl --kubeconfig "$KUBECONFIG" apply \
      --server-side --force-conflicts \
      -f "$TIGERA_MANIFEST"; then
    TIGERA_APPLIED=1
    break
  fi
  log "  kubectl apply attempt $attempt failed — waiting 15 s before retry"
  sleep 15
done
rm -f "$TIGERA_MANIFEST"
[[ "$TIGERA_APPLIED" -eq 1 ]] || die "Failed to apply Tigera operator after 3 attempts"

# Wait for the CRD to be fully Established before applying Installation.
# Merely checking for the CRD's existence is not sufficient — the API server
# needs to complete registration of the new API group endpoint, which happens
# only after the CRD transitions to the Established condition.
log "Waiting for installations.operator.tigera.io CRD to be Established..."
CRD_ESTABLISHED=0
for ((i=0; i<90; i++)); do
  if kubectl --kubeconfig "$KUBECONFIG" \
      wait crd/installations.operator.tigera.io \
      --for=condition=Established \
      --timeout=10s &>/dev/null 2>&1; then
    CRD_ESTABLISHED=1
    log "  CRD established after $((i * 5))s"
    break
  fi
  sleep 5
done
if [[ "$CRD_ESTABLISHED" -eq 0 ]]; then
  log "WARNING: installations CRD not Established after 450s — proceeding anyway"
fi
# Extra settle time: even after Established, the API discovery cache on the
# client side may not yet reflect the new group; give it a moment.
sleep 5

# Apply Calico Installation resource.
# Always use Iptables dataplane: Calico eBPF bootstraps by reaching the API
# server via ClusterIP (10.96.0.1), but ClusterIP routing depends on either
# kube-proxy or eBPF being already running.  Iptables mode works with
# kube-proxy and avoids this chicken-and-egg problem.
log "Configuring Calico with Iptables dataplane"
LINUX_DATAPLANE="Iptables"

INSTALLATION_APPLIED=0
for attempt in 1 2 3 4 5; do
  if cat <<EOF | kubectl --kubeconfig "$KUBECONFIG" apply -f -
apiVersion: operator.tigera.io/v1
kind: Installation
metadata:
  name: default
spec:
  calicoNetwork:
    linuxDataplane: "${LINUX_DATAPLANE}"
    bgp: Disabled
    ipPools:
    - blockSize: 26
      cidr: "${POD_CIDR}"
      encapsulation: VXLANCrossSubnet
      natOutgoing: Enabled
      nodeSelector: all()
EOF
  then
    INSTALLATION_APPLIED=1
    break
  fi
  log "  Installation apply attempt $attempt failed — waiting 15s before retry"
  sleep 15
done
[[ "$INSTALLATION_APPLIED" -eq 1 ]] || die "Failed to apply Calico Installation resource after 5 attempts"

# ── Step 8: Wait for cluster to be healthy ────────────────────────────────────

step "Waiting for cluster components to be ready"

log "Waiting for node to become Ready..."
for ((i=0; i<120; i++)); do
  READY_STATUS=$(kubectl --kubeconfig "$KUBECONFIG" \
    get node "$NODE_NAME" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
  if [[ "$READY_STATUS" == "True" ]]; then
    log "Node $NODE_NAME is Ready"
    break
  fi
  sleep 5
done

log "Waiting for kube-apiserver, etcd, kube-scheduler, kube-controller-manager..."
wait_for_pods_running kube-system tier=control-plane 4 60 || true

log "Waiting for CoreDNS..."
wait_for_pods_running kube-system k8s-app=kube-dns 2 120

log "Waiting for Calico node pods..."
wait_for_pods_running calico-system k8s-app=calico-node 1 180 \
  || wait_for_pods_running kube-system k8s-app=calico-node 1 60 || true

log "Calico API server not deployed in iptables mode — skipping wait"

# ── Step 9: Final cluster status ──────────────────────────────────────────────

step "Cluster status"
kubectl --kubeconfig "$KUBECONFIG" get nodes -o wide || true
echo ""
kubectl --kubeconfig "$KUBECONFIG" get pods --all-namespaces || true

log "setup-node.sh complete"
