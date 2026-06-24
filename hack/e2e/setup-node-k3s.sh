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
# setup-node-k3s.sh — Provision a single-node k3s cluster using the kube-air
#                     kubelet binary. Designed to run in a privileged container
#                     (no systemd required).
#
# Architecture:
#   k3s server --disable-agent   (API server, etcd, scheduler, controller-mgr)
#   containerd                   (CRI — standalone, not k3s-embedded)
#   kube-air kubelet             (node agent, connects to k3s API server)
#   bridge CNI                   (pod networking via containernetworking-plugins)
#
# Environment:
#   KUBELET_BIN         Path to pre-built kube-air kubelet binary. Required.
#   RESET_EXISTING      "1" to uninstall any existing k3s first. Default: 0
set -euo pipefail

KUBELET_BIN="${KUBELET_BIN:-}"
RESET_EXISTING="${RESET_EXISTING:-0}"

log()  { printf '[k3s-setup] %s\n' "$*"; }
die()  { printf '[k3s-setup] ERROR: %s\n' "$*" >&2; exit 1; }
step() { printf '\n[k3s-setup] ══ %s ══\n' "$*"; }

[[ -n "$KUBELET_BIN" ]] || die "KUBELET_BIN must be set"
[[ -f "$KUBELET_BIN" ]] || die "KUBELET_BIN not found: $KUBELET_BIN"
chmod +x "$KUBELET_BIN"

command -v k3s >/dev/null 2>&1 || die "k3s not found on PATH — ensure the build image includes /usr/local/bin/k3s"
log "k3s: $(k3s --version | head -1)"

# ── iptables symlinks ──────────────────────────────────────────────────────────
# The build image is assembled by rules_distroless which does not run postinst
# scripts, so update-alternatives never creates the unversioned /usr/sbin/iptables
# symlink.  The CNI bridge plugin calls "iptables" directly and fails with
# "failed to locate iptables" when the symlink is absent.
# Prefer iptables-legacy (direct kernel netfilter) over the nft wrapper.
for _cmd in iptables iptables-restore iptables-save \
            ip6tables ip6tables-restore ip6tables-save; do
  if ! command -v "$_cmd" >/dev/null 2>&1; then
    _legacy="/usr/sbin/${_cmd}-legacy"
    _nft="/usr/sbin/${_cmd}-nft"
    if [[ -x "$_legacy" ]]; then
      ln -sf "$_legacy" "/usr/sbin/${_cmd}"
      log "Linked /usr/sbin/${_cmd} -> ${_legacy}"
    elif [[ -x "$_nft" ]]; then
      ln -sf "$_nft" "/usr/sbin/${_cmd}"
      log "Linked /usr/sbin/${_cmd} -> ${_nft}"
    else
      log "Warning: no ${_cmd} variant found in /usr/sbin"
    fi
  fi
done
# Ensure /usr/sbin is on PATH for containerd/CNI processes.
export PATH="/usr/sbin:$PATH"

# ── Reset ──────────────────────────────────────────────────────────────────────

if [[ "$RESET_EXISTING" == "1" ]]; then
  log "Removing existing k3s state..."
  pkill -f 'k3s server' 2>/dev/null || true
  rm -rf /var/lib/rancher/k3s /etc/rancher/k3s /run/k3s
fi

# ── Start containerd ───────────────────────────────────────────────────────────

step "Starting containerd"

mkdir -p /run/containerd /var/lib/containerd /etc/containerd

# Generate default config, then switch to the native snapshotter when overlay
# mounts are unavailable (e.g. running inside a container on macOS with podman).
# We test overlayfs support by attempting a real mount; if it fails we fall back
# to native so image pulls don't break with "failed to mount ... tmpmounts".
containerd config default > /etc/containerd/config.toml

_use_native=0
if ! grep -q "snapshotter.*=.*\"native\"" /etc/containerd/config.toml; then
  # Quick probe: try an overlay mount into a temp dir.
  _probe_lower="$(mktemp -d)" _probe_upper="$(mktemp -d)" \
  _probe_work="$(mktemp -d)"  _probe_merged="$(mktemp -d)"
  if ! mount -t overlay overlay \
       -o "lowerdir=${_probe_lower},upperdir=${_probe_upper},workdir=${_probe_work}" \
       "${_probe_merged}" 2>/dev/null; then
    _use_native=1
  else
    umount "${_probe_merged}" 2>/dev/null || true
  fi
  rm -rf "${_probe_lower}" "${_probe_upper}" "${_probe_work}" "${_probe_merged}"
fi

if [[ "$_use_native" == "1" ]]; then
  log "overlayfs unavailable — switching containerd snapshotter to native"
  sed -i 's/snapshotter = "overlayfs"/snapshotter = "native"/' /etc/containerd/config.toml
  # Also patch the CRI plugin block if present
  sed -i 's/\(snapshotter\s*=\s*\)"overlayfs"/\1"native"/g' /etc/containerd/config.toml
else
  log "overlayfs available — using default snapshotter"
fi

nohup containerd >/tmp/containerd.log 2>&1 &
CONTAINERD_PID=$!
log "containerd started (PID $CONTAINERD_PID)"

for i in $(seq 1 30); do
  if [[ -S /run/containerd/containerd.sock ]]; then
    log "containerd socket ready"
    break
  fi
  if [[ $i -eq 30 ]]; then
    echo "--- containerd logs ---"
    tail -20 /tmp/containerd.log || true
    die "containerd socket never appeared after 30s"
  fi
  sleep 1
done

# ── Configure bridge CNI ───────────────────────────────────────────────────────

step "Configuring bridge CNI (10.88.0.0/16)"

mkdir -p /etc/cni/net.d

# containernetworking-plugins installs to /usr/lib/cni on Debian
if [[ -d /usr/lib/cni ]] && [[ ! -e /opt/cni/bin ]]; then
  mkdir -p /opt/cni
  ln -sf /usr/lib/cni /opt/cni/bin
fi

cat > /etc/cni/net.d/10-kubeair-e2e.conflist <<'CNIEOF'
{
  "cniVersion": "1.0.0",
  "name": "kubeair-e2e",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "cni0",
      "isGateway": true,
      "ipMasq": true,
      "promiscMode": true,
      "ipam": {
        "type": "host-local",
        "ranges": [[{"subnet": "10.88.0.0/16"}]],
        "routes": [{"dst": "0.0.0.0/0"}]
      }
    },
    {
      "type": "portmap",
      "capabilities": {"portMappings": true}
    }
  ]
}
CNIEOF

log "CNI config written to /etc/cni/net.d/10-kubeair-e2e.conflist"

# Load kernel modules required for bridge networking and CNI masquerade rules.
modprobe bridge 2>/dev/null || true
modprobe br_netfilter 2>/dev/null || true
modprobe overlay 2>/dev/null || true

# Enable sysctls needed for CNI bridge and pod networking.
sysctl -w net.ipv4.ip_forward=1 >/dev/null 2>&1 || true
sysctl -w net.bridge.bridge-nf-call-iptables=1 >/dev/null 2>&1 || true
sysctl -w net.bridge.bridge-nf-call-ip6tables=1 >/dev/null 2>&1 || true

# Pre-create the cni0 bridge so the CNI plugin never has to create it itself.
# Inside a container (e.g. podman on macOS) the bridge plugin's RTNETLINK
# "ip link add" call returns EPERM even with --privileged because the macOS VM
# kernel blocks new bridge device creation from nested namespaces.
# The bridge plugin will re-use an existing bridge without requiring that
# capability, so we create it here where we already have it.
if ! ip link show cni0 &>/dev/null; then
  if ip link add cni0 type bridge 2>/dev/null; then
    ip link set cni0 up
    ip addr add 10.88.0.1/16 dev cni0 2>/dev/null || true
    log "Pre-created cni0 bridge (10.88.0.1/16)"
  else
    log "Warning: could not pre-create cni0 bridge — CNI plugin will attempt it at runtime"
  fi
else
  log "cni0 bridge already exists"
fi

# ── Start k3s server (control plane only) ─────────────────────────────────────

step "Starting k3s server (control plane only, --disable-agent)"

mkdir -p /run/k3s /etc/rancher/k3s /var/lib/rancher/k3s

nohup k3s server \
  --disable-agent \
  --disable=traefik \
  --disable=servicelb \
  --disable-network-policy \
  --flannel-backend=none \
  --cluster-cidr=10.88.0.0/16 \
  --service-cidr=10.96.0.0/16 \
  --cluster-dns=10.96.0.10 \
  --write-kubeconfig-mode=644 \
  --tls-san=127.0.0.1 \
  >/tmp/k3s-server.log 2>&1 &

K3S_PID=$!
log "k3s server started (PID $K3S_PID)"

log "Waiting for k3s API server to be ready..."
for i in $(seq 1 60); do
  if [[ -f /etc/rancher/k3s/k3s.yaml ]] && \
     k3s kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml cluster-info \
       >/dev/null 2>&1; then
    log "k3s API server is ready"
    break
  fi
  if [[ $i -eq 60 ]]; then
    echo "--- k3s server logs ---"
    tail -30 /tmp/k3s-server.log || true
    die "k3s API server never became ready after 60 attempts"
  fi
  log "Attempt $i/60: API server not ready yet..."
  sleep 3
done

export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

# ── Start kube-air kubelet ─────────────────────────────────────────────────────

step "Starting kube-air kubelet"

NODE_NAME="$(hostname | tr '[:upper:]' '[:lower:]')"
mkdir -p /var/lib/kubelet /var/log/pods

# Remove any stale TLS serving cert so the kubelet regenerates it with the
# correct InternalIP SAN for this run.
rm -f /var/lib/kubelet/pki/kubelet-serving.crt /var/lib/kubelet/pki/kubelet-serving.key

nohup "$KUBELET_BIN" \
  --kubeconfig=/etc/rancher/k3s/k3s.yaml \
  --container-runtime-endpoint=unix:///run/containerd/containerd.sock \
  --node-name="$NODE_NAME" \
  --cluster-dns=10.96.0.10 \
  --cluster-domain=cluster.local \
  --root-dir=/var/lib/kubelet \
  >/tmp/kubelet.log 2>&1 &

KUBELET_PID=$!
echo "$KUBELET_PID" > /tmp/kubelet.pid
log "kube-air kubelet started (PID $KUBELET_PID)"

# ── Wait for node Ready ────────────────────────────────────────────────────────

step "Waiting for node ${NODE_NAME} to be Ready"

for i in $(seq 1 60); do
  if k3s kubectl wait node "$NODE_NAME" \
       --for=condition=Ready --timeout=5s >/dev/null 2>&1; then
    log "Node $NODE_NAME is Ready"
    break
  fi
  if [[ $i -eq 60 ]]; then
    echo "--- k3s server logs ---"
    tail -30 /tmp/k3s-server.log || true
    echo "--- kubelet logs ---"
    tail -30 /tmp/kubelet.log || true
    k3s kubectl get nodes || true
    die "Node $NODE_NAME never became Ready after 60 attempts"
  fi
  log "Attempt $i/60: node not Ready yet..."
  sleep 5
done

# ── Finalise ───────────────────────────────────────────────────────────────────

# Copy kubeconfig to standard location consumed by run-cluster-tests.sh
mkdir -p "$HOME/.kube"
cp /etc/rancher/k3s/k3s.yaml "$HOME/.kube/config"

step "Cluster ready"
k3s kubectl get nodes -o wide
k3s kubectl get pods --all-namespaces
