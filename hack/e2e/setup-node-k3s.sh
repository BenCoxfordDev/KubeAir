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

# ── Reset ──────────────────────────────────────────────────────────────────────

if [[ "$RESET_EXISTING" == "1" ]]; then
  log "Removing existing k3s state..."
  pkill -f 'k3s server' 2>/dev/null || true
  rm -rf /var/lib/rancher/k3s /etc/rancher/k3s /run/k3s
fi

# ── Start containerd ───────────────────────────────────────────────────────────

step "Starting containerd"

mkdir -p /run/containerd /var/lib/containerd /etc/containerd

if [[ ! -f /etc/containerd/config.toml ]]; then
  containerd config default > /etc/containerd/config.toml
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
