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
# setup-node.sh — Provision a single-node Kubernetes control plane using
#                 upstream binaries. Designed to run in a privileged container
#                 (no systemd, no k3s required).
#
# Architecture:
#   etcd                        (key-value store)
#   kube-apiserver              (API server)
#   kube-controller-manager     (controller loop)
#   kube-scheduler              (pod scheduler)
#   containerd                  (CRI)
#   kube-air kubelet            (node agent)
#   kube-proxy                  (service VIP / iptables)
#   bridge CNI                  (pod networking)
#   CoreDNS                     (cluster DNS, applied directly — no reconciler)
#
# PKI bootstrap is done with `kubeadm init phase certs/kubeconfig` which runs
# entirely offline (no running cluster required).
#
# Environment:
#   KUBELET_BIN         Path to pre-built kube-air kubelet binary. Required.
#   RESET_EXISTING      "1" to wipe existing state before init. Default: 0
set -euo pipefail

KUBELET_BIN="${KUBELET_BIN:-}"
RESET_EXISTING="${RESET_EXISTING:-0}"

log()  { printf '[setup-node] %s\n' "$*"; }
die()  { printf '[setup-node] ERROR: %s\n' "$*" >&2; exit 1; }
step() { printf '\n[setup-node] ══ %s ══\n' "$*"; }

[[ -n "$KUBELET_BIN" ]] || die "KUBELET_BIN must be set"
[[ -f "$KUBELET_BIN" ]] || die "KUBELET_BIN not found: $KUBELET_BIN"
chmod +x "$KUBELET_BIN"

for _bin in etcd kube-apiserver kube-controller-manager kube-scheduler kube-proxy kubeadm kubectl; do
  command -v "$_bin" >/dev/null 2>&1 || die "$_bin not found on PATH — ensure the build image is up to date"
done
log "Kubernetes binaries: $(kube-apiserver --version 2>/dev/null || true)"
log "etcd: $(etcd --version 2>/dev/null | head -1 || true)"

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

NODE_NAME="$(hostname | tr '[:upper:]' '[:lower:]')"

# ── Kill any leftover control-plane processes ─────────────────────────────────
# Always kill stale processes from previous runs so ports (6443, 2379, 2380)
# are free before we start new ones. Use multiple passes to catch stragglers.
for _pass in 1 2; do
  for _proc in kube-apiserver kube-controller-manager kube-scheduler kube-proxy etcd; do
    pkill -9 -f "$_proc" 2>/dev/null || true
  done
  [[ $_pass -lt 2 ]] && sleep 2
done
# Wait for TIME_WAIT sockets to release the ports.
sleep 3

# ── Reset ──────────────────────────────────────────────────────────────────────

if [[ "$RESET_EXISTING" == "1" ]]; then
  log "Wiping existing control-plane state..."
  rm -rf /var/lib/etcd /etc/kubernetes /var/lib/kubelet /run/kubernetes
fi

# ── Cleanup on exit ────────────────────────────────────────────────────────────
# Ensure all spawned processes are terminated when script exits on error/signal.
# On SUCCESS (exit 0), we intentionally leave the cluster running so the caller
# can run tests against it.  The caller is responsible for cleanup.
_pids=()
_setup_failed=0

cleanup() {
  local _exit_code=$?
  # Only kill cluster processes if we failed; on success they must stay alive.
  if [[ $_setup_failed -eq 1 ]] && [[ ${#_pids[@]} -gt 0 ]]; then
    log "Cleaning up spawned processes (setup failed)..."
    for _pid in "${_pids[@]}"; do
      if kill -0 "$_pid" 2>/dev/null; then
        log "Killing PID $_pid"
        kill -9 "$_pid" 2>/dev/null || true
      fi
    done
  fi
  return $_exit_code
}
trap '_setup_failed=1; cleanup' ERR
trap 'cleanup' SIGTERM SIGINT

# ── Start containerd ───────────────────────────────────────────────────────────

step "Starting containerd"

mkdir -p /run/containerd /var/lib/containerd /etc/containerd

# Generate default config, then switch to the native snapshotter when overlay
# mounts are unavailable (e.g. running inside a container on macOS with podman).
containerd config default > /etc/containerd/config.toml

_use_native=0
if ! grep -q "snapshotter.*=.*\"native\"" /etc/containerd/config.toml; then
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
  sed -i 's/\(snapshotter\s*=\s*\)"overlayfs"/\1"native"/g' /etc/containerd/config.toml
else
  log "overlayfs available — using default snapshotter"
fi

# ── cgroupv2 controller delegation ────────────────────────────────────────────
if [[ -f /sys/fs/cgroup/cgroup.subtree_control ]]; then
  echo "+cpu +cpuset +memory +io +pids" > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || true
  log "Delegated cgroupv2 controllers"
fi

nohup containerd >/tmp/containerd.log 2>&1 &
CONTAINERD_PID=$!
_pids+=("$CONTAINERD_PID")
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

# ── Pre-pull critical images ───────────────────────────────────────────────────
_prepull_image() {
  local img="$1"
  log "Pre-pulling image: $img"
  for _attempt in 1 2 3; do
    if ctr -n k8s.io images pull "$img" >/dev/null 2>&1; then
      log "  Pulled: $img"
      return 0
    fi
    log "  Attempt $_attempt/3 failed — retrying..."
    sleep 3
  done
  log "  Warning: could not pre-pull $img — will be pulled on demand"
}

_prepull_image "registry.k8s.io/pause:3.10"
# CoreDNS 1.13.1 — same version bundled in upstream k8s 1.35
_prepull_image "registry.k8s.io/coredns/coredns:v1.13.1"

# ── Configure bridge CNI ───────────────────────────────────────────────────────

step "Configuring bridge CNI (10.88.0.0/16)"

mkdir -p /etc/cni/net.d

# Detect where CNI plugins live and ensure /opt/cni/bin points there.
_cni_bin_dir=""
for _candidate in /opt/cni/bin /usr/lib/cni /usr/libexec/cni /usr/local/lib/cni; do
  if [[ -x "${_candidate}/bridge" ]]; then
    _cni_bin_dir="$_candidate"
    break
  fi
done

if [[ -z "$_cni_bin_dir" ]]; then
  # Last resort: search the whole filesystem
  _bridge_bin="$(find /usr /opt -name bridge -type f -perm /111 2>/dev/null | head -1)"
  if [[ -n "$_bridge_bin" ]]; then
    _cni_bin_dir="$(dirname "$_bridge_bin")"
  fi
fi

[[ -n "$_cni_bin_dir" ]] || die "CNI bridge plugin not found — ensure the build image is up to date"
log "CNI plugins directory: $_cni_bin_dir"

# Symlink /opt/cni/bin to the discovered dir if it isn't already the right place.
if [[ "$_cni_bin_dir" != "/opt/cni/bin" ]]; then
  mkdir -p /opt/cni
  rm -f /opt/cni/bin
  ln -sf "$_cni_bin_dir" /opt/cni/bin
  log "Linked /opt/cni/bin -> $_cni_bin_dir"
fi

# Determine whether portmap plugin is available — omit from config if not.
_have_portmap=0
[[ -x "${_cni_bin_dir}/portmap" ]] && _have_portmap=1
log "portmap plugin available: $_have_portmap"

if [[ "$_have_portmap" == 1 ]]; then
  cat > /etc/cni/net.d/10-kubeair-e2e.conflist <<'CNIEOF'
{
  "cniVersion": "0.4.0",
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
else
  cat > /etc/cni/net.d/10-kubeair-e2e.conflist <<'CNIEOF'
{
  "cniVersion": "0.4.0",
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
    }
  ]
}
CNIEOF
fi

log "CNI config written (portmap=${_have_portmap})"

# Validate that host-local IPAM plugin is also present (required for bridge CNI)
if [[ ! -x "${_cni_bin_dir}/host-local" ]]; then
  log "WARNING: host-local IPAM plugin not found in $_cni_bin_dir — CNI will fail"
  ls -la "$_cni_bin_dir" 2>/dev/null || true
fi

log "CNI binaries available: $(ls "$_cni_bin_dir" 2>/dev/null | tr '\n' ' ')"

modprobe bridge 2>/dev/null || true
modprobe br_netfilter 2>/dev/null || true
modprobe overlay 2>/dev/null || true

sysctl -w net.ipv4.ip_forward=1 >/dev/null 2>&1 || true
sysctl -w net.bridge.bridge-nf-call-iptables=1 >/dev/null 2>&1 || true
sysctl -w net.bridge.bridge-nf-call-ip6tables=1 >/dev/null 2>&1 || true

iptables -t nat -C POSTROUTING -s 10.88.0.0/16 ! -d 10.88.0.0/16 -j MASQUERADE 2>/dev/null || \
  iptables -t nat -A POSTROUTING -s 10.88.0.0/16 ! -d 10.88.0.0/16 -j MASQUERADE 2>/dev/null || true
log "iptables MASQUERADE rule ensured for 10.88.0.0/16"

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

# ── Bootstrap PKI with kubeadm ────────────────────────────────────────────────
# kubeadm init phase certs/kubeconfig runs entirely offline — no running
# cluster required.  All output goes to /etc/kubernetes/pki/ and
# /etc/kubernetes/*.conf.

step "Bootstrapping PKI and kubeconfigs (kubeadm)"

_NODE_IP="$(hostname -I | awk '{print $1}')"
_K8S_VER="$(kube-apiserver --version 2>/dev/null | awk '{print $2}' || echo 'v1.35.0')"

mkdir -p /etc/kubernetes/pki /var/lib/kubelet/pki /var/log/pods

# Always regenerate certs — node IP may have changed between runs.
rm -rf /etc/kubernetes/pki
mkdir -p /etc/kubernetes/pki

cat > /tmp/kubeadm-config.yaml <<KUBEADM_EOF
apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
kubernetesVersion: ${_K8S_VER}
controlPlaneEndpoint: "127.0.0.1:6443"
networking:
  podSubnet: "10.88.0.0/16"
  serviceSubnet: "10.96.0.0/12"
  dnsDomain: "cluster.local"
apiServer:
  certSANs:
    - "127.0.0.1"
    - "${_NODE_IP}"
    - "${NODE_NAME}"
---
apiVersion: kubeadm.k8s.io/v1beta4
kind: InitConfiguration
localAPIEndpoint:
  advertiseAddress: "${_NODE_IP}"
  bindPort: 6443
nodeRegistration:
  name: "${NODE_NAME}"
KUBEADM_EOF

kubeadm init phase certs all \
  --config /tmp/kubeadm-config.yaml \
  2>/tmp/kubeadm-certs.log \
  || { tail -20 /tmp/kubeadm-certs.log; die "kubeadm init phase certs failed"; }
log "PKI generated in /etc/kubernetes/pki/"

kubeadm init phase kubeconfig all \
  --config /tmp/kubeadm-config.yaml \
  2>/tmp/kubeadm-kubeconfig.log \
  || { tail -20 /tmp/kubeadm-kubeconfig.log; die "kubeadm init phase kubeconfig failed"; }
log "Kubeconfigs generated in /etc/kubernetes/"

# The generated kubeconfigs bind to _NODE_IP; rewrite the server address to
# 127.0.0.1 so kubectl/test binaries work from inside the same container.
sed -i "s|https://${_NODE_IP}:6443|https://127.0.0.1:6443|g" \
  /etc/kubernetes/admin.conf \
  /etc/kubernetes/controller-manager.conf \
  /etc/kubernetes/scheduler.conf \
  /etc/kubernetes/kubelet.conf 2>/dev/null || true

# Standard location for kubectl
mkdir -p "$HOME/.kube"
cp /etc/kubernetes/admin.conf "$HOME/.kube/config"

# ── Generate kubelet serving cert ─────────────────────────────────────────────
# Signed by the k8s CA so kube-apiserver (which trusts that CA) can verify
# the kubelet's TLS certificate for pod logs/exec/port-forward.

_KUBELET_CERT="/var/lib/kubelet/pki/kubelet-serving.crt"
_KUBELET_KEY="/var/lib/kubelet/pki/kubelet-serving.key"

log "Generating kubelet serving cert signed by k8s CA"
openssl req -newkey rsa:2048 -nodes \
  -keyout "$_KUBELET_KEY" \
  -subj "/CN=system:node:${NODE_NAME}/O=system:nodes" \
  -out /tmp/kubelet-serving.csr 2>/dev/null
openssl x509 -req \
  -in /tmp/kubelet-serving.csr \
  -CA /etc/kubernetes/pki/ca.crt \
  -CAkey /etc/kubernetes/pki/ca.key \
  -CAcreateserial \
  -out "$_KUBELET_CERT" \
  -days 365 \
  -extfile <(printf 'subjectAltName=IP:%s,IP:127.0.0.1,DNS:%s\n' "$_NODE_IP" "$NODE_NAME") \
  2>/dev/null
log "Kubelet serving cert generated (SAN: IP:${_NODE_IP}, IP:127.0.0.1, DNS:${NODE_NAME})"

# ── Start etcd ────────────────────────────────────────────────────────────────

step "Starting etcd"

mkdir -p /var/lib/etcd

nohup etcd \
  --data-dir=/var/lib/etcd \
  --listen-client-urls=https://127.0.0.1:2379 \
  --advertise-client-urls=https://127.0.0.1:2379 \
  --listen-peer-urls=https://127.0.0.1:2380 \
  --initial-advertise-peer-urls=https://127.0.0.1:2380 \
  --initial-cluster="default=https://127.0.0.1:2380" \
  --cert-file=/etc/kubernetes/pki/etcd/server.crt \
  --key-file=/etc/kubernetes/pki/etcd/server.key \
  --trusted-ca-file=/etc/kubernetes/pki/etcd/ca.crt \
  --client-cert-auth=true \
  --peer-cert-file=/etc/kubernetes/pki/etcd/peer.crt \
  --peer-key-file=/etc/kubernetes/pki/etcd/peer.key \
  --peer-trusted-ca-file=/etc/kubernetes/pki/etcd/ca.crt \
  --peer-client-cert-auth=true \
  >/tmp/etcd.log 2>&1 &
ETCD_PID=$!
_pids+=("$ETCD_PID")
log "etcd started (PID $ETCD_PID)"

# Wait for etcd to become healthy
for i in $(seq 1 30); do
  if curl -sf \
       --cacert /etc/kubernetes/pki/etcd/ca.crt \
       --cert /etc/kubernetes/pki/etcd/healthcheck-client.crt \
       --key /etc/kubernetes/pki/etcd/healthcheck-client.key \
       https://127.0.0.1:2379/health >/dev/null 2>&1; then
    log "etcd is healthy"
    break
  fi
  if [[ $i -eq 30 ]]; then
    echo "--- etcd logs ---"
    tail -20 /tmp/etcd.log || true
    die "etcd never became healthy after 30s"
  fi
  sleep 1
done

# ── Start kube-apiserver ──────────────────────────────────────────────────────

step "Starting kube-apiserver"

# Kill any lingering kube-apiserver processes and give kernel time to fully release port 6443.
# Even after pkill, the kernel may hold the port in TIME_WAIT state for up to 60s.
pkill -9 -f 'kube-apiserver' 2>/dev/null || true
sleep 5

# Temporarily disable pipefail to handle retry logic  
set +o pipefail

# Start kube-apiserver with retry logic — port may still be in TIME_WAIT initially.
# Maximum 6 attempts with 30-40s delays to ensure port is fully released.
_attempt=0
_success=false
while [[ $_attempt -lt 6 ]] && [[ "$_success" == "false" ]]; do
  _attempt=$((${_attempt} + 1))
  log "Attempt $_attempt/6: starting kube-apiserver..."
  
  nohup kube-apiserver \
    --etcd-servers=https://127.0.0.1:2379 \
    --etcd-cafile=/etc/kubernetes/pki/etcd/ca.crt \
    --etcd-certfile=/etc/kubernetes/pki/apiserver-etcd-client.crt \
    --etcd-keyfile=/etc/kubernetes/pki/apiserver-etcd-client.key \
    --service-cluster-ip-range=10.96.0.0/12 \
    --bind-address=0.0.0.0 \
    --advertise-address="${_NODE_IP}" \
    --secure-port=6443 \
    --tls-cert-file=/etc/kubernetes/pki/apiserver.crt \
    --tls-private-key-file=/etc/kubernetes/pki/apiserver.key \
    --client-ca-file=/etc/kubernetes/pki/ca.crt \
    --service-account-key-file=/etc/kubernetes/pki/sa.pub \
    --service-account-signing-key-file=/etc/kubernetes/pki/sa.key \
    --service-account-issuer=https://kubernetes.default.svc.cluster.local \
    --kubelet-client-certificate=/etc/kubernetes/pki/apiserver-kubelet-client.crt \
    --kubelet-client-key=/etc/kubernetes/pki/apiserver-kubelet-client.key \
    --authorization-mode=Node,RBAC \
    --requestheader-client-ca-file=/etc/kubernetes/pki/front-proxy-ca.crt \
    --requestheader-allowed-names=front-proxy-client \
    --requestheader-extra-headers-prefix=X-Remote-Extra- \
    --requestheader-group-headers=X-Remote-Group \
    --requestheader-username-headers=X-Remote-User \
    --proxy-client-cert-file=/etc/kubernetes/pki/front-proxy-client.crt \
    --proxy-client-key-file=/etc/kubernetes/pki/front-proxy-client.key \
    --enable-aggregator-routing=true \
    >/tmp/kube-apiserver.log 2>&1 &
  
  _pid=$!
  _pids+=("$_pid")
  log "kube-apiserver spawned (PID $_pid)"
  
  sleep 3
  
  if kill -0 $_pid 2>/dev/null; then
    log "kube-apiserver is running"
    _success=true
  else
    if grep -q "address already in use" /tmp/kube-apiserver.log 2>/dev/null; then
      if [[ $_attempt -lt 6 ]]; then
        log "Port 6443 in TIME_WAIT (attempt $_attempt/6), waiting 30s before retry..."
        sleep 30
      fi
    else
      log "kube-apiserver exited unexpectedly"
      tail -20 /tmp/kube-apiserver.log || true
      set -o pipefail
      die "kube-apiserver failed (check logs above)"
    fi
  fi
done

set -o pipefail

if [[ "$_success" != "true" ]]; then
  echo "--- kube-apiserver logs (last 30 lines) ---"
  tail -30 /tmp/kube-apiserver.log || true
  die "kube-apiserver never started after 6 attempts"
fi

log "kube-apiserver is ready"

# Wait for the API server to respond
for i in $(seq 1 120); do
  if curl -sf \
       --cacert /etc/kubernetes/pki/ca.crt \
       https://127.0.0.1:6443/readyz >/dev/null 2>&1; then
    log "kube-apiserver is ready"
    break
  fi
  if [[ $i -eq 120 ]]; then
    echo "--- kube-apiserver logs ---"
    tail -30 /tmp/kube-apiserver.log || true
    echo "--- kubeconfig server ---"
    grep "server:" /etc/kubernetes/admin.conf || true
    echo "--- curl error ---"
    curl -v --cacert /etc/kubernetes/pki/ca.crt https://127.0.0.1:6443/readyz 2>&1 | tail -10 || true
    die "kube-apiserver never became ready after 120s"
  fi
  sleep 1
done

# kubeadm 1.29+ creates admin.conf with group "kubernetes-admins" (not system:masters).
# The kubeadm:cluster-admins ClusterRoleBinding is only created by "kubeadm init",
# not by the individual phases we use. Create it now using super-admin.conf (system:masters).
kubectl --kubeconfig=/etc/kubernetes/super-admin.conf \
  create clusterrolebinding kubeadm:cluster-admins \
  --clusterrole=cluster-admin \
  --group=kubeadm:cluster-admins \
  2>/dev/null || true
log "kubeadm:cluster-admins ClusterRoleBinding ensured"

# Allow the e2e test framework (running as kubernetes-admin) to query the kubelet
# API directly via the apiserver proxy (needed for log/exec/portforward tests and
# for diagnostic collection: "kubectl get --raw /api/v1/nodes/<name>/proxy/pods").
kubectl --kubeconfig=/etc/kubernetes/super-admin.conf \
  create clusterrolebinding e2e:kubelet-api-admin \
  --clusterrole=system:kubelet-api-admin \
  --group=kubeadm:cluster-admins \
  2>/dev/null || true
log "e2e:kubelet-api-admin ClusterRoleBinding ensured"

# ── Start kube-controller-manager ────────────────────────────────────────────

step "Starting kube-controller-manager"

nohup kube-controller-manager \
  --kubeconfig=/etc/kubernetes/controller-manager.conf \
  --service-cluster-ip-range=10.96.0.0/12 \
  --cluster-cidr=10.88.0.0/16 \
  --allocate-node-cidrs=true \
  --cluster-signing-cert-file=/etc/kubernetes/pki/ca.crt \
  --cluster-signing-key-file=/etc/kubernetes/pki/ca.key \
  --root-ca-file=/etc/kubernetes/pki/ca.crt \
  --service-account-private-key-file=/etc/kubernetes/pki/sa.key \
  --use-service-account-credentials=true \
  --controllers='*,bootstrapsigner,tokencleaner' \
  >/tmp/kube-controller-manager.log 2>&1 &
CTRL_MGR_PID=$!
_pids+=("$CTRL_MGR_PID")
log "kube-controller-manager started (PID $CTRL_MGR_PID)"

# Wait for controller-manager to be healthy before proceeding.
# This ensures the SA token controller and kube-root-ca.crt injector are running
# before any namespaces are created, preventing "wait for service account" timeouts.
for i in $(seq 1 30); do
  if kubectl --kubeconfig=/etc/kubernetes/admin.conf \
       get --raw /healthz >/dev/null 2>&1 && \
     kill -0 "$CTRL_MGR_PID" 2>/dev/null; then
    # Process is running; give it a moment to initialize its controllers
    sleep 3
    log "kube-controller-manager is running"
    break
  fi
  if [[ $i -eq 30 ]]; then
    log "Warning: controller-manager not running after 60s — continuing anyway"
    tail -10 /tmp/kube-controller-manager.log || true
    break
  fi
  sleep 2
done

# ── Start kube-scheduler ──────────────────────────────────────────────────────

step "Starting kube-scheduler"

nohup kube-scheduler \
  --kubeconfig=/etc/kubernetes/scheduler.conf \
  >/tmp/kube-scheduler.log 2>&1 &
SCHED_PID=$!
_pids+=("$SCHED_PID")
log "kube-scheduler started (PID $SCHED_PID)"

# ── Start kube-air kubelet ────────────────────────────────────────────────────

step "Starting kube-air kubelet"

# Write a KubeletConfiguration that trusts the k8s CA so kube-apiserver's
# x509 client cert is accepted for pod log/exec requests.
cat > /var/lib/kubelet/kubelet-config.yaml <<KUBELET_CFG
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
authentication:
  x509:
    clientCAFile: /etc/kubernetes/pki/ca.crt
  anonymous:
    enabled: false
authorization:
  mode: AlwaysAllow
KUBELET_CFG

nohup "$KUBELET_BIN" \
  --kubeconfig=/etc/kubernetes/kubelet.conf \
  --container-runtime-endpoint=unix:///run/containerd/containerd.sock \
  --node-name="$NODE_NAME" \
  --cluster-dns=10.96.0.10 \
  --cluster-domain=cluster.local \
  --root-dir=/var/lib/kubelet \
  --config=/var/lib/kubelet/kubelet-config.yaml \
  --tls-cert-file="$_KUBELET_CERT" \
  --tls-private-key-file="$_KUBELET_KEY" \
  --cni-bin-dir="$_cni_bin_dir" \
  --cni-conf-dir=/etc/cni/net.d \
  >/tmp/kubelet.log 2>&1 &

KUBELET_PID=$!
_pids+=("$KUBELET_PID")
echo "$KUBELET_PID" > /tmp/kubelet.pid
log "kube-air kubelet started (PID $KUBELET_PID)"

# ── Start kube-proxy ──────────────────────────────────────────────────────────

step "Starting kube-proxy"

nohup kube-proxy \
  --kubeconfig=/etc/kubernetes/admin.conf \
  --proxy-mode=iptables \
  --cluster-cidr=10.88.0.0/16 \
  >/tmp/kube-proxy.log 2>&1 &
PROXY_PID=$!
_pids+=("$PROXY_PID")
log "kube-proxy started (PID $PROXY_PID)"

# ── Wait for node Ready ───────────────────────────────────────────────────────

step "Waiting for node ${NODE_NAME} to be Ready"

for i in $(seq 1 60); do
  if kubectl --kubeconfig=/etc/kubernetes/admin.conf \
       wait node "$NODE_NAME" \
       --for=condition=Ready --timeout=5s >/dev/null 2>&1; then
    log "Node $NODE_NAME is Ready"
    break
  fi
  if [[ $i -eq 60 ]]; then
    echo "--- kube-apiserver logs ---"
    tail -20 /tmp/kube-apiserver.log || true
    echo "--- kubelet logs ---"
    tail -30 /tmp/kubelet.log || true
    kubectl --kubeconfig=/etc/kubernetes/admin.conf get nodes || true
    die "Node $NODE_NAME never became Ready after 60 attempts"
  fi
  # Every 5 attempts, show kubelet status for diagnostics
  if (( i % 5 == 0 )); then
    log "Attempt $i/60: node not Ready - kubelet status:"
    tail -5 /tmp/kubelet.log 2>/dev/null | sed 's/^/  /' || true
  else
    log "Attempt $i/60: node not Ready yet..."
  fi
  sleep 5
done

# Wait for SA token controller to inject kube-root-ca.crt into kube-system.
# Tests that create namespaces check for this before proceeding; if we don't
# wait here they race and get "timed out waiting for the condition" failures.
step "Waiting for kube-root-ca.crt injection"
for i in $(seq 1 30); do
  if kubectl --kubeconfig=/etc/kubernetes/admin.conf \
       get configmap kube-root-ca.crt -n kube-system >/dev/null 2>&1; then
    log "kube-root-ca.crt is available"
    break
  fi
  if [[ $i -eq 30 ]]; then
    log "Warning: kube-root-ca.crt not injected after 60s — continuing anyway"
    break
  fi
  sleep 2
done

# ── Apply CoreDNS ─────────────────────────────────────────────────────────────
# Applied directly from a manifest — no AddOn controller to fight our config.
# Uses dnsPolicy=Default (so the CoreDNS pod uses the node's real resolver, not
# its own ClusterIP) and expire 8m in the forward block (to recycle connections
# before NAT conntrack expiry at ~13m inside nested containers on macOS).
#
# The forward plugin needs real upstream nameservers.  Inside a nested podman
# container on macOS, /etc/resolv.conf often lists 127.0.0.11 (podman's
# embedded DNS), which is a loopback address unreachable from pod network
# namespaces.  Detect non-loopback servers from /etc/resolv.conf; fall back
# to 8.8.8.8 8.8.4.4 when nothing usable is found.

_upstream_dns="$(grep -E '^nameserver[[:space:]]' /etc/resolv.conf \
  | awk '{print $2}' \
  | grep -v '^127\.' \
  | grep -v '^::1$' \
  | tr '\n' ' ' \
  | sed 's/[[:space:]]*$//' \
  || true)"

if [[ -z "$_upstream_dns" ]]; then
  log "No non-loopback nameservers found in /etc/resolv.conf — using 8.8.8.8 8.8.4.4"
  _upstream_dns="8.8.8.8 8.8.4.4"
else
  log "CoreDNS upstream DNS: $_upstream_dns"
fi

step "Applying CoreDNS"

kubectl --kubeconfig=/etc/kubernetes/admin.conf apply -f - <<COREDNS_EOF
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: coredns
  namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: system:coredns
rules:
  - apiGroups: [""]
    resources: [endpoints, services, pods, namespaces]
    verbs: [list, watch]
  - apiGroups: [""]
    resources: [nodes]
    verbs: [get]
  - apiGroups: [discovery.k8s.io]
    resources: [endpointslices]
    verbs: [list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: system:coredns
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: system:coredns
subjects:
  - kind: ServiceAccount
    name: coredns
    namespace: kube-system
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: coredns
  namespace: kube-system
data:
  Corefile: |
    .:53 {
        errors
        health {
           lameduck 5s
        }
        ready
        kubernetes cluster.local in-addr.arpa ip6.arpa {
           pods insecure
           fallthrough in-addr.arpa ip6.arpa
           ttl 30
        }
        prometheus :9153
        forward . ${_upstream_dns} {
            expire 8m
            max_fails 0
        }
        cache 30
        reload
        loadbalance
    }
---
apiVersion: v1
kind: Service
metadata:
  name: kube-dns
  namespace: kube-system
  labels:
    k8s-app: kube-dns
    kubernetes.io/cluster-service: "true"
    kubernetes.io/name: CoreDNS
spec:
  clusterIP: 10.96.0.10
  ports:
    - name: dns
      port: 53
      protocol: UDP
      targetPort: 53
    - name: dns-tcp
      port: 53
      protocol: TCP
      targetPort: 53
    - name: metrics
      port: 9153
      protocol: TCP
      targetPort: 9153
  selector:
    k8s-app: kube-dns
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: coredns
  namespace: kube-system
  labels:
    k8s-app: kube-dns
spec:
  replicas: 1
  selector:
    matchLabels:
      k8s-app: kube-dns
  template:
    metadata:
      labels:
        k8s-app: kube-dns
    spec:
      # dnsPolicy=Default: use the node's real resolver so CoreDNS can reach
      # upstream DNS (8.8.8.8 etc.) without looping back to itself.
      dnsPolicy: Default
      serviceAccountName: coredns
      tolerations:
        - key: CriticalAddonsOnly
          operator: Exists
        - key: node-role.kubernetes.io/control-plane
          effect: NoSchedule
      containers:
        - name: coredns
          image: registry.k8s.io/coredns/coredns:v1.13.1
          imagePullPolicy: IfNotPresent
          args: ["-conf", "/etc/coredns/Corefile"]
          volumeMounts:
            - name: config-volume
              mountPath: /etc/coredns
              readOnly: true
          ports:
            - containerPort: 53
              name: dns
              protocol: UDP
            - containerPort: 53
              name: dns-tcp
              protocol: TCP
            - containerPort: 9153
              name: metrics
              protocol: TCP
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              add: [NET_BIND_SERVICE]
              drop: [ALL]
            readOnlyRootFilesystem: true
          # No readinessProbe: prevents Not-Ready flips when the forward plugin's
          # upstream health-check connection expires (~13m NAT conntrack timeout
          # inside nested containers on macOS).
      volumes:
        - name: config-volume
          configMap:
            name: coredns
            items:
              - key: Corefile
                path: Corefile
COREDNS_EOF

log "CoreDNS manifest applied"

# ── Wait for CoreDNS ──────────────────────────────────────────────────────────

step "Waiting for CoreDNS to become Ready"

for i in $(seq 1 60); do
  _dns_status="$(kubectl --kubeconfig=/etc/kubernetes/admin.conf \
    get pods -n kube-system -l k8s-app=kube-dns \
    --no-headers 2>/dev/null || true)"

  _dns_ready="$(echo "$_dns_status" | awk '{print $2}' | grep -c '1/1' || true)"
  if [[ "${_dns_ready:-0}" -ge 1 ]]; then
    log "CoreDNS pod is Ready"
    break
  fi

  # Fail fast on crash loop — no point waiting 5m
  if echo "$_dns_status" | grep -q 'CrashLoopBackOff\|Error'; then
    _dns_pod="$(kubectl --kubeconfig=/etc/kubernetes/admin.conf \
      get pods -n kube-system -l k8s-app=kube-dns \
      --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null | head -1)"
    log "CoreDNS is crash-looping. Pod status:"
    echo "$_dns_status"
    if [[ -n "$_dns_pod" ]]; then
      log "CoreDNS pod logs:"
      kubectl --kubeconfig=/etc/kubernetes/admin.conf logs -n kube-system "$_dns_pod" --tail=40 2>/dev/null || true
      log "CoreDNS previous pod logs:"
      kubectl --kubeconfig=/etc/kubernetes/admin.conf logs -n kube-system "$_dns_pod" --previous --tail=40 2>/dev/null || true
    fi
    log "Warning: CoreDNS crash-looping — continuing anyway"
    break
  fi

  if [[ $i -eq 60 ]]; then
    log "Warning: CoreDNS pod not Ready after 5m — continuing anyway"
    echo "$_dns_status"
    _dns_pod="$(kubectl --kubeconfig=/etc/kubernetes/admin.conf \
      get pods -n kube-system -l k8s-app=kube-dns \
      --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null | head -1)"
    if [[ -n "$_dns_pod" ]]; then
      log "CoreDNS describe:"
      kubectl --kubeconfig=/etc/kubernetes/admin.conf describe pod -n kube-system "$_dns_pod" 2>/dev/null | tail -40 || true
      log "CoreDNS pod logs:"
      kubectl --kubeconfig=/etc/kubernetes/admin.conf logs -n kube-system "$_dns_pod" --tail=40 2>/dev/null || true
    fi
    break
  fi

  # Every ~20s print the current status and extra context if ContainerCreating
  _mod="$(expr "$i" % 4 2>/dev/null || echo 1)"
  if [[ "$_mod" == "0" ]]; then
    _pod_state="$(echo "$_dns_status" | awk 'NR==1{print $3}' | head -1)"
    log "Attempt $i/60: ${_pod_state:-no pod yet}"
    if [[ "$_pod_state" == "ContainerCreating" ]]; then
      _dns_pod="$(kubectl --kubeconfig=/etc/kubernetes/admin.conf \
        get pods -n kube-system -l k8s-app=kube-dns \
        --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null | head -1)"
      if [[ -n "$_dns_pod" ]]; then
        log "  describe events:"
        kubectl --kubeconfig=/etc/kubernetes/admin.conf describe pod -n kube-system "$_dns_pod" 2>/dev/null \
          | grep -A5 'Events:' | tail -10 || true
      fi
      log "  recent kubelet log:"
      tail -5 /tmp/kubelet.log 2>/dev/null | sed 's/^/    /' || true
    fi
  fi
  sleep 5
done

step "Cluster ready"
kubectl --kubeconfig=/etc/kubernetes/admin.conf get nodes -o wide
kubectl --kubeconfig=/etc/kubernetes/admin.conf get pods --all-namespaces

# Write all tracked PIDs to a file so the caller can tear down the cluster.
printf '%s\n' "${_pids[@]}" > /tmp/cluster-pids.txt
log "Cluster PIDs written to /tmp/cluster-pids.txt"
