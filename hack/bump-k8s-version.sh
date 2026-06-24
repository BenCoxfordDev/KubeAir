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
# bump-k8s-version.sh — Update the Kubernetes version across the repository.
#
# Usage:
#   hack/bump-k8s-version.sh <new-version>
#
# Example:
#   hack/bump-k8s-version.sh v1.35.0
#
# What this script updates:
#   .version                      Canonical Kubernetes version (read by scripts/workflows/build)
#   Cargo.toml                    workspace.package.version
#   MODULE.bazel                  module version declaration
#   hack/deps/k8s-versions.bzl    K8S_VERSION constant + tool download URLs
#   hack/build-image/BUILD.bazel  image push tag (major.minor)
#   SECURITY.md                   supported-versions table
#   justfile                      UPSTREAM_K8S_VERSION example comment
#
# ⚠  MANUAL STEPS REQUIRED AFTER RUNNING:
#   hack/deps/k8s-versions.bzl contains sha256 hashes tied to specific releases.
#   These are NOT updated automatically — see the printed instructions at the end.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

log()  { printf '[bump] %s\n' "$*"; }
die()  { printf '[bump] ERROR: %s\n' "$*" >&2; exit 1; }

# ── Validate argument ──────────────────────────────────────────────────────────

NEW_VERSION="${1:-}"
if [[ -z "$NEW_VERSION" ]]; then
  echo "Usage: $0 <version>"
  echo "Example: $0 v1.35.0"
  exit 1
fi

# Normalise: ensure it has a leading 'v'
[[ "$NEW_VERSION" == v* ]] || NEW_VERSION="v${NEW_VERSION}"

# Basic semver sanity check (vX.Y.Z)
if ! [[ "$NEW_VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  die "Version must be in the form vX.Y.Z (e.g. v1.35.0), got: $NEW_VERSION"
fi

# ── Read current version ───────────────────────────────────────────────────────

OLD_VERSION="$(tr -d '[:space:]' < "$REPO_ROOT/.version")"

if [[ "$OLD_VERSION" == "$NEW_VERSION" ]]; then
  log "Already at $NEW_VERSION — nothing to do."
  exit 0
fi

# Derive bare (no 'v') and major.minor variants
OLD_BARE="${OLD_VERSION#v}"     # e.g. 1.33.0
NEW_BARE="${NEW_VERSION#v}"     # e.g. 1.35.0
OLD_MM="${OLD_BARE%.*}"         # e.g. 1.33
NEW_MM="${NEW_BARE%.*}"         # e.g. 1.35

log "Bumping Kubernetes version: $OLD_VERSION → $NEW_VERSION"
echo ""

# Helper: in-place perl substitution; works identically on macOS and Linux.
# Usage: inplace_replace <file> <perl-expression>
inplace_replace() {
  perl -i -pe "$2" "$1"
}

# ── .version ──────────────────────────────────────────────────────────────────

log "  .version"
printf '%s\n' "$NEW_VERSION" > "$REPO_ROOT/.version"

# ── Cargo.toml ────────────────────────────────────────────────────────────────

log "  Cargo.toml (workspace.package.version)"
inplace_replace "$REPO_ROOT/Cargo.toml" \
  "s{^version = \"\Q${OLD_BARE}\E\"}{version = \"${NEW_BARE}\"}"

# ── MODULE.bazel ──────────────────────────────────────────────────────────────
# Updates both the kubeair module version and all K8s tool download URLs.

log "  MODULE.bazel (module version + K8s tool URLs)"
inplace_replace "$REPO_ROOT/MODULE.bazel" \
  "s/\Q${OLD_BARE}\E/${NEW_BARE}/g"

# ── hack/deps/k8s-versions.bzl ────────────────────────────────────────────────
# K8S_VERSION constant and all tool download URL versions (kept in sync with MODULE.bazel).

log "  hack/deps/k8s-versions.bzl (K8S_VERSION + download URLs)"
inplace_replace "$REPO_ROOT/hack/deps/k8s-versions.bzl" \
  "s/\Q${OLD_BARE}\E/${NEW_BARE}/g; s/\Q${OLD_VERSION}\E/${NEW_VERSION}/g"

# ── hack/build-image/BUILD.bazel ──────────────────────────────────────────────
# Image push tags use major.minor only.

log "  hack/build-image/BUILD.bazel (image push tags)"
inplace_replace "$REPO_ROOT/hack/build-image/BUILD.bazel" \
  "s/tag = \"\Q${OLD_MM}\E-/tag = \"${NEW_MM}-/g"

# ── SECURITY.md ───────────────────────────────────────────────────────────────

log "  SECURITY.md (supported-versions table)"
inplace_replace "$REPO_ROOT/SECURITY.md" \
  "s/\Q${OLD_MM}.x\E/${NEW_MM}.x/g; s/\Q< ${OLD_MM}\E/< ${NEW_MM}/g"

# ── justfile ──────────────────────────────────────────────────────────────────

log "  justfile (UPSTREAM_K8S_VERSION example)"
inplace_replace "$REPO_ROOT/justfile" \
  "s/UPSTREAM_K8S_VERSION=\Q${OLD_VERSION}\E/UPSTREAM_K8S_VERSION=${NEW_VERSION}/g"

# ── Summary ───────────────────────────────────────────────────────────────────

echo ""
log "Done. $OLD_VERSION → $NEW_VERSION"
echo ""
printf '[bump] ⚠  MANUAL STEPS REQUIRED\n'
printf '[bump]\n'
printf '[bump] hack/deps/k8s-versions.bzl sha256 hashes are version-specific and were NOT updated.\n'
printf '[bump] Fetch and replace each one in hack/deps/k8s-versions.bzl:\n'
printf '[bump]\n'
printf '[bump]   kubectl amd64:  curl -fsSL https://dl.k8s.io/release/%s/bin/linux/amd64/kubectl | sha256sum\n' "$NEW_VERSION"
printf '[bump]   kubectl arm64:  curl -fsSL https://dl.k8s.io/release/%s/bin/linux/arm64/kubectl | sha256sum\n' "$NEW_VERSION"
printf '[bump]   kubeadm amd64:  curl -fsSL https://dl.k8s.io/release/%s/bin/linux/amd64/kubeadm | sha256sum\n' "$NEW_VERSION"
printf '[bump]   kubeadm arm64:  curl -fsSL https://dl.k8s.io/release/%s/bin/linux/arm64/kubeadm | sha256sum\n' "$NEW_VERSION"
printf '[bump]   crictl amd64:  curl -fsSL https://github.com/kubernetes-sigs/cri-tools/releases/download/%s/crictl-%s-linux-amd64.tar.gz | sha256sum\n' "$NEW_VERSION" "$NEW_VERSION"
printf '[bump]   crictl arm64:  curl -fsSL https://github.com/kubernetes-sigs/cri-tools/releases/download/%s/crictl-%s-linux-arm64.tar.gz | sha256sum\n' "$NEW_VERSION" "$NEW_VERSION"
printf '[bump]   k3s amd64:  curl -fsSL https://github.com/k3s-io/k3s/releases/download/%s%%2Bk3s1/k3s | sha256sum\n' "$NEW_VERSION"
printf '[bump]   k3s arm64:  curl -fsSL https://github.com/k3s-io/k3s/releases/download/%s%%2Bk3s1/k3s-arm64 | sha256sum\n' "$NEW_VERSION"
printf '[bump]\n'
printf '[bump] After updating hack/deps/k8s-versions.bzl, regenerate the Bazel lock file:\n'
printf '[bump]   bazel mod tidy\n'
