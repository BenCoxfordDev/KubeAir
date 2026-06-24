#!/usr/bin/env bash
set -euo pipefail

cd "$BUILD_WORKSPACE_DIRECTORY"

# cargo-deny invokes `cargo metadata` internally to resolve the dependency graph.
# In the hermetic CI container, cargo is not on PATH (Rust is managed by Bazel).
# After `bazel build //...` runs, the Rust toolchain is already downloaded into
# Bazel's output base — locate cargo there so cargo-deny can invoke it.
if ! command -v cargo >/dev/null 2>&1; then
  _output_base=$(bazel info output_base 2>/dev/null)
  _cargo=$(find "$_output_base/external" -maxdepth 5 -name "cargo" \
    -path "*/bin/cargo" -type f 2>/dev/null | head -1)
  if [ -n "$_cargo" ]; then
    export PATH="$(dirname "$_cargo"):$PATH"
  else
    echo "error: cargo not found; run 'bazel build //...' first to download the Rust toolchain" >&2
    exit 1
  fi
  unset _output_base _cargo
fi

# Find cargo-deny from Bazel runfiles if not already on PATH.
if ! command -v cargo-deny >/dev/null 2>&1; then
  # RUNFILES_DIR is set by `bazel run`; fall back to $0.runfiles for direct invocation.
  _runfiles="${RUNFILES_DIR:-${0}.runfiles}"
  for _cd_dir in "$_runfiles"/*cargo_deny_*; do
    if [ -x "$_cd_dir/cargo-deny" ]; then
      export PATH="$_cd_dir:$PATH"
      break
    fi
  done
  unset _runfiles _cd_dir
fi

# If still not found, search Bazel's output_base/external (covers local `bazel run`).
if ! command -v cargo-deny >/dev/null 2>&1; then
  _output_base=$(bazel info output_base 2>/dev/null)
  _cd=$(find "$_output_base/external" -maxdepth 3 -name "cargo-deny" -type f 2>/dev/null | head -1)
  if [ -n "$_cd" ]; then
    export PATH="$(dirname "$_cd"):$PATH"
  fi
  unset _output_base _cd
fi

cargo-deny check
