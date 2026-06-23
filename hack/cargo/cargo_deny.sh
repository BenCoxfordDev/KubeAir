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

cargo-deny check
