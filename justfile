build arch="native":
  #!/usr/bin/env bash
  set -euo pipefail

  case "{{arch}}" in
    amd64|x86_64)
      cargo zigbuild --release --target x86_64-unknown-linux-gnu
      ;;
    arm64|aarch64)
      cargo zigbuild --release --target aarch64-unknown-linux-gnu
      ;;
    native)
      cargo build --release
      ;;
    *)
      echo "Unsupported arch: {{arch}}. Use one of: native, amd64, arm64"
      exit 1
      ;;
  esac

test:
  cargo test --workspace --all-targets --all-features
  cargo test -p kubelet --test conformance -- --nocapture

conformance-smoke:
  cargo test -p kubelet --test conformance -- --nocapture

real-runtime-smoke:
  cargo test -p kubelet-cri test_health_check -- --ignored --nocapture
  cargo test -p kubelet-adapters test_cni_from_real_ci_dirs_detects_config -- --ignored --nocapture
  cargo test -p kubelet --test conformance -- --nocapture

lint:
  cargo clippy --all-targets -- -D warnings
  cargo fmt --all -- --check
  cargo deny check

auto-fix:
  cargo clippy --fix --workspace --all-targets --all-features --allow-dirty --allow-staged

generate-lockfile:
  cargo generate-lockfile

fmt:
  cargo fmt --all
