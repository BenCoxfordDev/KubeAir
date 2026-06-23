
alias b := build
alias g := gazelle
alias t := test

build arch="native":
  #!/usr/bin/env bash
  set -euo pipefail

  EXTRA_FLAGS="${BAZEL_EXTRA_FLAGS:-}"

  case "{{arch}}" in
    amd64|x86_64)
      bazel build //src:kubelet_linux_x86_64 $EXTRA_FLAGS
      ;;
    arm64|aarch64)
      bazel build //src:kubelet_linux_arm64 $EXTRA_FLAGS
      ;;
    native)
      bazel build //src:main $EXTRA_FLAGS
      ;;
    *)
      echo "Unsupported arch: {{arch}}. Use one of: native, amd64, arm64"
      exit 1
      ;;
  esac

test path="//...":
  bazel test {{path}}

conformance:
  bazel test //tests/conformance:conformance_test

smoke:
  bazel test //tests/smoke:smoke_test

fmt:
  bazel run --@rules_rust//rust/settings:rustfmt.toml=//:rustfmt.toml @rules_rust//:rustfmt

bench:
  bazel run //benches:pod_operations
  bazel run //benches:server_throughput
  bazel run //benches:memory_profile

gazelle:
  bazel run //:gazelle

verify:
  #!/usr/bin/env bash
  set -euo pipefail
  
  bazel build //...
  bazel run //:cargo_deny

generate-lockfile:
  CARGO_BAZEL_REPIN=1 bazel fetch //...

lock-build-image:
  bazel run //hack/build-image:lock_amd64 -- --autofix || true
  bazel run //hack/build-image:lock_arm64 -- --autofix || true

# Build and load the CI build image into the local podman/docker daemon.
# After running this, use `BUILD_IMAGE=ghcr.io/bencoxforddev/kubeair/build:local just e2e`
# to run e2e tests against the locally-built image.
build-image arch="amd64":
  #!/usr/bin/env bash
  set -euo pipefail
  case "{{arch}}" in
    amd64|x86_64)
      bazel run //hack/build-image:load_amd64
      ;;
    arm64|aarch64)
      bazel run //hack/build-image:load_arm64
      ;;
    *)
      echo "Unsupported arch: {{arch}}. Use one of: amd64, arm64"
      exit 1
      ;;
  esac
  echo "Image loaded as ghcr.io/bencoxforddev/kubeair/build:local"
  echo "Run: just e2e-local"

# Run e2e tests against the locally-built image (requires `just build-image` first).
e2e-local:
  BUILD_IMAGE=ghcr.io/bencoxforddev/kubeair/build:local bash hack/e2e/run-e2e.sh

e2e:
  bash hack/e2e/run-e2e.sh

# Run upstream Kubernetes Go conformance/e2e tests in a local privileged container.
# Mirrors the k8s-go-e2e.yml CI workflow.
# Override env vars as needed, e.g.:
#   RUN_CONFORMANCE=1 RUN_E2E=0 just go-e2e
#   UPSTREAM_K8S_VERSION=v1.33.0 just go-e2e
go-e2e:
  bash hack/e2e/run-go-e2e.sh

go-e2e-local:
  BUILD_IMAGE=ghcr.io/bencoxforddev/kubeair/build:local bash hack/e2e/run-go-e2e.sh
