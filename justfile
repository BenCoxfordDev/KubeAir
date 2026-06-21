
alias b := build
alias g := gazelle
alias t := test

build arch="native":
  #!/usr/bin/env bash
  set -euo pipefail

  case "{{arch}}" in
    amd64|x86_64)
      bazel build //... --platforms=@rules_rust//rust/platform:linux_x86_64
      ;;
    arm64|aarch64)
      bazel build //... --platforms=@rules_rust//rust/platform:linux_arm64
      ;;
    native)
      bazel build //...
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
  bazel run @rules_rust//:rustfmt

bench:
  bazel run //benches:pod_operations
  bazel run //benches:server_throughput
  bazel run //benches:memory_profile

gazelle:
  bazel run //:gazelle

verify:
  bazel build //...
  bazel run //:cargo_deny

generate-lockfile:
  CARGO_BAZEL_REPIN=1 bazel fetch //...
