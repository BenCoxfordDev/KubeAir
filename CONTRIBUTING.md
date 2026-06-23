# Contributing to KubeAir

Thank you for your interest in contributing. This document covers how to get set up, the contribution workflow, and the standards we hold code to.

## Code of Conduct

This project follows the [CNCF Project Code of Conduct](https://github.com/cncf/foundation/blob/main/code-of-conduct.md). By participating you agree to uphold it.

## Getting Started

### Prerequisites

- Rust toolchain — pinned in `rust-toolchain.toml`, `rustup` installs it automatically
- `protoc` (Protocol Buffers compiler) for CRI gRPC stubs

```bash
# macOS
brew install protobuf

# Ubuntu/Debian
sudo apt-get install protobuf-compiler
```

### Build and test

```bash
# Build
just build

# Run all tests
just test

# Verify (must pass before submitting)
just verify

# Format
just fmt
```

## Contribution Workflow

1. **Open an issue first** for non-trivial changes. Discuss the approach before investing time in an implementation.
2. **Fork and branch** — create a feature branch from `main`.
3. **Write tests** — all changes must include unit tests. Bug fixes should include a regression test. See [Testing](#testing) below.
4. **Ensure CI passes** — `just verify` and `just test` must pass locally before pushing.
5. **Open a pull request** against `main`. Keep PRs focused; one logical change per PR.
6. **PR description** — explain *what* changed and *why*. This feeds into auto-generated release notes. Commits are squashed.

## Testing

KubeAir has four test layers:

| Layer             | Location                                 | When to add                                                      |
| ----------------- | ---------------------------------------- | ---------------------------------------------------------------- |
| Unit tests        | `#[test]` inside each crate's `src/` | Always, for domain logic                                         |
| Integration tests | `tests/integration/`                   | Cross-crate behaviour, lifecycle flows                           |
| Conformance tests | `tests/conformance/`                   | Kubernetes spec compliance                                       |
| E2E tests         | `tests/e2e/` + `hack/e2e/`           | Full cluster behaviour (run via `just e2e`) |

Run the conformance/smoke suite before submitting any change that touches pod lifecycle, container state, or the kubelet API:

```bash
just conformance

just smoke
```

## Code Standards

- **Idiomatic Rust** — follow standard Rust idioms. Prefer `?` over `unwrap`/`expect` in non-test code.
- **No panics in production paths** — use `Result` and propagate errors. Reserve `expect` for invariants that are genuinely impossible to violate.
- **Keep footprint small** — KubeAir's key value proposition is low memory and CPU usage. Avoid unnecessary allocations; prefer `Arc` sharing over cloning large structures.
- **No unsafe unless necessary** — any `unsafe` block requires a comment explaining the invariant being upheld.
- **Clippy clean** — `just verify`. Zero warnings required.
- **Formatted** — `just fmt`. All code must be formatted before merging.

## Architecture

KubeAir uses a hexagonal (ports and adapters) architecture. The layers are:

- `kubelet-core` — pure domain logic, no I/O. This is where pod lifecycle state machines, QoS classification, and configuration live.
- `kubelet-ports` — trait definitions (interfaces) for CRI, CNI, storage, node reporter, and pod source.
- `kubelet-adapters` — concrete implementations of those traits (containerd CRI, file-based pod source, volume management, eviction).
- `kubelet-app` — application layer: sync loop, HTTP server, CLI argument parsing, metrics.

Keep domain logic in `kubelet-core`. Adapters in `kubelet-adapters` should be thin — translate between the external system and the port trait, nothing more.

## Kubernetes Compatibility

KubeAir targets the two most recent Kubernetes minor releases. If your change adds or modifies behaviour that is Kubernetes-version-specific, update the compatibility table in [docs/development/releases.md](docs/development/releases.md).

## Commit Messages

Use the conventional commits format:

```
<type>: <short description>

[optional body]
```

Types: `fix`, `feat`, `perf`, `test`, `docs`, `refactor`, `chore`, `release`.

Examples:

- `fix: set Pod Ready condition in update_pod_status`
- `feat: support projected volume sources`
- `perf: cap blocking thread pool to reduce RSS`

## Questions

Open a GitHub Discussion or file an issue with the `question` label.
