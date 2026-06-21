# CLAUDE.md — KubeAir Agent Guide

This file provides context for AI coding agents (Claude, Copilot, etc.) working in this repository.
It covers build/test commands, project conventions, architecture, and links to key documents.

## Key Documents

| Document | Purpose |
|---|---|
| [CONTRIBUTING.md](CONTRIBUTING.md) | Contribution workflow, PR standards, code standards |
| [SECURITY.md](SECURITY.md) | Vulnerability reporting, disclosure policy, scope |
| [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) | Community standards |
| [docs/development/development.md](docs/development/development.md) | Architecture deep-dive, Go→Rust mapping |
| [docs/development/cheatsheet.md](docs/development/cheatsheet.md) | Quick command reference |
| [docs/development/releases.md](docs/development/releases.md) | Release process |

## Rust Toolchain

The toolchain is pinned in `rust-toolchain.toml`. `rustup` installs it automatically on first use.

```
channel = "1.96.0"
```

## Build Commands

```bash
# Native build to machine
just build

# Cross-compile for Linux amd64 (requires cargo-zigbuild)
just build amd64

# Cross-compile for Linux arm64
just build arm64
```

## Testing

KubeAir has five test layers - unit, integration, conformance, smoke, and e2e. Always run the relevant layers before opening a PR.

```bash
just test
```

### Conformance tests

Kubernetes spec compliance in `tests/conformance/`. **Run before any change touching pod lifecycle, container state, or the kubelet API:**

```bash
just conformance
```

### Smoke tests

```bash
just smoke
```

### E2E tests

Full cluster tests in `tests/e2e/`. They are `#[ignore]` and require a running Kubernetes cluster (KUBECONFIG set or `/etc/kubernetes/admin.conf` present) and `kubectl` on PATH.

```bash
# Provision a local Colima cluster and run everything
bash hack/e2e/colima-run.sh

# Run a specific e2e test against an existing cluster
cargo test --test workload_features_test -- --ignored e2e_configmap_env_injection --nocapture
```

## Linting and Formatting

CI fails on any clippy warning or formatting deviation. Always run before pushing:

```bash
# Check lint and format
just verify

# Format code
just fmt
```

## Benchmarks

```bash
# run benchmark tests
just benchmark 
```

HTML reports are written to `target/criterion/`.

## Architecture Overview

KubeAir uses a **(ports and adapters)** architecture. Dependencies flow inward — the domain layer has no I/O.

```
src/main.rs
    └── kubelet-app      (sync loop, HTTP server, CLI, metrics)
            ├── kubelet-ports    (port traits: CRI, CNI, storage, reporter, source)
            └── kubelet-adapters (concrete adapters: mock CRI, volume, eviction, prober…)
                    └── kubelet-core   (pure domain: types, FSM, pod manager, QoS)
```

See [docs/development/development.md](docs/development/development.md) for the full diagram and Go→Rust package mapping.

## Crate Map

| Crate | Role |
|---|---|
| `kubelet-core` | Pure domain: types, lifecycle FSM, pod manager, QoS |
| `kubelet-ports` | Port traits: CRI, CNI, storage, node reporter, pod source |
| `kubelet-adapters` | Concrete adapters: mock CRI, file pod source, eviction, volume |
| `kubelet-app` | Application: sync loop, HTTP server, CLI, metrics |
| `kubelet` (root) | Binary entry point (`src/main.rs`) |

---

## Code Conventions

- **Errors** — use `Result` and `?` in production paths. Never `unwrap`/`expect` outside tests or documented invariants.
- **No panics in production** — if a condition is genuinely impossible, add a comment explaining why before using `expect`.
- **No unnecessary allocations** — KubeAir's value is low memory/CPU. Prefer `Arc` sharing over cloning large structures.
- **`unsafe`** — requires a comment explaining the invariant being upheld. Minimise scope.
- **Clippy clean** — `just verify` runs `cargo clippy -- -D warnings`. Zero warnings required.
- **Formatted** — all code must pass `cargo fmt` before merging.

## Contributing Workflow

1. Open an issue before non-trivial changes.
2. Fork, create a branch from `main`.
3. Write tests: unit tests always; regression tests for bug fixes.
4. `just verify && just test` must pass locally.
5. Open a PR against `main` — one logical change per PR.
6. Squash commits; PR description feeds auto-generated release notes.

Full details: [CONTRIBUTING.md](CONTRIBUTING.md)

---

## Security

Do **not** open public GitHub issues for vulnerabilities. Report privately via
[GitHub Security Advisories](https://github.com/BenCoxfordDev/kubeair/security/advisories/new).

`cargo audit` is run before every release. Full policy: [SECURITY.md](SECURITY.md)
