# Release Management

This document defines the release process, versioning strategy, and Kubernetes compatibility policy for KubeAir.

## Versioning Strategy

KubeAir uses a **Kubernetes-aligned versioning scheme**:

```
<k8s-major>.<k8s-minor>.<patch>
```

- The `major.minor` component tracks the upstream Kubernetes release the kubelet is compatible with (e.g., `1.33.x` means "compatible with Kubernetes 1.33 clusters").
- The `patch` component increments for bug fixes, security patches, and minor improvements within a Kubernetes minor cycle.

**Examples:**

| KubeAir Version | Meaning                                        |
| --------------- | ---------------------------------------------- |
| `1.33.0`      | First release targeting Kubernetes 1.33        |
| `1.33.1`      | Patch release: bug fix, same K8s compatibility |
| `1.34.0`      | First release targeting Kubernetes 1.34        |

### Why not pure SemVer?

KubeAir is a node agent. Its API contract is with the Kubernetes API server, not with library consumers. Aligning `major.minor` with the Kubernetes version makes compatibility immediately legible without requiring a separate compatibility matrix lookup. This mirrors conventions used by `kube-proxy` and other Kubernetes components.

### Patch version increments

Increment `patch` for:

- Bug fixes and correctness improvements
- Security dependency updates
- Performance improvements
- Documentation-only releases do **not** get a tag

## Release Cadence

Kubernetes releases three minor versions per year, approximately every **4 months**:

| Month    | Kubernetes Release |
| -------- | ------------------ |
| April    | 1.N (e.g., 1.33)   |
| August   | 1.N+1 (e.g., 1.34) |
| December | 1.N+2 (e.g., 1.35) |

KubeAir targets **one release per Kubernetes minor**, made within **4 weeks** of the upstream Kubernetes release. Patch releases are made as needed between Kubernetes minor cycles for fixes.

KubeAir tracks the **two most recent Kubernetes minor releases** as actively supported. Older versions receive critical security patches only.

## Keeping Current with Kubernetes Releases

When a new Kubernetes minor is released:

1. **Update `k8s-openapi`** in `Cargo.toml` to the new version feature flag (e.g., `features = ["v1_34"]`).
2. **Update `kube`** to the latest release compatible with the new API version.
3. **Re-pin dependencies** by running `just generate-lockfile` to pull latest patch versions of all dependencies.
4. **Re-run the full test suite** (`just test`) and the live cluster e2e suite (`bash hack/e2e/colima-run.sh`) against a cluster running the new Kubernetes version. All conformance and e2e tests must pass before a release is tagged.
5. **Update the compatibility table** in this file.
6. **Update `[workspace.package] version`** in `Cargo.toml` to the new `major.minor.0`.
7. **Tag and push** (see release steps below).

### Tracking upstream Kubernetes changes

Monitor these resources for changes that affect kubelet behaviour:

- [Kubernetes Changelog](https://github.com/kubernetes/kubernetes/blob/master/CHANGELOG/) — scan `kubelet` section for each release
- [KEP tracker](https://kep.k8s.io/) — filter by SIG-Node for upcoming kubelet features
- [k8s-openapi releases](https://github.com/Arnavion/k8s-openapi/releases) — signals when new API types are available
- [kube-rs releases](https://github.com/kube-rs/kube/releases) — client library updates

Subscribe to `kubernetes-dev` digest and SIG-Node meeting notes to stay ahead of API deprecations.

## Release Process

Releases are **fully automated** via [`.github/workflows/release.yml`](../../.github/workflows/release.yml). The only manual step is bumping the version in `Cargo.toml` and merging to `main`.

### How the CI pipeline works

```
PR to main (Cargo.toml changed)
          │
          ▼
    ┌─────────────┐
    │   prepare   │  Reads [workspace.package] version from Cargo.toml.
    │             │  Checks if tag vX.Y.Z already exists on origin.
    │             │  If not: creates and pushes the git tag automatically.
    └──────┬──────┘
           │ (if new tag)
           ▼
    ┌──────────────────┐
    │  create_release  │  Creates a GitHub Release for the tag.
    │                  │  Release notes are auto-generated from
    │                  │  merged PRs and commits since last tag.
    └──────┬───────────┘
           │
           ▼
    ┌──────────────────────────┐
    │   build_and_upload       │  Matrix: amd64 + arm64 (ubuntu-24.04).
    │   (matrix: amd64, arm64) │  Builds with --locked --release.
    │                          │  Packages: kubelet-vX.Y.Z-linux_<arch>.tar.gz
    │                          │  Uploads binary + .sha256 to the release.
    └──────────────────────────┘
```

The idempotency check (tag already exists → skip) means it is safe to push other changes to `Cargo.toml` without cutting an unintended release, as long as the version number itself has not changed.

If a release already exists and you need to rebuild the artifacts for the same version, run the `Release` workflow manually with `rebuild_assets=true` and `tag=vX.Y.Z`. That path checks out the tagged commit, rebuilds both archives, and replaces the matching assets on the existing GitHub release without changing `Cargo.toml`.

### Cutting a release

1. **Complete the pre-release checklist** (see below).
2. **Bump `[workspace.package] version`** in `Cargo.toml` to the new version (e.g., `1.33.1`).
3. **Merge to `main`** — the CI pipeline takes over: it tags, creates the GitHub Release, builds both architectures, and uploads the artifacts.

No manual `git tag` or binary builds are required.

### Patching an existing tagged release

Sometimes a critical fix must be applied to an already-tagged version without bumping to a new version (e.g., rebuilding artifacts after a CI failure, or hot-patching a security issue on a tag that CI missed).

**Scenario A — Rebuild artifacts only (no code change)**

Run the `Release` workflow manually from the GitHub Actions UI:

- `rebuild_assets=true`
- `tag=vX.Y.Z`

The pipeline checks out the existing tag, rebuilds both `amd64` and `arm64` archives, and replaces the matching assets on the GitHub Release without touching `Cargo.toml` or creating a new tag.

**Scenario B — Code fix where `main` is still on the same minor**

Use this when `main` is still at `1.33.x` (i.e. the fix and the next release share the same minor):

1. Create a branch from the existing tag: `git checkout -b fix/1.33.1 v1.33.0`
2. Apply the fix and update `[workspace.package] version` in `Cargo.toml` to `1.33.1`.
3. Open a PR against `main` and merge — CI cuts `v1.33.1` automatically via the normal pipeline.

**Scenario C — Code fix where `main` has already moved to a newer minor**

Use this when `main` is already at `1.34.x` (or later) but `1.33.x` still needs a patch (e.g. a security fix for a supported older minor):

1. Create a long-lived maintenance branch from the last good tag if one does not already exist:
   ```bash
   git checkout -b release/v1.33 v1.33.0
   git push origin release/v1.33
   ```
2. Branch off that maintenance branch for the fix:
   ```bash
   git checkout -b patch/v1.33.1 release/v1.33
   ```
3. Apply the fixes and bump `[workspace.package] version` in `Cargo.toml` to `1.33.1`.
4. Open a PR **against `release/1.33`** (not `main`) and merge.
5. If the fix also applies to `main`, open a separate cherry-pick PR against `main`.
6. Merging into `release/1.33` triggers the CI release pipeline automatically (same as `main`) — it tags `v1.33.1` and publishes the release artifacts.

### Pre-release checklist

- [ ] `just verify` passes with zero warnings and no policy violations
- [ ] `just test` passes (all unit, integration, and conformance suites)
- [ ] E2E suite passes: 0 failures (`bash hack/e2e/colima-run.sh`)
- [ ] Dependencies audited: `cargo audit` reports no high/critical advisories
- [ ] `[workspace.package] version` in `Cargo.toml` is updated
- [ ] Compatibility table in this file is updated
- [ ] PR description summarises changes (used as auto-generated release notes source)

### Release artifacts

The pipeline produces two artifacts per release, downloadable from the GitHub Releases page:

| File                                         | Description                              |
| -------------------------------------------- | ---------------------------------------- |
| `kubelet-vX.Y.Z-linux_amd64.tar.gz`        | amd64 binary (x86_64-unknown-linux-gnu)  |
| `kubelet-vX.Y.Z-linux_amd64.tar.gz.sha256` | SHA-256 checksum                         |
| `kubelet-vX.Y.Z-linux_arm64.tar.gz`        | arm64 binary (aarch64-unknown-linux-gnu) |
| `kubelet-vX.Y.Z-linux_arm64.tar.gz.sha256` | SHA-256 checksum                         |

To build locally for testing before merging:

```bash
just build amd64   # → bazel-bin/src/kubelet_linux_x86_64
just build arm64   # → bazel-bin/src/kubelet_linux_arm64
```

## Security and Vulnerabilities

### Dependency auditing

Run `cargo audit` before every release. Address all **critical** and **high** severity advisories before tagging. Medium severity advisories should be resolved within 30 days or explicitly acknowledged.

```bash
cargo install cargo-audit  # one-time install
cargo audit
```

For dependency policy checks (license, bans, advisories), run `just verify` which includes `cargo_deny`.

### Vulnerability disclosure

- Security issues should be reported privately before public disclosure.
- Critical CVEs in transitive dependencies (e.g., OpenSSL/rustls, tokio) warrant an out-of-band patch release, bypassing the normal Kubernetes release cadence.
- Security patch releases follow the same tagging process but should be released within **48 hours** of a verified fix.

### Rust toolchain security

The pinned toolchain is defined in `rust-toolchain.toml`. Update the toolchain version:

- When the current channel reaches end-of-life
- When a security advisory affects the Rust standard library or compiler
- At minimum once per Kubernetes minor cycle

## Kubernetes Version Compatibility

The table below tracks which KubeAir release targets which Kubernetes cluster version, and the corresponding `k8s-openapi` feature flag and Rust toolchain used.

| KubeAir Version | Kubernetes Version | k8s-openapi feature | Rust Toolchain | Status |
| --------------- | ------------------ | ------------------- | -------------- | ------ |
| `1.33.x`      | 1.33               | `v1_30`           | 1.96.0         | Active |

> **Note:** `k8s-openapi` version lag is normal — the crate lags slightly behind Kubernetes releases. Use the highest available feature flag that is ≤ the target cluster version.

### Supported Kubernetes versions

KubeAir guarantees conformance against the **current** and **previous** Kubernetes minor release. Clusters more than two minors behind are unsupported but may work.

### Version skew policy

KubeAir follows the [Kubernetes version skew policy](https://kubernetes.io/docs/setup/release/version-skew-policy/) for kubelets: the kubelet may be at most **2 minor versions behind** the kube-apiserver, and must never be newer than the kube-apiserver.

## Rust Toolchain Compatibility

The Rust toolchain version is pinned in `rust-toolchain.toml`. Consumers building from source need exactly this toolchain; `rustup` will install it automatically.

When updating the toolchain:

1. Edit `rust-toolchain.toml` with the new channel version.
2. Run `just verify && just test` to confirm no regressions.
3. Update the compatibility table above.
4. Include the toolchain bump in the release notes.

There is no minimum supported Rust version (MSRV) guarantee for KubeAir — consumers should always build with the pinned toolchain. This avoids the maintenance burden of testing against multiple Rust releases for a node-agent binary that is not distributed as a library.
