"""
hack/deps/k8s-versions.bzl

Bzlmod module extension that declares the Kubernetes-versioned tool repositories
(kubectl, kubeadm, crictl, k3s) baked into the hermetic CI build image.

Used in MODULE.bazel as:
  k8s_deps_ext = use_extension("//hack/deps:k8s-versions.bzl", "k8s_deps_ext")
  use_repo(k8s_deps_ext, "kubectl_linux_amd64", ...)

To upgrade Kubernetes:
  1. Run:  hack/bump-k8s-version.sh <new-version>
     Updates .version, Cargo.toml, MODULE.bazel module version, K8S_VERSION,
     URLs in this file, and hack/build-image/BUILD.bazel image tags.
  2. Fetch new sha256 hashes and update K8S_TOOLS below + MODULE.bazel module version.
  3. Run:  bazel mod tidy

SHA-256 hashes can be fetched with:
  curl -fsSL <url> | sha256sum

k3s releases:    https://github.com/k3s-io/k3s/releases
crictl releases: https://github.com/kubernetes-sigs/cri-tools/releases
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

# Kubernetes version pinned in this repository.
# Must match .version and [workspace.package].version in Cargo.toml.
K8S_VERSION = "v1.35.0"

# Single source of truth for K8s tool URLs and sha256 hashes.
# bump-k8s-version.sh rewrites the version strings here using perl in-place substitution.
K8S_TOOLS = {
    "kubectl_linux_amd64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "9efe8d3facb23e1618cba36fb1c4e15ac9dc3ed5a2c2e18109e4a66b2bac12dc",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kubectl"],
    },
    "kubectl_linux_arm64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "48541d119455ac5bcc5043275ccda792371e0b112483aa0b29378439cf6322b9",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/arm64/kubectl"],
    },
    "kubeadm_linux_amd64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "5a65cfec0648cabec124c41be8c61040baf2ba27a99f047db9ca08cac9344987",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kubeadm"],
    },
    "kubeadm_linux_arm64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "746c0ee45f4d32ec5046fb10d4354f145ba1ff0c997f9712d46036650ad26340",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/arm64/kubeadm"],
    },
    "crictl_linux_amd64": {
        "kind": "http_archive",
        "build_file_content": 'exports_files(["crictl"])',
        "sha256": "8307399e714626e69d1213a4cd18c8dec3d0201ecdac009b1802115df8973f0f",
        "urls": ["https://github.com/kubernetes-sigs/cri-tools/releases/download/v1.35.0/crictl-v1.35.0-linux-amd64.tar.gz"],
    },
    "crictl_linux_arm64": {
        "kind": "http_archive",
        "build_file_content": 'exports_files(["crictl"])',
        "sha256": "e1f34918d77d5b4be85d48f5d713ca617698a371b049ea1486000a5e86ab1ff3",
        "urls": ["https://github.com/kubernetes-sigs/cri-tools/releases/download/v1.35.0/crictl-v1.35.0-linux-arm64.tar.gz"],
    },
    "k3s_linux_amd64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "32af5d569ecae4bf503b68c21e29885687265a514eb33c45bf0873fff4cb4b63",
        "urls": ["https://github.com/k3s-io/k3s/releases/download/v1.35.0%2Bk3s1/k3s"],
    },
    "k3s_linux_arm64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "1637c3cfaa5abb442acc87d8641299df5f6119d00e43c91d11377b6c8a118d72",
        "urls": ["https://github.com/k3s-io/k3s/releases/download/v1.35.0%2Bk3s1/k3s-arm64"],
    },
}

def _k8s_deps_impl(mctx):  # buildifier: disable=unused-variable
    for name, tool in K8S_TOOLS.items():
        if tool["kind"] == "http_file":
            http_file(
                name = name,
                executable = tool["executable"],
                sha256 = tool["sha256"],
                urls = tool["urls"],
            )
        else:
            http_archive(
                name = name,
                build_file_content = tool["build_file_content"],
                sha256 = tool["sha256"],
                urls = tool["urls"],
            )

k8s_deps_ext = module_extension(
    implementation = _k8s_deps_impl,
)
