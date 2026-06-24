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
        "sha256": "a2e984a18a0c063279d692533031c1eff93a262afcc0afdc517375432d060989",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kubectl"],
    },
    "kubectl_linux_arm64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "58f82f9fe796c375c5c4b8439850b0f3f4d401a52434052f2df46035a8789e25",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/arm64/kubectl"],
    },
    "kubeadm_linux_amd64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "729e7fb34e4f1bfcf2bdaf2a14891ed64bd18c47aaab42f8cc5030875276cfed",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kubeadm"],
    },
    "kubeadm_linux_arm64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "1dac7dc2c6a56548bbc6bf8a7ecf4734f2e733fb336d7293d84541ebe52d0e50",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/arm64/kubeadm"],
    },
    "crictl_linux_amd64": {
        "kind": "http_archive",
        "build_file_content": 'exports_files(["crictl"])',
        "sha256": "2e141e5b22cb189c40365a11807d69b76b9b3caced89fac2f4ec879408ce2177",
        "urls": ["https://github.com/kubernetes-sigs/cri-tools/releases/download/v1.35.0/crictl-v1.35.0-linux-amd64.tar.gz"],
    },
    "crictl_linux_arm64": {
        "kind": "http_archive",
        "build_file_content": 'exports_files(["crictl"])',
        "sha256": "519071de89b64c43e2a1661bb5489c6c3fd5e9e5fcef75e50e542b0c891f1118",
        "urls": ["https://github.com/kubernetes-sigs/cri-tools/releases/download/v1.35.0/crictl-v1.35.0-linux-arm64.tar.gz"],
    },
    "k3s_linux_amd64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "959c9310a6ab893958d1c95bc5d7609de9d7884630c8832180f059369b6dc331",
        "urls": ["https://github.com/k3s-io/k3s/releases/download/v1.35.0%2Bk3s1/k3s"],
    },
    "k3s_linux_arm64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "c0b1673a1f2b740cb7bb08355c885efb6a4aa5c8022e5bb306c621c8c1492883",
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
