"""
hack/deps/k8s-versions.bzl

Bzlmod module extension that declares the Kubernetes-versioned tool repositories
baked into the hermetic CI build image.

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

Kubernetes binary releases: https://dl.k8s.io/release/
etcd releases:              https://github.com/etcd-io/etcd/releases
crictl releases:            https://github.com/kubernetes-sigs/cri-tools/releases
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
    # ── Kubernetes control-plane binaries ──────────────────────────────────────
    "kube_apiserver_linux_amd64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "999e4874de50139f40929d6a9f4844efc66ad09647dc0e84031daca4711f12b6",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kube-apiserver"],
    },
    "kube_apiserver_linux_arm64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "1aed32db5585e85950560f8fdb09f4dfe6e5f3bcf81f0996f24ce4869cfcd7b8",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/arm64/kube-apiserver"],
    },
    "kube_controller_manager_linux_amd64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "d175511887be863be36c5b7ef7e859d22f08bd0e80cebca628edd48f0171c3f7",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kube-controller-manager"],
    },
    "kube_controller_manager_linux_arm64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "2cc00a6bd20db30272701922d84ab19927e1cca924f1025562de68bc7f71a2a7",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/arm64/kube-controller-manager"],
    },
    "kube_scheduler_linux_amd64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "2af8bc7d55a92e3a0534c446a73397bd16d85b015fc11216f6c53e0fbf131561",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kube-scheduler"],
    },
    "kube_scheduler_linux_arm64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "8a5451c957cb8bde11caee0a8dc2d5bc6e1b003490b7e156b8c75ec5ede6415a",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/arm64/kube-scheduler"],
    },
    "kube_proxy_linux_amd64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "6489ece90b0400cc275204601126d797f86b4b3642672227460280ee659c8f54",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kube-proxy"],
    },
    "kube_proxy_linux_arm64": {
        "kind": "http_file",
        "executable": True,
        "sha256": "d7254f50c4aea1963c534dbdbae58ac16087667ebb129bfa781bcdd9f7920972",
        "urls": ["https://dl.k8s.io/release/v1.35.0/bin/linux/arm64/kube-proxy"],
    },
    # ── etcd (separate versioning from Kubernetes) ─────────────────────────────
    # Use the upstream etcd release that matches kubeadm's DefaultEtcdVersion for
    # this Kubernetes release (kubeadm constants.go: DefaultEtcdVersion = "3.5.21").
    "etcd_linux_amd64": {
        "kind": "http_archive",
        "build_file_content": 'exports_files(["etcd", "etcdctl"])',
        "strip_prefix": "etcd-v3.5.21-linux-amd64",
        "sha256": "adddda4b06718e68671ffabff2f8cee48488ba61ad82900e639d108f2148501c",
        "urls": ["https://github.com/etcd-io/etcd/releases/download/v3.5.21/etcd-v3.5.21-linux-amd64.tar.gz"],
    },
    "etcd_linux_arm64": {
        "kind": "http_archive",
        "build_file_content": 'exports_files(["etcd", "etcdctl"])',
        "strip_prefix": "etcd-v3.5.21-linux-arm64",
        "sha256": "95bf6918623a097c0385b96f139d90248614485e781ec9bee4768dbb6c79c53f",
        "urls": ["https://github.com/etcd-io/etcd/releases/download/v3.5.21/etcd-v3.5.21-linux-arm64.tar.gz"],
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
            kwargs = dict(
                name = name,
                build_file_content = tool["build_file_content"],
                sha256 = tool["sha256"],
                urls = tool["urls"],
            )
            if "strip_prefix" in tool:
                kwargs["strip_prefix"] = tool["strip_prefix"]
            http_archive(**kwargs)

k8s_deps_ext = module_extension(
    implementation = _k8s_deps_impl,
)
