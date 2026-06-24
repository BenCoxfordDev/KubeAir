/*
Copyright 2026 Ben Coxford.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/// Read the Kubernetes version from the workspace-root `.version` file and
/// expose it as the `KUBERNETES_VERSION` compile-time environment variable.
///
/// The `.version` file is the single source of truth for the Kubernetes
/// version that KubeAir targets (e.g. `v1.35.0`).  Changing that file is the
/// only thing needed to update the version reported by `kubelet --version` and
/// the `kubelet_version` field written to the node's status in the API server.
fn main() {
    // CARGO_MANIFEST_DIR = <workspace>/crates/kubelet-app
    // .version lives two levels up at <workspace>/.version
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR is always set by Cargo");
    let version_path = std::path::Path::new(&manifest_dir).join("../../.version");

    let version = std::fs::read_to_string(&version_path)
        .unwrap_or_else(|e| panic!("Failed to read .version at {}: {}", version_path.display(), e))
        .trim()
        .to_string();

    println!("cargo:rustc-env=KUBERNETES_VERSION={version}");
    // Re-run this build script if .version changes.
    println!("cargo:rerun-if-changed=../../.version");
}
