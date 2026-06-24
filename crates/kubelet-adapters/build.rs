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

/// Minimum `protoc` version required (3.7.2).
const MIN_PROTOC: (u32, u32, u32) = (3, 7, 2);

/// Verify that the `protoc` binary on PATH is at least [`MIN_PROTOC`].
/// Emits a clear, actionable error at build time if not.
fn check_protoc_version() {
    let output = std::process::Command::new("protoc")
        .arg("--version")
        .output()
        .expect("protoc not found on PATH — install protobuf-compiler >= 3.7.2");

    let stdout = String::from_utf8_lossy(&output.stdout);
    // protoc prints: "libprotoc X.Y.Z"
    let version_str = stdout
        .trim()
        .strip_prefix("libprotoc ")
        .unwrap_or_else(|| panic!("Unexpected protoc --version output: '{stdout}'"));

    let parts: Vec<u32> = version_str
        .split('.')
        .take(3)
        .map(|s| s.parse().unwrap_or(0))
        .collect();

    let actual = (
        parts.first().copied().unwrap_or(0),
        parts.get(1).copied().unwrap_or(0),
        parts.get(2).copied().unwrap_or(0),
    );

    assert!(
        actual >= MIN_PROTOC,
        "protoc {}.{}.{} is too old — kubelet-adapters requires >= {}.{}.{}",
        actual.0,
        actual.1,
        actual.2,
        MIN_PROTOC.0,
        MIN_PROTOC.1,
        MIN_PROTOC.2,
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Expose the Kubernetes version from the workspace-root `.version` file so
    // that node-status reporting can use `env!("KUBERNETES_VERSION")`.
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is always set by Cargo");
    let version_path = std::path::Path::new(&manifest_dir).join("../../.version");
    let version = std::fs::read_to_string(&version_path)
        .unwrap_or_else(|e| {
            panic!(
                "Failed to read .version at {}: {}",
                version_path.display(),
                e
            )
        })
        .trim()
        .to_string();
    println!("cargo:rustc-env=KUBERNETES_VERSION={version}");
    println!("cargo:rerun-if-changed=../../.version");

    check_protoc_version();
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/device_plugin.proto"], &["proto/"])?;
    Ok(())
}
