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

//! Integration tests for projected volume mount behaviour.
//!
//! Focuses on regression coverage for the bug where secrets inside a projected
//! volume always received hardcoded 0o600 permissions instead of honouring the
//! volume's `defaultMode`.
//!
//! Mirrors:
//!   [sig-storage] Projected secret should be consumable from pods in volume
//!   with defaultMode set [LinuxOnly]

use kubelet_adapters::volume::configmap::ConfigMapData;
use kubelet_adapters::volume::projected::{
    ProjectedVolumeData, ProjectedVolumeManager, ProjectedVolumeSource,
};
use kubelet_adapters::volume::secret::SecretData;
use std::collections::HashMap;
use tempfile::TempDir;

fn secret(name: &str) -> SecretData {
    SecretData {
        namespace: "default".to_string(),
        name: name.to_string(),
        data: [("token".to_string(), b"supersecret".to_vec())]
            .into_iter()
            .collect(),
        secret_type: "Opaque".to_string(),
    }
}

fn configmap(name: &str) -> ConfigMapData {
    ConfigMapData {
        namespace: "default".to_string(),
        name: name.to_string(),
        data: [("app.conf".to_string(), "debug=true".to_string())]
            .into_iter()
            .collect(),
        binary_data: HashMap::new(),
    }
}

/// Projected secret files must honour `defaultMode`, not the old hardcoded 0o600.
/// Regression test for: projected Secret always mounted with 0o600 regardless of
/// the volume's defaultMode field.
#[cfg(unix)]
#[test]
fn integ_projected_secret_respects_default_mode_644() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    let mgr = ProjectedVolumeManager::new(dir.path());
    let target = dir.path().join("projected");

    let mut secrets = HashMap::new();
    secrets.insert("my-secret".to_string(), secret("my-secret"));

    let sources = vec![ProjectedVolumeSource::Secret {
        name: "my-secret".to_string(),
        namespace: "default".to_string(),
        items: vec![],
        optional: false,
    }];

    mgr.mount(
        &sources,
        &target,
        0o644,
        ProjectedVolumeData {
            configmaps: &HashMap::new(),
            secrets: &secrets,
            sa_token: None,
        },
    )
    .unwrap();

    let mode = std::fs::metadata(target.join("token"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o644,
        "Projected secret file must use defaultMode 0o644, not hardcoded 0o600; got {:#o}",
        mode
    );
}

/// A defaultMode of 0o600 must still produce 0o600 files (no regression for strict mode).
#[cfg(unix)]
#[test]
fn integ_projected_secret_respects_default_mode_600() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    let mgr = ProjectedVolumeManager::new(dir.path());
    let target = dir.path().join("projected");

    let mut secrets = HashMap::new();
    secrets.insert("my-secret".to_string(), secret("my-secret"));

    let sources = vec![ProjectedVolumeSource::Secret {
        name: "my-secret".to_string(),
        namespace: "default".to_string(),
        items: vec![],
        optional: false,
    }];

    mgr.mount(
        &sources,
        &target,
        0o600,
        ProjectedVolumeData {
            configmaps: &HashMap::new(),
            secrets: &secrets,
            sa_token: None,
        },
    )
    .unwrap();

    let mode = std::fs::metadata(target.join("token"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
}

/// When a projected volume combines a ConfigMap and a Secret, both must use the
/// same `defaultMode` — the pre-bug code would give the ConfigMap 0o644 but the
/// Secret hardcoded 0o600.
#[cfg(unix)]
#[test]
fn integ_projected_configmap_and_secret_share_default_mode() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    let mgr = ProjectedVolumeManager::new(dir.path());
    let target = dir.path().join("projected");

    let mut cms = HashMap::new();
    cms.insert("my-cm".to_string(), configmap("my-cm"));

    let mut secrets = HashMap::new();
    secrets.insert("my-secret".to_string(), secret("my-secret"));

    let sources = vec![
        ProjectedVolumeSource::ConfigMap {
            name: "my-cm".to_string(),
            namespace: "default".to_string(),
            items: vec![],
            optional: false,
        },
        ProjectedVolumeSource::Secret {
            name: "my-secret".to_string(),
            namespace: "default".to_string(),
            items: vec![],
            optional: false,
        },
    ];

    // Use an unusual mode to make it obvious if either source ignores it.
    mgr.mount(
        &sources,
        &target,
        0o440,
        ProjectedVolumeData {
            configmaps: &cms,
            secrets: &secrets,
            sa_token: None,
        },
    )
    .unwrap();

    let cm_mode = std::fs::metadata(target.join("app.conf"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    let sec_mode = std::fs::metadata(target.join("token"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(
        cm_mode, 0o440,
        "ConfigMap file in projected volume should be 0o440, got {:#o}",
        cm_mode
    );
    assert_eq!(
        sec_mode, 0o440,
        "Secret file in projected volume should be 0o440 (same as ConfigMap), got {:#o}",
        sec_mode
    );
}
