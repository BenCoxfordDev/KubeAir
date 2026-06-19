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

//! Secret volume plugin.
//!
//! Mounts a Secret's data as files, enforcing 0o644 default with 0o600
//! for defaultMode if not specified (secrets are sensitive).
//! Mirrors pkg/volume/secret/secret.go.

use kubelet_core::error::{KubeletError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// A resolved Secret.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretData {
    pub namespace: String,
    pub name: String,
    /// Key -> base64-decoded bytes.
    pub data: HashMap<String, Vec<u8>>,
    /// Secret type (e.g. "kubernetes.io/dockerconfigjson").
    pub secret_type: String,
}

impl SecretData {
    pub fn string_value(&self, key: &str) -> Option<String> {
        self.data
            .get(key)
            .and_then(|b| String::from_utf8(b.clone()).ok())
    }
}

/// Manages Secret volume mounts.
pub struct SecretVolumeManager {
    base_dir: PathBuf,
}

impl SecretVolumeManager {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Mount a Secret at `target_path`.
    /// Secret files are created with `default_mode` (default: 0o644; use 0o600 for sensitive).
    pub fn mount(
        &self,
        secret: &SecretData,
        target_path: &Path,
        items: &[(String, String)],
        default_mode: u32,
    ) -> Result<()> {
        std::fs::create_dir_all(target_path)
            .map_err(|e| KubeletError::Storage(format!("create secret dir: {}", e)))?;

        let to_mount: Vec<(String, &[u8])> = if items.is_empty() {
            secret
                .data
                .iter()
                .map(|(k, v)| (k.clone(), v.as_slice()))
                .collect()
        } else {
            items
                .iter()
                .filter_map(|(key, path)| {
                    secret.data.get(key).map(|v| (path.clone(), v.as_slice()))
                })
                .collect()
        };

        for (filename, content) in &to_mount {
            let filename = sanitize_filename(filename);
            let file_path = target_path.join(&filename);

            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| KubeletError::Storage(format!("create secret subdir: {}", e)))?;
            }

            std::fs::write(&file_path, content).map_err(|e| {
                KubeletError::Storage(format!("write secret key '{}': {}", filename, e))
            })?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(default_mode))
                    .map_err(|e| KubeletError::Storage(format!("chmod secret key: {}", e)))?;
            }

            debug!(key = %filename, "Mounted Secret key");
        }

        // Additionally secure the directory itself.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(target_path, std::fs::Permissions::from_mode(0o700));
        }

        info!(
            secret = %format!("{}/{}", secret.namespace, secret.name),
            target = %target_path.display(),
            "Secret volume mounted"
        );
        Ok(())
    }

    pub fn unmount(&self, target_path: &Path) -> Result<()> {
        if target_path.exists() {
            std::fs::remove_dir_all(target_path)
                .map_err(|e| KubeletError::Storage(format!("remove secret dir: {}", e)))?;
        }
        Ok(())
    }

    pub fn staging_path(&self, pod_uid: &str, volume_name: &str) -> PathBuf {
        self.base_dir
            .join("pods")
            .join(pod_uid)
            .join("volumes")
            .join("kubernetes.io~secret")
            .join(volume_name)
    }
}

fn sanitize_filename(name: &str) -> String {
    name.replace("../", "").replace("..", "")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_secret() -> SecretData {
        SecretData {
            namespace: "default".to_string(),
            name: "db-creds".to_string(),
            data: [
                ("username".to_string(), b"admin".to_vec()),
                ("password".to_string(), b"s3cr3t".to_vec()),
            ]
            .into_iter()
            .collect(),
            secret_type: "Opaque".to_string(),
        }
    }

    #[test]
    fn test_mount_all_keys() {
        let dir = TempDir::new().unwrap();
        let mgr = SecretVolumeManager::new(dir.path());
        let target = dir.path().join("secret");
        mgr.mount(&sample_secret(), &target, &[], 0o644).unwrap();
        assert!(target.join("username").exists());
        assert!(target.join("password").exists());
        assert_eq!(std::fs::read(target.join("username")).unwrap(), b"admin");
    }

    #[test]
    fn test_mount_specific_items() {
        let dir = TempDir::new().unwrap();
        let mgr = SecretVolumeManager::new(dir.path());
        let target = dir.path().join("secret");
        let items = vec![("username".to_string(), "user".to_string())];
        mgr.mount(&sample_secret(), &target, &items, 0o600).unwrap();
        assert!(target.join("user").exists());
        assert!(!target.join("password").exists());
    }

    #[test]
    fn test_string_value() {
        let s = sample_secret();
        assert_eq!(s.string_value("username").unwrap(), "admin");
    }

    #[test]
    fn test_unmount_cleans_up() {
        let dir = TempDir::new().unwrap();
        let mgr = SecretVolumeManager::new(dir.path());
        let target = dir.path().join("secret");
        mgr.mount(&sample_secret(), &target, &[], 0o644).unwrap();
        mgr.unmount(&target).unwrap();
        assert!(!target.exists());
    }
}
