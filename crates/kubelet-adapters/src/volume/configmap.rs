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

//! ConfigMap volume plugin.
//!
//! Mounts a ConfigMap's data as files in a directory.
//! Mirrors pkg/volume/configmap/configmap.go.

use kubelet_core::error::{KubeletError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// A resolved ConfigMap (fetched from API server or cache).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigMapData {
    pub namespace: String,
    pub name: String,
    /// Key -> string value.
    pub data: HashMap<String, String>,
    /// Key -> binary value (binaryData).
    pub binary_data: HashMap<String, Vec<u8>>,
}

/// Manages ConfigMap volume mounts.
pub struct ConfigMapVolumeManager {
    /// Root directory where ConfigMap volumes are staged.
    base_dir: PathBuf,
}

impl ConfigMapVolumeManager {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Mount a ConfigMap volume at `target_path`.
    ///
    /// Each key in the ConfigMap becomes a file in the target directory.
    /// If `items` is specified, only those keys are mounted (with optional path rename).
    pub fn mount(
        &self,
        cm: &ConfigMapData,
        target_path: &Path,
        items: &[(String, String)], // key -> path mapping (empty = all keys)
        default_mode: u32,
    ) -> Result<()> {
        std::fs::create_dir_all(target_path)
            .map_err(|e| KubeletError::Storage(format!("create configmap dir: {}", e)))?;

        let to_mount: Vec<(String, Vec<u8>)> = if items.is_empty() {
            // Mount all keys.
            let mut all = vec![];
            for (k, v) in &cm.data {
                all.push((k.clone(), v.as_bytes().to_vec()));
            }
            for (k, v) in &cm.binary_data {
                all.push((k.clone(), v.clone()));
            }
            all
        } else {
            // Mount only the specified items.
            items
                .iter()
                .filter_map(|(key, path)| {
                    if let Some(v) = cm.data.get(key) {
                        Some((path.clone(), v.as_bytes().to_vec()))
                    } else {
                        cm.binary_data.get(key).map(|v| (path.clone(), v.clone()))
                    }
                })
                .collect()
        };

        for (filename, content) in to_mount {
            // Prevent path traversal.
            let filename = sanitize_filename(&filename);
            let file_path = target_path.join(&filename);

            // Create parent directories if the item path has subdirs.
            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    KubeletError::Storage(format!("create configmap subdir: {}", e))
                })?;
            }

            std::fs::write(&file_path, &content).map_err(|e| {
                KubeletError::Storage(format!("write configmap key '{}': {}", filename, e))
            })?;

            // Set permissions.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(default_mode))
                    .map_err(|e| KubeletError::Storage(format!("chmod configmap key: {}", e)))?;
            }

            debug!(key = %filename, "Mounted ConfigMap key");
        }

        info!(
            configmap = %format!("{}/{}", cm.namespace, cm.name),
            target = %target_path.display(),
            "ConfigMap volume mounted"
        );
        Ok(())
    }

    /// Unmount a ConfigMap volume (remove the target directory).
    pub fn unmount(&self, target_path: &Path) -> Result<()> {
        if target_path.exists() {
            std::fs::remove_dir_all(target_path)
                .map_err(|e| KubeletError::Storage(format!("remove configmap dir: {}", e)))?;
        }
        Ok(())
    }

    /// Get the staging path for this volume within the kubelet root.
    pub fn staging_path(&self, pod_uid: &str, volume_name: &str) -> PathBuf {
        self.base_dir
            .join("pods")
            .join(pod_uid)
            .join("volumes")
            .join("kubernetes.io~configmap")
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

    fn sample_cm() -> ConfigMapData {
        ConfigMapData {
            namespace: "default".to_string(),
            name: "my-config".to_string(),
            data: [
                (
                    "app.conf".to_string(),
                    "debug=true\nport=8080\n".to_string(),
                ),
                ("log.conf".to_string(), "level=info\n".to_string()),
            ]
            .into_iter()
            .collect(),
            binary_data: HashMap::new(),
        }
    }

    #[test]
    fn test_mount_all_keys() {
        let dir = TempDir::new().unwrap();
        let mgr = ConfigMapVolumeManager::new(dir.path());
        let target = dir.path().join("target");
        let cm = sample_cm();
        mgr.mount(&cm, &target, &[], 0o644).unwrap();
        assert!(target.join("app.conf").exists());
        assert!(target.join("log.conf").exists());
        let content = std::fs::read_to_string(target.join("app.conf")).unwrap();
        assert!(content.contains("debug=true"));
    }

    #[test]
    fn test_mount_specific_items() {
        let dir = TempDir::new().unwrap();
        let mgr = ConfigMapVolumeManager::new(dir.path());
        let target = dir.path().join("target");
        let cm = sample_cm();
        let items = vec![("app.conf".to_string(), "config/app".to_string())];
        mgr.mount(&cm, &target, &items, 0o644).unwrap();
        assert!(target.join("config/app").exists());
        assert!(!target.join("log.conf").exists());
    }

    #[test]
    fn test_unmount_removes_dir() {
        let dir = TempDir::new().unwrap();
        let mgr = ConfigMapVolumeManager::new(dir.path());
        let target = dir.path().join("target");
        let cm = sample_cm();
        mgr.mount(&cm, &target, &[], 0o644).unwrap();
        assert!(target.exists());
        mgr.unmount(&target).unwrap();
        assert!(!target.exists());
    }

    #[test]
    fn test_sanitize_prevents_traversal() {
        assert_eq!(sanitize_filename("../etc/passwd"), "etc/passwd");
        assert_eq!(sanitize_filename("safe.conf"), "safe.conf");
    }

    #[test]
    fn test_staging_path() {
        let dir = TempDir::new().unwrap();
        let mgr = ConfigMapVolumeManager::new(dir.path());
        let path = mgr.staging_path("uid-1", "my-config");
        assert!(path.to_str().unwrap().contains("kubernetes.io~configmap"));
        assert!(path.to_str().unwrap().contains("my-config"));
    }
}
