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

//! Projected volume plugin.
//!
//! A projected volume combines multiple sources (ConfigMap, Secret,
//! ServiceAccountToken, DownwardAPI) into a single directory mount.
//! Mirrors pkg/volume/projected/projected.go.

use super::configmap::{ConfigMapData, ConfigMapVolumeManager};
use super::secret::{SecretData, SecretVolumeManager};
use super::service_account::ServiceAccountToken;
use kubelet_core::error::{KubeletError, Result};
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// A single source within a projected volume.
#[derive(Debug, Clone)]
pub enum ProjectedVolumeSource {
    ConfigMap {
        name: String,
        namespace: String,
        items: Vec<(String, String)>, // key -> path
        optional: bool,
    },
    Secret {
        name: String,
        namespace: String,
        items: Vec<(String, String)>,
        optional: bool,
    },
    ServiceAccountToken {
        audience: Option<String>,
        expiration_seconds: u64,
        path: String,
    },
    DownwardAPI {
        items: Vec<DownwardAPIItem>,
    },
}

#[derive(Debug, Clone)]
pub struct DownwardAPIItem {
    pub path: String,
    pub field_path: Option<String>,     // e.g. "metadata.name"
    pub resource_field: Option<String>, // e.g. "requests.cpu"
}

/// Mounts a projected volume combining multiple sources.
pub struct ProjectedVolumeManager {
    cm_mgr: ConfigMapVolumeManager,
    secret_mgr: SecretVolumeManager,
}

impl ProjectedVolumeManager {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        let base = base_dir.into();
        Self {
            cm_mgr: ConfigMapVolumeManager::new(base.clone()),
            secret_mgr: SecretVolumeManager::new(base),
        }
    }

    /// Mount all sources into `target_path`.
    pub fn mount(
        &self,
        sources: &[ProjectedVolumeSource],
        target_path: &Path,
        default_mode: u32,
        // Resolved data (caller fetches from API server).
        configmaps: &std::collections::HashMap<String, ConfigMapData>,
        secrets: &std::collections::HashMap<String, SecretData>,
        sa_token: Option<&ServiceAccountToken>,
    ) -> Result<()> {
        std::fs::create_dir_all(target_path)
            .map_err(|e| KubeletError::Storage(format!("create projected dir: {}", e)))?;

        for source in sources {
            match source {
                ProjectedVolumeSource::ConfigMap {
                    name,
                    items,
                    optional,
                    ..
                } => match configmaps.get(name.as_str()) {
                    Some(cm) => self.cm_mgr.mount(cm, target_path, items, default_mode)?,
                    None if *optional => {
                        debug!(configmap = %name, "Optional ConfigMap not found, skipping");
                    }
                    None => {
                        return Err(KubeletError::Storage(format!(
                            "projected volume: required ConfigMap '{}' not found",
                            name
                        )));
                    }
                },
                ProjectedVolumeSource::Secret {
                    name,
                    items,
                    optional,
                    ..
                } => match secrets.get(name.as_str()) {
                    Some(s) => self.secret_mgr.mount(s, target_path, items, 0o600)?,
                    None if *optional => {
                        debug!(secret = %name, "Optional Secret not found, skipping");
                    }
                    None => {
                        return Err(KubeletError::Storage(format!(
                            "projected volume: required Secret '{}' not found",
                            name
                        )));
                    }
                },
                ProjectedVolumeSource::ServiceAccountToken { path, .. } => {
                    if let Some(token) = sa_token {
                        let token_path = target_path.join(path);
                        if let Some(parent) = token_path.parent() {
                            std::fs::create_dir_all(parent).map_err(|e| {
                                KubeletError::Storage(format!("sa token dir: {}", e))
                            })?;
                        }
                        std::fs::write(&token_path, &token.token)
                            .map_err(|e| KubeletError::Storage(format!("write sa token: {}", e)))?;
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt;
                            let _ = std::fs::set_permissions(
                                &token_path,
                                std::fs::Permissions::from_mode(0o600),
                            );
                        }
                        debug!(path = %token_path.display(), "Projected ServiceAccountToken written");
                    }
                }
                ProjectedVolumeSource::DownwardAPI { items } => {
                    for item in items {
                        let item_path = target_path.join(&item.path);
                        if let Some(parent) = item_path.parent() {
                            std::fs::create_dir_all(parent).map_err(|e| {
                                KubeletError::Storage(format!("downward api dir: {}", e))
                            })?;
                        }
                        // Field values are provided by the pod manager at mount time.
                        // Here we write an empty placeholder; real impl reads from pod spec.
                        let value = item.field_path.as_deref().unwrap_or("").to_string();
                        std::fs::write(&item_path, value.as_bytes()).map_err(|e| {
                            KubeletError::Storage(format!(
                                "write downwardAPI '{}': {}",
                                item.path, e
                            ))
                        })?;
                        debug!(path = %item.path, "DownwardAPI item written");
                    }
                }
            }
        }

        info!(target = %target_path.display(), "Projected volume mounted");
        Ok(())
    }

    pub fn unmount(&self, target_path: &Path) -> Result<()> {
        if target_path.exists() {
            std::fs::remove_dir_all(target_path)
                .map_err(|e| KubeletError::Storage(format!("remove projected dir: {}", e)))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::configmap::ConfigMapData;
    use super::super::secret::SecretData;
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    #[test]
    fn test_mount_configmap_source() {
        let dir = TempDir::new().unwrap();
        let mgr = ProjectedVolumeManager::new(dir.path());
        let target = dir.path().join("projected");

        let mut cms = HashMap::new();
        cms.insert(
            "my-cm".to_string(),
            ConfigMapData {
                namespace: "default".to_string(),
                name: "my-cm".to_string(),
                data: [("key1".to_string(), "val1".to_string())]
                    .into_iter()
                    .collect(),
                binary_data: HashMap::new(),
            },
        );

        let sources = vec![ProjectedVolumeSource::ConfigMap {
            name: "my-cm".to_string(),
            namespace: "default".to_string(),
            items: vec![],
            optional: false,
        }];

        mgr.mount(&sources, &target, 0o644, &cms, &HashMap::new(), None)
            .unwrap();
        assert!(target.join("key1").exists());
    }

    #[test]
    fn test_mount_optional_missing_configmap_ok() {
        let dir = TempDir::new().unwrap();
        let mgr = ProjectedVolumeManager::new(dir.path());
        let target = dir.path().join("projected");
        let sources = vec![ProjectedVolumeSource::ConfigMap {
            name: "missing-cm".to_string(),
            namespace: "default".to_string(),
            items: vec![],
            optional: true,
        }];
        mgr.mount(
            &sources,
            &target,
            0o644,
            &HashMap::new(),
            &HashMap::new(),
            None,
        )
        .unwrap();
    }

    #[test]
    fn test_mount_required_missing_configmap_fails() {
        let dir = TempDir::new().unwrap();
        let mgr = ProjectedVolumeManager::new(dir.path());
        let target = dir.path().join("projected");
        let sources = vec![ProjectedVolumeSource::ConfigMap {
            name: "missing-cm".to_string(),
            namespace: "default".to_string(),
            items: vec![],
            optional: false,
        }];
        assert!(
            mgr.mount(
                &sources,
                &target,
                0o644,
                &HashMap::new(),
                &HashMap::new(),
                None
            )
            .is_err()
        );
    }

    #[test]
    fn test_mount_sa_token() {
        let dir = TempDir::new().unwrap();
        let mgr = ProjectedVolumeManager::new(dir.path());
        let target = dir.path().join("projected");
        let token = ServiceAccountToken {
            token: "eyJhbGciOiJSUzI1NiJ9.test".to_string(),
            expiry: chrono::Utc::now() + chrono::Duration::hours(1),
            audience: "https://kubernetes.default.svc".to_string(),
        };
        let sources = vec![ProjectedVolumeSource::ServiceAccountToken {
            audience: None,
            expiration_seconds: 3600,
            path: "token".to_string(),
        }];
        mgr.mount(
            &sources,
            &target,
            0o644,
            &HashMap::new(),
            &HashMap::new(),
            Some(&token),
        )
        .unwrap();
        assert!(target.join("token").exists());
    }
}
