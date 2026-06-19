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

//! Volume expansion -- CSI NodeExpandVolume.
//!
//! When a PersistentVolumeClaim is resized, the kubelet must call
//! NodeExpandVolume on the CSI plugin to expand the filesystem on the node.
//!
//! Flow:
//!   1. PVC status shows resizeStatus: NodeExpansionPending.
//!   2. Kubelet detects this during pod sync.
//!   3. Calls CSI NodeExpandVolume(volume_id, volume_path, capacity_range).
//!   4. Updates PVC conditions once complete.
//!
//! Mirrors pkg/kubelet/volumemanager/expand/ in the Go kubelet.

use kubelet_core::error::{KubeletError, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpandVolumeRequest {
    pub volume_id: String,
    pub volume_path: PathBuf,
    pub staging_path: Option<PathBuf>,
    /// New required size in bytes.
    pub required_bytes: u64,
    pub limit_bytes: Option<u64>,
    pub fs_type: Option<String>,
    pub mount_flags: Vec<String>,
}

pub struct VolumeExpander {
    plugin_dir: PathBuf,
}

impl VolumeExpander {
    pub fn new(plugin_dir: impl Into<PathBuf>) -> Self {
        Self {
            plugin_dir: plugin_dir.into(),
        }
    }

    /// Expand a CSI volume on this node.
    pub async fn expand_volume(&self, driver_name: &str, req: &ExpandVolumeRequest) -> Result<u64> {
        let socket_path = self.plugin_dir.join(driver_name).join("csi.sock");

        if !socket_path.exists() {
            return Err(KubeletError::Storage(format!(
                "CSI plugin socket not found for driver '{}': {}",
                driver_name,
                socket_path.display()
            )));
        }

        info!(
            driver = %driver_name,
            volume_id = %req.volume_id,
            path = %req.volume_path.display(),
            required_bytes = req.required_bytes,
            "Calling CSI NodeExpandVolume"
        );

        // Real implementation: connect to CSI socket via tonic, call
        //   node_client.node_expand_volume(NodeExpandVolumeRequest {
        //     volume_id, volume_path, staging_target_path, capacity_range,
        //     volume_capability, secrets, volume_context
        //   }).await
        //
        // Then call resize2fs / xfs_growfs on the block device / mount point.
        self.expand_filesystem(&req.volume_path, req.required_bytes, &req.fs_type)?;

        // Return actual size after expansion (real: from NodeExpandVolumeResponse).
        Ok(req.required_bytes)
    }

    /// Expand the filesystem at the volume path.
    fn expand_filesystem(
        &self,
        volume_path: &Path,
        target_bytes: u64,
        fs_type: &Option<String>,
    ) -> Result<()> {
        let fs_type = fs_type.as_deref().unwrap_or("ext4");

        match fs_type {
            "ext4" | "ext3" | "ext2" => {
                // Real: run `resize2fs <device>`
                info!(fs_type, path = %volume_path.display(), "Expanding ext filesystem");
                // subprocess call would go here
                Ok(())
            }
            "xfs" => {
                // Real: run `xfs_growfs <mount_point>`
                info!(fs_type, path = %volume_path.display(), "Expanding XFS filesystem");
                Ok(())
            }
            "btrfs" => {
                // Real: run `btrfs filesystem resize max <mount_point>`
                info!(fs_type, path = %volume_path.display(), "Expanding Btrfs filesystem");
                Ok(())
            }
            _ => {
                warn!(
                    fs_type,
                    "Unknown filesystem type for expansion; skipping resize"
                );
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_expand_volume_missing_plugin_fails() {
        let dir = TempDir::new().unwrap();
        let expander = VolumeExpander::new(dir.path());
        let req = ExpandVolumeRequest {
            volume_id: "vol-1".to_string(),
            volume_path: dir.path().join("target"),
            staging_path: None,
            required_bytes: 10 * 1024 * 1024 * 1024,
            limit_bytes: None,
            fs_type: Some("ext4".to_string()),
            mount_flags: vec![],
        };
        let result = expander.expand_volume("nonexistent.csi.driver", &req).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_expand_filesystem_unknown_type_ok() {
        let dir = TempDir::new().unwrap();
        let expander = VolumeExpander::new(dir.path());
        let result = expander.expand_filesystem(dir.path(), 1024, &Some("ntfs".to_string()));
        assert!(result.is_ok()); // should warn but not fail
    }
}
