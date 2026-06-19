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

//! Storage port - volume mounting interface.

use async_trait::async_trait;
use kubelet_core::error::Result;
use kubelet_core::pod::VolumeSpec;
use std::path::PathBuf;

/// Mount parameters for a volume.
#[derive(Debug, Clone)]
pub struct MountRequest {
    pub pod_uid: String,
    pub pod_namespace: String,
    pub volume_name: String,
    pub volume_spec: VolumeSpec,
    pub mount_path: PathBuf,
    pub read_only: bool,
}

/// Unmount parameters for a volume.
#[derive(Debug, Clone)]
pub struct UnmountRequest {
    pub pod_uid: String,
    pub volume_name: String,
    pub mount_path: PathBuf,
}

/// Port for volume management operations.
#[async_trait]
pub trait VolumeManager: Send + Sync {
    /// Mount all volumes for a pod.
    async fn mount_volumes(&self, request: Vec<MountRequest>) -> Result<Vec<PathBuf>>;

    /// Unmount all volumes for a pod.
    async fn unmount_volumes(&self, requests: Vec<UnmountRequest>) -> Result<()>;

    /// Check if volumes for a pod are already mounted.
    async fn volumes_mounted(&self, pod_uid: &str) -> Result<bool>;

    /// List mounted volumes for a pod.
    async fn list_mounted_volumes(&self, pod_uid: &str) -> Result<Vec<MountedVolume>>;
}

#[derive(Debug, Clone)]
pub struct MountedVolume {
    pub pod_uid: String,
    pub volume_name: String,
    pub mount_path: PathBuf,
    pub device_path: Option<PathBuf>,
    pub read_only: bool,
}
