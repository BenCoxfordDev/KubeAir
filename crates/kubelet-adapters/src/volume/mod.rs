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

//! Volume manager adapter - handles EmptyDir, HostPath, ConfigMap, Secret, Projected, and ServiceAccount volumes.

pub mod configmap;
pub mod projected;
pub mod secret;
pub mod service_account;

use crate::csi::{CsiVolumeContext, CsiVolumeManager, proto};
use async_trait::async_trait;
use k8s_openapi::api::core::v1::{PersistentVolume, PersistentVolumeClaim};
use kubelet_core::error::{KubeletError, Result};
use kubelet_core::pod::{VolumeSource, VolumeSpec};
use kubelet_ports::driven::storage::{MountRequest, MountedVolume, UnmountRequest, VolumeManager};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Simple volume manager that handles EmptyDir and HostPath volumes.
pub struct LocalVolumeManager {
    root_dir: PathBuf,
    mounted: Arc<Mutex<HashMap<String, Vec<MountedVolume>>>>,
    /// Paths currently mounted as tmpfs (EmptyDir medium:Memory).
    tmpfs_paths: Arc<Mutex<HashSet<PathBuf>>>,
}

impl LocalVolumeManager {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
            mounted: Arc::new(Mutex::new(HashMap::new())),
            tmpfs_paths: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn pod_volume_dir(&self, pod_uid: &str) -> PathBuf {
        self.root_dir.join("pods").join(pod_uid).join("volumes")
    }

    async fn mount_emptydir(
        &self,
        pod_uid: &str,
        volume_name: &str,
        medium: Option<&str>,
    ) -> Result<PathBuf> {
        let mount_path = self
            .pod_volume_dir(pod_uid)
            .join("kubernetes.io~empty-dir")
            .join(volume_name);
        tokio::fs::create_dir_all(&mount_path).await?;

        if medium
            .map(|m| m.eq_ignore_ascii_case("memory"))
            .unwrap_or(false)
        {
            // Mount a tmpfs so the directory shows up as type "tmpfs" inside the container.
            let status = tokio::process::Command::new("mount")
                .args(["-t", "tmpfs", "tmpfs", mount_path.to_str().unwrap_or("")])
                .status()
                .await
                .map_err(|e| {
                    KubeletError::VolumeMount(format!(
                        "failed to run mount for tmpfs at '{}': {}",
                        mount_path.display(),
                        e
                    ))
                })?;
            if !status.success() {
                return Err(KubeletError::VolumeMount(format!(
                    "mount -t tmpfs failed at '{}' (exit: {})",
                    mount_path.display(),
                    status
                )));
            }
            self.tmpfs_paths.lock().await.insert(mount_path.clone());
        }

        // EmptyDir directories must be world-writable (0777) so non-root containers
        // can write to them. The process umask typically produces 0755; fix it explicitly.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = tokio::fs::set_permissions(&mount_path, std::fs::Permissions::from_mode(0o777))
                .await;
        }
        debug!(pod_uid, volume_name, medium = ?medium, path = %mount_path.display(), "Mounted EmptyDir");
        Ok(mount_path)
    }

    async fn mount_hostpath(&self, path: &str, path_type: Option<&str>) -> Result<PathBuf> {
        let host_path = PathBuf::from(path);

        match path_type {
            Some("Directory") => {
                if !host_path.is_dir() {
                    return Err(KubeletError::VolumeMount(format!(
                        "hostPath '{}' must be an existing directory",
                        host_path.display()
                    )));
                }
            }
            Some("DirectoryOrCreate") => {
                if !host_path.exists()
                    && let Err(e) = tokio::fs::create_dir_all(&host_path).await
                {
                    // Mirror Go kubelet behaviour: treat mkdir failure as non-fatal.
                    // On nodes with a read-only root filesystem (e.g. squashfs with
                    // selective overlays), the directory may not be creatable but the
                    // mount can still succeed if the container runtime tolerates it,
                    // or the workload creates the path itself.  Log and continue.
                    debug!(
                        path = %host_path.display(),
                        error = %e,
                        "DirectoryOrCreate: could not create host path directory (non-fatal)"
                    );
                }
                if host_path.exists() && !host_path.is_dir() {
                    return Err(KubeletError::VolumeMount(format!(
                        "hostPath '{}' is not a directory",
                        host_path.display()
                    )));
                }
            }
            Some("File") => {
                if !host_path.is_file() {
                    return Err(KubeletError::VolumeMount(format!(
                        "hostPath '{}' must be an existing file",
                        host_path.display()
                    )));
                }
            }
            Some("FileOrCreate") => {
                if let Some(parent) = host_path.parent() {
                    // Non-fatal — same rationale as DirectoryOrCreate above.
                    let _ = tokio::fs::create_dir_all(parent).await;
                }
                if !host_path.exists() {
                    tokio::fs::write(&host_path, []).await?;
                }
                if !host_path.is_file() {
                    return Err(KubeletError::VolumeMount(format!(
                        "hostPath '{}' is not a file",
                        host_path.display()
                    )));
                }
            }
            Some("Socket") | Some("CharDevice") | Some("BlockDevice") => {
                if !host_path.exists() {
                    return Err(KubeletError::VolumeMount(format!(
                        "hostPath '{}' does not exist",
                        host_path.display()
                    )));
                }
            }
            _ => {
                if !host_path.exists() {
                    // Non-fatal — same rationale as DirectoryOrCreate above.
                    let _ = tokio::fs::create_dir_all(&host_path).await;
                }
            }
        }

        Ok(host_path)
    }
}

#[async_trait]
impl VolumeManager for LocalVolumeManager {
    async fn mount_volumes(&self, requests: Vec<MountRequest>) -> Result<Vec<PathBuf>> {
        let mut mount_paths = Vec::new();

        for req in &requests {
            let path = match &req.volume_spec.source {
                VolumeSource::EmptyDir { medium, .. } => {
                    self.mount_emptydir(&req.pod_uid, &req.volume_name, medium.as_deref())
                        .await?
                }
                VolumeSource::HostPath { path, path_type } => {
                    self.mount_hostpath(path, path_type.as_deref()).await?
                }
                VolumeSource::ConfigMap { .. } => {
                    let p = self
                        .pod_volume_dir(&req.pod_uid)
                        .join("kubernetes.io~configmap")
                        .join(&req.volume_name);
                    tokio::fs::create_dir_all(&p).await?;
                    p
                }
                VolumeSource::Secret { .. } => {
                    let p = self
                        .pod_volume_dir(&req.pod_uid)
                        .join("kubernetes.io~secret")
                        .join(&req.volume_name);
                    tokio::fs::create_dir_all(&p).await?;
                    p
                }
                VolumeSource::Projected { .. } => {
                    // Matches Go kubelet: /var/lib/kubelet/pods/<uid>/volumes/kubernetes.io~projected/<name>
                    let p = self
                        .pod_volume_dir(&req.pod_uid)
                        .join("kubernetes.io~projected")
                        .join(&req.volume_name);
                    tokio::fs::create_dir_all(&p).await?;
                    p
                }
                VolumeSource::DownwardAPI { .. } => {
                    let p = self
                        .pod_volume_dir(&req.pod_uid)
                        .join("kubernetes.io~downward-api")
                        .join(&req.volume_name);
                    tokio::fs::create_dir_all(&p).await?;
                    p
                }
                _ => {
                    // Unsupported volume type - return error
                    return Err(KubeletError::VolumeMount(format!(
                        "Unsupported volume type for '{}'",
                        req.volume_name
                    )));
                }
            };

            let mounted = MountedVolume {
                pod_uid: req.pod_uid.clone(),
                volume_name: req.volume_name.clone(),
                mount_path: path.clone(),
                device_path: None,
                read_only: req.read_only,
            };

            self.mounted
                .lock()
                .await
                .entry(req.pod_uid.clone())
                .or_default()
                .push(mounted);

            mount_paths.push(path);
        }

        Ok(mount_paths)
    }

    async fn unmount_volumes(&self, requests: Vec<UnmountRequest>) -> Result<()> {
        let mut mounted = self.mounted.lock().await;
        let mut tmpfs = self.tmpfs_paths.lock().await;

        for req in &requests {
            // Umount any tmpfs (EmptyDir medium:Memory) before the pod directory is removed.
            let emptydir_path = self
                .pod_volume_dir(&req.pod_uid)
                .join("kubernetes.io~empty-dir")
                .join(&req.volume_name);
            if tmpfs.remove(&emptydir_path) {
                let _ = tokio::process::Command::new("umount")
                    .arg(&emptydir_path)
                    .status()
                    .await;
            }

            if let Some(vols) = mounted.get_mut(&req.pod_uid) {
                vols.retain(|v| v.volume_name != req.volume_name);
            }
        }

        Ok(())
    }

    async fn volumes_mounted(&self, pod_uid: &str) -> Result<bool> {
        let mounted = self.mounted.lock().await;
        Ok(mounted.contains_key(pod_uid) && !mounted[pod_uid].is_empty())
    }

    async fn list_mounted_volumes(&self, pod_uid: &str) -> Result<Vec<MountedVolume>> {
        let mounted = self.mounted.lock().await;
        Ok(mounted.get(pod_uid).cloned().unwrap_or_default())
    }
}

/// Composite volume manager:
/// - Non-PVC volumes are delegated to `LocalVolumeManager`
/// - PVC volumes are routed through `CsiVolumeManager` or handled as local/hostpath
///   by first resolving the PVC to its backing PersistentVolume via the kube API.
pub struct CompositeVolumeManager {
    local: LocalVolumeManager,
    csi: Arc<Mutex<CsiVolumeManager>>,
    csi_mounted: Arc<Mutex<HashMap<String, Vec<MountedVolume>>>>,
    root_dir: PathBuf,
    kube_client: Option<kube::Client>,
}

impl CompositeVolumeManager {
    pub fn new(root_dir: impl Into<PathBuf>, csi_plugin_dir: impl Into<PathBuf>) -> Self {
        let root_dir = root_dir.into();
        let mut csi_mgr = CsiVolumeManager::new(csi_plugin_dir);
        csi_mgr.registry.scan();

        Self {
            local: LocalVolumeManager::new(root_dir.clone()),
            csi: Arc::new(Mutex::new(csi_mgr)),
            csi_mounted: Arc::new(Mutex::new(HashMap::new())),
            root_dir,
            kube_client: None,
        }
    }

    pub fn with_kube_client(mut self, client: kube::Client) -> Self {
        self.kube_client = Some(client);
        self
    }

    /// Resolve a PVC name in a namespace to the backing PersistentVolume spec.
    /// Returns `None` if no kube client is available or the PVC/PV cannot be fetched.
    async fn resolve_pvc_to_pv(
        &self,
        namespace: &str,
        claim_name: &str,
    ) -> Option<PersistentVolume> {
        use kube::Api;
        let client = self.kube_client.as_ref()?;
        let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
        let pvc = match pvcs.get(claim_name).await {
            Ok(p) => p,
            Err(e) => {
                warn!(namespace, claim_name, error = %e, "Failed to fetch PVC");
                return None;
            }
        };
        let pv_name = pvc.spec?.volume_name?;
        let pvs: Api<PersistentVolume> = Api::all(client.clone());
        match pvs.get(&pv_name).await {
            Ok(pv) => Some(pv),
            Err(e) => {
                warn!(pv_name, error = %e, "Failed to fetch PV");
                None
            }
        }
    }

    fn csi_driver_and_volume_id(&self, claim_name: &str) -> Result<(String, String)> {
        // Accepted formats:
        // - csi://<driver>/<volume-id>
        // - <claim-name> with KUBELET_CSI_DEFAULT_DRIVER set
        if let Some(rest) = claim_name.strip_prefix("csi://") {
            if let Some((driver, volume_id)) = rest.split_once('/')
                && !driver.is_empty()
                && !volume_id.is_empty()
            {
                return Ok((driver.to_string(), volume_id.to_string()));
            }
            return Err(KubeletError::VolumeMount(format!(
                "invalid CSI claim name '{}': expected csi://<driver>/<volume-id>",
                claim_name
            )));
        }

        match std::env::var("KUBELET_CSI_DEFAULT_DRIVER") {
            Ok(driver) if !driver.trim().is_empty() => Ok((driver, claim_name.to_string())),
            _ => Err(KubeletError::VolumeMount(format!(
                "PVC '{}' requires CSI driver mapping: use csi://<driver>/<volume-id> or set KUBELET_CSI_DEFAULT_DRIVER",
                claim_name
            ))),
        }
    }

    fn csi_staging_path(&self, req: &MountRequest) -> PathBuf {
        self.root_dir
            .join("pods")
            .join(&req.pod_uid)
            .join("volumes")
            .join("csi")
            .join("staging")
            .join(&req.volume_name)
    }

    async fn mount_csi_pvc(&self, req: &MountRequest, claim_name: &str) -> Result<PathBuf> {
        let (driver, volume_id) = self.csi_driver_and_volume_id(claim_name)?;
        let target_path = req.mount_path.clone();
        let staging_target_path = self.csi_staging_path(req);

        let ctx = CsiVolumeContext {
            volume_id: volume_id.clone(),
            volume_attributes: HashMap::from([("csi.storage.k8s.io/driver".to_string(), driver)]),
            target_path: target_path.clone(),
            staging_target_path: Some(staging_target_path),
            read_only: req.read_only,
            access_type: proto::AccessType::Mount,
            fs_type: None,
            mount_flags: vec![],
            secrets: HashMap::new(),
        };

        let mut csi = self.csi.lock().await;
        csi.registry.scan();
        csi.stage_volume(&ctx).await?;
        csi.publish_volume(&ctx).await?;
        drop(csi);

        let mounted = MountedVolume {
            pod_uid: req.pod_uid.clone(),
            volume_name: req.volume_name.clone(),
            mount_path: target_path.clone(),
            device_path: Some(PathBuf::from(volume_id)),
            read_only: req.read_only,
        };

        self.csi_mounted
            .lock()
            .await
            .entry(req.pod_uid.clone())
            .or_default()
            .push(mounted);

        Ok(target_path)
    }
}

#[async_trait]
impl VolumeManager for CompositeVolumeManager {
    async fn mount_volumes(&self, requests: Vec<MountRequest>) -> Result<Vec<PathBuf>> {
        let mut local_requests = Vec::new();
        let mut mounted_paths = Vec::new();

        for req in &requests {
            match &req.volume_spec.source {
                VolumeSource::PersistentVolumeClaim {
                    claim_name,
                    read_only,
                } => {
                    // Resolve PVC → PV to determine the actual volume type.
                    if let Some(pv) = self.resolve_pvc_to_pv(&req.pod_namespace, claim_name).await {
                        let spec = pv.spec.as_ref();

                        if let Some(local_src) = spec.and_then(|s| s.local.as_ref()) {
                            // PV type: local (spec.local.path) — bind-mount the host path.
                            let host_path = local_src.path.clone();
                            info!(
                                claim_name = %claim_name,
                                pv_path = %host_path,
                                "Mounting local PV as hostPath bind-mount"
                            );
                            let host_path_req = MountRequest {
                                pod_uid: req.pod_uid.clone(),
                                pod_namespace: req.pod_namespace.clone(),
                                volume_name: req.volume_name.clone(),
                                volume_spec: VolumeSpec {
                                    name: req.volume_spec.name.clone(),
                                    source: VolumeSource::HostPath {
                                        path: host_path,
                                        path_type: Some("Directory".to_string()),
                                    },
                                },
                                mount_path: req.mount_path.clone(),
                                read_only: *read_only,
                            };
                            local_requests.push(host_path_req);
                        } else if let Some(csi_src) = spec.and_then(|s| s.csi.as_ref()) {
                            // PV type: CSI (spec.csi) — use CSI driver + volume handle.
                            let driver = csi_src.driver.clone();
                            let volume_handle = csi_src.volume_handle.clone();
                            let csi_claim = format!("csi://{}/{}", driver, volume_handle);
                            let path = self.mount_csi_pvc(req, &csi_claim).await?;
                            mounted_paths.push(path);
                        } else if let Some(host_path_src) = spec.and_then(|s| s.host_path.as_ref())
                        {
                            // PV type: hostPath — treat directly as hostPath.
                            let host_path_req = MountRequest {
                                pod_uid: req.pod_uid.clone(),
                                pod_namespace: req.pod_namespace.clone(),
                                volume_name: req.volume_name.clone(),
                                volume_spec: VolumeSpec {
                                    name: req.volume_spec.name.clone(),
                                    source: VolumeSource::HostPath {
                                        path: host_path_src.path.clone(),
                                        path_type: host_path_src.type_.clone(),
                                    },
                                },
                                mount_path: req.mount_path.clone(),
                                read_only: *read_only,
                            };
                            local_requests.push(host_path_req);
                        } else {
                            // Unknown PV type — fall through to legacy CSI claim-name path.
                            warn!(
                                claim_name = %claim_name,
                                "PV has unknown type; falling back to CSI claim-name path"
                            );
                            let path = self.mount_csi_pvc(req, claim_name).await?;
                            mounted_paths.push(path);
                        }
                    } else {
                        // No kube client or PVC/PV lookup failed — legacy path.
                        let path = self.mount_csi_pvc(req, claim_name).await?;
                        mounted_paths.push(path);
                    }
                }
                _ => local_requests.push(req.clone()),
            }
        }

        if !local_requests.is_empty() {
            let mut local_paths = self.local.mount_volumes(local_requests).await?;
            mounted_paths.append(&mut local_paths);
        }

        Ok(mounted_paths)
    }

    async fn unmount_volumes(&self, requests: Vec<UnmountRequest>) -> Result<()> {
        let mut local_requests = Vec::new();

        for req in &requests {
            let csi_entry = {
                let mut by_pod = self.csi_mounted.lock().await;
                let maybe = by_pod.get_mut(&req.pod_uid).and_then(|vols| {
                    let pos = vols.iter().position(|v| v.volume_name == req.volume_name)?;
                    Some(vols.remove(pos))
                });
                if let Some(vols) = by_pod.get(&req.pod_uid)
                    && vols.is_empty()
                {
                    by_pod.remove(&req.pod_uid);
                }
                maybe
            };

            if let Some(csi_vol) = csi_entry {
                if let Some(device_path) = csi_vol.device_path.as_ref() {
                    let volume_id = device_path.to_string_lossy().to_string();
                    let mut csi = self.csi.lock().await;
                    csi.unpublish_volume(&volume_id, &csi_vol.mount_path)
                        .await?;
                }
            } else {
                local_requests.push(req.clone());
            }
        }

        if !local_requests.is_empty() {
            self.local.unmount_volumes(local_requests).await?;
        }

        Ok(())
    }

    async fn volumes_mounted(&self, pod_uid: &str) -> Result<bool> {
        if self.local.volumes_mounted(pod_uid).await? {
            return Ok(true);
        }
        let csi = self.csi_mounted.lock().await;
        Ok(csi.get(pod_uid).map(|v| !v.is_empty()).unwrap_or(false))
    }

    async fn list_mounted_volumes(&self, pod_uid: &str) -> Result<Vec<MountedVolume>> {
        let mut all = self.local.list_mounted_volumes(pod_uid).await?;
        let csi = self.csi_mounted.lock().await;
        if let Some(v) = csi.get(pod_uid) {
            all.extend(v.clone());
        }
        Ok(all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csi::CsiPlugin;
    use kubelet_core::pod::{VolumeSource, VolumeSpec};
    use kubelet_ports::driven::storage::MountRequest;
    use tempfile::TempDir;
    use tokio::net::UnixListener;

    fn emptydir_request(pod_uid: &str, vol_name: &str) -> MountRequest {
        MountRequest {
            pod_uid: pod_uid.to_string(),
            pod_namespace: "default".to_string(),
            volume_name: vol_name.to_string(),
            volume_spec: VolumeSpec {
                name: vol_name.to_string(),
                source: VolumeSource::EmptyDir {
                    medium: None,
                    size_limit: None,
                },
            },
            mount_path: PathBuf::from(format!("/mock/{}/{}", pod_uid, vol_name)),
            read_only: false,
        }
    }

    fn hostpath_request(
        pod_uid: &str,
        vol_name: &str,
        path: &std::path::Path,
        path_type: Option<&str>,
    ) -> MountRequest {
        MountRequest {
            pod_uid: pod_uid.to_string(),
            pod_namespace: "default".to_string(),
            volume_name: vol_name.to_string(),
            volume_spec: VolumeSpec {
                name: vol_name.to_string(),
                source: VolumeSource::HostPath {
                    path: path.to_string_lossy().to_string(),
                    path_type: path_type.map(|s| s.to_string()),
                },
            },
            mount_path: PathBuf::from(format!("/mock/{}/{}", pod_uid, vol_name)),
            read_only: false,
        }
    }

    fn pvc_request(
        pod_uid: &str,
        vol_name: &str,
        claim_name: &str,
        mount_path: &std::path::Path,
    ) -> MountRequest {
        MountRequest {
            pod_uid: pod_uid.to_string(),
            pod_namespace: "default".to_string(),
            volume_name: vol_name.to_string(),
            volume_spec: VolumeSpec {
                name: vol_name.to_string(),
                source: VolumeSource::PersistentVolumeClaim {
                    claim_name: claim_name.to_string(),
                    read_only: false,
                },
            },
            mount_path: mount_path.to_path_buf(),
            read_only: false,
        }
    }

    #[tokio::test]
    async fn test_mount_emptydir() {
        let dir = TempDir::new().unwrap();
        let mgr = LocalVolumeManager::new(dir.path());
        let req = emptydir_request("uid-001", "data");
        let paths = mgr.mount_volumes(vec![req]).await.unwrap();
        assert_eq!(paths.len(), 1);
        assert!(paths[0].exists());
    }

    #[tokio::test]
    async fn test_volumes_mounted_true_after_mount() {
        let dir = TempDir::new().unwrap();
        let mgr = LocalVolumeManager::new(dir.path());
        mgr.mount_volumes(vec![emptydir_request("uid-002", "cache")])
            .await
            .unwrap();
        assert!(mgr.volumes_mounted("uid-002").await.unwrap());
    }

    #[tokio::test]
    async fn test_volumes_mounted_false_initially() {
        let dir = TempDir::new().unwrap();
        let mgr = LocalVolumeManager::new(dir.path());
        assert!(!mgr.volumes_mounted("uid-003").await.unwrap());
    }

    #[tokio::test]
    async fn test_unmount_volumes() {
        let dir = TempDir::new().unwrap();
        let mgr = LocalVolumeManager::new(dir.path());
        mgr.mount_volumes(vec![emptydir_request("uid-004", "tmp")])
            .await
            .unwrap();
        mgr.unmount_volumes(vec![UnmountRequest {
            pod_uid: "uid-004".to_string(),
            volume_name: "tmp".to_string(),
            mount_path: PathBuf::from("/mock"),
        }])
        .await
        .unwrap();
        let vols = mgr.list_mounted_volumes("uid-004").await.unwrap();
        assert!(vols.is_empty());
    }

    #[tokio::test]
    async fn test_list_mounted_volumes() {
        let dir = TempDir::new().unwrap();
        let mgr = LocalVolumeManager::new(dir.path());
        mgr.mount_volumes(vec![
            emptydir_request("uid-005", "vol-a"),
            emptydir_request("uid-005", "vol-b"),
        ])
        .await
        .unwrap();
        let vols = mgr.list_mounted_volumes("uid-005").await.unwrap();
        assert_eq!(vols.len(), 2);
    }

    #[tokio::test]
    async fn test_mount_hostpath_file_or_create_creates_file() {
        let dir = TempDir::new().unwrap();
        let mgr = LocalVolumeManager::new(dir.path());
        let target = dir.path().join("etc/kubernetes/pki/sa.key");

        let req = hostpath_request("uid-006", "sa-key", &target, Some("FileOrCreate"));
        let paths = mgr.mount_volumes(vec![req]).await.unwrap();

        assert_eq!(paths.len(), 1);
        assert!(paths[0].is_file());
    }

    #[tokio::test]
    async fn test_mount_hostpath_file_requires_existing_file() {
        let dir = TempDir::new().unwrap();
        let mgr = LocalVolumeManager::new(dir.path());
        let target = dir.path().join("missing.key");

        let req = hostpath_request("uid-007", "sa-key", &target, Some("File"));
        let err = mgr.mount_volumes(vec![req]).await.unwrap_err();
        assert!(format!("{}", err).contains("must be an existing file"));
    }

    #[tokio::test]
    async fn test_composite_mount_pvc_via_csi() {
        let dir = TempDir::new().unwrap();
        let plugin_dir = dir.path().join("plugins").join("test-driver");
        std::fs::create_dir_all(&plugin_dir).unwrap();

        let socket = plugin_dir.join("csi.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let socket_task = tokio::spawn(async move {
            loop {
                if listener.accept().await.is_err() {
                    break;
                }
            }
        });

        let mgr = CompositeVolumeManager::new(dir.path(), dir.path().join("plugins"));

        let mount = dir.path().join("mnt/pvc");
        let req = pvc_request("uid-csi-1", "data", "csi://test-driver/vol-1", &mount);
        let paths = mgr.mount_volumes(vec![req]).await.unwrap();
        assert_eq!(paths.len(), 1);
        assert!(mount.exists());

        let vols = mgr.list_mounted_volumes("uid-csi-1").await.unwrap();
        assert_eq!(vols.len(), 1);

        mgr.unmount_volumes(vec![UnmountRequest {
            pod_uid: "uid-csi-1".to_string(),
            volume_name: "data".to_string(),
            mount_path: mount,
        }])
        .await
        .unwrap();

        socket_task.abort();
    }

    #[tokio::test]
    async fn test_composite_mount_pvc_requires_driver_mapping() {
        let dir = TempDir::new().unwrap();
        let mgr = CompositeVolumeManager::new(dir.path(), dir.path().join("plugins"));

        let mount = dir.path().join("mnt/pvc");
        let req = pvc_request("uid-csi-2", "data", "plain-claim", &mount);
        let err = mgr.mount_volumes(vec![req]).await.unwrap_err();
        assert!(format!("{}", err).contains("requires CSI driver mapping"));
    }
}
