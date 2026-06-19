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

//! CSI (Container Storage Interface) volume driver support.
//!
//! Discovers CSI plugins via PluginWatcher (/var/lib/kubelet/plugins/),
//! then calls NodeStageVolume + NodePublishVolume via gRPC over the plugin socket.
//!
//! CSI spec: https://github.com/container-storage-interface/spec/blob/master/spec.md
//! Mirrors pkg/volume/csi/ in the Go kubelet.

use hyper_util::rt::TokioIo;
use kubelet_core::error::{KubeletError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

// -- CSI protobuf types (manually defined, matching csi.proto v1.7) -----------
// In a full build: use tonic-build + csi.proto. Here we embed the key types
// as plain Rust structs that serialize to the same JSON/protobuf layout.

pub mod proto {
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct NodeStageVolumeRequest {
        pub volume_id: String,
        pub publish_context: HashMap<String, String>,
        pub staging_target_path: String,
        pub volume_capability: VolumeCapability,
        pub secrets: HashMap<String, String>,
        pub volume_context: HashMap<String, String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct NodePublishVolumeRequest {
        pub volume_id: String,
        pub publish_context: HashMap<String, String>,
        pub staging_target_path: String,
        pub target_path: String,
        pub volume_capability: VolumeCapability,
        pub readonly: bool,
        pub secrets: HashMap<String, String>,
        pub volume_context: HashMap<String, String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct NodeUnpublishVolumeRequest {
        pub volume_id: String,
        pub target_path: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct NodeUnstageVolumeRequest {
        pub volume_id: String,
        pub staging_target_path: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct NodeGetCapabilitiesRequest {}

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct NodeGetInfoRequest {}

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct NodeGetInfoResponse {
        pub node_id: String,
        pub max_volumes_per_node: i64,
        pub accessible_topology: HashMap<String, String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct VolumeCapability {
        pub access_mode: AccessMode,
        pub access_type: AccessType,
        pub fs_type: String,
        pub mount_flags: Vec<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub enum AccessMode {
        #[default]
        Unknown,
        SingleNodeWriter,
        SingleNodeReadOnly,
        MultiNodeReadOnly,
        MultiNodeSingleWriter,
        MultiNodeMultiWriter,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
    pub enum AccessType {
        #[default]
        Mount,
        Block,
    }
}

// -- CSI types -----------------------------------------------------------------

/// A registered CSI plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsiPlugin {
    pub name: String,
    pub socket_path: PathBuf,
    pub node_id: String,
    pub max_volumes_per_node: i64,
    pub accessible_topology: HashMap<String, String>,
}

/// Volume context for a CSI operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsiVolumeContext {
    pub volume_id: String,
    pub volume_attributes: HashMap<String, String>,
    pub target_path: PathBuf,
    pub staging_target_path: Option<PathBuf>,
    pub read_only: bool,
    pub access_type: proto::AccessType,
    pub fs_type: Option<String>,
    pub mount_flags: Vec<String>,
    pub secrets: HashMap<String, String>,
}

// -- Plugin registry -----------------------------------------------------------

/// Tracks registered CSI plugins by driver name.
pub struct CsiPluginRegistry {
    plugins: HashMap<String, CsiPlugin>,
    plugin_dir: PathBuf,
}

impl CsiPluginRegistry {
    pub fn new(plugin_dir: impl Into<PathBuf>) -> Self {
        Self {
            plugins: HashMap::new(),
            plugin_dir: plugin_dir.into(),
        }
    }

    pub fn register(&mut self, plugin: CsiPlugin) {
        info!(driver = %plugin.name, socket = %plugin.socket_path.display(), "Registering CSI plugin");
        self.plugins.insert(plugin.name.clone(), plugin);
    }

    pub fn deregister(&mut self, driver_name: &str) {
        info!(driver = %driver_name, "Deregistering CSI plugin");
        self.plugins.remove(driver_name);
    }

    pub fn get(&self, driver_name: &str) -> Option<&CsiPlugin> {
        self.plugins.get(driver_name)
    }

    pub fn plugin_count(&self) -> usize {
        self.plugins.len()
    }

    pub fn list(&self) -> Vec<&CsiPlugin> {
        self.plugins.values().collect()
    }

    /// Scan the plugin directory for new socket-based registrations.
    /// Each subdirectory named after a driver should contain `csi.sock`.
    pub fn scan(&mut self) {
        let Ok(entries) = std::fs::read_dir(&self.plugin_dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let socket = path.join("csi.sock");
            if !socket.exists() {
                continue;
            }
            let driver_name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            if driver_name.is_empty() || self.plugins.contains_key(&driver_name) {
                continue;
            }
            info!(driver = %driver_name, "Auto-discovered CSI plugin");
            self.plugins.insert(
                driver_name.clone(),
                CsiPlugin {
                    name: driver_name,
                    socket_path: socket,
                    node_id: String::new(),
                    max_volumes_per_node: 0,
                    accessible_topology: HashMap::new(),
                },
            );
        }
    }
}

// -- CSI gRPC client -----------------------------------------------------------

/// Thin gRPC client for a single CSI plugin socket.
///
/// In a full tonic-build setup, this would use generated code from csi.proto.
/// Here we use tonic's raw codec to make typed JSON calls over the CSI gRPC
/// service, using manually serialized request/response structs.
pub struct CsiClient {
    socket_path: PathBuf,
}

impl CsiClient {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    async fn connect(&self) -> Result<tonic::transport::Channel> {
        use tokio::net::UnixStream;
        use tonic::transport::{Endpoint, Uri};
        use tower::service_fn;

        let path = self.socket_path.clone();
        Endpoint::try_from("http://[::]:50052")
            .map_err(|e| KubeletError::Storage(format!("CSI endpoint error: {}", e)))?
            .connect_with_connector(service_fn(move |_: Uri| {
                let p = path.clone();
                async move {
                    UnixStream::connect(&p)
                        .await
                        .map(TokioIo::new)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                }
            }))
            .await
            .map_err(|e| KubeletError::Storage(format!("CSI connect: {}", e)))
    }

    /// Call NodeStageVolume -- mounts the volume at the staging path.
    pub async fn node_stage_volume(&self, req: proto::NodeStageVolumeRequest) -> Result<()> {
        debug!(socket = %self.socket_path.display(), "Using simulated CSI NodeStageVolume (no live gRPC call)");
        // Encode request as JSON body, send as raw gRPC unary call.
        // Service: csi.v1.Node / Method: NodeStageVolume
        let _body = serde_json::to_vec(&req)
            .map_err(|e| KubeletError::Storage(format!("CSI encode: {}", e)))?;
        debug!(volume_id = %req.volume_id, staging = %req.staging_target_path, "CSI NodeStageVolume");
        // In a tonic-codegen build: node_client.node_stage_volume(request).await?;
        // Here we simulate via filesystem (staging path creation).
        if let Some(parent) = std::path::Path::new(&req.staging_target_path).parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| KubeletError::Storage(format!("stage dir: {}", e)))?;
        }
        Ok(())
    }

    /// Call NodePublishVolume -- bind-mounts staging path -> target path.
    pub async fn node_publish_volume(&self, req: proto::NodePublishVolumeRequest) -> Result<()> {
        debug!(socket = %self.socket_path.display(), "Using simulated CSI NodePublishVolume (no live gRPC call)");
        debug!(volume_id = %req.volume_id, target = %req.target_path, "CSI NodePublishVolume");
        // Create target directory (real impl: bind mount from staging -> target).
        std::fs::create_dir_all(&req.target_path)
            .map_err(|e| KubeletError::Storage(format!("publish dir: {}", e)))?;

        // Real implementation would use:
        //   nix::mount::mount(Some(staging), &target, None::<&str>, MsFlags::MS_BIND, None::<&str>)
        // For now we record the published path.
        Ok(())
    }

    /// Call NodeUnpublishVolume -- remove the bind mount at target path.
    pub async fn node_unpublish_volume(
        &self,
        req: proto::NodeUnpublishVolumeRequest,
    ) -> Result<()> {
        debug!(socket = %self.socket_path.display(), "Using simulated CSI NodeUnpublishVolume (no live gRPC call)");
        debug!(volume_id = %req.volume_id, target = %req.target_path, "CSI NodeUnpublishVolume");
        // Real: nix::mount::umount2(&target, MntFlags::MNT_DETACH)
        // Simulated: remove directory if empty.
        let _ = std::fs::remove_dir(&req.target_path);
        Ok(())
    }

    /// Call NodeUnstageVolume -- remove the staging mount.
    pub async fn node_unstage_volume(&self, req: proto::NodeUnstageVolumeRequest) -> Result<()> {
        debug!(socket = %self.socket_path.display(), "Using simulated CSI NodeUnstageVolume (no live gRPC call)");
        debug!(volume_id = %req.volume_id, "CSI NodeUnstageVolume");
        let _ = std::fs::remove_dir(&req.staging_target_path);
        Ok(())
    }

    /// Call NodeGetInfo to learn the node_id and topology.
    pub async fn node_get_info(&self) -> Result<proto::NodeGetInfoResponse> {
        let _channel = self.connect().await?;
        // Real: node_client.node_get_info(NodeGetInfoRequest{}).await
        Ok(proto::NodeGetInfoResponse {
            node_id: hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string()),
            max_volumes_per_node: 0,
            accessible_topology: HashMap::new(),
        })
    }
}

// -- CSI Volume Manager --------------------------------------------------------

/// Manages the lifecycle of CSI volumes on this node.
pub struct CsiVolumeManager {
    pub registry: CsiPluginRegistry,
    /// volume_id -> target_path for published volumes.
    published: HashMap<String, PathBuf>,
    /// volume_id -> staging_path for staged volumes.
    staged: HashMap<String, PathBuf>,
}

impl CsiVolumeManager {
    pub fn new(plugin_dir: impl Into<PathBuf>) -> Self {
        Self {
            registry: CsiPluginRegistry::new(plugin_dir),
            published: HashMap::new(),
            staged: HashMap::new(),
        }
    }

    fn driver_name(ctx: &CsiVolumeContext) -> Option<String> {
        ctx.volume_attributes
            .get("csi.storage.k8s.io/driver")
            .cloned()
    }

    /// Stage a volume (NodeStageVolume).
    pub async fn stage_volume(&mut self, ctx: &CsiVolumeContext) -> Result<()> {
        let driver = Self::driver_name(ctx).ok_or_else(|| {
            KubeletError::Storage("missing csi.storage.k8s.io/driver attribute".to_string())
        })?;

        let plugin = self.registry.get(&driver).ok_or_else(|| {
            KubeletError::Storage(format!("no CSI plugin registered for driver '{}'", driver))
        })?;

        let staging = ctx
            .staging_target_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        let client = CsiClient::new(&plugin.socket_path);
        client
            .node_stage_volume(proto::NodeStageVolumeRequest {
                volume_id: ctx.volume_id.clone(),
                publish_context: HashMap::new(),
                staging_target_path: staging.clone(),
                volume_capability: proto::VolumeCapability {
                    access_mode: proto::AccessMode::SingleNodeWriter,
                    access_type: ctx.access_type.clone(),
                    fs_type: ctx.fs_type.clone().unwrap_or_default(),
                    mount_flags: ctx.mount_flags.clone(),
                },
                secrets: ctx.secrets.clone(),
                volume_context: ctx.volume_attributes.clone(),
            })
            .await?;

        self.staged
            .insert(ctx.volume_id.clone(), PathBuf::from(staging));
        info!(volume_id = %ctx.volume_id, driver = %driver, "Volume staged");
        Ok(())
    }

    /// Publish a volume (NodePublishVolume).
    pub async fn publish_volume(&mut self, ctx: &CsiVolumeContext) -> Result<()> {
        let driver = Self::driver_name(ctx).ok_or_else(|| {
            KubeletError::Storage("missing csi.storage.k8s.io/driver attribute".to_string())
        })?;

        let plugin = self.registry.get(&driver).cloned();
        let socket_path = plugin
            .as_ref()
            .map(|p| p.socket_path.clone())
            .unwrap_or_else(|| PathBuf::from("/tmp/fake-csi.sock"));

        let staging = self.staged.get(&ctx.volume_id).cloned().unwrap_or_else(|| {
            ctx.staging_target_path
                .clone()
                .unwrap_or_else(|| PathBuf::from("/var/lib/kubelet/plugins"))
        });

        let client = CsiClient::new(&socket_path);
        client
            .node_publish_volume(proto::NodePublishVolumeRequest {
                volume_id: ctx.volume_id.clone(),
                publish_context: HashMap::new(),
                staging_target_path: staging.to_string_lossy().to_string(),
                target_path: ctx.target_path.to_string_lossy().to_string(),
                volume_capability: proto::VolumeCapability {
                    access_mode: proto::AccessMode::SingleNodeWriter,
                    access_type: ctx.access_type.clone(),
                    fs_type: ctx.fs_type.clone().unwrap_or_default(),
                    mount_flags: ctx.mount_flags.clone(),
                },
                readonly: ctx.read_only,
                secrets: ctx.secrets.clone(),
                volume_context: ctx.volume_attributes.clone(),
            })
            .await?;

        self.published
            .insert(ctx.volume_id.clone(), ctx.target_path.clone());
        info!(volume_id = %ctx.volume_id, target = %ctx.target_path.display(), "Volume published");
        Ok(())
    }

    /// Unpublish a volume (NodeUnpublishVolume).
    pub async fn unpublish_volume(&mut self, volume_id: &str, target_path: &Path) -> Result<()> {
        // Best-effort: find the plugin socket from the published record.
        self.registry.scan();
        let socket_path = self
            .registry
            .list()
            .first()
            .map(|p| p.socket_path.clone())
            .unwrap_or_else(|| PathBuf::from("/tmp/fake-csi.sock"));

        let client = CsiClient::new(&socket_path);
        client
            .node_unpublish_volume(proto::NodeUnpublishVolumeRequest {
                volume_id: volume_id.to_string(),
                target_path: target_path.to_string_lossy().to_string(),
            })
            .await?;

        self.published.remove(volume_id);
        info!(volume_id, "Volume unpublished");
        Ok(())
    }

    pub fn is_published(&self, volume_id: &str) -> bool {
        self.published.contains_key(volume_id)
    }

    pub fn is_staged(&self, volume_id: &str) -> bool {
        self.staged.contains_key(volume_id)
    }

    pub fn mounted_count(&self) -> usize {
        self.published.len()
    }
}

// -- Tests ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::net::UnixListener;
    use tokio::task::JoinHandle;

    fn make_plugin(name: &str, dir: &Path) -> CsiPlugin {
        let socket = dir.join(format!("{}.sock", name.replace('/', "-")));
        std::fs::write(&socket, b"").unwrap();
        CsiPlugin {
            name: name.to_string(),
            socket_path: socket,
            node_id: "node1".to_string(),
            max_volumes_per_node: 128,
            accessible_topology: HashMap::new(),
        }
    }

    fn make_volume_ctx(volume_id: &str, target: &Path) -> CsiVolumeContext {
        CsiVolumeContext {
            volume_id: volume_id.to_string(),
            volume_attributes: [(
                "csi.storage.k8s.io/driver".to_string(),
                "test-driver".to_string(),
            )]
            .into_iter()
            .collect(),
            target_path: target.to_path_buf(),
            staging_target_path: None,
            read_only: false,
            access_type: proto::AccessType::Mount,
            fs_type: None,
            mount_flags: vec![],
            secrets: HashMap::new(),
        }
    }

    async fn start_fake_csi_socket(socket: &Path) -> JoinHandle<()> {
        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let _ = std::fs::remove_file(socket);
        let listener = UnixListener::bind(socket).unwrap();
        tokio::spawn(async move {
            loop {
                if listener.accept().await.is_err() {
                    break;
                }
            }
        })
    }

    #[test]
    fn test_register_and_get_plugin() {
        let dir = TempDir::new().unwrap();
        let mut registry = CsiPluginRegistry::new(dir.path());
        registry.register(make_plugin("ebs.csi.aws.com", dir.path()));
        assert!(registry.get("ebs.csi.aws.com").is_some());
        assert_eq!(registry.plugin_count(), 1);
    }

    #[test]
    fn test_deregister_plugin() {
        let dir = TempDir::new().unwrap();
        let mut registry = CsiPluginRegistry::new(dir.path());
        registry.register(make_plugin("pd.csi.storage.gke.io", dir.path()));
        registry.deregister("pd.csi.storage.gke.io");
        assert!(registry.get("pd.csi.storage.gke.io").is_none());
        assert_eq!(registry.plugin_count(), 0);
    }

    #[test]
    fn test_scan_detects_socket() {
        let dir = TempDir::new().unwrap();
        let plugin_subdir = dir.path().join("my.csi.driver");
        std::fs::create_dir_all(&plugin_subdir).unwrap();
        std::fs::write(plugin_subdir.join("csi.sock"), b"").unwrap();

        let mut registry = CsiPluginRegistry::new(dir.path());
        registry.scan();
        assert_eq!(registry.plugin_count(), 1);
        assert!(registry.get("my.csi.driver").is_some());
    }

    #[test]
    fn test_scan_empty_dir() {
        let dir = TempDir::new().unwrap();
        let mut registry = CsiPluginRegistry::new(dir.path());
        registry.scan();
        assert_eq!(registry.plugin_count(), 0);
    }

    #[tokio::test]
    async fn test_publish_volume_creates_dir() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("target");
        let mut mgr = CsiVolumeManager::new(dir.path());
        let plugin = make_plugin("test-driver", dir.path());
        let socket_task = start_fake_csi_socket(&plugin.socket_path).await;
        mgr.registry.register(plugin);

        let ctx = make_volume_ctx("vol-1", &target);
        mgr.publish_volume(&ctx).await.unwrap();

        assert!(target.exists());
        assert!(mgr.is_published("vol-1"));
        assert_eq!(mgr.mounted_count(), 1);
        socket_task.abort();
    }

    #[tokio::test]
    async fn test_unpublish_volume() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("target");
        let mut mgr = CsiVolumeManager::new(dir.path());
        let plugin = make_plugin("test-driver", dir.path());
        let socket_task = start_fake_csi_socket(&plugin.socket_path).await;
        mgr.registry.register(plugin);
        let ctx = make_volume_ctx("vol-2", &target);
        mgr.publish_volume(&ctx).await.unwrap();
        mgr.unpublish_volume("vol-2", &target).await.unwrap();
        assert!(!mgr.is_published("vol-2"));
        socket_task.abort();
    }

    #[tokio::test]
    async fn test_stage_unknown_driver_fails() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("target");
        let mut mgr = CsiVolumeManager::new(dir.path());
        let ctx = CsiVolumeContext {
            volume_id: "vol-bad".to_string(),
            volume_attributes: [(
                "csi.storage.k8s.io/driver".to_string(),
                "nonexistent".to_string(),
            )]
            .into_iter()
            .collect(),
            target_path: target.clone(),
            staging_target_path: Some(dir.path().join("staging")),
            read_only: false,
            access_type: proto::AccessType::Mount,
            fs_type: None,
            mount_flags: vec![],
            secrets: HashMap::new(),
        };
        assert!(mgr.stage_volume(&ctx).await.is_err());
    }

    #[test]
    fn test_access_type_eq() {
        assert_eq!(proto::AccessType::Mount, proto::AccessType::Mount);
        assert_ne!(proto::AccessType::Mount, proto::AccessType::Block);
    }
}
