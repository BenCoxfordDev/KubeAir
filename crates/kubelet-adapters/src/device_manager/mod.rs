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

//! Device Plugin framework -- mirrors pkg/kubelet/cm/devicemanager.
//!
//! Allows external device plugins (GPU, FPGA, SR-IOV NIC, etc.) to advertise
//! and allocate extended resources to containers.
//!
//! Protocol:
//!   1. Plugin registers via gRPC to /var/lib/kubelet/device-plugins/kubelet.sock.
//!   2. Kubelet calls ListAndWatch to discover available devices.
//!   3. On pod admission, kubelet calls Allocate(device_ids) on the plugin.
//!   4. Plugin returns environment variables, mounts, and device node paths.
//!   5. Kubelet injects these into the container spec before calling CRI.
//!
//! References:
//!   pkg/kubelet/cm/devicemanager/manager.go
//!   k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1/api.proto

use hyper_util::rt::TokioIo;
use kubelet_core::error::{KubeletError, Result};
use kubelet_core::types::PodUID;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::UnixStream;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::{debug, error, info, warn};

pub mod proto {
    tonic::include_proto!("v1beta1");
}

use proto::{
    device_plugin_client::DevicePluginClient,
    device_plugin_server::{DevicePlugin, DevicePluginServer},
    AllocateRequest as ProtoAllocateRequest, AllocateResponse as ProtoAllocateResponse,
    ContainerAllocateRequest as ProtoContainerAllocateRequest,
    ContainerAllocateResponse as ProtoContainerAllocateResponse, Device as ProtoDevice,
    DevicePluginOptions, Empty, ListAndWatchResponse as ProtoListAndWatchResponse,
};

// -- Device types --------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DeviceHealth {
    Healthy,
    Unhealthy,
}

/// A single device advertised by a plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Device {
    pub id: String,
    pub health: DeviceHealth,
    /// NUMA node affinity for topology hints.
    pub numa_node: Option<u32>,
}

/// What a plugin returns when Allocate is called.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AllocateResponse {
    /// Env vars to inject into the container.
    pub envs: HashMap<String, String>,
    /// Paths to host devices to expose (e.g. /dev/nvidia0).
    pub devices: Vec<DeviceSpec>,
    /// Mounts to add to the container.
    pub mounts: Vec<Mount>,
    /// Annotations to add to the container.
    pub annotations: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceSpec {
    pub container_path: String,
    pub host_path: String,
    pub permissions: String, // "r", "w", "rw", "m"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mount {
    pub container_path: String,
    pub host_path: String,
    pub read_only: bool,
}

type DeviceIdsByResource = HashMap<String, Vec<String>>;
type ContainerDeviceAllocations = HashMap<String, DeviceIdsByResource>;
type PodAllocations = HashMap<String, ContainerDeviceAllocations>;

fn allocation_key(pod_uid: &PodUID, container_name: &str) -> String {
    format!("{}:{}", pod_uid.0, container_name)
}

fn connect_device_plugin_client(socket_path: PathBuf) -> Result<DevicePluginClient<Channel>> {
    let channel = Endpoint::try_from("http://[::]:50053")
        .map_err(|e| KubeletError::Resource(format!("invalid device plugin endpoint: {}", e)))?
        .connect_with_connector_lazy(service_fn(move |_: Uri| {
            let path = socket_path.clone();
            async move { UnixStream::connect(&path).await.map(TokioIo::new) }
        }));

    Ok(DevicePluginClient::new(channel))
}

fn proto_device_to_local(device: ProtoDevice) -> Device {
    let health = if device.health.eq_ignore_ascii_case("healthy") {
        DeviceHealth::Healthy
    } else {
        DeviceHealth::Unhealthy
    };

    let numa_node = device
        .topology
        .and_then(|topology| topology.nodes.into_iter().next())
        .and_then(|node| u32::try_from(node.id).ok());

    Device {
        id: device.id,
        health,
        numa_node,
    }
}

fn proto_allocate_response_to_local(response: ProtoContainerAllocateResponse) -> AllocateResponse {
    AllocateResponse {
        envs: response.envs,
        devices: response
            .devices
            .into_iter()
            .map(|device| DeviceSpec {
                container_path: device.container_path,
                host_path: device.host_path,
                permissions: device.permissions,
            })
            .collect(),
        mounts: response
            .mounts
            .into_iter()
            .map(|mount| Mount {
                container_path: mount.container_path,
                host_path: mount.host_path,
                read_only: mount.read_only,
            })
            .collect(),
        annotations: response.annotations,
    }
}

// -- Plugin endpoint -----------------------------------------------------------

/// Represents a registered device plugin.
#[derive(Debug)]
pub struct PluginEndpoint {
    pub resource_name: String,
    pub socket_path: PathBuf,
    pub devices: Vec<Device>,
    /// allocations: container_id -> device_ids
    pub allocations: HashMap<String, Vec<String>>,
}

impl PluginEndpoint {
    pub fn new(resource_name: impl Into<String>, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            resource_name: resource_name.into(),
            socket_path: socket_path.into(),
            devices: vec![],
            allocations: HashMap::new(),
        }
    }

    pub fn healthy_count(&self) -> usize {
        self.devices
            .iter()
            .filter(|d| d.health == DeviceHealth::Healthy)
            .count()
    }

    pub fn available_count(&self) -> usize {
        let allocated: std::collections::HashSet<&str> = self
            .allocations
            .values()
            .flat_map(|ids| ids.iter().map(|s| s.as_str()))
            .collect();
        self.devices
            .iter()
            .filter(|d| d.health == DeviceHealth::Healthy && !allocated.contains(d.id.as_str()))
            .count()
    }

    /// Take `count` available device IDs.
    pub fn allocate_devices(&mut self, count: usize) -> Option<Vec<String>> {
        let allocated_set: std::collections::HashSet<String> = self
            .allocations
            .values()
            .flat_map(|ids| ids.iter().cloned())
            .collect();

        let available: Vec<String> = self
            .devices
            .iter()
            .filter(|d| d.health == DeviceHealth::Healthy && !allocated_set.contains(&d.id))
            .take(count)
            .map(|d| d.id.clone())
            .collect();

        if available.len() < count {
            return None;
        }
        Some(available)
    }

    /// Simulate a gRPC Allocate call to the plugin socket.
    pub async fn call_allocate(&self, device_ids: Vec<String>) -> Result<AllocateResponse> {
        let mut client = connect_device_plugin_client(self.socket_path.clone())?;
        let response = client
            .allocate(ProtoAllocateRequest {
                container_requests: vec![ProtoContainerAllocateRequest {
                    devices_i_ds: device_ids,
                }],
            })
            .await
            .map_err(|e| {
                KubeletError::Resource(format!("device plugin Allocate RPC failed: {}", e))
            })?
            .into_inner();

        let container_response =
            response
                .container_responses
                .into_iter()
                .next()
                .ok_or_else(|| {
                    KubeletError::Resource(format!(
                        "device plugin '{}' returned no container allocation response",
                        self.resource_name
                    ))
                })?;

        Ok(proto_allocate_response_to_local(container_response))
    }

    pub async fn list_and_watch_once(&self) -> Result<Vec<Device>> {
        let mut client = connect_device_plugin_client(self.socket_path.clone())?;
        let mut stream = client
            .list_and_watch(Empty {})
            .await
            .map_err(|e| {
                KubeletError::Resource(format!("device plugin ListAndWatch RPC failed: {}", e))
            })?
            .into_inner();

        let update = stream.message().await.map_err(|e| {
            KubeletError::Resource(format!("device plugin ListAndWatch stream failed: {}", e))
        })?;

        let update = update.ok_or_else(|| {
            KubeletError::Resource(format!(
                "device plugin '{}' closed ListAndWatch without publishing devices",
                self.resource_name
            ))
        })?;

        Ok(update
            .devices
            .into_iter()
            .map(proto_device_to_local)
            .collect())
    }
}

// -- Device Manager ------------------------------------------------------------

/// Central device manager. Tracks all registered plugins and their allocations.
pub struct DeviceManager {
    /// resource_name -> plugin endpoint
    plugins: Arc<RwLock<HashMap<String, PluginEndpoint>>>,
    /// plugin socket directory
    socket_dir: PathBuf,
    /// pod -> container -> resource -> device_ids
    pod_allocations: Arc<RwLock<PodAllocations>>,
}

/// Persisted registry of resource_name -> socket_path.
/// Written on every registration so we can reload state after a kubelet restart.
#[derive(Debug, Default, Serialize, Deserialize)]
struct PluginRegistryCheckpoint {
    entries: HashMap<String, String>,
}

impl DeviceManager {
    pub fn new(socket_dir: impl Into<PathBuf>) -> Self {
        Self {
            plugins: Arc::new(RwLock::new(HashMap::new())),
            socket_dir: socket_dir.into(),
            pod_allocations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn registry_checkpoint_path(&self) -> PathBuf {
        self.socket_dir.join("device_plugin_registry.json")
    }

    /// Persist resource_name -> socket_path mapping so it survives kubelet restarts.
    fn save_plugin_checkpoint(&self, resource_name: &str, socket_path: &Path) {
        let checkpoint_path = self.registry_checkpoint_path();
        let mut checkpoint: PluginRegistryCheckpoint = std::fs::read_to_string(&checkpoint_path)
            .ok()
            .and_then(|data| serde_json::from_str(&data).ok())
            .unwrap_or_default();
        checkpoint.entries.insert(
            resource_name.to_string(),
            socket_path.to_string_lossy().into_owned(),
        );
        if let Ok(data) = serde_json::to_string_pretty(&checkpoint) {
            if let Err(err) = std::fs::write(&checkpoint_path, data) {
                warn!(path = %checkpoint_path.display(), error = %err, "Failed to persist device plugin registry");
            }
        }
    }

    /// Re-discover plugins that were registered before this kubelet instance started.
    ///
    /// Reads the registry checkpoint and re-registers each plugin whose socket file
    /// still exists. Called once at startup so existing containers can be allocated
    /// devices without waiting for plugins to re-register themselves.
    pub async fn rediscover_plugins(&self) {
        let checkpoint_path = self.registry_checkpoint_path();
        let checkpoint: PluginRegistryCheckpoint = match std::fs::read_to_string(&checkpoint_path) {
            Ok(data) => match serde_json::from_str(&data) {
                Ok(c) => c,
                Err(err) => {
                    warn!(path = %checkpoint_path.display(), error = %err, "Device plugin registry checkpoint is malformed; skipping rediscovery");
                    return;
                }
            },
            Err(_) => {
                // No checkpoint yet — first run or file deleted.
                return;
            }
        };

        for (resource_name, socket_path_str) in &checkpoint.entries {
            let socket_path = PathBuf::from(socket_path_str);
            if socket_path.exists() {
                info!(resource = %resource_name, socket = %socket_path.display(), "Rediscovering device plugin from checkpoint");
                self.register_plugin(resource_name, &socket_path).await;
            } else {
                info!(resource = %resource_name, socket = %socket_path.display(), "Device plugin socket no longer present; skipping rediscovery");
            }
        }
    }

    /// Register a plugin. Called when a plugin connects to the registration socket.
    pub async fn register_plugin(&self, resource_name: &str, socket_path: &Path) {
        let mut plugins = self.plugins.write().await;
        let endpoint = PluginEndpoint::new(resource_name, socket_path);
        info!(resource = %resource_name, socket = %socket_path.display(), "Device plugin registered");
        plugins.insert(resource_name.to_string(), endpoint);
        drop(plugins);
        self.save_plugin_checkpoint(resource_name, socket_path);
        self.spawn_plugin_watch(resource_name.to_string(), socket_path.to_path_buf());
    }

    fn spawn_plugin_watch(&self, resource_name: String, socket_path: PathBuf) {
        let plugins = self.plugins.clone();
        tokio::spawn(async move {
            let endpoint = PluginEndpoint::new(resource_name.clone(), socket_path.clone());
            let mut client = match connect_device_plugin_client(socket_path.clone()) {
                Ok(client) => client,
                Err(err) => {
                    warn!(resource = %resource_name, socket = %socket_path.display(), error = %err, "Device plugin watch connect failed");
                    return;
                }
            };

            let mut stream = match client.list_and_watch(Empty {}).await {
                Ok(response) => response.into_inner(),
                Err(err) => {
                    warn!(resource = %resource_name, socket = %socket_path.display(), error = %err, "Device plugin ListAndWatch setup failed");
                    return;
                }
            };

            loop {
                match stream.message().await {
                    Ok(Some(update)) => {
                        let devices: Vec<Device> = update
                            .devices
                            .into_iter()
                            .map(proto_device_to_local)
                            .collect();
                        let healthy = devices
                            .iter()
                            .filter(|device| device.health == DeviceHealth::Healthy)
                            .count();
                        let mut guard = plugins.write().await;
                        if let Some(plugin) = guard.get_mut(&resource_name) {
                            plugin.devices = devices;
                            info!(resource = %resource_name, total = plugin.devices.len(), healthy, "Device list updated from ListAndWatch");
                        } else {
                            debug!(resource = %resource_name, "Device plugin removed before ListAndWatch update");
                            break;
                        }
                    }
                    Ok(None) => {
                        debug!(resource = %resource_name, socket = %endpoint.socket_path.display(), "Device plugin ListAndWatch stream ended");
                        break;
                    }
                    Err(err) => {
                        warn!(resource = %resource_name, socket = %endpoint.socket_path.display(), error = %err, "Device plugin ListAndWatch stream failed");
                        break;
                    }
                }
            }
        });
    }

    /// Update device list from a ListAndWatch stream event.
    pub async fn update_devices(&self, resource_name: &str, devices: Vec<Device>) {
        let mut plugins = self.plugins.write().await;
        if let Some(ep) = plugins.get_mut(resource_name) {
            let healthy = devices
                .iter()
                .filter(|d| d.health == DeviceHealth::Healthy)
                .count();
            info!(resource = %resource_name, total = devices.len(), healthy, "Device list updated");
            ep.devices = devices;
        }
    }

    /// Allocate devices for a container.
    ///
    /// Called during pod admission. Returns the `AllocateResponse` to inject
    /// into the container spec before calling CRI CreateContainer.
    pub async fn allocate(
        &self,
        pod_uid: &PodUID,
        container_name: &str,
        resource_name: &str,
        count: usize,
    ) -> Result<AllocateResponse> {
        let allocation_key = allocation_key(pod_uid, container_name);
        let device_ids;
        let socket_path;
        {
            let mut plugins = self.plugins.write().await;

            let ep = plugins.get_mut(resource_name).ok_or_else(|| {
                KubeletError::Resource(format!("no device plugin for resource '{}'", resource_name))
            })?;

            device_ids = ep.allocate_devices(count).ok_or_else(|| {
                KubeletError::Resource(format!(
                    "not enough '{}' devices: need {}, available {}",
                    resource_name,
                    count,
                    ep.available_count()
                ))
            })?;
            socket_path = ep.socket_path.clone();
            ep.allocations
                .insert(allocation_key.clone(), device_ids.clone());
        }

        info!(
            pod = %pod_uid.0, container = %container_name,
            resource = %resource_name,
            devices = ?device_ids,
            "Allocating devices"
        );

        let response = match PluginEndpoint::new(resource_name, socket_path)
            .call_allocate(device_ids.clone())
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let mut plugins = self.plugins.write().await;
                if let Some(ep) = plugins.get_mut(resource_name) {
                    ep.allocations.remove(&allocation_key);
                }
                return Err(err);
            }
        };

        // Record in pod allocations for cleanup.
        let mut pod_allocs = self.pod_allocations.write().await;
        pod_allocs
            .entry(pod_uid.0.clone())
            .or_default()
            .entry(container_name.to_string())
            .or_default()
            .entry(resource_name.to_string())
            .or_default()
            .extend(device_ids);

        Ok(response)
    }

    pub async fn refresh_plugin_devices(&self, resource_name: &str) -> Result<()> {
        let endpoint = {
            let plugins = self.plugins.read().await;
            let ep = plugins.get(resource_name).ok_or_else(|| {
                KubeletError::Resource(format!("no device plugin for resource '{}'", resource_name))
            })?;
            PluginEndpoint::new(resource_name, ep.socket_path.clone())
        };
        let devices = endpoint.list_and_watch_once().await?;
        self.update_devices(resource_name, devices).await;
        Ok(())
    }

    /// Release all devices allocated to a pod.
    pub async fn deallocate_pod(&self, pod_uid: &PodUID) {
        let mut pod_allocs = self.pod_allocations.write().await;
        if let Some(container_allocations) = pod_allocs.remove(&pod_uid.0) {
            info!(pod = %pod_uid.0, "Device allocations released");
            let mut plugins = self.plugins.write().await;
            for (container_name, resources) in container_allocations {
                let key = allocation_key(pod_uid, &container_name);
                for (resource_name, released_ids) in resources {
                    if let Some(ep) = plugins.get_mut(&resource_name) {
                        if let Some(allocated_ids) = ep.allocations.get_mut(&key) {
                            allocated_ids.retain(|id| !released_ids.contains(id));
                            if allocated_ids.is_empty() {
                                ep.allocations.remove(&key);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Return extended resource capacities to advertise on the Node.
    pub async fn extended_resources(&self) -> HashMap<String, u64> {
        let plugins = self.plugins.read().await;
        plugins
            .iter()
            .map(|(name, ep)| (name.clone(), ep.healthy_count() as u64))
            .collect()
    }

    pub async fn plugin_count(&self) -> usize {
        self.plugins.read().await.len()
    }

    /// Scan the socket directory for new plugin sockets.
    pub async fn scan_and_register(&self) {
        let Ok(entries) = std::fs::read_dir(&self.socket_dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("sock") {
                continue;
            }
            let resource_name = path
                .file_stem()
                .and_then(|n| n.to_str())
                .and_then(|name| {
                    let (domain, resource) = name.split_once('-')?;
                    Some(format!("{}/{}", domain, resource))
                })
                .unwrap_or_default();
            if resource_name.is_empty() {
                continue;
            }
            self.register_plugin(&resource_name, &path).await;
        }
    }
}

// -- Tests ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use futures::Stream;
    use kubelet_core::types::PodUID;
    use tempfile::TempDir;
    use tokio::net::UnixListener;
    use tonic::{Request, Response, Status};

    type WatchStream =
        Pin<Box<dyn Stream<Item = std::result::Result<ProtoListAndWatchResponse, Status>> + Send>>;

    #[derive(Clone)]
    struct MockDevicePlugin {
        devices: Vec<ProtoDevice>,
        allocate_response: ProtoContainerAllocateResponse,
    }

    #[tonic::async_trait]
    impl DevicePlugin for MockDevicePlugin {
        type ListAndWatchStream = WatchStream;

        async fn get_device_plugin_options(
            &self,
            _request: Request<Empty>,
        ) -> std::result::Result<Response<DevicePluginOptions>, Status> {
            Ok(Response::new(DevicePluginOptions {
                pre_start_required: false,
                get_preferred_allocation_available: false,
            }))
        }

        async fn list_and_watch(
            &self,
            _request: Request<Empty>,
        ) -> std::result::Result<Response<Self::ListAndWatchStream>, Status> {
            let response = ProtoListAndWatchResponse {
                devices: self.devices.clone(),
            };
            Ok(Response::new(Box::pin(stream::iter(vec![Ok(response)]))))
        }

        async fn allocate(
            &self,
            request: Request<ProtoAllocateRequest>,
        ) -> std::result::Result<Response<ProtoAllocateResponse>, Status> {
            let request = request.into_inner();
            if request.container_requests.len() != 1 {
                return Err(Status::invalid_argument("expected one container request"));
            }
            Ok(Response::new(ProtoAllocateResponse {
                container_responses: vec![self.allocate_response.clone()],
            }))
        }
    }

    async fn spawn_mock_device_plugin(
        socket_path: &Path,
        devices: Vec<ProtoDevice>,
        allocate_response: ProtoContainerAllocateResponse,
    ) {
        let _ = std::fs::remove_file(socket_path);
        let listener = UnixListener::bind(socket_path).unwrap();
        let service = MockDevicePlugin {
            devices,
            allocate_response,
        };
        tokio::spawn(async move {
            let incoming = futures::stream::unfold(listener, |listener| async {
                match listener.accept().await {
                    Ok((stream, _)) => Some((Ok::<_, std::io::Error>(stream), listener)),
                    Err(_) => None,
                }
            });
            tonic::transport::Server::builder()
                .add_service(DevicePluginServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
    }

    fn proto_gpu_device(id: &str, healthy: bool) -> ProtoDevice {
        ProtoDevice {
            id: id.to_string(),
            health: if healthy {
                "Healthy".to_string()
            } else {
                "Unhealthy".to_string()
            },
            topology: Some(proto::TopologyInfo {
                nodes: vec![proto::NumaNode { id: 0 }],
            }),
        }
    }

    fn gpu_allocate_response(device_id: &str) -> ProtoContainerAllocateResponse {
        let mut envs = HashMap::new();
        envs.insert("NVIDIA_VISIBLE_DEVICES".to_string(), device_id.to_string());

        let mut annotations = HashMap::new();
        annotations.insert("plugin.kubelet.rs/test".to_string(), "true".to_string());

        ProtoContainerAllocateResponse {
            envs,
            mounts: vec![proto::Mount {
                container_path: "/usr/lib/libnvidia.so".to_string(),
                host_path: "/host/usr/lib/libnvidia.so".to_string(),
                read_only: true,
            }],
            devices: vec![proto::DeviceSpec {
                container_path: "/dev/nvidia0".to_string(),
                host_path: "/dev/nvidia0".to_string(),
                permissions: "rw".to_string(),
            }],
            annotations,
        }
    }

    fn gpu_device(id: &str, healthy: bool) -> Device {
        Device {
            id: id.to_string(),
            health: if healthy {
                DeviceHealth::Healthy
            } else {
                DeviceHealth::Unhealthy
            },
            numa_node: Some(0),
        }
    }

    #[tokio::test]
    async fn test_register_plugin() {
        let dir = TempDir::new().unwrap();
        let mgr = DeviceManager::new(dir.path());
        let sock = dir.path().join("kubelet.sock");
        mgr.register_plugin("nvidia.com/gpu", &sock).await;
        assert_eq!(mgr.plugin_count().await, 1);
    }

    #[tokio::test]
    async fn test_update_devices() {
        let dir = TempDir::new().unwrap();
        let mgr = DeviceManager::new(dir.path());
        let sock = dir.path().join("kubelet.sock");
        mgr.register_plugin("nvidia.com/gpu", &sock).await;
        mgr.update_devices(
            "nvidia.com/gpu",
            vec![
                gpu_device("GPU-0", true),
                gpu_device("GPU-1", true),
                gpu_device("GPU-2", false),
            ],
        )
        .await;
        let resources = mgr.extended_resources().await;
        assert_eq!(resources["nvidia.com/gpu"], 2); // only healthy
    }

    #[tokio::test]
    async fn test_allocate_success() {
        let dir = TempDir::new().unwrap();
        let mgr = DeviceManager::new(dir.path());
        let sock = dir.path().join("nvidia.com-gpu.sock");
        spawn_mock_device_plugin(
            &sock,
            vec![
                proto_gpu_device("GPU-0", true),
                proto_gpu_device("GPU-1", true),
            ],
            gpu_allocate_response("GPU-0"),
        )
        .await;
        mgr.register_plugin("nvidia.com/gpu", &sock).await;
        mgr.refresh_plugin_devices("nvidia.com/gpu").await.unwrap();

        let resp = mgr
            .allocate(&PodUID::new("uid-gpu"), "train", "nvidia.com/gpu", 1)
            .await
            .unwrap();
        assert_eq!(
            resp.envs.get("NVIDIA_VISIBLE_DEVICES"),
            Some(&"GPU-0".to_string())
        );
        assert_eq!(resp.mounts.len(), 1);
        assert_eq!(resp.devices.len(), 1);
        assert_eq!(
            resp.annotations.get("plugin.kubelet.rs/test"),
            Some(&"true".to_string())
        );
    }

    #[tokio::test]
    async fn test_allocate_insufficient_devices_fails() {
        let dir = TempDir::new().unwrap();
        let mgr = DeviceManager::new(dir.path());
        let sock = dir.path().join("kubelet.sock");
        mgr.register_plugin("nvidia.com/gpu", &sock).await;
        mgr.update_devices("nvidia.com/gpu", vec![gpu_device("GPU-0", true)])
            .await;

        // Want 2 but only 1 available
        let result = mgr
            .allocate(&PodUID::new("uid-big"), "train", "nvidia.com/gpu", 2)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_allocate_unknown_resource_fails() {
        let dir = TempDir::new().unwrap();
        let mgr = DeviceManager::new(dir.path());
        let result = mgr
            .allocate(&PodUID::new("uid-1"), "app", "example.com/fpga", 1)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_deallocate_pod() {
        let dir = TempDir::new().unwrap();
        let mgr = DeviceManager::new(dir.path());
        let sock = dir.path().join("nvidia.com-gpu.sock");
        spawn_mock_device_plugin(
            &sock,
            vec![proto_gpu_device("GPU-0", true)],
            gpu_allocate_response("GPU-0"),
        )
        .await;
        mgr.register_plugin("nvidia.com/gpu", &sock).await;
        mgr.refresh_plugin_devices("nvidia.com/gpu").await.unwrap();

        let uid = PodUID::new("uid-dealloc");
        mgr.allocate(&uid, "app", "nvidia.com/gpu", 1)
            .await
            .unwrap();
        mgr.deallocate_pod(&uid).await;
        // After dealloc, pod should not have entries.
        let allocs = mgr.pod_allocations.read().await;
        assert!(!allocs.contains_key("uid-dealloc"));
        drop(allocs);

        let plugins = mgr.plugins.read().await;
        let plugin = plugins.get("nvidia.com/gpu").unwrap();
        assert_eq!(plugin.available_count(), 1);
    }

    #[tokio::test]
    async fn test_refresh_plugin_devices_uses_list_and_watch() {
        let dir = TempDir::new().unwrap();
        let mgr = DeviceManager::new(dir.path());
        let sock = dir.path().join("example.com-fpga.sock");
        spawn_mock_device_plugin(
            &sock,
            vec![
                proto_gpu_device("fpga0", true),
                proto_gpu_device("fpga1", false),
            ],
            ProtoContainerAllocateResponse::default(),
        )
        .await;

        mgr.register_plugin("example.com/fpga", &sock).await;
        mgr.refresh_plugin_devices("example.com/fpga")
            .await
            .unwrap();

        let resources = mgr.extended_resources().await;
        assert_eq!(resources["example.com/fpga"], 1);
    }

    #[test]
    fn test_plugin_endpoint_available_count() {
        let mut ep = PluginEndpoint::new("test/res", "/tmp/test.sock");
        ep.devices = vec![
            Device {
                id: "d0".to_string(),
                health: DeviceHealth::Healthy,
                numa_node: None,
            },
            Device {
                id: "d1".to_string(),
                health: DeviceHealth::Healthy,
                numa_node: None,
            },
            Device {
                id: "d2".to_string(),
                health: DeviceHealth::Unhealthy,
                numa_node: None,
            },
        ];
        assert_eq!(ep.available_count(), 2);
        assert_eq!(ep.healthy_count(), 2);
    }

    /// Verify that register_plugin writes a checkpoint and rediscover_plugins
    /// re-registers entries whose socket files still exist.
    #[tokio::test]
    async fn test_checkpoint_persistence_and_rediscovery() {
        let dir = TempDir::new().unwrap();
        let sock_path = dir.path().join("ds-gpu.sock");

        // Spawn a real mock plugin so ListAndWatch can succeed in rediscover.
        spawn_mock_device_plugin(
            &sock_path,
            vec![proto_gpu_device("GPU-0", true)],
            gpu_allocate_response("GPU-0"),
        )
        .await;

        // Register via first DeviceManager instance — simulates initial kubelet run.
        {
            let mgr = DeviceManager::new(dir.path());
            mgr.register_plugin("nvidia.com/gpu", &sock_path).await;
        }

        // Checkpoint file must now exist with the entry.
        let checkpoint_path = dir.path().join("device_plugin_registry.json");
        assert!(checkpoint_path.exists(), "checkpoint file must be created");
        let data = std::fs::read_to_string(&checkpoint_path).unwrap();
        let cp: PluginRegistryCheckpoint = serde_json::from_str(&data).unwrap();
        assert!(
            cp.entries.contains_key("nvidia.com/gpu"),
            "checkpoint must contain registered resource"
        );

        // Simulate kubelet restart: new DeviceManager, call rediscover.
        let mgr2 = DeviceManager::new(dir.path());
        assert_eq!(mgr2.plugin_count().await, 0, "starts empty");
        mgr2.rediscover_plugins().await;
        // Give spawn_plugin_watch a tick to run.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            mgr2.plugin_count().await,
            1,
            "plugin must be re-registered from checkpoint"
        );
    }

    /// Verify that rediscover_plugins skips entries whose socket no longer exists.
    #[tokio::test]
    async fn test_rediscovery_skips_missing_socket() {
        let dir = TempDir::new().unwrap();

        // Write a fake checkpoint pointing at a non-existent socket.
        let checkpoint_path = dir.path().join("device_plugin_registry.json");
        let mut entries = HashMap::new();
        entries.insert(
            "vendor.io/stale".to_string(),
            dir.path().join("stale.sock").to_string_lossy().into_owned(),
        );
        let cp = PluginRegistryCheckpoint { entries };
        std::fs::write(&checkpoint_path, serde_json::to_string(&cp).unwrap()).unwrap();

        let mgr = DeviceManager::new(dir.path());
        mgr.rediscover_plugins().await;
        assert_eq!(mgr.plugin_count().await, 0, "stale socket must be skipped");
    }
}
