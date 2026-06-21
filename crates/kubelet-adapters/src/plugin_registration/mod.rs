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

//! Plugin registration server -- mirrors pkg/kubelet/pluginmanager.
//!
//! Device plugins and CSI drivers register themselves by connecting to the
//! kubelet's registration socket at:
//!   /var/lib/kubelet/device-plugins/kubelet.sock
//!
//! Registration flow:
//!   1. Plugin opens a gRPC connection to kubelet.sock.
//!   2. Plugin calls Register(RegistrationRequest) with its:
//!      - endpoint (its own socket path)
//!      - resource_name (for device plugins: e.g. "nvidia.com/gpu")
//!      - version
//!   3. Kubelet validates and calls ListAndWatch on the plugin's endpoint.
//!
//! References:
//!   k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1/api.proto
//!   pkg/kubelet/pluginmanager/pluginwatcher/plugin_watcher.go

use kubelet_core::error::{KubeletError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

// Import generated proto types for Registration service.
use crate::device_manager::proto::{
    Empty, RegisterRequest,
    registration_server::{Registration, RegistrationServer},
};

// -- Registration types --------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PluginType {
    DevicePlugin,
    CsiPlugin,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationRequest {
    /// Plugin type.
    pub plugin_type: PluginType,
    /// Plugin's own gRPC socket endpoint.
    pub endpoint: String,
    /// Resource name (device plugin) or driver name (CSI).
    pub resource_name: String,
    /// Plugin API version (e.g. "v1beta1").
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationResponse {
    pub error: String,
}

/// A successfully registered plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredPlugin {
    pub request: RegistrationRequest,
    pub socket_path: PathBuf,
    pub registered_at: std::time::SystemTime,
}

type RegisterCallback = Box<dyn Fn(&RegisteredPlugin) + Send + Sync>;
type DeregisterCallback = Box<dyn Fn(&str) + Send + Sync>;

// -- Plugin registry -----------------------------------------------------------

/// Tracks all registered plugins (device + CSI).
pub struct PluginRegistry {
    plugins: HashMap<String, RegisteredPlugin>,
    /// Callbacks for newly registered plugins.
    on_register: Vec<RegisterCallback>,
    on_deregister: Vec<DeregisterCallback>,
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: HashMap::new(),
            on_register: vec![],
            on_deregister: vec![],
        }
    }

    pub fn on_register<F: Fn(&RegisteredPlugin) + Send + Sync + 'static>(&mut self, f: F) {
        self.on_register.push(Box::new(f));
    }

    pub fn on_deregister<F: Fn(&str) + Send + Sync + 'static>(&mut self, f: F) {
        self.on_deregister.push(Box::new(f));
    }

    pub fn register(&mut self, req: RegistrationRequest, socket_path: PathBuf) -> Result<()> {
        let key = req.resource_name.clone();
        let plugin = RegisteredPlugin {
            request: req,
            socket_path,
            registered_at: std::time::SystemTime::now(),
        };
        info!(resource = %plugin.request.resource_name, "Plugin registered");
        for cb in &self.on_register {
            cb(&plugin);
        }
        self.plugins.insert(key, plugin);
        Ok(())
    }

    pub fn deregister(&mut self, resource_name: &str) {
        if self.plugins.remove(resource_name).is_some() {
            info!(resource = %resource_name, "Plugin deregistered");
            for cb in &self.on_deregister {
                cb(resource_name);
            }
        }
    }

    pub fn get(&self, resource_name: &str) -> Option<&RegisteredPlugin> {
        self.plugins.get(resource_name)
    }

    pub fn list(&self) -> Vec<&RegisteredPlugin> {
        self.plugins.values().collect()
    }

    pub fn count(&self) -> usize {
        self.plugins.len()
    }

    pub fn device_plugins(&self) -> Vec<&RegisteredPlugin> {
        self.plugins
            .values()
            .filter(|p| p.request.plugin_type == PluginType::DevicePlugin)
            .collect()
    }

    pub fn csi_plugins(&self) -> Vec<&RegisteredPlugin> {
        self.plugins
            .values()
            .filter(|p| p.request.plugin_type == PluginType::CsiPlugin)
            .collect()
    }
}

// -- Registration server -------------------------------------------------------

/// gRPC Registration service handler — accepts Register() calls from device plugins.
struct RegistrationServiceImpl {
    plugin_dir: PathBuf,
    registry: Arc<RwLock<PluginRegistry>>,
}

#[tonic::async_trait]
impl Registration for RegistrationServiceImpl {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> std::result::Result<Response<Empty>, Status> {
        let req = request.into_inner();
        info!(
            resource = %req.resource_name,
            endpoint = %req.endpoint,
            version = %req.version,
            "Device plugin registration request received"
        );

        // Endpoint is just the socket filename; resolve full path.
        let socket_path = if req.endpoint.starts_with('/') {
            PathBuf::from(&req.endpoint)
        } else {
            self.plugin_dir.join(&req.endpoint)
        };

        if !socket_path.exists() {
            warn!(
                resource = %req.resource_name,
                socket = %socket_path.display(),
                "Plugin socket does not exist at registration time"
            );
            // Don't reject — plugin may be starting up. Register anyway.
        }

        let registration = RegistrationRequest {
            plugin_type: PluginType::DevicePlugin,
            endpoint: socket_path.to_string_lossy().to_string(),
            resource_name: req.resource_name.clone(),
            version: req.version,
        };

        let mut registry = self.registry.write().await;
        registry
            .register(registration, socket_path)
            .map_err(|e| Status::internal(format!("registration failed: {e}")))?;

        Ok(Response::new(Empty {}))
    }
}

/// Listens on the kubelet registration socket for incoming plugin registrations.
pub struct PluginRegistrationServer {
    socket_path: PathBuf,
    registry: Arc<RwLock<PluginRegistry>>,
}

impl PluginRegistrationServer {
    pub fn new(socket_path: impl Into<PathBuf>, registry: Arc<RwLock<PluginRegistry>>) -> Self {
        Self {
            socket_path: socket_path.into(),
            registry,
        }
    }

    /// Bind the registration socket and serve the Registration gRPC service.
    /// Device plugins call Register() on this socket to register themselves.
    pub async fn serve(self) -> Result<()> {
        if let Some(parent) = self.socket_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| KubeletError::Runtime(format!("create plugin dir: {e}")))?;
        }

        // Remove ALL *.sock files in the plugin directory on startup.
        //
        // Device plugins watch their own socket files via
        // inotify. Removing them triggers the plugin to immediately recreate
        // the socket and re-register with kubelet — typically within a few
        // seconds. Without this, plugins only retry on their own schedule
        // (often hourly), leaving DeviceManager empty after a kubelet restart.
        if let Some(plugin_dir) = self.socket_path.parent() {
            match std::fs::read_dir(plugin_dir) {
                Ok(entries) => {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().is_some_and(|ext| ext == "sock") {
                            if let Err(e) = std::fs::remove_file(&path) {
                                warn!(path = %path.display(), error = %e, "Failed to remove stale plugin socket");
                            } else {
                                info!(path = %path.display(), "Removed stale plugin socket to trigger device plugin re-registration");
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(dir = %plugin_dir.display(), error = %e, "Failed to read plugin dir for socket cleanup");
                }
            }
        }

        let plugin_dir = self
            .socket_path
            .parent()
            .unwrap_or(Path::new("/var/lib/kubelet/device-plugins"))
            .to_path_buf();

        let listener = UnixListener::bind(&self.socket_path).map_err(|e| {
            KubeletError::Runtime(format!(
                "bind registration socket {}: {e}",
                self.socket_path.display()
            ))
        })?;

        // Make the socket world-writable so that device plugins running as non-root
        // (and conformance tests running as a non-root user) can connect.
        // We use the libc chmod syscall directly since Rust's set_permissions may
        // not work correctly for socket files on Linux.
        #[cfg(unix)]
        {
            use std::ffi::CString;
            let path_cstr =
                CString::new(self.socket_path.as_os_str().as_encoded_bytes()).unwrap_or_default();
            let ret = unsafe { libc::chmod(path_cstr.as_ptr(), 0o666) };
            if ret != 0 {
                let err = std::io::Error::last_os_error();
                warn!(socket = %self.socket_path.display(), error = %err,
                      "Failed to chmod plugin registration socket to 0666");
            }
            // Re-stat to check the actual mode after chmod (diagnostic).
            {
                use std::os::unix::fs::MetadataExt;
                let actual_mode = std::fs::metadata(&self.socket_path)
                    .ok()
                    .map(|m| format!("{:04o}", m.mode() & 0o777))
                    .unwrap_or_else(|| "unknown".to_string());
                warn!(socket = %self.socket_path.display(),
                      chmod_ret = ret,
                      actual_mode = %actual_mode,
                      "Plugin registration socket chmod diagnostic");
            }
            // Also attempt via chmod(1) as a belt-and-suspenders fallback in case
            // the libc call is silently ignored (seen on some Linux+AppArmor configs).
            match std::process::Command::new("chmod")
                .args(["0666", &self.socket_path.to_string_lossy()])
                .output()
            {
                Ok(out) if !out.status.success() => {
                    warn!(socket = %self.socket_path.display(),
                          stderr = %String::from_utf8_lossy(&out.stderr),
                          "chmod(1) failed for plugin registration socket");
                }
                Err(e) => {
                    warn!(socket = %self.socket_path.display(), error = %e,
                          "Failed to run chmod(1) for plugin registration socket");
                }
                Ok(_) => {}
            }
        }

        info!(socket = %self.socket_path.display(), "Plugin registration server started");

        let svc = RegistrationServiceImpl {
            plugin_dir,
            registry: self.registry,
        };

        tonic::transport::Server::builder()
            .add_service(RegistrationServer::new(svc))
            .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
            .await
            .map_err(|e| KubeletError::Runtime(format!("registration server error: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device_manager::proto::registration_client::RegistrationClient;
    use hyper_util::rt::TokioIo;
    use tempfile::TempDir;
    use tokio::net::UnixStream;
    use tonic::transport::{Channel, Endpoint, Uri};
    use tower::service_fn;

    // ── PluginRegistry unit tests ─────────────────────────────────────────────

    #[test]
    fn test_plugin_registry_register_and_get() {
        let mut registry = PluginRegistry::new();
        let req = RegistrationRequest {
            plugin_type: PluginType::DevicePlugin,
            endpoint: "/var/lib/kubelet/device-plugins/nvidia.sock".to_string(),
            resource_name: "nvidia.com/gpu".to_string(),
            version: "v1beta1".to_string(),
        };
        registry
            .register(req, "/var/lib/kubelet/device-plugins/nvidia.sock".into())
            .unwrap();
        assert!(registry.get("nvidia.com/gpu").is_some());
        assert_eq!(registry.count(), 1);
    }

    #[test]
    fn test_plugin_registry_deregister() {
        let mut registry = PluginRegistry::new();
        registry
            .register(
                RegistrationRequest {
                    plugin_type: PluginType::DevicePlugin,
                    endpoint: "/tmp/gpu.sock".to_string(),
                    resource_name: "nvidia.com/gpu".to_string(),
                    version: "v1beta1".to_string(),
                },
                "/tmp/gpu.sock".into(),
            )
            .unwrap();
        registry.deregister("nvidia.com/gpu");
        assert!(registry.get("nvidia.com/gpu").is_none());
        assert_eq!(registry.count(), 0);
    }

    #[test]
    fn test_plugin_registry_filter_by_type() {
        let mut registry = PluginRegistry::new();
        registry
            .register(
                RegistrationRequest {
                    plugin_type: PluginType::DevicePlugin,
                    endpoint: "/tmp/gpu.sock".to_string(),
                    resource_name: "nvidia.com/gpu".to_string(),
                    version: "v1beta1".to_string(),
                },
                "/tmp/gpu.sock".into(),
            )
            .unwrap();
        registry
            .register(
                RegistrationRequest {
                    plugin_type: PluginType::CsiPlugin,
                    endpoint: "/tmp/ebs.sock".to_string(),
                    resource_name: "ebs.csi.aws.com".to_string(),
                    version: "v1".to_string(),
                },
                "/tmp/ebs.sock".into(),
            )
            .unwrap();
        assert_eq!(registry.device_plugins().len(), 1);
        assert_eq!(registry.csi_plugins().len(), 1);
    }

    #[test]
    fn test_plugin_registry_on_register_callback() {
        let mut registry = PluginRegistry::new();
        let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_clone = called.clone();
        registry.on_register(move |_| {
            called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        registry
            .register(
                RegistrationRequest {
                    plugin_type: PluginType::DevicePlugin,
                    endpoint: "/tmp/test.sock".to_string(),
                    resource_name: "test.com/resource".to_string(),
                    version: "v1beta1".to_string(),
                },
                "/tmp/test.sock".into(),
            )
            .unwrap();
        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn test_plugin_registry_on_deregister_callback() {
        let mut registry = PluginRegistry::new();
        let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_clone = called.clone();
        registry.on_deregister(move |name| {
            assert_eq!(name, "test.com/resource");
            called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        registry
            .register(
                RegistrationRequest {
                    plugin_type: PluginType::DevicePlugin,
                    endpoint: "/tmp/cb_dereg.sock".to_string(),
                    resource_name: "test.com/resource".to_string(),
                    version: "v1beta1".to_string(),
                },
                "/tmp/cb_dereg.sock".into(),
            )
            .unwrap();
        registry.deregister("test.com/resource");
        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn test_plugin_registry_re_register_overwrites() {
        let mut registry = PluginRegistry::new();
        registry
            .register(
                RegistrationRequest {
                    plugin_type: PluginType::DevicePlugin,
                    endpoint: "/tmp/old.sock".to_string(),
                    resource_name: "test.com/res".to_string(),
                    version: "v1beta1".to_string(),
                },
                "/tmp/old.sock".into(),
            )
            .unwrap();
        registry
            .register(
                RegistrationRequest {
                    plugin_type: PluginType::DevicePlugin,
                    endpoint: "/tmp/new.sock".to_string(),
                    resource_name: "test.com/res".to_string(),
                    version: "v1beta1".to_string(),
                },
                "/tmp/new.sock".into(),
            )
            .unwrap();
        // Count should still be 1 — re-register overwrites.
        assert_eq!(registry.count(), 1);
        assert_eq!(
            registry.get("test.com/res").unwrap().socket_path,
            PathBuf::from("/tmp/new.sock")
        );
    }

    // ── PluginRegistrationServer gRPC unit tests ──────────────────────────────

    /// Build a RegistrationClient connected to a Unix socket path.
    async fn make_registration_client(sock: &Path) -> RegistrationClient<Channel> {
        let sock = sock.to_path_buf();
        let channel = Endpoint::try_from("http://[::]:1")
            .unwrap()
            .connect_with_connector(service_fn(move |_: Uri| {
                let path = sock.clone();
                async move { UnixStream::connect(&path).await.map(TokioIo::new) }
            }))
            .await
            .expect("connect to registration socket");
        RegistrationClient::new(channel)
    }

    /// Spawn the PluginRegistrationServer in a background task and wait until
    /// its socket is ready.
    async fn spawn_registration_server(
        socket_path: PathBuf,
        registry: Arc<RwLock<PluginRegistry>>,
    ) {
        let sock = socket_path.clone();
        tokio::spawn(async move {
            PluginRegistrationServer::new(sock, registry)
                .serve()
                .await
                .ok();
        });
        // Poll until the socket file appears (server is ready).
        for _ in 0..50 {
            if socket_path.exists() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("registration server socket did not appear within 1 s");
    }

    #[tokio::test]
    async fn test_grpc_register_rpc_populates_registry() {
        let dir = TempDir::new().unwrap();
        let kubelet_sock = dir.path().join("kubelet.sock");
        let registry = Arc::new(RwLock::new(PluginRegistry::new()));

        spawn_registration_server(kubelet_sock.clone(), registry.clone()).await;

        let mut client = make_registration_client(&kubelet_sock).await;
        client
            .register(RegisterRequest {
                version: "v1beta1".to_string(),
                endpoint: "/var/lib/kubelet/device-plugins/example.com-fpga.sock".to_string(),
                resource_name: "example.com/fpga".to_string(),
                options: vec![],
            })
            .await
            .expect("Register RPC should succeed");

        let guard = registry.read().await;
        let plugin = guard
            .get("example.com/fpga")
            .expect("plugin should be registered");
        assert_eq!(plugin.request.resource_name, "example.com/fpga");
        assert_eq!(plugin.request.version, "v1beta1");
    }

    #[tokio::test]
    async fn test_grpc_register_rpc_absolute_endpoint_path() {
        let dir = TempDir::new().unwrap();
        let kubelet_sock = dir.path().join("kubelet.sock");
        let registry = Arc::new(RwLock::new(PluginRegistry::new()));

        spawn_registration_server(kubelet_sock.clone(), registry.clone()).await;

        let plugin_sock = dir.path().join("example.com-sound.sock");
        // Create the plugin socket so the server can stat it.
        std::fs::write(&plugin_sock, b"").unwrap();

        let mut client = make_registration_client(&kubelet_sock).await;
        client
            .register(RegisterRequest {
                version: "v1beta1".to_string(),
                endpoint: plugin_sock.to_string_lossy().to_string(),
                resource_name: "example.com/sound".to_string(),
                options: vec![],
            })
            .await
            .expect("Register RPC should succeed");

        let guard = registry.read().await;
        let plugin = guard.get("example.com/sound").unwrap();
        assert_eq!(plugin.socket_path, plugin_sock);
    }

    #[tokio::test]
    async fn test_grpc_register_rpc_relative_endpoint_resolved_to_plugin_dir() {
        let dir = TempDir::new().unwrap();
        let kubelet_sock = dir.path().join("kubelet.sock");
        let registry = Arc::new(RwLock::new(PluginRegistry::new()));

        spawn_registration_server(kubelet_sock.clone(), registry.clone()).await;

        let mut client = make_registration_client(&kubelet_sock).await;
        // Pass only filename — server should resolve to parent dir.
        client
            .register(RegisterRequest {
                version: "v1beta1".to_string(),
                endpoint: "example.com-display.sock".to_string(),
                resource_name: "example.com/display".to_string(),
                options: vec![],
            })
            .await
            .expect("Register RPC with relative endpoint should succeed");

        let guard = registry.read().await;
        let plugin = guard.get("example.com/display").unwrap();
        // Resolved socket path must be under the same directory as kubelet.sock.
        assert_eq!(
            plugin.socket_path,
            dir.path().join("example.com-display.sock")
        );
    }

    #[tokio::test]
    async fn test_grpc_register_multiple_plugins() {
        let dir = TempDir::new().unwrap();
        let kubelet_sock = dir.path().join("kubelet.sock");
        let registry = Arc::new(RwLock::new(PluginRegistry::new()));

        spawn_registration_server(kubelet_sock.clone(), registry.clone()).await;

        let mut client = make_registration_client(&kubelet_sock).await;
        for (resource, endpoint) in [
            ("example.com/gpu", "/tmp/gpu.sock"),
            ("example.com/fpga", "/tmp/fpga.sock"),
            ("example.com/sound", "/tmp/sound.sock"),
        ] {
            client
                .register(RegisterRequest {
                    version: "v1beta1".to_string(),
                    endpoint: endpoint.to_string(),
                    resource_name: resource.to_string(),
                    options: vec![],
                })
                .await
                .unwrap_or_else(|e| panic!("Register({resource}) failed: {e}"));
        }

        let guard = registry.read().await;
        assert_eq!(guard.count(), 3);
        assert!(guard.get("example.com/gpu").is_some());
        assert!(guard.get("example.com/fpga").is_some());
        assert!(guard.get("example.com/sound").is_some());
    }

    #[tokio::test]
    async fn test_grpc_register_re_register_overwrites_previous() {
        let dir = TempDir::new().unwrap();
        let kubelet_sock = dir.path().join("kubelet.sock");
        let registry = Arc::new(RwLock::new(PluginRegistry::new()));

        spawn_registration_server(kubelet_sock.clone(), registry.clone()).await;

        let mut client = make_registration_client(&kubelet_sock).await;
        client
            .register(RegisterRequest {
                version: "v1beta1".to_string(),
                endpoint: "/tmp/v1.sock".to_string(),
                resource_name: "example.com/res".to_string(),
                options: vec![],
            })
            .await
            .unwrap();
        client
            .register(RegisterRequest {
                version: "v1beta1".to_string(),
                endpoint: "/tmp/v2.sock".to_string(),
                resource_name: "example.com/res".to_string(),
                options: vec![],
            })
            .await
            .unwrap();

        let guard = registry.read().await;
        assert_eq!(guard.count(), 1, "re-register must not create a duplicate");
        assert_eq!(
            guard.get("example.com/res").unwrap().socket_path,
            PathBuf::from("/tmp/v2.sock")
        );
    }
}
