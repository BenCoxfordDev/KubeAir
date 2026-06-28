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

//! kubelet-app: Application layer that wires domain + ports + adapters together.
//!
//! This is the composition root of the kubelet. It:
//! 1. Reads and validates configuration
//! 2. Creates all adapters
//! 3. Wires adapters to ports
//! 4. Starts the main control loops

pub mod cli;
pub mod exec_handler;
pub mod metrics;
pub mod pod_worker;
pub mod runtime_manager;
pub mod server;
pub mod streaming;
pub mod sync_loop;
pub mod tls_server;

use crate::runtime_manager::RuntimeManager;
use kubelet_adapters::cgroup::CgroupManager;
use kubelet_adapters::checkpoint::CheckpointManager;
use kubelet_adapters::cni::CniNetworkPlugin;
use kubelet_adapters::device_manager::DeviceManager;
use kubelet_adapters::file_config::FilePodSource;
use kubelet_adapters::kube_reporter::KubePodSource;
use kubelet_adapters::kube_reporter::{KubeConnectMode, KubeNodeReporter};
use kubelet_adapters::mock_runtime::MockRuntime;
use kubelet_adapters::network::NoopNetworkPlugin;
use kubelet_adapters::plugin_registration::{PluginRegistrationServer, PluginRegistry};
use kubelet_adapters::runtime_class::RuntimeClassManager;
use kubelet_adapters::sandbox_builder::NodeDnsConfig;
use kubelet_adapters::url_config::UrlPodSource;
use kubelet_adapters::volume::CompositeVolumeManager;
use kubelet_core::config::KubeletConfig;
use kubelet_core::node::NodeStatus;
use kubelet_core::pod::PodOperation;
use kubelet_core::pod::PodUpdate;
use kubelet_core::pod::manager::PodManager;
use kubelet_cri::ContainerdClient;
use kubelet_ports::driven::container_runtime::{
    ContainerRuntime, CreateSandboxConfig, ImageManager,
};
use kubelet_ports::driven::network::NetworkPlugin;
use kubelet_ports::driven::node_reporter::NodeReporter;
use kubelet_ports::driven::pod_source::{MergedPodSource, PodSource};
use kubelet_ports::driven::storage::VolumeManager;
use std::collections::HashMap;
use std::fs;
use std::net::{IpAddr, UdpSocket};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use uuid::Uuid;

/// The main kubelet runtime.
pub struct Kubelet {
    config: KubeletConfig,
    runtime: Arc<dyn ContainerRuntime>,
    image_manager: Arc<dyn ImageManager>,
    reporter: Arc<dyn NodeReporter>,
    network: Arc<dyn NetworkPlugin>,
    volume_manager: Arc<dyn VolumeManager>,
    pod_manager: Arc<PodManager>,
    update_rx: Option<mpsc::Receiver<PodUpdate>>,
    kube_client: Option<kube::Client>,
}

fn ensure_dev_tls_config(config: &KubeletConfig) -> anyhow::Result<crate::tls_server::TlsConfig> {
    let pki_dir = config.root_dir.join("pki");
    fs::create_dir_all(&pki_dir)?;

    let cert_path = pki_dir.join("kubelet-serving.crt");
    let key_path = pki_dir.join("kubelet-serving.key");

    if !(cert_path.exists() && key_path.exists()) {
        let internal_ip = detect_internal_ip();
        let mut sans = vec![
            config.node_name.clone(),
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ];
        // Include the node's InternalIP so the e2e framework can reach the
        // kubelet via its advertised address without certificate errors.
        if internal_ip != "127.0.0.1" && !sans.contains(&internal_ip) {
            sans.push(internal_ip);
        }
        let certified = rcgen::generate_simple_self_signed(sans)
            .map_err(|e| anyhow::anyhow!("generate self-signed serving cert: {}", e))?;
        fs::write(&cert_path, certified.cert.pem())?;
        fs::write(&key_path, certified.key_pair.serialize_pem())?;
    }

    Ok(crate::tls_server::TlsConfig {
        cert_pem_path: cert_path,
        key_pem_path: key_path,
        client_ca_pem_path: config.client_ca_file.clone(),
    })
}

impl Kubelet {
    /// Create a new Kubelet instance from configuration.
    pub async fn new(config: KubeletConfig) -> Self {
        let (update_tx, update_rx) = mpsc::channel(512);

        let (runtime, image_manager): (Arc<dyn ContainerRuntime>, Arc<dyn ImageManager>) = {
            let endpoint = config.container_runtime_endpoint.clone();
            let maybe_socket = endpoint
                .strip_prefix("unix://")
                .map(std::path::PathBuf::from);

            if let Some(socket_path) = maybe_socket {
                // Wait up to 15s for the CRI socket to appear (containerd may still be starting).
                let mut waited_ms = 0u64;
                while !socket_path.exists() && waited_ms < 15_000 {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    waited_ms += 500;
                }

                if !socket_path.exists() {
                    warn!(
                        endpoint = %endpoint,
                        socket = %socket_path.display(),
                        "CRI socket missing; falling back to mock runtime"
                    );
                    let rt = Arc::new(MockRuntime::new());
                    (rt.clone(), rt)
                } else {
                    // Retry connecting to containerd — it may not be ready yet on startup.
                    let mut client_result = ContainerdClient::connect(endpoint.clone()).await;
                    if client_result.is_err() {
                        for delay_ms in [500u64, 1000, 2000, 4000, 8000] {
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                            client_result = ContainerdClient::connect(endpoint.clone()).await;
                            if client_result.is_ok() {
                                break;
                            }
                        }
                    }
                    match client_result {
                        Ok(client) => {
                            info!(endpoint = %endpoint, "Using CRI runtime adapter");
                            let rt = Arc::new(client);
                            (rt.clone(), rt)
                        }
                        Err(e) => {
                            warn!(
                                endpoint = %endpoint,
                                error = %e,
                                "Falling back to mock runtime"
                            );
                            let rt = Arc::new(MockRuntime::new());
                            (rt.clone(), rt)
                        }
                    }
                }
            } else {
                warn!(
                    endpoint = %endpoint,
                    "Non-unix CRI endpoint; falling back to mock runtime"
                );
                let rt = Arc::new(MockRuntime::new());
                (rt.clone(), rt)
            }
        };

        let reporter_mode = kube_connect_mode_from_config(&config);
        let kube_client = KubeNodeReporter::try_connect(&reporter_mode).await;

        let reporter: Arc<dyn NodeReporter> =
            Arc::new(KubeNodeReporter::with_mode(config.node_name.clone(), reporter_mode).await);
        let network: Arc<dyn NetworkPlugin> = {
            let plugin_dir = std::path::PathBuf::from("/opt/cni/bin");
            let config_dir = std::path::PathBuf::from("/etc/cni/net.d");
            if plugin_dir.exists() && config_dir.exists() {
                let cni = CniNetworkPlugin::from_config_dir(plugin_dir, config_dir);
                info!(plugin = cni.name(), "Using CNI network plugin adapter");
                Arc::new(cni)
            } else {
                warn!("CNI plugin/config directories missing; falling back to noop network plugin");
                Arc::new(NoopNetworkPlugin)
            }
        };

        let volume_manager: Arc<dyn VolumeManager> = {
            let mut mgr = CompositeVolumeManager::new(
                config.root_dir.clone(),
                config.root_dir.join("plugins"),
            );
            if let Some(ref client) = kube_client {
                mgr = mgr.with_kube_client(client.clone());
            }
            Arc::new(mgr)
        };

        let pod_manager = Arc::new(PodManager::new(update_tx));

        Self {
            config,
            runtime,
            image_manager,
            reporter,
            network,
            volume_manager,
            pod_manager,
            update_rx: Some(update_rx),
            kube_client,
        }
    }

    /// Initialize node status with basic system info.
    pub fn initial_node_status(&self) -> NodeStatus {
        let mut status = NodeStatus::new(&self.config.node_name);
        use chrono::Utc;
        use kubelet_core::node::{
            NodeAddress, NodeAddressType, NodeAllocatable, NodeCapacity, NodeCondition,
            NodeConditionStatus, NodeConditionType, NodeSystemInfo,
        };

        status.capacity = NodeCapacity {
            cpu_cores: num_cpus_available(),
            memory_bytes: total_memory_bytes(),
            pods: self.config.max_pods,
            ephemeral_storage_bytes: 100 * 1024 * 1024 * 1024, // 100Gi placeholder
            hugepages: Default::default(),
            extended_resources: Default::default(),
        };
        status.allocatable = NodeAllocatable {
            cpu_millicores: (status.capacity.cpu_cores * 1000.0) as u64,
            memory_bytes: status.capacity.memory_bytes,
            pods: status.capacity.pods,
            ephemeral_storage_bytes: status.capacity.ephemeral_storage_bytes,
        };
        let internal_ip = detect_internal_ip();
        status.addresses = vec![
            NodeAddress {
                address_type: NodeAddressType::Hostname,
                address: self.config.node_name.clone(),
            },
            NodeAddress {
                address_type: NodeAddressType::InternalIP,
                address: internal_ip,
            },
        ];
        status.system_info = NodeSystemInfo {
            kubelet_version: env!("KUBERNETES_VERSION").to_string(),
            operating_system: std::env::consts::OS.to_string(),
            architecture: std::env::consts::ARCH.to_string(),
            ..Default::default()
        };
        status.set_condition(NodeCondition {
            condition_type: NodeConditionType::Ready,
            status: NodeConditionStatus::True,
            last_heartbeat_time: Utc::now(),
            last_transition_time: Utc::now(),
            reason: "KubeletReady".to_string(),
            message: "kubelet is posting ready status".to_string(),
        });
        status
    }

    /// Return a reference to the pod manager.
    pub fn pod_manager(&self) -> Arc<PodManager> {
        self.pod_manager.clone()
    }

    /// Return the node name.
    pub fn node_name(&self) -> &str {
        &self.config.node_name
    }

    /// Initialize the container network by creating and removing a dummy pod sandbox.
    /// This forces containerd to initialize CNI plugins, which is required for the
    /// kubelet to report NetworkReady=true. Without this, containerd reports:
    /// "Network plugin returns error: cni plugin not initialized"
    async fn initialize_network(&self) -> anyhow::Result<()> {
        let dummy_sandbox_id = format!("dummy-cni-init-{}", Uuid::new_v4());

        info!(sandbox_id = %dummy_sandbox_id, "Initializing container network with dummy sandbox");

        let config = CreateSandboxConfig {
            pod_uid: "dummy-uid".to_string(),
            pod_name: "dummy-cni-init".to_string(),
            pod_namespace: "default".to_string(),
            hostname: "dummy".to_string(),
            log_directory: "/tmp".to_string(),
            dns_config: None,
            port_mappings: vec![],
            labels: HashMap::new(),
            annotations: HashMap::new(),
            linux_cgroup_parent: "".to_string(),
            sysctls: HashMap::new(),
            host_network: false,
            host_pid: false,
            host_ipc: false,
            runtime_handler: "".to_string(),
            sandbox_image: self.config.pod_infra_container_image.clone(),
            supplemental_groups: vec![],
            privileged: false,
            share_process_namespace: false,
        };

        // Create dummy sandbox to initialize CNI
        match self.runtime.run_pod_sandbox(config).await {
            Ok(sandbox_id) => {
                // Try to set up network for the sandbox (this initializes CNI)
                let _ = self
                    .network
                    .setup_pod(
                        "dummy-uid",
                        "default",
                        "dummy-cni-init",
                        &sandbox_id,
                        &HashMap::new(),
                    )
                    .await;

                // Stop and remove the dummy sandbox
                if let Err(e) = self.runtime.stop_pod_sandbox(&sandbox_id).await {
                    warn!(error = %e, sandbox_id = %sandbox_id, "Failed to stop dummy sandbox (non-fatal)");
                }

                info!(sandbox_id = %sandbox_id, "Container network initialization complete");
                Ok(())
            }
            Err(e) => {
                warn!(error = %e, "Failed to create dummy sandbox for network initialization (non-fatal, continuing)");
                Ok(())
            }
        }
    }

    /// Start the kubelet runtime: API server, pod sync loop, and heartbeat loops.
    pub async fn run(mut self) -> anyhow::Result<()> {
        let addr: std::net::SocketAddr = format!("{}:{}", self.config.address, self.config.port)
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid listen address: {}", e))?;

        if self.config.sync_frequency.is_zero() {
            return Err(anyhow::anyhow!(
                "invalid configuration: sync_frequency must be non-zero"
            ));
        }
        if self.config.node_status_update_frequency.is_zero() {
            return Err(anyhow::anyhow!(
                "invalid configuration: node_status_update_frequency must be non-zero"
            ));
        }

        // Initialize container network (force CNI initialization in containerd)
        self.initialize_network().await.ok(); // Non-fatal if it fails

        let mut sources: Vec<Box<dyn PodSource>> = Vec::new();
        let pod_source_mode = kube_connect_mode_from_config(&self.config);
        sources.push(Box::new(
            KubePodSource::with_mode(self.config.node_name.clone(), pod_source_mode).await,
        ));
        if let Some(path) = self.config.static_pod_path.clone() {
            sources.push(Box::new(FilePodSource::new(
                path,
                self.config.node_name.clone(),
                self.config.file_check_frequency,
            )));
        }
        if let Some(url) = self.config.static_pod_url.clone() {
            sources.push(Box::new(
                UrlPodSource::new(
                    url,
                    self.config.node_name.clone(),
                    self.config.file_check_frequency,
                )
                .await,
            ));
        }

        let (source_tx, mut source_rx) = mpsc::channel(512);
        let merged_source = MergedPodSource::new(sources);
        let mut source_handle = tokio::spawn(async move { merged_source.run(source_tx).await });

        let source_pod_manager = self.pod_manager.clone();
        let mut source_dispatch_handle = tokio::spawn(async move {
            while let Some(update) = source_rx.recv().await {
                let result = match update.op {
                    PodOperation::Remove => {
                        let uid = update.pod.uid.clone();
                        source_pod_manager.remove(&uid, Some(update.pod)).await
                    }
                    PodOperation::Add | PodOperation::Update | PodOperation::Reconcile => {
                        source_pod_manager.upsert(update.pod).await
                    }
                };
                if let Err(e) = result {
                    warn!(error = %e, "Failed to apply pod update from source");
                }
            }
            Ok::<(), kubelet_core::error::KubeletError>(())
        });

        let router = crate::server::build_router(crate::server::ServerState {
            pod_manager: self.pod_manager.clone(),
            runtime: self.runtime.clone(),
            node_name: self.config.node_name.clone(),
            anonymous_auth: self.config.anonymous_auth_enabled,
            always_allow: self.config.always_allow,
            log_dir: self
                .config
                .root_dir
                .join("logs")
                .to_string_lossy()
                .to_string(),
            kube_client: self.kube_client.clone(),
        });
        let healthz_router = router.clone();

        let tls = match (&self.config.tls_cert_file, &self.config.tls_private_key_file) {
            (Some(cert), Some(key)) => Some(crate::tls_server::TlsConfig {
                cert_pem_path: cert.clone(),
                key_pem_path: key.clone(),
                client_ca_pem_path: self.config.client_ca_file.clone(),
            }),
            (None, None) => ensure_dev_tls_config(&self.config).map_or_else(
                |e| {
                    warn!(error = %e, "No TLS files configured and failed to generate fallback cert; serving HTTP on kubelet port");
                    None
                },
                Some,
            ),
            _ => {
                warn!("Only one TLS file configured; serving HTTP. Set both tls_cert_file and tls_private_key_file for HTTPS");
                None
            }
        };

        let mut update_rx = self
            .update_rx
            .take()
            .ok_or_else(|| anyhow::anyhow!("kubelet update receiver already consumed"))?;

        let checkpoint_mgr = Arc::new(CheckpointManager::new(
            self.config.root_dir.join("checkpoints"),
        )?);
        let cgroup_mgr = Arc::new(CgroupManager::new(
            "/sys/fs/cgroup",
            !cfg!(target_os = "linux"),
        ));
        let runtime_overheads = {
            let runtime_class_mgr = RuntimeClassManager::new();
            let mut map = std::collections::HashMap::new();
            for name in runtime_class_mgr.list_names() {
                let overhead = runtime_class_mgr.overhead_for(Some(name));
                if !overhead.is_empty() {
                    map.insert(name.to_string(), overhead);
                }
            }
            Arc::new(map)
        };

        // Create the device manager; it will be wired to the plugin registry
        // (below) so Register() calls flow through to Allocate().
        let plugin_dir = self.config.root_dir.join("device-plugins");
        let device_manager = Arc::new(DeviceManager::new(plugin_dir.clone()));

        let runtime_manager = Arc::new(RuntimeManager::new(
            self.pod_manager.clone(),
            self.runtime.clone(),
            self.image_manager.clone(),
            self.volume_manager.clone(),
            self.reporter.clone(),
            checkpoint_mgr,
            cgroup_mgr,
            runtime_overheads,
            self.config.cgroup_driver.clone(),
            self.config.root_dir.clone(),
            self.config
                .root_dir
                .join("logs")
                .to_string_lossy()
                .to_string(),
            self.config.pod_infra_container_image.clone(),
            self.kube_client.take(),
            self.config.node_name.clone(),
            NodeDnsConfig {
                cluster_dns: self.config.cluster_dns.clone(),
                cluster_domain: self.config.cluster_domain.clone(),
                resolv_conf_path: "/etc/resolv.conf".to_string(),
            },
            device_manager.clone(),
        ));

        // Spawn an independent PLEG poll loop so container exits are detected
        // within ~1 second rather than waiting for the next API-server relist.
        let pleg_manager = runtime_manager.clone();
        let pleg_handle = tokio::spawn(async move {
            pleg_manager.run_pleg_loop(Duration::from_secs(1)).await;
            Ok::<(), anyhow::Error>(())
        });

        // Spawn the image GC loop. Every 5 minutes it checks disk usage and
        // evicts LRU unused images when usage exceeds the high-water threshold.
        {
            let gc_image_manager = self.image_manager.clone();
            let gc_pod_manager = self.pod_manager.clone();
            let gc_high = self.config.image_gc_high_threshold_percent;
            let gc_low = self.config.image_gc_low_threshold_percent;
            // Prefer the image filesystem path; fall back to the kubelet root dir.
            let gc_fs_path = self.config.root_dir.to_string_lossy().to_string();
            tokio::spawn(async move {
                run_image_gc_loop(
                    gc_image_manager,
                    gc_pod_manager,
                    &gc_fs_path,
                    gc_high,
                    gc_low,
                )
                .await;
            });
        }

        // Spawn the eviction manager loop.  Every 10 seconds it samples node
        // resource pressure and evicts pods when thresholds are exceeded,
        // mirroring the Go kubelet eviction_manager goroutine.
        {
            let eviction_rm = runtime_manager.clone();
            let eviction_pod_manager = self.pod_manager.clone();
            let eviction_hard = self.config.eviction_hard.clone();
            let eviction_soft = self.config.eviction_soft.clone();
            let eviction_fs_path = self.config.root_dir.to_string_lossy().to_string();
            tokio::spawn(async move {
                run_eviction_loop(
                    eviction_rm,
                    eviction_pod_manager,
                    &eviction_hard,
                    &eviction_soft,
                    &eviction_fs_path,
                )
                .await;
            });
        }

        let reconcile_pod_manager = self.pod_manager.clone();
        let reconcile_frequency = self.config.sync_frequency;
        let mut runtime_handle = tokio::spawn(async move {
            let mut reconcile_tick = tokio::time::interval(reconcile_frequency);

            loop {
                tokio::select! {
                    maybe_update = update_rx.recv() => {
                        match maybe_update {
                            Some(update) => runtime_manager.handle_update(update).await,
                            None => {
                                warn!("Runtime loop: update channel closed");
                                break;
                            }
                        }
                    }
                    _ = reconcile_tick.tick() => {
                        // Spawn reconcile_all in a separate task so it never
                        // blocks update_rx consumption. Without this, if the
                        // number of desired pods exceeds the channel capacity
                        // the reconcile arm deadlocks (it fills update_tx but
                        // update_rx is never drained while the arm is running).
                        let rm = reconcile_pod_manager.clone();
                        tokio::spawn(async move {
                            if let Err(e) = rm.reconcile_all().await {
                                error!(error = %e, "Runtime loop reconcile_all failed");
                            }
                        });
                    }
                }
            }

            Ok::<(), anyhow::Error>(())
        });

        // Upstream node-e2e probes kubelet health on localhost:10248.
        let healthz_addr: std::net::SocketAddr = "127.0.0.1:10248"
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid healthz address: {}", e))?;
        let mut healthz_server_handle =
            tokio::spawn(async move { crate::server::serve(healthz_addr, healthz_router).await });

        let mut server_handle =
            tokio::spawn(async move { crate::tls_server::serve_tls(addr, router, tls).await });

        let reporter_status = self.reporter.clone();
        let mut node_status_tick = tokio::time::interval(self.config.node_status_update_frequency);
        let node_status_template = self.initial_node_status();
        let node_status_handle = tokio::spawn(async move {
            loop {
                node_status_tick.tick().await;
                if let Err(e) = reporter_status
                    .report_node_status(&node_status_template)
                    .await
                {
                    error!(error = %e, "Failed to report node status");
                }
            }
        });

        let reporter_lease = self.reporter.clone();
        let node_name = self.config.node_name.clone();
        let lease_duration_seconds = self.config.node_lease_duration_seconds;
        let lease_interval = Duration::from_secs((lease_duration_seconds / 2).max(1) as u64);
        let mut lease_tick = tokio::time::interval(lease_interval);
        let lease_handle = tokio::spawn(async move {
            loop {
                lease_tick.tick().await;
                if let Err(e) = reporter_lease
                    .renew_node_lease(&node_name, lease_duration_seconds)
                    .await
                {
                    error!(error = %e, "Failed to renew node lease");
                }
            }
        });

        // Spawn the device plugin registration gRPC server.
        // Device plugins connect to this socket and call Register() to announce
        // themselves.  The on_register callback immediately wires each new plugin
        // into the DeviceManager so Allocate() can reach it.
        let plugin_registry = Arc::new(tokio::sync::RwLock::new(PluginRegistry::new()));
        {
            let dm = device_manager.clone();
            plugin_registry.write().await.on_register(move |plugin| {
                let dm = dm.clone();
                let resource = plugin.request.resource_name.clone();
                let socket = plugin.socket_path.clone();
                tokio::spawn(async move {
                    dm.register_plugin(&resource, &socket).await;
                });
            });
        }
        let plugin_registry_for_server = plugin_registry.clone();
        let plugin_socket = plugin_dir.join("kubelet.sock");
        tokio::spawn(async move {
            if let Err(e) = PluginRegistrationServer::new(plugin_socket, plugin_registry_for_server)
                .serve()
                .await
            {
                error!(error = %e, "Device plugin registration server exited");
            }
        });

        // Re-discover device plugins from the last-run registry checkpoint.
        // This repopulates DeviceManager immediately so pods can be allocated
        // devices without waiting for all plugins to re-register themselves.
        {
            let dm = device_manager.clone();
            tokio::spawn(async move {
                // Brief delay so kubelet.sock is bound before we call ListAndWatch
                // on the plugin sockets (plugins may also be warming up).
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                dm.rediscover_plugins().await;
            });
        }

        info!("Kubelet runtime started");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown signal received");
            }
            res = &mut runtime_handle => {
                match res {
                    Ok(Ok(())) => warn!("Runtime manager loop exited"),
                    Ok(Err(e)) => return Err(anyhow::anyhow!("runtime loop failed: {}", e)),
                    Err(e) => return Err(anyhow::anyhow!("runtime loop task panicked: {}", e)),
                }
            }
            res = &mut server_handle => {
                match res {
                    Ok(Ok(())) => warn!("Server exited"),
                    Ok(Err(e)) => return Err(anyhow::anyhow!("server failed: {}", e)),
                    Err(e) => return Err(anyhow::anyhow!("server task panicked: {}", e)),
                }
            }
            res = &mut healthz_server_handle => {
                match res {
                    Ok(Ok(())) => warn!("Healthz server exited"),
                    Ok(Err(e)) => return Err(anyhow::anyhow!("healthz server failed: {}", e)),
                    Err(e) => return Err(anyhow::anyhow!("healthz server task panicked: {}", e)),
                }
            }
            res = &mut source_handle => {
                match res {
                    Ok(Ok(())) => warn!("Pod source exited"),
                    Ok(Err(e)) => return Err(anyhow::anyhow!("pod source failed: {}", e)),
                    Err(e) => return Err(anyhow::anyhow!("pod source task panicked: {}", e)),
                }
            }
            res = &mut source_dispatch_handle => {
                match res {
                    Ok(Ok(())) => warn!("Pod source dispatcher exited"),
                    Ok(Err(e)) => return Err(anyhow::anyhow!("pod source dispatcher failed: {}", e)),
                    Err(e) => return Err(anyhow::anyhow!("pod source dispatcher panicked: {}", e)),
                }
            }
        }

        node_status_handle.abort();
        lease_handle.abort();
        source_handle.abort();
        source_dispatch_handle.abort();
        runtime_handle.abort();
        pleg_handle.abort();
        server_handle.abort();
        healthz_server_handle.abort();

        Ok(())
    }
}

fn num_cpus_available() -> f64 {
    // In a real implementation we'd read /proc/cpuinfo or use sysinfo crate
    std::thread::available_parallelism()
        .map(|n| n.get() as f64)
        .unwrap_or(1.0)
}

fn total_memory_bytes() -> u64 {
    // Placeholder - real impl would read /proc/meminfo
    8 * 1024 * 1024 * 1024 // 8 GiB
}

fn detect_internal_ip() -> String {
    // Pick the default-route source IP so apiserver proxy does not reject
    // loopback-only node addresses.
    if let Ok(sock) = UdpSocket::bind("0.0.0.0:0")
        && sock.connect("1.1.1.1:80").is_ok()
        && let Ok(addr) = sock.local_addr()
        && let IpAddr::V4(v4) = addr.ip()
        && !v4.is_loopback()
    {
        return v4.to_string();
    }
    warn!("Falling back to loopback InternalIP; apiserver node proxy may fail");
    "127.0.0.1".to_string()
}

fn kube_connect_mode_from_config(config: &KubeletConfig) -> KubeConnectMode {
    if let Some(path) = config.kubeconfig_path.clone() {
        KubeConnectMode::Kubeconfig { path }
    } else if std::path::Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists() {
        KubeConnectMode::InCluster
    } else {
        KubeConnectMode::Standalone
    }
}

/// Periodically evaluate resource pressure and evict pods when thresholds are exceeded.
///
/// Mirrors the Go kubelet's `evictionManager.synchronize()` goroutine:
///  - Runs every 10 seconds.
///  - Collects node resource signals (memory, disk, PIDs).
///  - Calls `EvictionManager::evaluate()` to rank and select eviction candidates.
///  - For each decision, calls `RuntimeManager::evict_pod()` which marks the pod
///    `Failed/Evicted` and terminates it.
async fn run_eviction_loop(
    runtime_manager: Arc<crate::runtime_manager::RuntimeManager>,
    pod_manager: Arc<kubelet_core::pod::manager::PodManager>,
    eviction_hard: &std::collections::HashMap<String, String>,
    eviction_soft: &std::collections::HashMap<String, String>,
    fs_path: &str,
) {
    use kubelet_adapters::eviction::NodeResources;
    use kubelet_adapters::eviction::manager::EvictionManager;
    use kubelet_adapters::eviction::manager::PodResourceUsage;
    use kubelet_adapters::eviction::pressure::collect_signals;

    let mut mgr = EvictionManager::new(eviction_hard, eviction_soft, None);
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        let signals = collect_signals(fs_path, "/var/lib/containerd");
        let resources = NodeResources {
            available_memory_bytes: signals.memory_available_bytes,
            total_memory_bytes: signals.memory_available_bytes + signals.nodefs_total_bytes,
            available_disk_bytes: signals.nodefs_available_bytes,
            total_disk_bytes: signals.nodefs_total_bytes,
            available_pids: signals.pid_available,
            total_pids: signals.pid_available,
        };

        let pods = pod_manager.list();
        // Usage map is empty for now — the ranker falls back to 0 bytes per pod,
        // which still correctly orders by QoS class (BestEffort first, Guaranteed last).
        let usage = std::collections::HashMap::<_, PodResourceUsage>::new();

        let decisions = mgr.evaluate(&resources, &pods, &usage);
        for decision in decisions {
            runtime_manager.evict_pod(decision).await;
        }
    }
}

/// Periodically garbage-collect unused container images.
///
/// Mirrors the Go kubelet's `imageGCManager.GarbageCollect()` loop:
///  - Runs every 5 minutes.
///  - Skips GC if disk usage is below `high_threshold_percent`.
///  - Frees LRU unused images until usage drops below `low_threshold_percent`.
///  - Never removes images that are currently referenced by a running/desired pod.
async fn run_image_gc_loop(
    image_manager: Arc<dyn kubelet_ports::driven::container_runtime::ImageManager>,
    pod_manager: Arc<kubelet_core::pod::manager::PodManager>,
    fs_path: &str,
    high_threshold_percent: u8,
    low_threshold_percent: u8,
) {
    use chrono::Duration;
    use kubelet_adapters::eviction::pressure::collect_signals;
    use kubelet_adapters::image_gc::{ImageGcConfig, ImageGcManager};

    let config = ImageGcConfig {
        high_threshold: high_threshold_percent as f64 / 100.0,
        low_threshold: low_threshold_percent as f64 / 100.0,
        min_age: Duration::minutes(2),
    };
    let mut gc_mgr = ImageGcManager::new(config);
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        // Sync tracked images with what's actually available on disk.
        let available = match image_manager.list_images().await {
            Ok(imgs) => imgs,
            Err(e) => {
                warn!(error = %e, "Image GC: failed to list images");
                continue;
            }
        };
        gc_mgr.sync(available);

        // Pin images that are currently referenced by desired pods.
        let desired_images: std::collections::HashSet<String> = pod_manager
            .list()
            .into_iter()
            .flat_map(|p| {
                p.containers
                    .iter()
                    .chain(p.init_containers.iter())
                    .chain(p.ephemeral_containers.iter())
                    .map(|c| c.image.clone())
                    .collect::<Vec<_>>()
            })
            .collect();

        // Mark images in use by desired pods; unmark all others.
        // We use image tags/refs for matching since we track by ID but pods
        // reference by tag. Reset all pins and re-apply from desired set.
        // The GcManager protects by image ID; do a best-effort tag→ID match.
        let available_imgs = match image_manager.list_images().await {
            Ok(imgs) => imgs,
            Err(_) => continue,
        };
        for img in &available_imgs {
            let in_use = img.repo_tags.iter().any(|t| desired_images.contains(t))
                || img.repo_digests.iter().any(|d| desired_images.contains(d));
            if in_use {
                gc_mgr.mark_in_use(&img.id);
            } else {
                gc_mgr.mark_not_in_use(&img.id);
            }
        }

        // Check disk usage on the image filesystem.
        let signals = collect_signals(fs_path, "/var/lib/containerd");
        let used = signals
            .imagefs_total_bytes
            .saturating_sub(signals.imagefs_available_bytes);
        let total = signals.imagefs_total_bytes;
        if total == 0 {
            continue;
        }

        let plan = gc_mgr.gc_candidates(used, total);
        if plan.is_empty() {
            continue;
        }

        let used_pct = (used as f64 / total as f64 * 100.0) as u8;
        info!(
            used_pct,
            to_delete = plan.to_delete.len(),
            bytes_to_free = plan.bytes_to_free,
            "Image GC: starting garbage collection"
        );

        for image_id in &plan.to_delete {
            match image_manager.remove_image(image_id).await {
                Ok(()) => {
                    info!(image_id, "Image GC: removed image");
                    gc_mgr.remove_image(image_id);
                }
                Err(e) => {
                    warn!(image_id, error = %e, "Image GC: failed to remove image");
                }
            }
        }
    }
}
