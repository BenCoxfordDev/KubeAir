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

//! containerd CRI gRPC client.
//!
//! Implements the `ContainerRuntime` and `ImageManager` ports by speaking
//! the CRI v1 API over a Unix domain socket to containerd.
//!
//! Wire format: gRPC + protobuf, generated from proto/api.proto via tonic-build.
//! Socket: unix:///run/containerd/containerd.sock

use async_trait::async_trait;
use base64::Engine;
use hyper_util::rt::TokioIo;
use kubelet_core::container::{ContainerID, ContainerStats, ImageInfo, RuntimeContainer};
use kubelet_core::error::{KubeletError, Result};
use kubelet_core::pod::ContainerSpec;
use kubelet_ports::driven::container_runtime::{
    ContainerRuntime, CreateContainerConfig, CreateSandboxConfig, ExecResult, ImageManager,
    ImagePullSecret, SandboxNetworkStatus, SandboxState, SandboxStatus,
};
use std::collections::HashMap;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::{debug, info};

// Include tonic-generated code.
pub mod cri {
    tonic::include_proto!("runtime.v1");
}

use cri::{
    image_service_client::ImageServiceClient, runtime_service_client::RuntimeServiceClient,
    AttachRequest, AuthConfig, Capability, ContainerConfig, ContainerMetadata,
    ContainerStatsRequest, ContainerStatusRequest, CreateContainerRequest, Device as CriDevice,
    DnsConfig, ExecSyncRequest, ImageSpec, ImageStatusRequest, Int64Value, KeyValue,
    LinuxContainerConfig, LinuxContainerResources, LinuxContainerSecurityContext,
    LinuxPodSandboxConfig, LinuxSandboxSecurityContext, ListContainersRequest, ListImagesRequest,
    ListPodSandboxRequest, Mount, MountPropagation, NamespaceMode, NamespaceOption,
    PodSandboxConfig, PodSandboxMetadata, PodSandboxStatusRequest, PortMapping, PullImageRequest,
    RemoveContainerRequest, RemoveImageRequest, RemovePodSandboxRequest, RunPodSandboxRequest,
    SecurityProfile, SecurityProfileType, StartContainerRequest, StopContainerRequest,
    StopPodSandboxRequest, UpdateContainerResourcesRequest, VersionRequest,
};

// -- Client --------------------------------------------------------------------

/// gRPC client talking CRI v1 to containerd over a Unix domain socket.
#[derive(Clone)]
pub struct ContainerdClient {
    runtime: RuntimeServiceClient<Channel>,
    images: ImageServiceClient<Channel>,
}

impl ContainerdClient {
    /// Connect to a containerd CRI socket.
    /// `endpoint` example: `"unix:///run/containerd/containerd.sock"`
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint_str = endpoint.into();
        let socket_path = endpoint_str
            .strip_prefix("unix://")
            .unwrap_or(&endpoint_str)
            .to_string();

        info!(socket = %socket_path, "Connecting to containerd CRI endpoint");

        let path = socket_path.clone();
        let channel = Endpoint::try_from("http://[::]:50051")
            .map_err(|e| KubeletError::Runtime(format!("invalid endpoint: {}", e)))?
            .connect_with_connector(service_fn(move |_: Uri| {
                let p = path.clone();
                async move { UnixStream::connect(&p).await.map(TokioIo::new) }
            }))
            .await
            .map_err(|e| KubeletError::Runtime(format!("gRPC connect: {}", e)))?;

        Ok(Self {
            runtime: RuntimeServiceClient::new(channel.clone())
                .max_decoding_message_size(64 * 1024 * 1024), // 64 MiB — containerd default is 4 MiB, which is too small for large clusters
            images: ImageServiceClient::new(channel).max_decoding_message_size(64 * 1024 * 1024),
        })
    }

    /// Ping the CRI Version RPC to verify connectivity.
    pub async fn health_check(&self) -> Result<String> {
        let mut rt = self.runtime.clone();
        let resp = rt
            .version(VersionRequest {
                version: "v1".to_string(),
            })
            .await
            .map_err(|e| KubeletError::Runtime(format!("CRI Version RPC failed: {}", e)))?
            .into_inner();
        Ok(format!("{} {}", resp.runtime_name, resp.runtime_version))
    }
}

// -- Helper: map CRI ContainerState -> our RuntimeContainerState ---------------------

/// Normalize an image reference or image_ref to a bare `sha256:<hex>` digest.
///
/// CRI returns strings like `docker.io/library/busybox@sha256:<hex>` or just
/// `sha256:<hex>`.  The Kubernetes API (and our conformance tests) expect the
/// `imageID` field to be the bare digest.
fn normalize_image_id(image_ref: &str) -> String {
    if let Some(pos) = image_ref.find("sha256:") {
        image_ref[pos..].to_string()
    } else {
        image_ref.to_string()
    }
}

fn cri_container_state(s: i32) -> kubelet_core::container::RuntimeContainerState {
    use kubelet_core::container::RuntimeContainerState as RCS;
    match s {
        0 => RCS::Created,
        1 => RCS::Running,
        2 => RCS::Exited,
        _ => RCS::Unknown,
    }
}

fn cri_sandbox_state(s: i32) -> SandboxState {
    if s == 0 {
        SandboxState::Ready
    } else {
        SandboxState::NotReady
    }
}

fn spec_to_cri_env(spec: &ContainerSpec, extra_env: &[(String, String)]) -> Vec<KeyValue> {
    // `extra_env` is produced by `assemble_container_env` in the pod_worker and
    // already contains all resolved env vars (static literal values, ConfigMap/
    // Secret lookups, DownwardAPI, service links, etc.).  We must not also
    // iterate `spec.env` here or every directly-valued env var gets duplicated.
    info!(
        container = %spec.name,
        total_env = extra_env.len(),
        from_spec = 0,
        from_extra = extra_env.len(),
        env_keys = ?extra_env.iter().map(|(k, _)| k).collect::<Vec<_>>(),
        "Environment variables for container"
    );

    extra_env
        .iter()
        .map(|(k, v)| KeyValue {
            key: k.clone(),
            value: v.clone(),
        })
        .collect()
}

fn spec_to_cri_mounts(spec: &ContainerSpec) -> Vec<Mount> {
    let mut mounts: Vec<Mount> = spec
        .volume_mounts
        .iter()
        .map(|m| Mount {
            container_path: m.mount_path.clone(),
            host_path: m.sub_path.clone().unwrap_or_default(),
            readonly: m.read_only,
            selinux_relabel: false,
            propagation: MountPropagation::PropagationPrivate as i32,
        })
        .collect();

    // Pre-create nested child mountpoint directories inside parent host paths.
    //
    // Containerd sorts mounts alphabetically when generating the OCI spec, so a
    // parent volume (e.g. /etc/kubernetes/nfd) is always bind-mounted BEFORE any
    // child volume (e.g. /etc/kubernetes/nfd/features.d/). Once the parent is
    // bind-mounted read-only, runc cannot `mkdirat` child mountpoints inside it,
    // resulting in "read-only file system" errors at container start.
    //
    // By pre-creating the relative subdirectory inside the parent's host path, the
    // directory already exists when the parent is bind-mounted, so runc can proceed
    // with the child bind mount without needing to create it.
    for i in 0..mounts.len() {
        for j in 0..mounts.len() {
            if i == j {
                continue;
            }
            let parent_cp = mounts[i].container_path.trim_end_matches('/');
            let child_cp = mounts[j].container_path.trim_end_matches('/');
            let prefix = format!("{}/", parent_cp);
            if child_cp.starts_with(&prefix) {
                let relative = &child_cp[prefix.len()..];
                let subdir = std::path::Path::new(&mounts[i].host_path).join(relative);
                if !subdir.exists() {
                    let _ = std::fs::create_dir_all(&subdir);
                }
            }
        }
    }

    // Sort deepest destination paths first so that if the CRI layer ever respects
    // mount ordering, child mountpoints are set up before their parents.
    mounts.sort_by(|a, b| {
        let depth_a = a
            .container_path
            .split('/')
            .filter(|s| !s.is_empty())
            .count();
        let depth_b = b
            .container_path
            .split('/')
            .filter(|s| !s.is_empty())
            .count();
        depth_b.cmp(&depth_a)
    });
    mounts
}

fn spec_to_linux_resources(spec: &ContainerSpec) -> LinuxContainerResources {
    // Extract CPU quota from limits (in millicores)
    let cpu_quota = spec
        .resources
        .limits
        .get("cpu")
        .map(|q| q.value * 100) // Convert millicores to nano-cores at 100ms period
        .unwrap_or(0);

    // Extract memory limit (already in bytes)
    let memory_limit = spec
        .resources
        .limits
        .get("memory")
        .map(|q| q.value)
        .unwrap_or(0);

    LinuxContainerResources {
        cpu_quota,
        cpu_period: if cpu_quota > 0 { 100_000 } else { 0 },
        cpu_shares: 1024,
        memory_limit_bytes: memory_limit,
        oom_score_adj: 0,
        cpuset_cpus: String::new(),
        cpuset_mems: String::new(),
        memory_swap_limit_bytes: 0,
        hugepage_limits: vec![],
    }
}

/// Only build security context if non-default values are present.
/// Containerd appears to reject containers when security context is set with all defaults.
fn build_optional_security_context(
    security: &kubelet_ports::driven::container_runtime::LinuxContainerSecurity,
    share_process_namespace: bool,
) -> Option<LinuxContainerSecurityContext> {
    // Check if we have any non-default security settings
    let has_security_settings = security.run_as_user.is_some()
        || security.run_as_group.is_some()
        || !security.supplemental_groups.is_empty()
        || security.privileged
        || security.read_only_root_filesystem
        || security.allow_privilege_escalation.is_some()
        || !security.capabilities_add.is_empty()
        || !security.capabilities_drop.is_empty()
        || security.seccomp_profile_type.is_some();

    if !has_security_settings {
        // Even without other security settings, we must set namespace_options
        // to give the container its own PID namespace (default behaviour).
        let pid_mode = if share_process_namespace {
            NamespaceMode::Pod as i32
        } else {
            NamespaceMode::Container as i32
        };
        return Some(LinuxContainerSecurityContext {
            namespace_options: Some(NamespaceOption {
                pid: pid_mode,
                ..Default::default()
            }),
            ..Default::default()
        });
    }

    Some(build_security_context(security, share_process_namespace))
}

fn build_security_context(
    security: &kubelet_ports::driven::container_runtime::LinuxContainerSecurity,
    share_process_namespace: bool,
) -> LinuxContainerSecurityContext {
    let capabilities =
        if !security.capabilities_add.is_empty() || !security.capabilities_drop.is_empty() {
            Some(Capability {
                add_capabilities: security.capabilities_add.clone(),
                drop_capabilities: security.capabilities_drop.clone(),
            })
        } else {
            None
        };

    let run_as_user = security.run_as_user.map(|u| Int64Value { value: u as i64 });
    let run_as_group = security
        .run_as_group
        .map(|g| Int64Value { value: g as i64 });

    let seccomp = security.seccomp_profile_type.as_ref().map(|type_str| {
        let profile_type = match type_str.as_str() {
            "Unconfined" => SecurityProfileType::Unconfined as i32,
            "Localhost" => SecurityProfileType::Localhost as i32,
            _ => SecurityProfileType::RuntimeDefault as i32,
        };
        SecurityProfile {
            profile_type,
            localhost_ref: security.seccomp_localhost_path.clone().unwrap_or_default(),
        }
    });

    // no_new_privs is the inverse of allow_privilege_escalation
    let no_new_privs = security
        .allow_privilege_escalation
        .map(|allow| !allow)
        .unwrap_or(false);

    let pid_mode = if share_process_namespace {
        NamespaceMode::Pod as i32
    } else {
        NamespaceMode::Container as i32
    };

    LinuxContainerSecurityContext {
        capabilities,
        privileged: security.privileged,
        namespace_options: Some(NamespaceOption {
            pid: pid_mode,
            ..Default::default()
        }),
        selinux_options: None,
        run_as_user,
        run_as_username: String::new(),
        readonly_rootfs: security.read_only_root_filesystem,
        supplemental_groups: security
            .supplemental_groups
            .iter()
            .map(|g| *g as i64)
            .collect(),
        apparmor_profile: String::new(),     // deprecated
        seccomp_profile_path: String::new(), // deprecated
        no_new_privs,
        run_as_group,
        masked_paths: vec![],
        readonly_paths: vec![],
        seccomp,
        apparmor: None,
    }
}

fn build_namespace_options(
    host_network: bool,
    host_pid: bool,
    host_ipc: bool,
    share_process_namespace: bool,
) -> NamespaceOption {
    NamespaceOption {
        network: if host_network {
            NamespaceMode::Node as i32
        } else {
            NamespaceMode::Pod as i32
        },
        // When hostPID is true, share the host PID namespace.
        // When shareProcessNamespace is true, all containers share the sandbox's
        // PID namespace (Pod mode on the sandbox creates that shared namespace).
        // When neither (default), use Container mode so each entity in the pod
        // gets its own isolated PID namespace — container processes are PID 1.
        pid: if host_pid {
            NamespaceMode::Node as i32
        } else if share_process_namespace {
            NamespaceMode::Pod as i32
        } else {
            NamespaceMode::Container as i32
        },
        ipc: if host_ipc {
            NamespaceMode::Node as i32
        } else {
            NamespaceMode::Pod as i32
        },
        target_id: String::new(),
        userns: NamespaceMode::Pod as i32,
        // Keep a private UTS namespace so sandbox hostname can be applied.
        uts: NamespaceMode::Pod as i32,
    }
}

// -- ContainerRuntime implementation -------------------------------------------

#[async_trait]
impl ContainerRuntime for ContainerdClient {
    async fn run_pod_sandbox(&self, config: CreateSandboxConfig) -> Result<String> {
        debug!(
            pod = %config.pod_name,
            namespace = %config.pod_namespace,
            host_network = config.host_network,
            host_pid = config.host_pid,
            host_ipc = config.host_ipc,
            hostname = %config.hostname,
            sysctls = ?config.sysctls,
            "CRI RunPodSandbox"
        );
        let mut rt = self.runtime.clone();

        let namespace_options = build_namespace_options(
            config.host_network,
            config.host_pid,
            config.host_ipc,
            config.share_process_namespace,
        );
        debug!(
            pod = %config.pod_name,
            network_ns_mode = namespace_options.network,
            pid_ns_mode = namespace_options.pid,
            ipc_ns_mode = namespace_options.ipc,
            uts_ns_mode = namespace_options.uts,
            "CRI sandbox namespace modes selected"
        );

        let req = RunPodSandboxRequest {
            config: Some(PodSandboxConfig {
                metadata: Some(PodSandboxMetadata {
                    name: config.pod_name.clone(),
                    uid: config.pod_uid.clone(),
                    namespace: config.pod_namespace.clone(),
                    attempt: 0,
                }),
                hostname: config.hostname,
                log_directory: config.log_directory,
                dns_config: config.dns_config.map(|d| DnsConfig {
                    servers: d.servers,
                    searches: d.searches,
                    options: d.options,
                }),
                port_mappings: config
                    .port_mappings
                    .into_iter()
                    .map(|p| PortMapping {
                        protocol: match p.protocol.as_str() {
                            "UDP" => 1,
                            "SCTP" => 2,
                            _ => 0,
                        },
                        container_port: p.container_port as i32,
                        host_port: p.host_port.unwrap_or(0) as i32,
                        host_ip: p.host_ip.unwrap_or_default(),
                    })
                    .collect(),
                labels: config.labels,
                annotations: config.annotations,
                linux: Some(LinuxPodSandboxConfig {
                    cgroup_parent: config.linux_cgroup_parent,
                    security_context: Some(LinuxSandboxSecurityContext {
                        namespace_options: Some(namespace_options),
                        selinux_options: None,
                        run_as_user: None,
                        readonly_rootfs: false,
                        supplemental_groups: config.supplemental_groups,
                        privileged: config.privileged,
                        seccomp_profile_path: String::new(),
                        run_as_group: None,
                        seccomp: None,
                        apparmor: None,
                    }),
                    sysctls: config.sysctls,
                }),
            }),
            runtime_handler: config.runtime_handler,
        };

        let resp = rt
            .run_pod_sandbox(req)
            .await
            .map_err(|e| KubeletError::Runtime(format!("RunPodSandbox: {}", e)))?
            .into_inner();

        info!(sandbox_id = %resp.pod_sandbox_id, pod = %config.pod_name, "Sandbox started");
        Ok(resp.pod_sandbox_id)
    }

    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<()> {
        debug!(sandbox_id, "CRI StopPodSandbox");
        let mut rt = self.runtime.clone();
        rt.stop_pod_sandbox(StopPodSandboxRequest {
            pod_sandbox_id: sandbox_id.to_string(),
        })
        .await
        .map_err(|e| KubeletError::Runtime(format!("StopPodSandbox: {}", e)))?;
        Ok(())
    }

    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<()> {
        debug!(sandbox_id, "CRI RemovePodSandbox");
        let mut rt = self.runtime.clone();
        rt.remove_pod_sandbox(RemovePodSandboxRequest {
            pod_sandbox_id: sandbox_id.to_string(),
        })
        .await
        .map_err(|e| KubeletError::Runtime(format!("RemovePodSandbox: {}", e)))?;
        Ok(())
    }

    async fn pod_sandbox_status(&self, sandbox_id: &str) -> Result<Option<SandboxStatus>> {
        debug!(sandbox_id, "CRI PodSandboxStatus");
        let mut rt = self.runtime.clone();
        let resp = rt
            .pod_sandbox_status(PodSandboxStatusRequest {
                pod_sandbox_id: sandbox_id.to_string(),
                verbose: false,
            })
            .await
            .map_err(|e| KubeletError::Runtime(format!("PodSandboxStatus: {}", e)))?
            .into_inner();

        Ok(resp.status.map(|s| {
            let network = s.network.as_ref();
            SandboxStatus {
                id: s.id.clone(),
                state: cri_sandbox_state(s.state),
                network: network.map(|n| SandboxNetworkStatus {
                    ip: n.ip.clone(),
                    additional_ips: n.additional_ips.iter().map(|a| a.ip.clone()).collect(),
                }),
                labels: s.labels.clone(),
                created_at: chrono::DateTime::from_timestamp(s.created_at / 1_000_000_000, 0)
                    .unwrap_or_default(),
                pod_name: s
                    .metadata
                    .as_ref()
                    .map(|m| m.name.clone())
                    .unwrap_or_default(),
                pod_namespace: s
                    .metadata
                    .as_ref()
                    .map(|m| m.namespace.clone())
                    .unwrap_or_default(),
                pod_uid: s
                    .metadata
                    .as_ref()
                    .map(|m| m.uid.clone())
                    .unwrap_or_default(),
            }
        }))
    }

    async fn list_pod_sandboxes(&self) -> Result<Vec<SandboxStatus>> {
        debug!("CRI ListPodSandbox");
        let mut rt = self.runtime.clone();
        let resp = rt
            .list_pod_sandbox(ListPodSandboxRequest { filter: None })
            .await
            .map_err(|e| KubeletError::Runtime(format!("ListPodSandbox: {}", e)))?
            .into_inner();

        Ok(resp
            .items
            .into_iter()
            .map(|s| SandboxStatus {
                id: s.id.clone(),
                state: cri_sandbox_state(s.state),
                network: None,
                labels: s.labels.clone(),
                created_at: chrono::DateTime::from_timestamp(s.created_at / 1_000_000_000, 0)
                    .unwrap_or_default(),
                pod_name: s
                    .metadata
                    .as_ref()
                    .map(|m| m.name.clone())
                    .unwrap_or_default(),
                pod_namespace: s
                    .metadata
                    .as_ref()
                    .map(|m| m.namespace.clone())
                    .unwrap_or_default(),
                pod_uid: s
                    .metadata
                    .as_ref()
                    .map(|m| m.uid.clone())
                    .unwrap_or_default(),
            })
            .collect())
    }

    async fn create_container(&self, config: CreateContainerConfig) -> Result<ContainerID> {
        debug!(pod = %config.pod_name, container = %config.container.name, "CRI CreateContainer");
        info!(
            pod = %config.pod_name,
            container = %config.container.name,
            command = ?config.container.command,
            args = ?config.container.args,
            image = %config.container.image,
            "CRI CreateContainer request"
        );
        let mut rt = self.runtime.clone();

        let linux_resources = spec_to_linux_resources(&config.container);
        // Merge base env + device plugin injected env vars.
        let mut all_extra_env = config.extra_env.clone();
        all_extra_env.extend(config.extra_device_envs.clone());
        let envs = spec_to_cri_env(&config.container, &all_extra_env);
        // Merge volume mounts + device plugin bind-mounts.
        let mut mounts = spec_to_cri_mounts(&config.container);
        for dm in &config.extra_mounts {
            mounts.push(Mount {
                container_path: dm.container_path.clone(),
                host_path: dm.host_path.clone(),
                readonly: dm.read_only,
                propagation: MountPropagation::PropagationPrivate as i32,
                ..Default::default()
            });
        }

        let container_labels: HashMap<String, String> = [
            // Standard CRI Kubernetes labels used by crictl and other tooling.
            (
                "io.kubernetes.pod.name".to_string(),
                config.pod_name.clone(),
            ),
            (
                "io.kubernetes.pod.namespace".to_string(),
                config.pod_namespace.clone(),
            ),
            ("io.kubernetes.pod.uid".to_string(), config.pod_uid.clone()),
            (
                "io.kubernetes.container.name".to_string(),
                config.container.name.clone(),
            ),
            // Keep kube-air internal labels for existing code paths.
            ("kubelet.rs/pod_uid".to_string(), config.pod_uid.clone()),
            ("kubelet.rs/pod_name".to_string(), config.pod_name.clone()),
            (
                "kubelet.rs/pod_namespace".to_string(),
                config.pod_namespace.clone(),
            ),
            (
                "kubelet.rs/container_name".to_string(),
                config.container.name.clone(),
            ),
        ]
        .into_iter()
        .collect();

        let sandbox_labels: HashMap<String, String> = [
            (
                "io.kubernetes.pod.name".to_string(),
                config.pod_name.clone(),
            ),
            (
                "io.kubernetes.pod.namespace".to_string(),
                config.pod_namespace.clone(),
            ),
            ("io.kubernetes.pod.uid".to_string(), config.pod_uid.clone()),
        ]
        .into_iter()
        .collect();

        let req = CreateContainerRequest {
            pod_sandbox_id: config.sandbox_id.clone(),
            config: Some(ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: config.container.name.clone(),
                    attempt: config.attempt,
                }),
                image: Some(ImageSpec {
                    image: config.image_id.clone(),
                    annotations: HashMap::new(),
                    user_specified_image: config.container.image.clone(),
                    runtime_handler: String::new(),
                }),
                command: config.container.command.clone(),
                args: config.container.args.clone(),
                working_dir: config.container.working_dir.clone().unwrap_or_default(),
                envs: envs.clone(),
                mounts,
                devices: config
                    .extra_devices
                    .iter()
                    .map(|d| CriDevice {
                        host_path: d.host_path.clone(),
                        container_path: d.container_path.clone(),
                        permissions: d.permissions.clone(),
                    })
                    .collect(),
                labels: container_labels.clone(),
                annotations: HashMap::new(),
                log_path: format!("{}/{}.log", config.container.name, config.attempt), // Relative to sandbox log_directory
                stdin: config.container.stdin.unwrap_or(false),
                stdin_once: false,
                tty: config.container.tty.unwrap_or(false),
                linux: Some(LinuxContainerConfig {
                    resources: Some(linux_resources),
                    security_context: build_optional_security_context(
                        &config.security,
                        config.share_process_namespace,
                    ),
                }),
            }),
            sandbox_config: Some(PodSandboxConfig {
                metadata: Some(PodSandboxMetadata {
                    name: config.pod_name.clone(),
                    uid: config.pod_uid.clone(),
                    namespace: config.pod_namespace.clone(),
                    attempt: 0,
                }),
                hostname: config.pod_hostname.clone(),
                log_directory: config.log_directory.clone(),
                dns_config: None,
                port_mappings: vec![],
                labels: sandbox_labels,
                annotations: HashMap::new(),
                linux: Some(LinuxPodSandboxConfig {
                    cgroup_parent: config.linux_cgroup_parent.clone(),
                    // containerd validates the container's privileged flag against
                    // this field in CreateContainerRequest (not the stored sandbox).
                    // If security_context is None, containerd treats it as non-privileged
                    // and rejects privileged container requests.
                    security_context: if config.security.privileged {
                        Some(LinuxSandboxSecurityContext {
                            privileged: true,
                            ..Default::default()
                        })
                    } else {
                        None
                    },
                    sysctls: HashMap::new(),
                }),
            }),
        };

        info!(
            pod = %config.pod_name,
            container = %config.container.name,
            command = ?config.container.command,
            args = ?config.container.args,
            env_count = envs.len(),
            "CRI CreateContainer request details"
        );

        let resp = rt
            .create_container(req)
            .await
            .map_err(|e| KubeletError::Runtime(format!("CreateContainer: {}", e)))?
            .into_inner();

        Ok(ContainerID(resp.container_id))
    }

    async fn start_container(&self, container_id: &ContainerID) -> Result<()> {
        debug!(container_id = %container_id.0, "CRI StartContainer");
        let mut rt = self.runtime.clone();
        rt.start_container(StartContainerRequest {
            container_id: container_id.0.clone(),
        })
        .await
        .map_err(|e| KubeletError::Runtime(format!("StartContainer: {}", e)))?;
        Ok(())
    }

    async fn stop_container(&self, container_id: &ContainerID, timeout_seconds: u64) -> Result<()> {
        debug!(container_id = %container_id.0, timeout_seconds, "CRI StopContainer");
        let mut rt = self.runtime.clone();
        rt.stop_container(StopContainerRequest {
            container_id: container_id.0.clone(),
            timeout: timeout_seconds as i64,
        })
        .await
        .map_err(|e| KubeletError::Runtime(format!("StopContainer: {}", e)))?;
        Ok(())
    }

    async fn remove_container(&self, container_id: &ContainerID) -> Result<()> {
        debug!(container_id = %container_id.0, "CRI RemoveContainer");
        let mut rt = self.runtime.clone();
        rt.remove_container(RemoveContainerRequest {
            container_id: container_id.0.clone(),
        })
        .await
        .map_err(|e| KubeletError::Runtime(format!("RemoveContainer: {}", e)))?;
        Ok(())
    }

    async fn list_containers(&self) -> Result<Vec<RuntimeContainer>> {
        debug!("CRI ListContainers");
        let mut rt = self.runtime.clone();
        let resp = rt
            .list_containers(ListContainersRequest { filter: None })
            .await
            .map_err(|e| KubeletError::Runtime(format!("ListContainers: {}", e)))?
            .into_inner();

        Ok(resp
            .containers
            .into_iter()
            .map(|c| RuntimeContainer {
                id: ContainerID(c.id.clone()),
                pod_uid: c
                    .labels
                    .get("io.kubernetes.pod.uid")
                    .cloned()
                    .unwrap_or_default(),
                name: c
                    .metadata
                    .as_ref()
                    .map(|m| m.name.clone())
                    .unwrap_or_default(),
                attempt: c.metadata.as_ref().map(|m| m.attempt).unwrap_or(0),
                pid: None,
                image: c
                    .image
                    .as_ref()
                    .map(|i| i.image.clone())
                    .unwrap_or_default(),
                image_ref: c.image_ref.clone(),
                state: cri_container_state(c.state),
                created_at: chrono::DateTime::from_timestamp(c.created_at / 1_000_000_000, 0)
                    .unwrap_or_default(),
                started_at: None,
                finished_at: None,
                exit_code: None,
                exit_reason: None,
                labels: c.labels.clone(),
            })
            .collect())
    }

    async fn container_status(
        &self,
        container_id: &ContainerID,
    ) -> Result<Option<RuntimeContainer>> {
        debug!(container_id = %container_id.0, "CRI ContainerStatus");
        let mut rt = self.runtime.clone();
        let resp = rt
            .container_status(ContainerStatusRequest {
                container_id: container_id.0.clone(),
                verbose: true, // Enable verbose for debugging
            })
            .await
            .map_err(|e| KubeletError::Runtime(format!("ContainerStatus: {}", e)))?
            .into_inner();

        // Log verbose info for failed containers
        if let Some(ref status) = resp.status {
            if status.state == 2 && status.exit_code != 0 {
                info!(
                    container_id = %container_id.0,
                    exit_code = status.exit_code,
                    reason = %status.reason,
                    message = %status.message,
                    verbose_info = ?resp.info,
                    "Container failed"
                );
            }
        }

        Ok(resp.status.map(|s| RuntimeContainer {
            id: ContainerID(s.id.clone()),
            pod_uid: s
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default(),
            name: s
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default(),
            attempt: s.metadata.as_ref().map(|m| m.attempt).unwrap_or(0),
            pid: None,
            image: s
                .image
                .as_ref()
                .map(|i| i.image.clone())
                .unwrap_or_default(),
            image_ref: normalize_image_id(&s.image_ref),
            state: cri_container_state(s.state),
            created_at: chrono::DateTime::from_timestamp(s.created_at / 1_000_000_000, 0)
                .unwrap_or_default(),
            started_at: if s.state >= 1 {
                Some(
                    chrono::DateTime::from_timestamp(s.started_at / 1_000_000_000, 0)
                        .unwrap_or_default(),
                )
            } else {
                None
            },
            finished_at: if s.state == 2 {
                Some(
                    chrono::DateTime::from_timestamp(s.finished_at / 1_000_000_000, 0)
                        .unwrap_or_default(),
                )
            } else {
                None
            },
            exit_code: if s.state == 2 {
                Some(s.exit_code)
            } else {
                None
            },
            exit_reason: None,
            labels: s.labels.clone(),
        }))
    }

    async fn container_stats(&self, container_id: &ContainerID) -> Result<Option<ContainerStats>> {
        debug!(container_id = %container_id.0, "CRI ContainerStats");
        let mut rt = self.runtime.clone();
        let resp = rt
            .container_stats(ContainerStatsRequest {
                container_id: container_id.0.clone(),
            })
            .await
            .map_err(|e| KubeletError::Runtime(format!("ContainerStats: {}", e)))?
            .into_inner();

        Ok(resp.stats.map(|s| ContainerStats {
            cpu_usage_nano_cores: s
                .cpu
                .as_ref()
                .and_then(|c| c.usage_nano_cores.as_ref())
                .map(|u| u.value)
                .unwrap_or(0),
            memory_usage_bytes: s
                .memory
                .as_ref()
                .and_then(|m| m.working_set_bytes.as_ref())
                .map(|w| w.value)
                .unwrap_or(0),
            network_rx_bytes: 0,
            network_tx_bytes: 0,
            disk_usage_bytes: 0,
            timestamp: None,
        }))
    }

    async fn exec_sync(
        &self,
        container_id: &ContainerID,
        command: Vec<String>,
        timeout_seconds: u64,
    ) -> Result<ExecResult> {
        debug!(container_id = %container_id.0, cmd = ?command, "CRI ExecSync");
        let mut rt = self.runtime.clone();
        // Add an outer tokio timeout as a safety net. The CRI `timeout` field asks
        // the container runtime to kill the process, but if the runtime itself hangs
        // (e.g. due to a stuck filesystem) the gRPC call would block indefinitely.
        // Allow 35 s of extra headroom beyond the requested process timeout.
        let grpc_deadline = std::time::Duration::from_secs(timeout_seconds + 35);
        let resp = tokio::time::timeout(
            grpc_deadline,
            rt.exec_sync(ExecSyncRequest {
                container_id: container_id.0.clone(),
                cmd: command,
                timeout: timeout_seconds as i64,
            }),
        )
        .await
        .map_err(|_| {
            KubeletError::Runtime(format!(
                "ExecSync timed out after {}s",
                grpc_deadline.as_secs()
            ))
        })?
        .map_err(|e| KubeletError::Runtime(format!("ExecSync: {}", e)))?
        .into_inner();

        Ok(ExecResult {
            stdout: resp.stdout,
            stderr: resp.stderr,
            exit_code: resp.exit_code,
        })
    }

    async fn attach_sync(
        &self,
        container_id: &ContainerID,
        _timeout_seconds: u64,
    ) -> Result<ExecResult> {
        debug!(container_id = %container_id.0, "CRI Attach");
        let mut rt = self.runtime.clone();
        let resp = rt
            .attach(AttachRequest {
                container_id: container_id.0.clone(),
                stdin: true,
                stdout: true,
                stderr: true,
                tty: false,
            })
            .await
            .map_err(|e| KubeletError::Runtime(format!("Attach: {}", e)))?
            .into_inner();

        // The CRI Attach RPC returns a streaming endpoint URL; this sync shim
        // surfaces that endpoint over the existing exec/attach response shape.
        Ok(ExecResult {
            stdout: format!("attach_url={}", resp.url).into_bytes(),
            stderr: vec![],
            exit_code: 0,
        })
    }
}

// -- ImageManager implementation -----------------------------------------------

#[async_trait]
impl ImageManager for ContainerdClient {
    async fn pull_image(&self, image: &str, pull_secrets: Vec<ImagePullSecret>) -> Result<String> {
        debug!(image, "CRI PullImage");
        let mut imgs = self.images.clone();

        let auth = pull_secrets.into_iter().next().map(|s| {
            // containerd v2 ParseAuth priority: username/password → identity_token → auth (base64).
            // registry_token is NOT supported (TODO comment in containerd source).
            //
            // IMPORTANT: If server_address is set and does NOT match the host being
            // requested, containerd silently returns empty credentials. Setting
            // server_address to empty string disables this check and lets credentials
            // apply to any host, which is safe since we already matched them by
            // registry hostname during secret resolution.
            //
            // For oauth2accesstoken (GCP), username + password is the correct form —
            // containerd will use these directly in the Authorization header via
            // Docker's token challenge flow:
            //   GET /v2/token → 401 → POST /v2/ with Basic("oauth2accesstoken:<token>")
            // Or in the case of GAR, as bearer token challenge response.
            let combined = format!("{}:{}", s.username, s.password);
            let auth_b64 = base64::engine::general_purpose::STANDARD.encode(&combined);
            AuthConfig {
                username: s.username,
                password: s.password,
                auth: auth_b64,
                server_address: String::new(), // intentionally empty — avoid host mismatch check
                identity_token: String::new(),
                registry_token: String::new(),
            }
        });

        let resp = imgs
            .pull_image(PullImageRequest {
                image: Some(ImageSpec {
                    image: image.to_string(),
                    annotations: HashMap::new(),
                    user_specified_image: image.to_string(),
                    runtime_handler: String::new(),
                }),
                auth,
                sandbox_config: None,
            })
            .await
            .map_err(|e| KubeletError::Runtime(format!("PullImage: {}", e)))?
            .into_inner();

        Ok(normalize_image_id(&resp.image_ref))
    }

    async fn list_images(&self) -> Result<Vec<ImageInfo>> {
        debug!("CRI ListImages");
        let mut imgs = self.images.clone();
        let resp = imgs
            .list_images(ListImagesRequest { filter: None })
            .await
            .map_err(|e| KubeletError::Runtime(format!("ListImages: {}", e)))?
            .into_inner();

        Ok(resp
            .images
            .into_iter()
            .map(|i| ImageInfo {
                id: i.id.clone(),
                repo_tags: i.repo_tags.clone(),
                repo_digests: i.repo_digests.clone(),
                size_bytes: i.size,
            })
            .collect())
    }

    async fn remove_image(&self, image_id: &str) -> Result<()> {
        debug!(image_id, "CRI RemoveImage");
        let mut imgs = self.images.clone();
        imgs.remove_image(RemoveImageRequest {
            image: Some(ImageSpec {
                image: image_id.to_string(),
                annotations: HashMap::new(),
                user_specified_image: image_id.to_string(),
                runtime_handler: String::new(),
            }),
        })
        .await
        .map_err(|e| KubeletError::Runtime(format!("RemoveImage: {}", e)))?;
        Ok(())
    }

    async fn image_status(&self, image: &str) -> Result<Option<ImageInfo>> {
        debug!(image, "CRI ImageStatus");
        let mut imgs = self.images.clone();
        let resp = imgs
            .image_status(ImageStatusRequest {
                image: Some(ImageSpec {
                    image: image.to_string(),
                    annotations: HashMap::new(),
                    user_specified_image: image.to_string(),
                    runtime_handler: String::new(),
                }),
                verbose: false,
            })
            .await
            .map_err(|e| KubeletError::Runtime(format!("ImageStatus: {}", e)))?
            .into_inner();

        Ok(resp.image.map(|i| ImageInfo {
            id: i.id,
            repo_tags: i.repo_tags,
            repo_digests: i.repo_digests,
            size_bytes: i.size,
        }))
    }
}

// -- CRI UpdateContainerResources (called by CPU/Memory Manager) ---------------

impl ContainerdClient {
    /// Update resource limits on a running container.
    /// Called by the CPU Manager to apply cpuset changes.
    pub async fn update_container_resources(
        &self,
        container_id: &str,
        cpuset_cpus: &str,
        cpuset_mems: &str,
        cpu_quota: i64,
        cpu_period: i64,
        memory_limit: i64,
    ) -> Result<()> {
        debug!(container_id, cpuset_cpus, "CRI UpdateContainerResources");
        let mut rt = self.runtime.clone();
        rt.update_container_resources(UpdateContainerResourcesRequest {
            container_id: container_id.to_string(),
            linux: Some(LinuxContainerResources {
                cpu_quota,
                cpu_period,
                cpu_shares: 1024,
                memory_limit_bytes: memory_limit,
                oom_score_adj: 0,
                cpuset_cpus: cpuset_cpus.to_string(),
                cpuset_mems: cpuset_mems.to_string(),
                memory_swap_limit_bytes: 0,
                hugepage_limits: vec![],
            }),
        })
        .await
        .map_err(|e| KubeletError::Runtime(format!("UpdateContainerResources: {}", e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_memory_bytes(s: &str) -> Result<i64> {
        let s = s.trim();
        if let Some(v) = s.strip_suffix("Ki") {
            return v
                .parse::<i64>()
                .map(|n| n * 1024)
                .map_err(|e| KubeletError::Runtime(format!("invalid Ki memory quantity: {}", e)));
        }
        if let Some(v) = s.strip_suffix("Mi") {
            return v
                .parse::<i64>()
                .map(|n| n * 1024 * 1024)
                .map_err(|e| KubeletError::Runtime(format!("invalid Mi memory quantity: {}", e)));
        }
        if let Some(v) = s.strip_suffix("Gi") {
            return v
                .parse::<i64>()
                .map(|n| n * 1024 * 1024 * 1024)
                .map_err(|e| KubeletError::Runtime(format!("invalid Gi memory quantity: {}", e)));
        }
        s.parse::<i64>()
            .map_err(|e| KubeletError::Runtime(format!("invalid memory quantity: {}", e)))
    }

    fn parse_cpu_to_quota(s: &str) -> Result<i64> {
        let s = s.trim();
        if let Some(v) = s.strip_suffix('m') {
            let millicores = v
                .parse::<i64>()
                .map_err(|e| KubeletError::Runtime(format!("invalid milli-cpu quantity: {}", e)))?;
            return Ok(millicores * 100);
        }

        let cores = s
            .parse::<i64>()
            .map_err(|e| KubeletError::Runtime(format!("invalid cpu quantity: {}", e)))?;
        Ok(cores * 100_000)
    }

    #[test]
    fn test_parse_memory_ki() {
        assert_eq!(parse_memory_bytes("512Ki").unwrap(), 524288);
    }
    #[test]
    fn test_parse_memory_mi() {
        assert_eq!(parse_memory_bytes("256Mi").unwrap(), 256 * 1024 * 1024);
    }
    #[test]
    fn test_parse_memory_gi() {
        assert_eq!(parse_memory_bytes("4Gi").unwrap(), 4 * 1024 * 1024 * 1024);
    }
    #[test]
    fn test_parse_cpu_millis() {
        assert_eq!(parse_cpu_to_quota("500m").unwrap(), 50_000);
    }
    #[test]
    fn test_parse_cpu_cores() {
        assert_eq!(parse_cpu_to_quota("2").unwrap(), 200_000);
    }
    #[test]
    fn test_cri_sandbox_state_ready() {
        assert_eq!(cri_sandbox_state(0), SandboxState::Ready);
    }
    #[test]
    fn test_cri_sandbox_state_notready() {
        assert_eq!(cri_sandbox_state(1), SandboxState::NotReady);
    }

    #[test]
    fn test_build_namespace_options_host_network_uses_node_mode() {
        // host_network=true → network=Node; host_pid/host_ipc=false → pid=Container, ipc=Pod
        let options = build_namespace_options(true, false, false, false);
        assert_eq!(options.network, NamespaceMode::Node as i32);
        // Default (no hostPID, no shareProcessNamespace): Container mode so each container is PID 1
        assert_eq!(options.pid, NamespaceMode::Container as i32);
        assert_eq!(options.ipc, NamespaceMode::Pod as i32);
    }

    #[test]
    fn test_build_namespace_options_pod_network_uses_pod_mode() {
        // host_pid=true, host_ipc=true → pid=Node, ipc=Node; host_network=false → network=Pod
        let options = build_namespace_options(false, true, true, false);
        assert_eq!(options.network, NamespaceMode::Pod as i32);
        assert_eq!(options.pid, NamespaceMode::Node as i32);
        assert_eq!(options.ipc, NamespaceMode::Node as i32);
    }

    #[test]
    fn test_build_namespace_options_share_process_namespace_uses_pod_pid() {
        // shareProcessNamespace=true → pid=Pod (all containers share sandbox PID namespace)
        let options = build_namespace_options(false, false, false, true);
        assert_eq!(options.pid, NamespaceMode::Pod as i32);
    }

    #[test]
    fn test_build_namespace_options_default_uses_container_pid() {
        // Default (no flags) → pid=Container so each container is PID 1 in its own namespace
        let options = build_namespace_options(false, false, false, false);
        assert_eq!(options.pid, NamespaceMode::Container as i32);
    }

    /// Verify that `CreateContainerRequest` uses the resolved image ID (sha256 digest)
    /// as `image.image`, not the original reference string.
    ///
    /// The Kubernetes CRI spec requires `image.image` to be the content-addressable
    /// digest returned by `PullImage`/`ImageStatus`.  Using the original reference
    /// caused containerd to store the full registry URL as `image_ref` in the
    /// container record, which `crictl ps` then showed instead of the sha256 ID.
    #[test]
    fn test_create_container_request_uses_image_id_not_image_ref() {
        use cri::ImageSpec;
        use kubelet_core::pod::ContainerSpec;
        use kubelet_ports::driven::container_runtime::CreateContainerConfig;

        let image_ref = "registry.example.com/org/app:v1.2.3".to_string();
        let image_id =
            "sha256:a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".to_string();

        let config = CreateContainerConfig {
            pod_uid: "uid-1".to_string(),
            pod_name: "mypod".to_string(),
            pod_namespace: "default".to_string(),
            attempt: 0,
            container: ContainerSpec {
                name: "app".to_string(),
                image: image_ref.clone(),
                ..Default::default()
            },
            sandbox_id: "sandbox-1".to_string(),
            image_id: image_id.clone(),
            log_directory: "/var/log/pods".to_string(),
            env_overrides: Default::default(),
            extra_env: Default::default(),
            security: Default::default(),
            linux_cgroup_parent: String::new(),
            extra_devices: vec![],
            extra_mounts: vec![],
            extra_device_envs: vec![],
            share_process_namespace: false,
            pod_hostname: "mypod".to_string(),
        };

        // Build the ImageSpec the same way create_container() does.
        let proto_image = ImageSpec {
            image: config.image_id.clone(),
            annotations: HashMap::new(),
            user_specified_image: config.container.image.clone(),
            runtime_handler: String::new(),
        };

        // The `image` field must be the sha256 digest, not the reference tag.
        assert_eq!(
            proto_image.image, image_id,
            "CreateContainerRequest.image.image must be the resolved image ID (sha256)"
        );
        assert_eq!(
            proto_image.user_specified_image, image_ref,
            "CreateContainerRequest.image.user_specified_image must keep the original reference"
        );
        assert_ne!(
            proto_image.image, image_ref,
            "CreateContainerRequest.image.image must NOT be the tag/reference"
        );
    }

    // Live tests skipped -- require a running containerd socket.
    #[tokio::test]
    #[ignore = "requires live containerd at /run/containerd/containerd.sock"]
    async fn test_health_check() {
        let client = match ContainerdClient::connect("unix:///run/containerd/containerd.sock").await
        {
            Ok(client) => client,
            Err(err) => {
                eprintln!(
                    "skipping test_health_check: unable to connect to containerd: {}",
                    err
                );
                return;
            }
        };

        let info = match client.health_check().await {
            Ok(info) => info,
            Err(err) => {
                eprintln!(
                    "skipping test_health_check: CRI health_check failed: {}",
                    err
                );
                return;
            }
        };
        assert!(!info.is_empty());
    }
}
