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

//! Container Runtime Interface (CRI) port.
//!
//! Mirrors the kubelet's CRI client interface for container lifecycle management.

use async_trait::async_trait;
use kubelet_core::container::{ContainerID, ContainerStats, ImageInfo, RuntimeContainer};
use kubelet_core::error::Result;
use kubelet_core::pod::ContainerSpec;
use std::collections::HashMap;

/// Configuration for creating a new container.
#[derive(Debug, Clone)]
pub struct CreateContainerConfig {
    pub pod_uid: String,
    pub pod_name: String,
    pub pod_namespace: String,
    pub attempt: u32,
    pub container: ContainerSpec,
    pub sandbox_id: String,
    pub image_id: String,
    pub log_directory: String,
    pub env_overrides: HashMap<String, String>,
    pub extra_env: Vec<(String, String)>,
    pub security: LinuxContainerSecurity,
    pub linux_cgroup_parent: String,
    /// Device nodes injected by device plugins (from Allocate responses).
    pub extra_devices: Vec<DeviceMount>,
    /// Extra mounts injected by device plugins (from Allocate responses).
    pub extra_mounts: Vec<DevicePluginMount>,
    /// Extra env vars injected by device plugins (from Allocate responses).
    pub extra_device_envs: Vec<(String, String)>,
    /// Whether all containers in the pod share a single PID namespace
    /// (pod.spec.shareProcessNamespace). When true, containers join the
    /// sandbox's PID namespace and are NOT PID 1. When false (default), each
    /// container runs in its own PID namespace and IS PID 1.
    pub share_process_namespace: bool,
    /// The pod's effective hostname (spec.hostname or metadata.name).
    /// Used to set `HOSTNAME` env var in the container and to identify the
    /// pod in the sandbox config of CreateContainerRequest.
    pub pod_hostname: String,
}

/// A host device node to expose inside a container.
#[derive(Debug, Clone)]
pub struct DeviceMount {
    pub host_path: String,
    pub container_path: String,
    /// Linux cgroup permissions string, e.g. "rw", "r", "rwm".
    pub permissions: String,
}

/// A bind-mount injected by a device plugin.
#[derive(Debug, Clone)]
pub struct DevicePluginMount {
    pub host_path: String,
    pub container_path: String,
    pub read_only: bool,
}

/// Linux security context for a container.
#[derive(Debug, Clone, Default)]
pub struct LinuxContainerSecurity {
    pub run_as_user: Option<u32>,
    pub run_as_group: Option<u32>,
    pub supplemental_groups: Vec<u32>,
    pub privileged: bool,
    pub read_only_root_filesystem: bool,
    pub allow_privilege_escalation: Option<bool>,
    pub capabilities_add: Vec<String>,
    pub capabilities_drop: Vec<String>,
    pub seccomp_profile_type: Option<String>,
    pub seccomp_localhost_path: Option<String>,
    pub apparmor_profile: Option<String>,
}

/// Configuration for creating a pod sandbox.
#[derive(Debug, Clone)]
pub struct CreateSandboxConfig {
    pub pod_uid: String,
    pub pod_name: String,
    pub pod_namespace: String,
    pub hostname: String,
    pub log_directory: String,
    pub dns_config: Option<DnsConfigSpec>,
    pub port_mappings: Vec<PortMappingSpec>,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub linux_cgroup_parent: String,
    pub sysctls: HashMap<String, String>,
    pub host_network: bool,
    pub host_pid: bool,
    pub host_ipc: bool,
    pub runtime_handler: String,
    pub sandbox_image: String,
    /// fsGroup + supplementalGroups to set on the pod sandbox.
    pub supplemental_groups: Vec<i64>,
    /// Whether the sandbox should allow privileged containers. Must be set to
    /// true when any container in the pod has `securityContext.privileged: true`;
    /// containerd rejects privileged container creation if the sandbox itself
    /// was not created with this flag.
    pub privileged: bool,
    /// Whether all containers in the pod share a single PID namespace
    /// (pod.spec.shareProcessNamespace). When true, containers join the
    /// sandbox's PID namespace. When false (default), each container gets its
    /// own isolated PID namespace and is PID 1.
    pub share_process_namespace: bool,
}

#[derive(Debug, Clone)]
pub struct DnsConfigSpec {
    pub servers: Vec<String>,
    pub searches: Vec<String>,
    pub options: Vec<String>,
}

/// Backward compat alias.
pub type SandboxDnsConfig = DnsConfigSpec;

#[derive(Debug, Clone)]
pub struct PortMappingSpec {
    pub container_port: u16,
    pub host_port: Option<u16>,
    pub protocol: String,
    pub host_ip: Option<String>,
}

/// Backward compat alias.
pub type PortMapping = PortMappingSpec;

/// Sandbox (pod infra container) status.
#[derive(Debug, Clone)]
pub struct SandboxStatus {
    pub id: String,
    pub pod_uid: String,
    pub pod_name: String,
    pub pod_namespace: String,
    pub state: SandboxState,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub network: Option<SandboxNetworkStatus>,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SandboxState {
    Ready,
    NotReady,
}

#[derive(Debug, Clone)]
pub struct SandboxNetworkStatus {
    pub ip: String,
    pub additional_ips: Vec<String>,
}

/// Container Runtime Interface port.
#[async_trait]
pub trait ContainerRuntime: Send + Sync {
    // Sandbox (pause container / pod infra) operations
    async fn run_pod_sandbox(&self, config: CreateSandboxConfig) -> Result<String>;
    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<()>;
    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<()>;
    async fn pod_sandbox_status(&self, sandbox_id: &str) -> Result<Option<SandboxStatus>>;
    async fn list_pod_sandboxes(&self) -> Result<Vec<SandboxStatus>>;

    // Container operations
    async fn create_container(&self, config: CreateContainerConfig) -> Result<ContainerID>;
    async fn start_container(&self, container_id: &ContainerID) -> Result<()>;
    async fn stop_container(&self, container_id: &ContainerID, timeout_seconds: u64) -> Result<()>;
    async fn remove_container(&self, container_id: &ContainerID) -> Result<()>;
    async fn list_containers(&self) -> Result<Vec<RuntimeContainer>>;
    async fn container_status(
        &self,
        container_id: &ContainerID,
    ) -> Result<Option<RuntimeContainer>>;
    async fn container_stats(&self, container_id: &ContainerID) -> Result<Option<ContainerStats>>;

    // Exec operations
    async fn exec_sync(
        &self,
        container_id: &ContainerID,
        command: Vec<String>,
        timeout_seconds: u64,
    ) -> Result<ExecResult>;

    // Attach operations
    async fn attach_sync(
        &self,
        container_id: &ContainerID,
        timeout_seconds: u64,
    ) -> Result<ExecResult>;

    /// Update container TTY size for interactive attach/exec sessions.
    ///
    /// Runtimes that do not support resize can keep the default no-op.
    async fn update_container_tty_size(
        &self,
        _container_id: &ContainerID,
        _width: u32,
        _height: u32,
    ) -> Result<()> {
        Ok(())
    }
}

/// Result of a synchronous exec command.
#[derive(Debug, Clone)]
pub struct ExecResult {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
}

/// Image manager port for pulling and managing container images.
#[async_trait]
pub trait ImageManager: Send + Sync {
    async fn pull_image(&self, image: &str, pull_secrets: Vec<ImagePullSecret>) -> Result<String>; // returns image ID

    async fn list_images(&self) -> Result<Vec<ImageInfo>>;
    async fn remove_image(&self, image_id: &str) -> Result<()>;
    async fn image_status(&self, image: &str) -> Result<Option<ImageInfo>>;
}

#[derive(Debug, Clone)]
pub struct ImagePullSecret {
    pub server: String,
    pub username: String,
    pub password: String,
}
