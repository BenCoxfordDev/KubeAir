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

//! CRI v1 type definitions.
//!
//! These mirror the Kubernetes CRI API proto types:
//!   k8s.io/cri-api/pkg/apis/runtime/v1/api.proto
//!
//! Rather than depending on protoc code generation, we define equivalent
//! Rust structs that are serialized to/from the gRPC wire format via prost.
//! Each struct includes From impls to convert to/from our domain types.

use chrono::DateTime;
use kubelet_core::container::{ContainerID, ImageInfo, RuntimeContainer, RuntimeContainerState};
use kubelet_core::pod::ContainerSpec;
use std::collections::HashMap;

// -- Sandbox (pause container) -------------------------------------------------

/// CRI RunPodSandboxRequest
#[derive(Debug, Clone)]
pub struct CriSandboxConfig {
    pub metadata: CriPodSandboxMetadata,
    pub hostname: String,
    pub log_directory: String,
    pub dns_config: Option<CriDnsConfig>,
    pub port_mappings: Vec<CriPortMapping>,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub linux: Option<CriLinuxPodSandboxConfig>,
}

#[derive(Debug, Clone)]
pub struct CriPodSandboxMetadata {
    pub name: String,
    pub uid: String,
    pub namespace: String,
    pub attempt: u32,
}

#[derive(Debug, Clone)]
pub struct CriDnsConfig {
    pub servers: Vec<String>,
    pub searches: Vec<String>,
    pub options: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CriPortMapping {
    pub protocol: CriProtocol,
    pub container_port: i32,
    pub host_port: i32,
    pub host_ip: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CriProtocol {
    Tcp = 0,
    Udp = 1,
    Sctp = 2,
}

#[derive(Debug, Clone)]
pub struct CriLinuxPodSandboxConfig {
    pub cgroup_parent: String,
    pub security_context: Option<CriLinuxSandboxSecurityContext>,
    pub sysctls: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct CriLinuxSandboxSecurityContext {
    pub privileged: bool,
    pub run_as_user: Option<i64>,
    pub run_as_group: Option<i64>,
    pub supplemental_groups: Vec<i64>,
}

/// CRI PodSandboxStatus response
#[derive(Debug, Clone)]
pub struct CriPodSandboxStatus {
    pub id: String,
    pub metadata: CriPodSandboxMetadata,
    pub state: CriSandboxState,
    pub created_at: i64, // Unix nanoseconds
    pub network: Option<CriPodSandboxNetworkStatus>,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CriSandboxState {
    SandboxReady = 0,
    SandboxNotReady = 1,
}

#[derive(Debug, Clone)]
pub struct CriPodSandboxNetworkStatus {
    pub ip: String,
    pub additional_ips: Vec<CriPodIp>,
}

#[derive(Debug, Clone)]
pub struct CriPodIp {
    pub ip: String,
}

// -- Container -----------------------------------------------------------------

/// CRI CreateContainerRequest config
#[derive(Debug, Clone)]
pub struct CriContainerConfig {
    pub metadata: CriContainerMetadata,
    pub image: CriImageSpec,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub working_dir: String,
    pub envs: Vec<CriKeyValue>,
    pub mounts: Vec<CriMount>,
    pub log_path: String,
    pub stdin: bool,
    pub stdin_once: bool,
    pub tty: bool,
    pub linux: Option<CriLinuxContainerConfig>,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct CriContainerMetadata {
    pub name: String,
    pub attempt: u32,
}

#[derive(Debug, Clone)]
pub struct CriImageSpec {
    pub image: String,
    pub annotations: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct CriKeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct CriMount {
    pub host_path: String,
    pub container_path: String,
    pub readonly: bool,
    pub propagation: i32,
}

#[derive(Debug, Clone)]
pub struct CriLinuxContainerConfig {
    pub resources: Option<CriLinuxContainerResources>,
    pub security_context: Option<CriLinuxContainerSecurityContext>,
}

#[derive(Debug, Clone)]
pub struct CriLinuxContainerResources {
    pub cpu_period: i64,
    pub cpu_quota: i64,
    pub cpu_shares: i64,
    pub memory_limit_in_bytes: i64,
    pub oom_score_adj: i64,
    pub cpuset_cpus: String,
    pub cpuset_mems: String,
}

#[derive(Debug, Clone)]
pub struct CriLinuxContainerSecurityContext {
    pub privileged: bool,
    pub run_as_user: Option<i64>,
    pub run_as_group: Option<i64>,
    pub readonly_rootfs: bool,
    pub capabilities: Option<CriCapability>,
}

#[derive(Debug, Clone)]
pub struct CriCapability {
    pub add_capabilities: Vec<String>,
    pub drop_capabilities: Vec<String>,
}

/// CRI ContainerStatus response
#[derive(Debug, Clone)]
pub struct CriContainerStatus {
    pub id: String,
    pub metadata: CriContainerMetadata,
    pub state: CriContainerState,
    pub created_at: i64,
    pub started_at: i64,
    pub finished_at: i64,
    pub exit_code: i32,
    pub image: CriImageSpec,
    pub image_ref: String,
    pub reason: String,
    pub message: String,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub mounts: Vec<CriMount>,
    pub log_path: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CriContainerState {
    ContainerCreated = 0,
    ContainerRunning = 1,
    ContainerExited = 2,
    ContainerUnknown = 3,
}

// -- Image ---------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CriImage {
    pub id: String,
    pub repo_tags: Vec<String>,
    pub repo_digests: Vec<String>,
    pub size: u64,
    pub uid: Option<CriInt64Value>,
    pub username: String,
    pub spec: Option<CriImageSpec>,
    pub pinned: bool,
}

#[derive(Debug, Clone)]
pub struct CriInt64Value {
    pub value: i64,
}

// -- Exec ----------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CriExecSyncRequest {
    pub container_id: String,
    pub cmd: Vec<String>,
    pub timeout: i64,
}

#[derive(Debug, Clone)]
pub struct CriExecSyncResponse {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
}

// -- Conversions ---------------------------------------------------------------

impl From<CriContainerStatus> for RuntimeContainer {
    fn from(s: CriContainerStatus) -> Self {
        let state = match s.state {
            CriContainerState::ContainerRunning => RuntimeContainerState::Running,
            CriContainerState::ContainerExited => RuntimeContainerState::Exited,
            CriContainerState::ContainerCreated => RuntimeContainerState::Created,
            CriContainerState::ContainerUnknown => RuntimeContainerState::Unknown,
        };
        let started_at = if s.started_at > 0 {
            Some(DateTime::from_timestamp_nanos(s.started_at))
        } else {
            None
        };
        let finished_at = if s.finished_at > 0 {
            Some(DateTime::from_timestamp_nanos(s.finished_at))
        } else {
            None
        };
        RuntimeContainer {
            id: ContainerID::new(s.id),
            pod_uid: String::new(),
            name: s.metadata.name,
            attempt: s.metadata.attempt,
            pid: None,
            image: s.image.image,
            image_ref: s.image_ref,
            state,
            created_at: DateTime::from_timestamp_nanos(s.created_at),
            started_at,
            finished_at,
            exit_code: if s.exit_code != 0 {
                Some(s.exit_code)
            } else {
                None
            },
            exit_reason: if s.reason.is_empty() {
                None
            } else {
                Some(s.reason)
            },
            labels: s.labels,
        }
    }
}

impl From<CriImage> for ImageInfo {
    fn from(img: CriImage) -> Self {
        Self {
            id: img.id,
            repo_tags: img.repo_tags,
            repo_digests: img.repo_digests,
            size_bytes: img.size,
        }
    }
}

// -- Helpers -------------------------------------------------------------------

pub fn container_spec_to_cri_config(
    spec: &ContainerSpec,
    pod_name: &str,
    pod_uid: &str,
    attempt: u32,
    log_path: &str,
) -> CriContainerConfig {
    CriContainerConfig {
        metadata: CriContainerMetadata {
            name: spec.name.clone(),
            attempt,
        },
        image: CriImageSpec {
            image: spec.image.clone(),
            annotations: HashMap::new(),
        },
        command: spec.command.clone(),
        args: spec.args.clone(),
        working_dir: spec.working_dir.clone().unwrap_or_default(),
        envs: spec
            .env
            .iter()
            .filter_map(|e| {
                e.value.as_ref().map(|v| CriKeyValue {
                    key: e.name.clone(),
                    value: v.clone(),
                })
            })
            .collect(),
        mounts: spec
            .volume_mounts
            .iter()
            .map(|m| CriMount {
                host_path: String::new(), // resolved by volume manager
                container_path: m.mount_path.clone(),
                readonly: m.read_only,
                propagation: 0,
            })
            .collect(),
        log_path: log_path.to_string(),
        stdin: false,
        stdin_once: false,
        tty: false,
        linux: Some(CriLinuxContainerConfig {
            resources: None,
            security_context: spec.security_context.as_ref().map(|sc| {
                CriLinuxContainerSecurityContext {
                    privileged: sc.privileged.unwrap_or(false),
                    run_as_user: sc.run_as_user.map(|u| u as i64),
                    run_as_group: sc.run_as_group.map(|g| g as i64),
                    readonly_rootfs: sc.read_only_root_filesystem.unwrap_or(false),
                    capabilities: sc.capabilities.as_ref().map(|c| CriCapability {
                        add_capabilities: c.add.clone(),
                        drop_capabilities: c.drop.clone(),
                    }),
                }
            }),
        }),
        labels: HashMap::from([
            ("io.kubernetes.pod.name".to_string(), pod_name.to_string()),
            ("io.kubernetes.pod.uid".to_string(), pod_uid.to_string()),
            (
                "io.kubernetes.container.name".to_string(),
                spec.name.clone(),
            ),
        ]),
        annotations: HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::pod::{
        ContainerSpec, EnvVar, ImagePullPolicy, ResourceRequirements, VolumeMount,
    };

    fn sample_spec() -> ContainerSpec {
        ContainerSpec {
            name: "nginx".to_string(),
            image: "nginx:1.25".to_string(),
            command: vec!["nginx".to_string()],
            args: vec!["-g".to_string(), "daemon off;".to_string()],
            working_dir: Some("/app".to_string()),
            ports: vec![],
            env: vec![EnvVar {
                name: "PORT".to_string(),
                value: Some("8080".to_string()),
                value_from: None,
            }],
            resources: ResourceRequirements::default(),
            volume_mounts: vec![VolumeMount {
                name: "data".to_string(),
                mount_path: "/data".to_string(),
                sub_path: None,
                sub_path_expr: None,
                read_only: false,
            }],
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            image_pull_policy: ImagePullPolicy::IfNotPresent,
            security_context: None,
            termination_message_path: None,
            termination_message_policy: None,
            lifecycle: None,
            env_from: vec![],
            stdin: None,
            stdin_once: None,
            tty: None,
            restart_policy: None,
        }
    }

    #[test]
    fn test_container_spec_to_cri_config_basic() {
        let spec = sample_spec();
        let config = container_spec_to_cri_config(
            &spec,
            "my-pod",
            "uid-123",
            0,
            "/var/log/pods/default_my-pod_uid-123/nginx/0.log",
        );
        assert_eq!(config.metadata.name, "nginx");
        assert_eq!(config.image.image, "nginx:1.25");
        assert_eq!(config.command, vec!["nginx"]);
        assert_eq!(config.args, vec!["-g", "daemon off;"]);
        assert_eq!(config.working_dir, "/app");
    }

    #[test]
    fn test_env_vars_mapped() {
        let spec = sample_spec();
        let config = container_spec_to_cri_config(&spec, "pod", "uid", 0, "/log");
        assert_eq!(config.envs.len(), 1);
        assert_eq!(config.envs[0].key, "PORT");
        assert_eq!(config.envs[0].value, "8080");
    }

    #[test]
    fn test_volume_mounts_mapped() {
        let spec = sample_spec();
        let config = container_spec_to_cri_config(&spec, "pod", "uid", 0, "/log");
        assert_eq!(config.mounts.len(), 1);
        assert_eq!(config.mounts[0].container_path, "/data");
        assert!(!config.mounts[0].readonly);
    }

    #[test]
    fn test_labels_contain_k8s_metadata() {
        let spec = sample_spec();
        let config = container_spec_to_cri_config(&spec, "my-pod", "uid-abc", 0, "/log");
        assert_eq!(config.labels["io.kubernetes.pod.name"], "my-pod");
        assert_eq!(config.labels["io.kubernetes.pod.uid"], "uid-abc");
        assert_eq!(config.labels["io.kubernetes.container.name"], "nginx");
    }

    #[test]
    fn test_cri_container_status_to_runtime_container_running() {
        let status = CriContainerStatus {
            id: "ctr-abc".to_string(),
            metadata: CriContainerMetadata {
                name: "nginx".to_string(),
                attempt: 0,
            },
            state: CriContainerState::ContainerRunning,
            created_at: 1_000_000_000,
            started_at: 1_000_001_000,
            finished_at: 0,
            exit_code: 0,
            image: CriImageSpec {
                image: "nginx:1.25".to_string(),
                annotations: HashMap::new(),
            },
            image_ref: "sha256:abc".to_string(),
            reason: String::new(),
            message: String::new(),
            labels: HashMap::new(),
            annotations: HashMap::new(),
            mounts: vec![],
            log_path: "/log".to_string(),
        };
        let rc: RuntimeContainer = status.into();
        assert_eq!(rc.state, RuntimeContainerState::Running);
        assert_eq!(rc.name, "nginx");
    }

    #[test]
    fn test_cri_container_status_to_runtime_container_exited() {
        let status = CriContainerStatus {
            id: "ctr-def".to_string(),
            metadata: CriContainerMetadata {
                name: "job".to_string(),
                attempt: 0,
            },
            state: CriContainerState::ContainerExited,
            created_at: 1_000_000_000,
            started_at: 1_000_001_000,
            finished_at: 1_000_002_000,
            exit_code: 1,
            image: CriImageSpec {
                image: "alpine".to_string(),
                annotations: HashMap::new(),
            },
            image_ref: "sha256:def".to_string(),
            reason: "OOMKilled".to_string(),
            message: "container OOM killed".to_string(),
            labels: HashMap::new(),
            annotations: HashMap::new(),
            mounts: vec![],
            log_path: "/log".to_string(),
        };
        let rc: RuntimeContainer = status.into();
        assert_eq!(rc.state, RuntimeContainerState::Exited);
        assert_eq!(rc.exit_code, Some(1));
        assert_eq!(rc.exit_reason, Some("OOMKilled".to_string()));
    }

    #[test]
    fn test_cri_image_to_image_info() {
        let img = CriImage {
            id: "sha256:abc123".to_string(),
            repo_tags: vec!["nginx:latest".to_string()],
            repo_digests: vec!["sha256:abc123@sha256:digest".to_string()],
            size: 50_000_000,
            uid: None,
            username: String::new(),
            spec: None,
            pinned: false,
        };
        let info: ImageInfo = img.into();
        assert_eq!(info.id, "sha256:abc123");
        assert_eq!(info.repo_tags, vec!["nginx:latest"]);
        assert_eq!(info.size_bytes, 50_000_000);
    }

    #[test]
    fn test_cri_sandbox_state_eq() {
        assert_eq!(CriSandboxState::SandboxReady, CriSandboxState::SandboxReady);
        assert_ne!(
            CriSandboxState::SandboxReady,
            CriSandboxState::SandboxNotReady
        );
    }
}
