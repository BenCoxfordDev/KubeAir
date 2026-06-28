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

//! Pod domain model and lifecycle state machine.

pub mod lifecycle;
pub mod manager;
pub mod status;
pub mod sync;

use crate::types::{PodRef, PodUID, ResourceQuantity};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The desired state of a pod as declared by the API server (or static manifest).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodSpec {
    pub uid: PodUID,
    pub pod_ref: PodRef,
    pub containers: Vec<ContainerSpec>,
    pub init_containers: Vec<ContainerSpec>,
    pub ephemeral_containers: Vec<ContainerSpec>,
    pub volumes: Vec<VolumeSpec>,
    pub node_name: String,
    pub host_network: bool,
    pub host_pid: bool,
    pub host_ipc: bool,
    pub dns_config: Option<DnsConfig>,
    pub restart_policy: RestartPolicy,
    pub termination_grace_period_seconds: u64,
    pub service_account_name: String,
    pub priority: Option<i32>,
    pub tolerations: Vec<Toleration>,
    pub node_selector: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub labels: HashMap<String, String>,
    pub runtime_class_name: Option<String>,
    pub security_context: Option<PodSecurityContext>,
    pub readiness_gates: Vec<ReadinessGate>,
    pub active_deadline_seconds: Option<u64>,
    pub automount_service_account_token: Option<bool>,
    pub image_pull_secrets: Vec<LocalObjectReference>,
    pub enable_service_links: Option<bool>,
    pub share_process_namespace: Option<bool>,
    pub resource_claims: Vec<ResourceClaimRef>,
    pub host_aliases: Vec<HostAlias>,
    /// Pod-level hostname override (spec.hostname). When set, this is used as the
    /// pod's DNS hostname instead of the pod metadata name.
    pub hostname: Option<String>,
    /// Headless service subdomain (spec.subdomain). Combined with hostname to form
    /// the pod's FQDN: <hostname>.<subdomain>.<namespace>.svc.<cluster-domain>.
    pub subdomain: Option<String>,
    /// startTime from the pod's existing API server status, used to seed the
    /// local lifecycle state so we never overwrite it with Utc::now().
    pub observed_start_time: Option<chrono::DateTime<chrono::Utc>>,
}

/// A readiness gate -- extra condition that must be True before pod is Ready.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessGate {
    pub condition_type: String,
}

/// An entry added to a pod's /etc/hosts file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HostAlias {
    pub ip: String,
    pub hostnames: Vec<String>,
}

/// Reference to a named object in the same namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalObjectReference {
    pub name: String,
}

impl PodSpec {
    pub fn key(&self) -> String {
        format!("{}/{}", self.pod_ref.namespace, self.pod_ref.name)
    }

    /// Returns the effective hostname for the pod — `spec.hostname` if set,
    /// otherwise the pod metadata name (truncated to 63 chars per RFC 1123).
    pub fn effective_hostname(&self) -> String {
        self.hostname
            .as_deref()
            .filter(|h| !h.is_empty())
            .unwrap_or(&self.pod_ref.name)
            .chars()
            .take(63)
            .collect()
    }
}

/// Spec for a single container within a pod.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub working_dir: Option<String>,
    pub ports: Vec<ContainerPort>,
    pub env: Vec<EnvVar>,
    pub env_from: Vec<EnvFromSource>,
    pub resources: ResourceRequirements,
    pub volume_mounts: Vec<VolumeMount>,
    pub liveness_probe: Option<Probe>,
    pub readiness_probe: Option<Probe>,
    pub startup_probe: Option<Probe>,
    pub lifecycle: Option<Lifecycle>,
    pub image_pull_policy: ImagePullPolicy,
    pub security_context: Option<SecurityContext>,
    pub termination_message_path: Option<String>,
    pub termination_message_policy: Option<String>,
    pub stdin: Option<bool>,
    pub stdin_once: Option<bool>,
    pub tty: Option<bool>,
    pub restart_policy: Option<RestartPolicy>,
}

/// Pod reference to a Dynamic Resource Allocation claim.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceClaimRef {
    pub name: String,
    pub resource_class_name: Option<String>,
    pub allocated: bool,
    pub prepared: bool,
}

/// Dynamic resource class definition tracked by the kubelet.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceClass {
    pub name: String,
    pub driver_name: String,
}

/// Dynamic resource claim state tracked by the kubelet.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceClaim {
    pub namespace: String,
    pub name: String,
    pub class_name: String,
    pub allocated: bool,
    pub prepared: bool,
}

/// Container lifecycle hooks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lifecycle {
    pub post_start: Option<LifecycleHandler>,
    pub pre_stop: Option<LifecycleHandler>,
}

/// A lifecycle hook handler -- exec command or HTTP call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LifecycleHandler {
    Exec {
        command: Vec<String>,
    },
    HttpGet {
        path: String,
        port: u16,
        host: Option<String>,
        scheme: String,
    },
    TcpSocket {
        port: u16,
        host: Option<String>,
    },
    Sleep {
        seconds: u64,
    },
}

/// EnvFrom -- inject all keys from a ConfigMap or Secret as env vars.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvFromSource {
    pub prefix: Option<String>,
    pub config_map_ref: Option<EnvFromRef>,
    pub secret_ref: Option<EnvFromRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvFromRef {
    pub name: String,
    pub optional: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerPort {
    pub name: Option<String>,
    pub container_port: u16,
    pub host_port: Option<u16>,
    pub protocol: Protocol,
    pub host_ip: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Protocol {
    TCP,
    UDP,
    SCTP,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvVar {
    pub name: String,
    pub value: Option<String>,
    pub value_from: Option<EnvVarSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EnvVarSource {
    FieldRef {
        field_path: String,
    },
    ResourceFieldRef {
        container_name: Option<String>,
        resource: String,
    },
    ConfigMapKeyRef {
        name: String,
        key: String,
        optional: bool,
    },
    SecretKeyRef {
        name: String,
        key: String,
        optional: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceRequirements {
    pub requests: HashMap<String, ResourceQuantity>,
    pub limits: HashMap<String, ResourceQuantity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeMount {
    pub name: String,
    pub mount_path: String,
    pub sub_path: Option<String>,
    pub sub_path_expr: Option<String>,
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Probe {
    pub handler: ProbeHandler,
    pub initial_delay_seconds: u32,
    pub period_seconds: u32,
    pub timeout_seconds: u32,
    pub success_threshold: u32,
    pub failure_threshold: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProbeHandler {
    Exec {
        command: Vec<String>,
    },
    HttpGet {
        path: String,
        port: u16,
        host: Option<String>,
        scheme: String,
    },
    TcpSocket {
        port: u16,
        host: Option<String>,
    },
    Grpc {
        port: u16,
        service: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub enum ImagePullPolicy {
    Always,
    #[default]
    IfNotPresent,
    Never,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub enum RestartPolicy {
    #[default]
    Always,
    OnFailure,
    Never,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeSpec {
    pub name: String,
    pub source: VolumeSource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VolumeSource {
    EmptyDir {
        medium: Option<String>,
        size_limit: Option<ResourceQuantity>,
    },
    HostPath {
        path: String,
        path_type: Option<String>,
    },
    ConfigMap {
        name: String,
        items: Vec<KeyToPath>,
        optional: bool,
        default_mode: Option<i32>,
    },
    Secret {
        secret_name: String,
        items: Vec<KeyToPath>,
        optional: bool,
        default_mode: Option<i32>,
    },
    PersistentVolumeClaim {
        claim_name: String,
        read_only: bool,
    },
    Projected {
        sources: Vec<ProjectedVolumeSource>,
        default_mode: Option<i32>,
    },
    DownwardAPI {
        items: Vec<DownwardAPIVolumeFile>,
        default_mode: Option<i32>,
    },
    NFS {
        server: String,
        path: String,
        read_only: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyToPath {
    pub key: String,
    pub path: String,
    pub mode: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProjectedVolumeSource {
    ServiceAccountToken {
        audience: Option<String>,
        expiration_seconds: Option<u64>,
        path: String,
    },
    ConfigMap {
        name: String,
        items: Vec<KeyToPath>,
        optional: bool,
    },
    Secret {
        name: String,
        items: Vec<KeyToPath>,
        optional: bool,
    },
    DownwardAPI {
        items: Vec<DownwardAPIVolumeFile>,
    },
}

/// Identifies a container resource quantity exposed through the DownwardAPI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceFieldRef {
    /// Optional container name; defaults to the first container in the pod.
    pub container_name: Option<String>,
    /// Resource path, e.g. `limits.cpu`, `requests.memory`, `limits.ephemeral-storage`.
    pub resource: String,
    /// Divisor applied to the raw quantity before writing the value.
    /// k8s quantity string: `"1"` (bytes/cores), `"1m"` (millicores), `"1Ki"`, `"1Mi"`, etc.
    /// Defaults to `"1"` when absent.
    pub divisor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownwardAPIVolumeFile {
    pub path: String,
    pub field_ref: Option<String>,
    pub resource_field_ref: Option<ResourceFieldRef>,
    pub mode: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsConfig {
    pub nameservers: Vec<String>,
    pub searches: Vec<String>,
    pub options: Vec<DnsOption>,
    pub policy: DnsPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum DnsPolicy {
    ClusterFirstWithHostNet,
    #[default]
    ClusterFirst,
    Default,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsOption {
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Toleration {
    pub key: Option<String>,
    pub operator: TolerationOperator,
    pub value: Option<String>,
    pub effect: Option<TaintEffect>,
    pub toleration_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum TolerationOperator {
    #[default]
    Equal,
    Exists,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaintEffect {
    NoSchedule,
    PreferNoSchedule,
    NoExecute,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PodSecurityContext {
    pub run_as_user: Option<u32>,
    pub run_as_group: Option<u32>,
    pub run_as_non_root: Option<bool>,
    pub fs_group: Option<u32>,
    pub supplemental_groups: Vec<u32>,
    pub sysctls: Vec<Sysctl>,
    pub seccomp_profile: Option<SeccompSpec>,
    pub fs_group_change_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SecurityContext {
    pub run_as_user: Option<u32>,
    pub run_as_group: Option<u32>,
    pub run_as_non_root: Option<bool>,
    pub privileged: Option<bool>,
    pub read_only_root_filesystem: Option<bool>,
    pub allow_privilege_escalation: Option<bool>,
    pub capabilities: Option<Capabilities>,
    pub seccomp_profile: Option<SeccompSpec>,
    pub apparmor_profile: Option<AppArmorSpec>,
    pub proc_mount: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeccompSpec {
    pub type_: String, // "RuntimeDefault" | "Unconfined" | "Localhost"
    pub localhost_profile: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppArmorSpec {
    pub type_: String, // "RuntimeDefault" | "Unconfined" | "Localhost"
    pub localhost_profile: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capabilities {
    pub add: Vec<String>,
    pub drop: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sysctl {
    pub name: String,
    pub value: String,
}

/// A pod update event sent to the pod manager.
#[derive(Debug, Clone)]
pub struct PodUpdate {
    pub pod: PodSpec,
    pub op: PodOperation,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PodOperation {
    Add,
    Update,
    Remove,
    Reconcile,
}

impl Default for PodSpec {
    fn default() -> Self {
        Self {
            uid: PodUID::new(""),
            pod_ref: PodRef {
                name: String::new(),
                namespace: "default".to_string(),
            },
            containers: vec![],
            init_containers: vec![],
            ephemeral_containers: vec![],
            volumes: vec![],
            node_name: String::new(),
            host_network: false,
            host_pid: false,
            host_ipc: false,
            dns_config: None,
            restart_policy: RestartPolicy::Always,
            termination_grace_period_seconds: 30,
            service_account_name: "default".to_string(),
            priority: None,
            tolerations: vec![],
            node_selector: HashMap::new(),
            annotations: HashMap::new(),
            labels: HashMap::new(),
            runtime_class_name: None,
            security_context: None,
            readiness_gates: vec![],
            active_deadline_seconds: None,
            automount_service_account_token: None,
            image_pull_secrets: vec![],
            enable_service_links: None,
            share_process_namespace: None,
            resource_claims: vec![],
            host_aliases: vec![],
            hostname: None,
            subdomain: None,
            observed_start_time: None,
        }
    }
}

impl Default for ContainerSpec {
    fn default() -> Self {
        Self {
            name: String::new(),
            image: String::new(),
            command: vec![],
            args: vec![],
            working_dir: None,
            ports: vec![],
            env: vec![],
            env_from: vec![],
            resources: ResourceRequirements::default(),
            volume_mounts: vec![],
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            lifecycle: None,
            image_pull_policy: ImagePullPolicy::IfNotPresent,
            security_context: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            restart_policy: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pod(name: &str, hostname: Option<&str>) -> PodSpec {
        PodSpec {
            pod_ref: PodRef {
                name: name.to_string(),
                namespace: "default".to_string(),
            },
            hostname: hostname.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn effective_hostname_uses_pod_name_when_hostname_not_set() {
        let pod = make_pod("my-pod-0", None);
        assert_eq!(pod.effective_hostname(), "my-pod-0");
    }

    #[test]
    fn effective_hostname_uses_pod_name_when_hostname_is_empty_string() {
        let pod = make_pod("my-pod-0", Some(""));
        assert_eq!(pod.effective_hostname(), "my-pod-0");
    }

    #[test]
    fn effective_hostname_uses_spec_hostname_when_set() {
        let pod = make_pod("my-pod-0", Some("custom-host"));
        assert_eq!(pod.effective_hostname(), "custom-host");
    }

    #[test]
    fn effective_hostname_truncates_to_63_chars() {
        // Pod name longer than 63 chars (Kubernetes allows up to 253 for metadata.name,
        // but UTS hostname limit is 63).
        let long_name = "a".repeat(100);
        let pod = make_pod(&long_name, None);
        assert_eq!(pod.effective_hostname().len(), 63);
        assert_eq!(pod.effective_hostname(), "a".repeat(63));
    }

    #[test]
    fn effective_hostname_truncates_spec_hostname_to_63_chars() {
        let long_hostname = "b".repeat(80);
        let pod = make_pod("my-pod-0", Some(&long_hostname));
        assert_eq!(pod.effective_hostname().len(), 63);
        assert_eq!(pod.effective_hostname(), "b".repeat(63));
    }

    #[test]
    fn effective_hostname_exactly_63_chars_not_truncated() {
        let name = "c".repeat(63);
        let pod = make_pod(&name, None);
        assert_eq!(pod.effective_hostname().len(), 63);
    }
}
