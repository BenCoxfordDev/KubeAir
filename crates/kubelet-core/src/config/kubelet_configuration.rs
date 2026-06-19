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

//! KubeletConfiguration v1beta1 -- the YAML config file format.
//!
//! Mirrors `k8s.io/kubelet/config/v1beta1.KubeletConfiguration`.
//! Loaded via `--config <path>` and merged with CLI flag overrides.
//!
//! Duration strings follow Go's time.Duration format: "1m0s", "30s", "5m".

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use super::KubeletConfig;

// -- Duration parsing ----------------------------------------------------------

/// Parse a Go-style duration string into std::time::Duration.
/// Supports: ns, us/µs, ms, s, m, h and combinations like "1m30s".
pub fn parse_go_duration(s: &str) -> Result<Duration, String> {
    if s == "0" || s == "0s" {
        return Ok(Duration::ZERO);
    }

    let mut total_nanos: u64 = 0;
    let mut chars = s.chars().peekable();
    let mut had_unit = false;

    while chars.peek().is_some() {
        // Parse number (may be float for ms/s)
        let mut num_str = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() || c == '.' {
                num_str.push(c);
                chars.next();
            } else {
                break;
            }
        }
        if num_str.is_empty() {
            break;
        }

        // Parse unit
        let mut unit = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_alphabetic() || c == 'µ' {
                unit.push(c);
                chars.next();
            } else {
                break;
            }
        }

        let num: f64 = num_str
            .parse()
            .map_err(|_| format!("invalid number '{}' in duration '{}'", num_str, s))?;

        let nanos = match unit.as_str() {
            "ns" => num as u64,
            "us" | "µs" => (num * 1_000.0) as u64,
            "ms" => (num * 1_000_000.0) as u64,
            "s" => (num * 1_000_000_000.0) as u64,
            "m" => (num * 60.0 * 1_000_000_000.0) as u64,
            "h" => (num * 3600.0 * 1_000_000_000.0) as u64,
            other => return Err(format!("unknown duration unit '{}' in '{}'", other, s)),
        };

        total_nanos = total_nanos.saturating_add(nanos);
        had_unit = true;
    }

    if !had_unit {
        return Err(format!("no duration unit found in '{}'", s));
    }

    Ok(Duration::from_nanos(total_nanos))
}

fn deserialize_go_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_go_duration(&s).map_err(serde::de::Error::custom)
}

fn deserialize_go_duration_opt<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Option::<String>::deserialize(deserializer)?;
    match s {
        None => Ok(None),
        Some(s) => parse_go_duration(&s)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

// -- KubeletConfiguration ------------------------------------------------------

/// Full `KubeletConfiguration` as written to disk and loaded via `--config`.
///
/// Fields marked with `#[serde(default)]` are optional in the YAML;
/// missing fields fall back to Kubernetes upstream defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubeletConfiguration {
    /// Must be "kubelet.config.k8s.io/v1beta1"
    #[serde(default = "default_api_version")]
    pub api_version: String,

    /// Must be "KubeletConfiguration"
    #[serde(default = "default_kind")]
    pub kind: String,

    // -- Serving -----------------------------------------------------------
    /// Address to bind the kubelet API server on.
    #[serde(default = "default_address")]
    pub address: String,

    /// HTTPS port for the kubelet API server.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Read-only HTTP port. 0 disables it.
    #[serde(default = "default_read_only_port")]
    pub read_only_port: u16,

    // -- TLS ---------------------------------------------------------------
    /// Path to the TLS server certificate file.
    #[serde(default)]
    pub tls_cert_file: Option<PathBuf>,

    /// Path to the TLS private key file.
    #[serde(default)]
    pub tls_private_key_file: Option<PathBuf>,

    /// If true, rotate the server certificate automatically.
    #[serde(default)]
    pub rotate_certificates: bool,

    /// If true, enable TLS bootstrapping for the serving cert.
    #[serde(default)]
    pub server_tls_bootstrap: bool,

    // -- Node identity -----------------------------------------------------
    /// Override the node name (defaults to hostname).
    #[serde(default)]
    pub node_name: Option<String>,

    // -- Runtime -----------------------------------------------------------
    /// CRI socket endpoint, e.g. "unix:///run/containerd/containerd.sock"
    #[serde(default = "default_cre")]
    pub container_runtime_endpoint: String,

    /// Separate image service endpoint. Falls back to containerRuntimeEndpoint.
    #[serde(default)]
    pub image_service_endpoint: Option<String>,

    /// Pod infra container image. Default: "registry.k8s.io/pause:3.9"
    #[serde(default = "default_pod_infra_container_image")]
    pub pod_infra_container_image: String,

    /// Root directory for kubelet state.
    #[serde(default = "default_root_dir")]
    pub root_dir: PathBuf,

    // -- Pod limits --------------------------------------------------------
    /// Maximum number of pods per node.
    #[serde(default = "default_max_pods")]
    pub max_pods: u32,

    /// Pods per CPU core. 0 means unlimited.
    #[serde(default)]
    pub pods_per_core: u32,

    // -- Static pods -------------------------------------------------------
    /// Directory to watch for static pod manifests.
    #[serde(default, rename = "staticPodPath")]
    pub static_pod_path: Option<PathBuf>,

    /// URL to fetch static pod manifests from.
    #[serde(default, rename = "staticPodURL")]
    pub static_pod_url: Option<String>,

    // -- Networking --------------------------------------------------------
    /// Cluster DNS server IPs.
    #[serde(default = "default_cluster_dns")]
    pub cluster_dns: Vec<String>,

    /// Cluster domain.
    #[serde(default = "default_cluster_domain")]
    pub cluster_domain: String,

    // -- Cgroups -----------------------------------------------------------
    /// Create cgroups per QoS class.
    #[serde(default = "default_true")]
    pub cgroups_per_qos: bool,

    /// cgroup driver: "cgroupfs" or "systemd"
    #[serde(default = "default_cgroup_driver")]
    pub cgroup_driver: String,

    /// cgroup root; empty means use container runtime default.
    #[serde(default)]
    pub cgroup_root: Option<String>,

    // -- Eviction ---------------------------------------------------------
    /// Hard eviction thresholds. Pod is evicted immediately.
    #[serde(default = "default_eviction_hard")]
    pub eviction_hard: HashMap<String, String>,

    /// Soft eviction thresholds. Pod is evicted after grace period.
    #[serde(default)]
    pub eviction_soft: HashMap<String, String>,

    /// Grace period for soft eviction signals.
    #[serde(default)]
    pub eviction_soft_grace_period: HashMap<String, String>,

    /// Minimum time between eviction decisions.
    #[serde(
        default = "default_eviction_transition",
        deserialize_with = "deserialize_go_duration"
    )]
    pub eviction_pressure_transition_period: Duration,

    /// Minimum reclaim amounts per eviction signal.
    #[serde(default)]
    pub eviction_minimum_reclaim: HashMap<String, String>,

    // -- Image GC ---------------------------------------------------------
    /// Disk usage % that triggers image GC. Default: 85.
    #[serde(default = "default_gc_high")]
    pub image_gc_high_threshold_percent: u8,

    /// Disk usage % that GC targets. Default: 80.
    #[serde(default = "default_gc_low")]
    pub image_gc_low_threshold_percent: u8,

    /// Minimum age for an unused image before GC. Default: "2m0s"
    #[serde(
        default = "default_image_min_gc_age",
        deserialize_with = "deserialize_go_duration"
    )]
    pub image_minimum_gc_age: Duration,

    // -- Container GC -----------------------------------------------------
    /// Max size of a container log file. Default: "10Mi"
    #[serde(default = "default_log_max_size")]
    pub container_log_max_size: String,

    /// Max number of log files per container. Default: 5
    #[serde(default = "default_log_max_files")]
    pub container_log_max_files: u32,

    // -- Sync frequencies -------------------------------------------------
    /// How often the kubelet syncs pods. Default: "1m0s"
    #[serde(
        default = "default_sync_freq",
        deserialize_with = "deserialize_go_duration"
    )]
    pub sync_frequency: Duration,

    /// How often to check static pod files. Default: "20s"
    #[serde(
        default = "default_file_check_freq",
        deserialize_with = "deserialize_go_duration"
    )]
    pub file_check_frequency: Duration,

    /// How often to check static pod URLs. Default: "20s"
    #[serde(
        default = "default_http_check_freq",
        deserialize_with = "deserialize_go_duration"
    )]
    pub http_check_frequency: Duration,

    /// How often to post node status. Default: "10s"
    #[serde(
        default = "default_node_status_update_freq",
        deserialize_with = "deserialize_go_duration"
    )]
    pub node_status_update_frequency: Duration,

    /// How often the kubelet reports to the API server. Default: "5m0s"
    #[serde(
        default = "default_node_status_report_freq",
        deserialize_with = "deserialize_go_duration"
    )]
    pub node_status_report_frequency: Duration,

    /// Duration of node lease. Default: 40 seconds.
    #[serde(default = "default_lease_duration")]
    pub node_lease_duration_seconds: u32,

    // -- Resource reservation ---------------------------------------------
    /// Resources reserved for Kubernetes system daemons.
    #[serde(default)]
    pub kube_reserved: HashMap<String, String>,

    /// Resources reserved for non-Kubernetes components.
    #[serde(default)]
    pub system_reserved: HashMap<String, String>,

    // -- CPU / Memory managers --------------------------------------------
    /// CPU manager policy: "none" or "static"
    #[serde(default = "default_cpu_manager_policy")]
    pub cpu_manager_policy: String,

    /// Memory manager policy: "None" or "Static"
    #[serde(default = "default_memory_manager_policy")]
    pub memory_manager_policy: String,

    /// Topology manager policy: "none", "best-effort", "restricted", "single-numa-node"
    #[serde(default = "default_topology_manager_policy")]
    pub topology_manager_policy: String,

    /// Topology manager scope: "container" or "pod"
    #[serde(default = "default_topology_manager_scope")]
    pub topology_manager_scope: String,

    // -- Feature gates ----------------------------------------------------
    #[serde(default)]
    pub feature_gates: HashMap<String, bool>,

    // -- Security ---------------------------------------------------------
    /// Fail if swap is enabled. Default: true
    #[serde(default = "default_true")]
    pub fail_swap_on: bool,

    /// Allow privileged containers.
    #[serde(default = "default_true")]
    pub allow_privileged: bool,

    // -- Logging -----------------------------------------------------------
    #[serde(default)]
    pub logging: LoggingConfig,

    // -- Misc --------------------------------------------------------------
    /// Serialize image pulls (one at a time). Default: true
    #[serde(default = "default_true")]
    pub serialize_image_pulls: bool,

    /// Max open file descriptors. Default: 1000000
    #[serde(default = "default_max_open_files")]
    pub max_open_files: u64,

    /// Enforce node allocatable. Default: ["pods"]
    #[serde(default = "default_enforce_node_allocatable")]
    pub enforce_node_allocatable: Vec<String>,

    /// Reserved system CPU cores (e.g., "0,1")
    #[serde(default)]
    pub reserved_system_cpus: Option<String>,

    // -- Authentication / Authorization ------------------------------------
    /// Controls how requests to the kubelet are authenticated.
    #[serde(default)]
    pub authentication: KubeletAuthentication,

    /// Controls how requests to the kubelet are authorized.
    #[serde(default)]
    pub authorization: KubeletAuthorization,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubeletAuthentication {
    #[serde(default)]
    pub anonymous: KubeletAnonymousAuthentication,
    #[serde(default)]
    pub x509: KubeletX509Authentication,
}

impl Default for KubeletAuthentication {
    fn default() -> Self {
        Self {
            anonymous: KubeletAnonymousAuthentication { enabled: false },
            x509: KubeletX509Authentication::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KubeletX509Authentication {
    /// Path to the CA file used to verify client certificates (x509 mTLS).
    #[serde(default, rename = "clientCAFile")]
    pub client_ca_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KubeletAnonymousAuthentication {
    /// Whether anonymous requests are allowed. Default: false.
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubeletAuthorization {
    /// Authorization mode: "AlwaysAllow" or "Webhook". Default: "Webhook".
    #[serde(default = "default_authz_mode")]
    pub mode: String,
}

impl Default for KubeletAuthorization {
    fn default() -> Self {
        Self {
            mode: default_authz_mode(),
        }
    }
}

fn default_authz_mode() -> String {
    "Webhook".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LoggingConfig {
    #[serde(default = "default_log_verbosity")]
    pub verbosity: u8,
    #[serde(default = "default_log_format")]
    pub format: String,
}

// -- Defaults -----------------------------------------------------------------

fn default_api_version() -> String {
    "kubelet.config.k8s.io/v1beta1".to_string()
}
fn default_kind() -> String {
    "KubeletConfiguration".to_string()
}
fn default_address() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    10250
}
fn default_read_only_port() -> u16 {
    10255
}
fn default_cre() -> String {
    "unix:///run/containerd/containerd.sock".to_string()
}
fn default_pod_infra_container_image() -> String {
    "registry.k8s.io/pause:3.9".to_string()
}

fn default_root_dir() -> PathBuf {
    PathBuf::from("/var/lib/kubelet")
}
fn default_max_pods() -> u32 {
    110
}
fn default_cluster_dns() -> Vec<String> {
    vec!["10.96.0.10".to_string()]
}
fn default_cluster_domain() -> String {
    "cluster.local".to_string()
}
fn default_true() -> bool {
    true
}
fn default_cgroup_driver() -> String {
    "cgroupfs".to_string()
}
fn default_gc_high() -> u8 {
    85
}
fn default_gc_low() -> u8 {
    80
}
fn default_log_max_size() -> String {
    "10Mi".to_string()
}
fn default_log_max_files() -> u32 {
    5
}
fn default_lease_duration() -> u32 {
    40
}
fn default_max_open_files() -> u64 {
    1_000_000
}
fn default_log_verbosity() -> u8 {
    2
}
fn default_log_format() -> String {
    "text".to_string()
}
fn default_cpu_manager_policy() -> String {
    "none".to_string()
}
fn default_memory_manager_policy() -> String {
    "None".to_string()
}
fn default_topology_manager_policy() -> String {
    "none".to_string()
}
fn default_topology_manager_scope() -> String {
    "container".to_string()
}
fn default_enforce_node_allocatable() -> Vec<String> {
    vec!["pods".to_string()]
}

fn default_eviction_hard() -> HashMap<String, String> {
    [
        ("memory.available".to_string(), "100Mi".to_string()),
        ("nodefs.available".to_string(), "10%".to_string()),
        ("nodefs.inodesFree".to_string(), "5%".to_string()),
        ("imagefs.available".to_string(), "15%".to_string()),
    ]
    .into_iter()
    .collect()
}

fn default_eviction_transition() -> Duration {
    Duration::from_secs(300)
}
fn default_image_min_gc_age() -> Duration {
    Duration::from_secs(120)
}
fn default_sync_freq() -> Duration {
    Duration::from_secs(60)
}
fn default_file_check_freq() -> Duration {
    Duration::from_secs(20)
}
fn default_http_check_freq() -> Duration {
    Duration::from_secs(20)
}
fn default_node_status_update_freq() -> Duration {
    Duration::from_secs(10)
}
fn default_node_status_report_freq() -> Duration {
    Duration::from_secs(300)
}

// -- Conversion: KubeletConfiguration -> KubeletConfig ------------------------

impl KubeletConfiguration {
    /// Convert into a `KubeletConfig`, resolving hostname if node_name is absent.
    pub fn into_kubelet_config(self) -> Result<KubeletConfig, String> {
        let node_name = if let Some(name) = self.node_name.filter(|n| !n.is_empty()) {
            name
        } else {
            hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "localhost".to_string())
        };

        Ok(KubeletConfig {
            node_name,
            api_server_url: "https://localhost:6443".to_string(),
            kubeconfig_path: None,
            container_runtime_endpoint: self.container_runtime_endpoint,
            image_service_endpoint: self.image_service_endpoint,
            pod_infra_container_image: self.pod_infra_container_image,
            root_dir: self.root_dir,
            pods_per_core: self.pods_per_core,
            max_pods: self.max_pods,
            eviction_hard: self.eviction_hard,
            eviction_soft: self.eviction_soft,
            eviction_soft_grace_period: self.eviction_soft_grace_period,
            eviction_pressure_transition_period: self.eviction_pressure_transition_period,
            node_status_update_frequency: self.node_status_update_frequency,
            node_status_report_frequency: self.node_status_report_frequency,
            node_lease_duration_seconds: self.node_lease_duration_seconds,
            sync_frequency: self.sync_frequency,
            file_check_frequency: self.file_check_frequency,
            http_check_frequency: self.http_check_frequency,
            static_pod_path: self.static_pod_path,
            static_pod_url: self.static_pod_url,
            log_level: self.logging.verbosity,
            tls_cert_file: self.tls_cert_file,
            tls_private_key_file: self.tls_private_key_file,
            feature_gates: self.feature_gates,
            kube_reserved: self.kube_reserved,
            system_reserved: self.system_reserved,
            cluster_dns: self.cluster_dns,
            cluster_domain: self.cluster_domain,
            image_gc_high_threshold_percent: self.image_gc_high_threshold_percent,
            image_gc_low_threshold_percent: self.image_gc_low_threshold_percent,
            image_minimum_gc_age: self.image_minimum_gc_age,
            container_log_max_size: self.container_log_max_size,
            container_log_max_files: self.container_log_max_files,
            address: self.address,
            port: self.port,
            read_only_port: self.read_only_port,
            cpu_manager_policy: self.cpu_manager_policy,
            memory_manager_policy: self.memory_manager_policy,
            topology_manager_policy: self.topology_manager_policy,
            topology_manager_scope: self.topology_manager_scope,
            cgroup_driver: self.cgroup_driver,
            cgroups_per_qos: self.cgroups_per_qos,
            reserved_system_cpus: self.reserved_system_cpus,
            fail_swap_on: self.fail_swap_on,
            serialize_image_pulls: self.serialize_image_pulls,
            max_open_files: self.max_open_files,
            enforce_node_allocatable: self.enforce_node_allocatable,
            anonymous_auth_enabled: self.authentication.anonymous.enabled,
            always_allow: self.authorization.mode == "AlwaysAllow",
            client_ca_file: self.authentication.x509.client_ca_file,
        })
    }

    /// Load from a YAML file.
    pub fn from_file(path: &std::path::Path) -> Result<Self, String> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read config file '{}': {}", path.display(), e))?;
        serde_yaml::from_str(&contents)
            .map_err(|e| format!("failed to parse config file '{}': {}", path.display(), e))
    }
}

// -- Tests ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_go_duration_seconds() {
        assert_eq!(parse_go_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_go_duration("0s").unwrap(), Duration::ZERO);
        assert_eq!(parse_go_duration("0").unwrap(), Duration::ZERO);
    }

    #[test]
    fn test_parse_go_duration_minutes() {
        assert_eq!(parse_go_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_go_duration("1m0s").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_go_duration("5m0s").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn test_parse_go_duration_hours() {
        assert_eq!(parse_go_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(
            parse_go_duration("1h30m").unwrap(),
            Duration::from_secs(5400)
        );
    }

    #[test]
    fn test_parse_go_duration_ms() {
        assert_eq!(
            parse_go_duration("500ms").unwrap(),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn test_parse_go_duration_compound() {
        assert_eq!(
            parse_go_duration("2m30s").unwrap(),
            Duration::from_secs(150)
        );
    }

    #[test]
    fn test_parse_go_duration_invalid() {
        assert!(parse_go_duration("abc").is_err());
        assert!(parse_go_duration("5x").is_err());
    }

    #[test]
    fn test_deserialize_minimal_yaml() {
        let yaml = "apiVersion: kubelet.config.k8s.io/v1beta1\nkind: KubeletConfiguration\n";
        let cfg: KubeletConfiguration = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.max_pods, 110);
        assert_eq!(cfg.port, 10250);
        assert_eq!(cfg.cluster_domain, "cluster.local");
    }

    #[test]
    fn test_deserialize_custom_values() {
        let yaml = r#"
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
maxPods: 50
port: 10251
clusterDomain: my.cluster
syncFrequency: "2m0s"
evictionHard:
  memory.available: "200Mi"
"#;
        let cfg: KubeletConfiguration = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.max_pods, 50);
        assert_eq!(cfg.port, 10251);
        assert_eq!(cfg.cluster_domain, "my.cluster");
        assert_eq!(cfg.sync_frequency, Duration::from_secs(120));
        assert_eq!(cfg.eviction_hard.get("memory.available").unwrap(), "200Mi");
    }

    #[test]
    fn test_default_kubelet_configuration_is_valid() {
        let cfg = KubeletConfiguration {
            api_version: default_api_version(),
            kind: default_kind(),
            address: default_address(),
            port: default_port(),
            read_only_port: default_read_only_port(),
            tls_cert_file: None,
            tls_private_key_file: None,
            rotate_certificates: false,
            server_tls_bootstrap: false,
            node_name: Some("node1".to_string()),
            container_runtime_endpoint: default_cre(),
            image_service_endpoint: None,
            pod_infra_container_image: default_pod_infra_container_image(),
            root_dir: default_root_dir(),
            max_pods: default_max_pods(),
            pods_per_core: 0,
            static_pod_path: None,
            static_pod_url: None,
            cluster_dns: default_cluster_dns(),
            cluster_domain: default_cluster_domain(),
            cgroups_per_qos: true,
            cgroup_driver: default_cgroup_driver(),
            cgroup_root: None,
            eviction_hard: default_eviction_hard(),
            eviction_soft: Default::default(),
            eviction_soft_grace_period: Default::default(),
            eviction_pressure_transition_period: default_eviction_transition(),
            eviction_minimum_reclaim: Default::default(),
            image_gc_high_threshold_percent: default_gc_high(),
            image_gc_low_threshold_percent: default_gc_low(),
            image_minimum_gc_age: default_image_min_gc_age(),
            container_log_max_size: default_log_max_size(),
            container_log_max_files: default_log_max_files(),
            sync_frequency: default_sync_freq(),
            file_check_frequency: default_file_check_freq(),
            http_check_frequency: default_http_check_freq(),
            node_status_update_frequency: default_node_status_update_freq(),
            node_status_report_frequency: default_node_status_report_freq(),
            node_lease_duration_seconds: default_lease_duration(),
            kube_reserved: Default::default(),
            system_reserved: Default::default(),
            cpu_manager_policy: default_cpu_manager_policy(),
            memory_manager_policy: default_memory_manager_policy(),
            topology_manager_policy: default_topology_manager_policy(),
            topology_manager_scope: default_topology_manager_scope(),
            feature_gates: Default::default(),
            fail_swap_on: true,
            allow_privileged: true,
            logging: LoggingConfig {
                verbosity: 2,
                format: "text".to_string(),
            },
            serialize_image_pulls: true,
            max_open_files: default_max_open_files(),
            enforce_node_allocatable: default_enforce_node_allocatable(),
            reserved_system_cpus: None,
            authentication: KubeletAuthentication::default(),
            authorization: KubeletAuthorization::default(),
        };
        let kc = cfg.into_kubelet_config().unwrap();
        assert_eq!(kc.max_pods, 110);
        assert_eq!(kc.node_name, "node1");
    }
}
