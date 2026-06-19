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

//! Kubelet configuration domain types.
//!
//! Mirrors KubeletConfiguration from k8s.io/kubelet/config/v1beta1.

pub mod kubelet_configuration;
pub use kubelet_configuration::KubeletConfiguration;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

/// Full kubelet configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KubeletConfig {
    // Node identity
    pub node_name: String,

    // Kubernetes API server
    pub api_server_url: String,
    pub kubeconfig_path: Option<PathBuf>,

    // Runtime
    pub container_runtime_endpoint: String,
    pub image_service_endpoint: Option<String>,
    pub root_dir: PathBuf,
    pub pods_per_core: u32,
    pub max_pods: u32,

    // Eviction
    pub eviction_hard: HashMap<String, String>,
    pub eviction_soft: HashMap<String, String>,
    pub eviction_soft_grace_period: HashMap<String, String>,
    pub eviction_pressure_transition_period: Duration,

    // Node status
    pub node_status_update_frequency: Duration,
    pub node_status_report_frequency: Duration,
    pub node_lease_duration_seconds: u32,

    // Sync
    pub sync_frequency: Duration,
    pub file_check_frequency: Duration,
    pub http_check_frequency: Duration,

    // Static pods
    pub static_pod_path: Option<PathBuf>,
    pub static_pod_url: Option<String>,

    // Logging
    pub log_level: u8,

    // TLS
    pub tls_cert_file: Option<PathBuf>,
    pub tls_private_key_file: Option<PathBuf>,

    // Feature gates
    pub feature_gates: HashMap<String, bool>,

    // Resource reservation
    pub kube_reserved: HashMap<String, String>,
    pub system_reserved: HashMap<String, String>,

    // Network
    pub cluster_dns: Vec<String>,
    pub cluster_domain: String,

    // Image garbage collection
    pub image_gc_high_threshold_percent: u8,
    pub image_gc_low_threshold_percent: u8,
    pub image_minimum_gc_age: Duration,

    // Container garbage collection
    pub container_log_max_size: String,
    pub container_log_max_files: u32,

    // Serving
    pub address: String,
    pub port: u16,
    pub read_only_port: u16,

    // CPU / Memory / Topology managers
    pub cpu_manager_policy: String,
    pub memory_manager_policy: String,
    pub topology_manager_policy: String,
    pub topology_manager_scope: String,

    // Cgroup
    pub cgroup_driver: String,
    pub cgroups_per_qos: bool,

    // Security
    pub fail_swap_on: bool,

    // Images
    pub pod_infra_container_image: String,

    // Misc
    pub serialize_image_pulls: bool,
    pub max_open_files: u64,
    pub enforce_node_allocatable: Vec<String>,
    pub reserved_system_cpus: Option<String>,

    // Auth
    /// Allow requests with no credentials (maps to authentication.anonymous.enabled).
    pub anonymous_auth_enabled: bool,
    /// Skip authorization check entirely (maps to authorization.mode: AlwaysAllow).
    pub always_allow: bool,
    /// Path to the CA cert used to verify client certificates (x509 mTLS for kubelet serving).
    pub client_ca_file: Option<PathBuf>,
}

impl Default for KubeletConfig {
    fn default() -> Self {
        Self {
            node_name: String::new(),
            api_server_url: "https://localhost:6443".to_string(),
            kubeconfig_path: None,
            container_runtime_endpoint: "unix:///run/containerd/containerd.sock".to_string(),
            image_service_endpoint: None,
            root_dir: PathBuf::from("/var/lib/kubelet"),
            pods_per_core: 0,
            max_pods: 110,
            eviction_hard: [
                ("memory.available".to_string(), "100Mi".to_string()),
                ("nodefs.available".to_string(), "10%".to_string()),
                ("nodefs.inodesFree".to_string(), "5%".to_string()),
                ("imagefs.available".to_string(), "15%".to_string()),
            ]
            .into_iter()
            .collect(),
            eviction_soft: HashMap::new(),
            eviction_soft_grace_period: HashMap::new(),
            eviction_pressure_transition_period: Duration::from_secs(300),
            node_status_update_frequency: Duration::from_secs(10),
            node_status_report_frequency: Duration::from_secs(300),
            node_lease_duration_seconds: 40,
            sync_frequency: Duration::from_secs(60),
            file_check_frequency: Duration::from_secs(20),
            http_check_frequency: Duration::from_secs(20),
            static_pod_path: None,
            static_pod_url: None,
            log_level: 2,
            tls_cert_file: None,
            tls_private_key_file: None,
            feature_gates: HashMap::new(),
            kube_reserved: HashMap::new(),
            system_reserved: HashMap::new(),
            cluster_dns: vec!["10.96.0.10".to_string()],
            cluster_domain: "cluster.local".to_string(),
            image_gc_high_threshold_percent: 85,
            image_gc_low_threshold_percent: 80,
            image_minimum_gc_age: Duration::from_secs(120),
            container_log_max_size: "10Mi".to_string(),
            container_log_max_files: 5,
            address: "0.0.0.0".to_string(),
            port: 10250,
            read_only_port: 10255,
            cpu_manager_policy: "none".to_string(),
            memory_manager_policy: "None".to_string(),
            topology_manager_policy: "none".to_string(),
            topology_manager_scope: "container".to_string(),
            cgroup_driver: "cgroupfs".to_string(),
            cgroups_per_qos: true,
            fail_swap_on: true,
            pod_infra_container_image: "registry.k8s.io/pause:3.9".to_string(),
            serialize_image_pulls: true,
            max_open_files: 1_000_000,
            enforce_node_allocatable: vec!["pods".to_string()],
            reserved_system_cpus: None,
            anonymous_auth_enabled: false,
            always_allow: false,
            client_ca_file: None,
        }
    }
}

impl KubeletConfig {
    /// Validate that required fields are set.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if self.node_name.is_empty() {
            errors.push("node_name must be set".to_string());
        }
        if self.image_gc_high_threshold_percent <= self.image_gc_low_threshold_percent {
            errors.push(
                "image_gc_high_threshold_percent must be greater than image_gc_low_threshold_percent"
                    .to_string(),
            );
        }
        if self.max_pods == 0 {
            errors.push("max_pods must be > 0".to_string());
        }
        if self.port == 0 {
            errors.push("port must be non-zero".to_string());
        }
        if self.sync_frequency.is_zero() {
            errors.push("sync_frequency must be non-zero".to_string());
        }
        if self.node_status_update_frequency.is_zero() {
            errors.push("node_status_update_frequency must be non-zero".to_string());
        }
        if self.file_check_frequency.is_zero() {
            errors.push("file_check_frequency must be non-zero".to_string());
        }
        if self.http_check_frequency.is_zero() {
            errors.push("http_check_frequency must be non-zero".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_validation_fails_without_node_name() {
        let config = KubeletConfig::default();
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("node_name")));
    }

    #[test]
    fn test_valid_config_passes_validation() {
        let config = KubeletConfig {
            node_name: "node1".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_gc_thresholds_fail_validation() {
        let config = KubeletConfig {
            node_name: "node1".to_string(),
            image_gc_high_threshold_percent: 80,
            image_gc_low_threshold_percent: 85, // high < low, invalid
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_max_pods_fails_validation() {
        let config = KubeletConfig {
            node_name: "node1".to_string(),
            max_pods: 0,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_default_eviction_thresholds_are_set() {
        let config = KubeletConfig::default();
        assert!(config.eviction_hard.contains_key("memory.available"));
        assert!(config.eviction_hard.contains_key("nodefs.available"));
    }

    #[test]
    fn test_zero_frequencies_fail_validation() {
        let config = KubeletConfig {
            node_name: "node1".to_string(),
            sync_frequency: Duration::ZERO,
            node_status_update_frequency: Duration::ZERO,
            file_check_frequency: Duration::ZERO,
            http_check_frequency: Duration::ZERO,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e == "sync_frequency must be non-zero"));
        assert!(errors
            .iter()
            .any(|e| e == "node_status_update_frequency must be non-zero"));
        assert!(errors
            .iter()
            .any(|e| e == "file_check_frequency must be non-zero"));
        assert!(errors
            .iter()
            .any(|e| e == "http_check_frequency must be non-zero"));
    }
}
