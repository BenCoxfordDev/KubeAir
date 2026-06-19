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

//! CLI argument parsing for the kubelet binary.
//!
//! Mirrors the real kubelet's flag set. When `--config` is supplied, the
//! KubeletConfiguration YAML file is loaded first; any explicit CLI flags
//! override individual fields within it (flags win over config file).

use clap::Parser;
use kubelet_core::config::{KubeletConfig, KubeletConfiguration};
use std::path::PathBuf;

/// Kubernetes kubelet -- node agent
#[derive(Debug, Parser)]
#[command(name = "kubelet", version, about)]
pub struct KubeletArgs {
    // -- Config file -------------------------------------------------------
    /// Path to a KubeletConfiguration YAML file (kubelet.config.k8s.io/v1beta1).
    /// CLI flags override individual fields from the file.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    // -- Node identity -----------------------------------------------------
    /// Node name (defaults to hostname, or nodeName from --config).
    #[arg(long, env = "KUBELET_NODE_NAME")]
    pub node_name: Option<String>,

    // -- API server --------------------------------------------------------
    /// Kubernetes API server URL.
    #[arg(
        long,
        default_value = "https://localhost:6443",
        env = "KUBERNETES_SERVICE_HOST"
    )]
    pub api_server: Option<String>,

    /// Path to kubeconfig file.
    #[arg(long, env = "KUBECONFIG")]
    pub kubeconfig: Option<PathBuf>,

    // -- Runtime -----------------------------------------------------------
    /// Container runtime endpoint (overrides --config).
    #[arg(long)]
    pub container_runtime_endpoint: Option<String>,

    /// Pod infra container image (overrides --config).
    #[arg(long)]
    pub pod_infra_container_image: Option<String>,

    /// Root directory for kubelet state (overrides --config).
    #[arg(long)]
    pub root_dir: Option<PathBuf>,

    // -- Pods -------------------------------------------------------------
    /// Maximum number of pods (overrides --config).
    #[arg(long)]
    pub max_pods: Option<u32>,

    /// Static pod manifests directory (overrides --config).
    #[arg(long)]
    pub pod_manifest_path: Option<PathBuf>,

    // -- Networking --------------------------------------------------------
    /// Cluster DNS IP(s), comma-separated (overrides --config).
    #[arg(long)]
    pub cluster_dns: Option<String>,

    /// Cluster domain (overrides --config).
    #[arg(long)]
    pub cluster_domain: Option<String>,

    // -- Serving -----------------------------------------------------------
    /// Kubelet listening address (overrides --config).
    #[arg(long)]
    pub address: Option<String>,

    /// Kubelet HTTPS port (overrides --config).
    #[arg(long)]
    pub port: Option<u16>,

    /// Hostname override (maps to node name if provided).
    #[arg(long)]
    pub hostname_override: Option<String>,

    /// Optional drop-in config directory.
    ///
    /// Accepted for compatibility with upstream e2e runner invocations.
    #[arg(long)]
    pub config_dir: Option<PathBuf>,

    /// Path to bootstrap kubeconfig (kubeadm passes this for TLS bootstrapping).
    ///
    /// Accepted for compatibility. After `kubeadm init` the node already has a
    /// fully-signed `kubelet.conf`; we use `--kubeconfig` instead.
    #[arg(long, hide = true)]
    pub bootstrap_kubeconfig: Option<PathBuf>,

    // -- TLS ---------------------------------------------------------------
    /// Path to TLS certificate file (overrides --config).
    #[arg(long)]
    pub tls_cert_file: Option<PathBuf>,

    /// Path to TLS private key file (overrides --config).
    #[arg(long)]
    pub tls_private_key_file: Option<PathBuf>,

    // -- Managers ---------------------------------------------------------
    /// CPU manager policy: "none" or "static" (overrides --config).
    #[arg(long)]
    pub cpu_manager_policy: Option<String>,

    /// Topology manager policy: "none", "best-effort", "restricted",
    /// "single-numa-node" (overrides --config).
    #[arg(long)]
    pub topology_manager_policy: Option<String>,

    // -- Cgroup ------------------------------------------------------------
    /// cgroup driver: "cgroupfs" or "systemd" (overrides --config).
    #[arg(long)]
    pub cgroup_driver: Option<String>,

    // -- Logging -----------------------------------------------------------
    /// Log verbosity level 0-10 (overrides --config logging.verbosity).
    /// Can be specified multiple times; last value wins.
    #[arg(short, long, overrides_with = "v")]
    pub v: Option<u8>,

    // -- Compatibility shims (accepted but unused) -------------------------
    //
    // kubeadm and upstream e2e runners inject these flags via
    // /var/lib/kubelet/kubeadm-flags.env and the 10-kubeadm.conf drop-in.
    // They must be accepted so clap does not reject the invocation with
    // "unexpected argument".  Their values are applied via --config or
    // have been superseded in recent Kubernetes versions.
    /// Node IP(s) (kubeadm sets this on multi-homed hosts; accepted,
    /// ignored — node IP is detected automatically from the default route).
    #[arg(long, hide = true)]
    pub node_ip: Option<String>,

    /// Resolv.conf path (superseded by `resolvConf` in KubeletConfiguration).
    #[arg(long, hide = true)]
    pub resolv_conf: Option<PathBuf>,

    /// Feature gates (handled via KubeletConfiguration.featureGates).
    #[arg(long, hide = true)]
    pub feature_gates: Option<String>,

    /// Cloud provider name (deprecated in K8s 1.29+; accepted for compat).
    #[arg(long, hide = true)]
    pub cloud_provider: Option<String>,

    /// CNI binary directory (accepted for compat; CNI is auto-detected).
    #[arg(long, hide = true)]
    pub cni_bin_dir: Option<PathBuf>,

    /// CNI config directory (accepted for compat; CNI is auto-detected).
    #[arg(long, hide = true)]
    pub cni_conf_dir: Option<PathBuf>,

    /// Allowed unsafe sysctls pattern (accepted for compat).
    #[arg(long, hide = true)]
    pub allowed_unsafe_sysctls: Option<String>,

    /// Node labels applied at registration (kubeadm sets these; accepted).
    #[arg(long, hide = true)]
    pub node_labels: Option<String>,

    /// Eviction pressure transition period (overridden by --config).
    #[arg(long, hide = true)]
    pub eviction_pressure_transition_period: Option<String>,

    /// Image pull progress deadline (accepted for compat).
    #[arg(long, hide = true)]
    pub image_pull_progress_deadline: Option<String>,

    /// Healthz bind address (accepted for compat; we always use 127.0.0.1:10248).
    #[arg(long, hide = true)]
    pub healthz_bind_address: Option<String>,

    /// Healthz port (accepted for compat; we always use 10248).
    #[arg(long, hide = true)]
    pub healthz_port: Option<u16>,

    /// Volume plugin directory (accepted for compat).
    #[arg(long, hide = true)]
    pub volume_plugin_dir: Option<PathBuf>,

    /// Protect kernel defaults (accepted for compat).
    #[arg(long, hide = true)]
    pub protect_kernel_defaults: Option<bool>,

    /// Rotate server certificates (accepted for compat; handled via --config).
    #[arg(long, hide = true)]
    pub rotate_server_certificates: Option<bool>,
}

impl KubeletArgs {
    /// Resolve arguments into a `KubeletConfig`.
    ///
    /// Order of precedence (highest -> lowest):
    ///   1. Explicit CLI flags
    ///   2. `--config` YAML file fields
    ///   3. Built-in defaults (`KubeletConfig::default()`)
    pub fn into_config(self) -> anyhow::Result<KubeletConfig> {
        // Start with defaults.
        let mut config = KubeletConfig::default();

        // Layer 1: load --config file if provided.
        if let Some(config_path) = &self.config {
            let kube_cfg = KubeletConfiguration::from_file(config_path)
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            config = kube_cfg
                .into_kubelet_config()
                .map_err(|e| anyhow::anyhow!("{}", e))?;
        }

        // kubeadm writes `0s` for duration fields it expects the kubelet to
        // default-fill.  Replace any zero durations with built-in defaults now,
        // before CLI overrides, so the validator never sees zeros.
        let defaults = KubeletConfig::default();
        if config.sync_frequency.is_zero() {
            config.sync_frequency = defaults.sync_frequency;
        }
        if config.node_status_update_frequency.is_zero() {
            config.node_status_update_frequency = defaults.node_status_update_frequency;
        }
        if config.file_check_frequency.is_zero() {
            config.file_check_frequency = defaults.file_check_frequency;
        }
        if config.http_check_frequency.is_zero() {
            config.http_check_frequency = defaults.http_check_frequency;
        }

        // Layer 2: apply explicit CLI flag overrides.
        if let Some(name) = self.node_name {
            config.node_name = name;
        }
        if config.node_name.is_empty() {
            config.node_name = hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "localhost".to_string());
        }

        if let Some(url) = self.api_server {
            config.api_server_url = url;
        }
        if let Some(kc) = self.kubeconfig {
            config.kubeconfig_path = Some(kc);
        }
        if let Some(cre) = self.container_runtime_endpoint {
            config.container_runtime_endpoint = cre;
        }
        if let Some(pici) = self.pod_infra_container_image {
            config.pod_infra_container_image = pici;
        }
        if let Some(root) = self.root_dir {
            config.root_dir = root;
        }
        if let Some(mp) = self.max_pods {
            config.max_pods = mp;
        }
        if let Some(path) = self.pod_manifest_path {
            config.static_pod_path = Some(path);
        }
        if let Some(dns) = self.cluster_dns {
            config.cluster_dns = dns.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Some(domain) = self.cluster_domain {
            config.cluster_domain = domain;
        }
        if let Some(addr) = self.address {
            config.address = addr;
        }
        if let Some(port) = self.port {
            config.port = port;
        }
        if let Some(hostname) = self.hostname_override {
            config.node_name = hostname;
        }
        if let Some(cert) = self.tls_cert_file {
            config.tls_cert_file = Some(cert);
        }
        if let Some(key) = self.tls_private_key_file {
            config.tls_private_key_file = Some(key);
        }
        if let Some(policy) = self.cpu_manager_policy {
            config.cpu_manager_policy = policy;
        }
        if let Some(policy) = self.topology_manager_policy {
            config.topology_manager_policy = policy;
        }
        if let Some(driver) = self.cgroup_driver {
            config.cgroup_driver = driver;
        }
        if let Some(v) = self.v {
            config.log_level = v;
        }

        config
            .validate()
            .map_err(|errs| anyhow::anyhow!("Invalid configuration: {}", errs.join(", ")))?;

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args_defaults_produce_valid_config() {
        let args = KubeletArgs {
            config: None,
            node_name: Some("test-node".to_string()),
            api_server: Some("https://localhost:6443".to_string()),
            kubeconfig: None,
            container_runtime_endpoint: None,
            pod_infra_container_image: None,
            root_dir: None,
            max_pods: None,
            pod_manifest_path: None,
            cluster_dns: None,
            cluster_domain: None,
            address: None,
            port: None,
            hostname_override: None,
            config_dir: None,
            bootstrap_kubeconfig: None,
            tls_cert_file: None,
            tls_private_key_file: None,
            cpu_manager_policy: None,
            topology_manager_policy: None,
            cgroup_driver: None,
            v: None,
            node_ip: None,
            resolv_conf: None,
            feature_gates: None,
            cloud_provider: None,
            cni_bin_dir: None,
            cni_conf_dir: None,
            allowed_unsafe_sysctls: None,
            node_labels: None,
            eviction_pressure_transition_period: None,
            image_pull_progress_deadline: None,
            healthz_bind_address: None,
            healthz_port: None,
            volume_plugin_dir: None,
            protect_kernel_defaults: None,
            rotate_server_certificates: None,
        };
        let config = args.into_config().unwrap();
        assert_eq!(config.node_name, "test-node");
        assert_eq!(config.max_pods, 110);
        assert_eq!(config.port, 10250);
    }

    #[test]
    fn test_cli_flags_override_defaults() {
        let args = KubeletArgs {
            config: None,
            node_name: Some("my-node".to_string()),
            api_server: Some("https://localhost:6443".to_string()),
            kubeconfig: None,
            container_runtime_endpoint: Some("unix:///run/crio/crio.sock".to_string()),
            pod_infra_container_image: None,
            root_dir: None,
            max_pods: Some(200),
            pod_manifest_path: None,
            cluster_dns: Some("10.0.0.10,10.0.0.11".to_string()),
            cluster_domain: Some("prod.internal".to_string()),
            address: Some("127.0.0.1".to_string()),
            port: Some(10251),
            hostname_override: Some("override-node".to_string()),
            config_dir: None,
            bootstrap_kubeconfig: None,
            tls_cert_file: None,
            tls_private_key_file: None,
            cpu_manager_policy: Some("static".to_string()),
            topology_manager_policy: Some("best-effort".to_string()),
            cgroup_driver: Some("systemd".to_string()),
            v: Some(4),
            node_ip: None,
            resolv_conf: None,
            feature_gates: None,
            cloud_provider: None,
            cni_bin_dir: None,
            cni_conf_dir: None,
            allowed_unsafe_sysctls: None,
            node_labels: None,
            eviction_pressure_transition_period: None,
            image_pull_progress_deadline: None,
            healthz_bind_address: None,
            healthz_port: None,
            volume_plugin_dir: None,
            protect_kernel_defaults: None,
            rotate_server_certificates: None,
        };
        let config = args.into_config().unwrap();
        assert_eq!(config.max_pods, 200);
        assert_eq!(config.port, 10251);
        assert_eq!(config.address, "127.0.0.1");
        assert_eq!(config.node_name, "override-node");
        assert_eq!(config.cluster_domain, "prod.internal");
        assert_eq!(config.cluster_dns, vec!["10.0.0.10", "10.0.0.11"]);
        assert_eq!(
            config.container_runtime_endpoint,
            "unix:///run/crio/crio.sock"
        );
        assert_eq!(config.cpu_manager_policy, "static");
        assert_eq!(config.cgroup_driver, "systemd");
        assert_eq!(config.log_level, 4);
    }
}
