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

//! CRI sandbox configuration builder.
//!
//! Translates a `PodSpec` into a complete `CreateSandboxConfig` that is passed
//! to the container runtime's `run_pod_sandbox()` call.
//!
//! Handles:
//!   - DNS configuration (clusterDNS, clusterDomain, dnsPolicy, dnsConfig overrides)
//!   - Host namespaces (hostNetwork, hostPID, hostIPC)
//!   - Pod-level sysctls
//!   - Port mappings
//!   - cgroup parent path
//!   - RuntimeClass handler
//!   - Security context (seccomp, apparmor, runAsUser, supplementalGroups)
//!
//! Mirrors pkg/kubelet/kuberuntime/kuberuntime_sandbox.go.

use kubelet_core::pod::{DnsPolicy, PodSpec};
use kubelet_ports::driven::container_runtime::{
    CreateSandboxConfig, DnsConfigSpec, PortMappingSpec,
};
use std::collections::HashMap;
use tracing::debug;

/// Node-level DNS settings (from KubeletConfig).
#[derive(Debug, Clone)]
pub struct NodeDnsConfig {
    pub cluster_dns: Vec<String>,
    pub cluster_domain: String,
    pub resolv_conf_path: String, // typically /etc/resolv.conf
}

impl Default for NodeDnsConfig {
    fn default() -> Self {
        Self {
            cluster_dns: vec!["10.96.0.10".to_string()],
            cluster_domain: "cluster.local".to_string(),
            resolv_conf_path: "/etc/resolv.conf".to_string(),
        }
    }
}

/// Build a full `CreateSandboxConfig` from a `PodSpec`.
pub fn build_sandbox_config(
    pod: &PodSpec,
    node_dns: &NodeDnsConfig,
    runtime_handler: &str,
    log_dir: &str,
    pod_infra_container_image: &str,
) -> CreateSandboxConfig {
    let dns = build_dns_config(pod, node_dns);
    let port_mappings = build_port_mappings(pod);
    let cgroup_parent = build_cgroup_parent(pod);
    let sysctls = build_sysctls(pod);

    let mut labels = pod.labels.clone();
    labels.insert(
        "io.kubernetes.pod.name".to_string(),
        pod.pod_ref.name.clone(),
    );
    labels.insert(
        "io.kubernetes.pod.namespace".to_string(),
        pod.pod_ref.namespace.clone(),
    );
    labels.insert("io.kubernetes.pod.uid".to_string(), pod.uid.0.clone());

    let mut annotations = pod.annotations.clone();

    // Encode seccomp profile for legacy annotation support.
    if let Some(sc) = &pod.security_context {
        if let Some(seccomp) = &sc.seccomp_profile {
            let profile_str = match seccomp.type_.as_str() {
                "RuntimeDefault" => "runtime/default".to_string(),
                "Unconfined" => "unconfined".to_string(),
                "Localhost" => format!(
                    "localhost/{}",
                    seccomp.localhost_profile.as_deref().unwrap_or("")
                ),
                _ => "runtime/default".to_string(),
            };
            annotations.insert(
                "seccomp.security.alpha.kubernetes.io/pod".to_string(),
                profile_str,
            );
        }
    }

    CreateSandboxConfig {
        pod_name: pod.pod_ref.name.clone(),
        pod_uid: pod.uid.0.clone(),
        pod_namespace: pod.pod_ref.namespace.clone(),
        // When hostNetwork is true the pod shares the host's UTS namespace, so
        // the hostname cannot be changed from the node's hostname.  Leave it
        // empty so runc does not attempt a sethostname(2) in a shared namespace.
        // Some runtimes reject setting a hostname when sandbox creation does not
        // use a private UTS namespace (common for hostNetwork control-plane pods).
        // Leave hostname empty in that case to avoid sandbox create failures.
        hostname: if pod.host_network {
            String::new()
        } else {
            pod.effective_hostname()
        },
        log_directory: format!(
            "{}/{}_{}_{}",
            log_dir, pod.pod_ref.namespace, pod.pod_ref.name, pod.uid.0
        ),
        dns_config: Some(dns),
        port_mappings,
        labels,
        annotations,
        linux_cgroup_parent: cgroup_parent,
        sysctls,
        // Host namespaces.
        host_network: pod.host_network,
        host_pid: pod.host_pid,
        host_ipc: pod.host_ipc,
        runtime_handler: runtime_handler.to_string(),
        sandbox_image: pod_infra_container_image.to_string(),
        supplemental_groups: build_supplemental_groups(pod),
        // Any privileged container requires the sandbox itself to be privileged.
        privileged: pod
            .containers
            .iter()
            .chain(pod.init_containers.iter())
            .any(|c| {
                c.security_context
                    .as_ref()
                    .and_then(|sc| sc.privileged)
                    .unwrap_or(false)
            }),
        share_process_namespace: pod.share_process_namespace.unwrap_or(false),
    }
}

/// Resolve DNS config for a pod based on its dnsPolicy.
pub fn build_dns_config(pod: &PodSpec, node_dns: &NodeDnsConfig) -> DnsConfigSpec {
    let effective_policy = pod
        .dns_config
        .as_ref()
        .map(|d| &d.policy)
        .unwrap_or(&DnsPolicy::ClusterFirst);
    let (mut nameservers, mut searches, mut options) = match effective_policy {
        DnsPolicy::ClusterFirst | DnsPolicy::ClusterFirstWithHostNet => {
            let searches = vec![
                format!("{}.svc.{}", pod.pod_ref.namespace, node_dns.cluster_domain),
                format!("svc.{}", node_dns.cluster_domain),
                node_dns.cluster_domain.clone(),
            ];
            (
                node_dns.cluster_dns.clone(),
                searches,
                vec!["ndots:5".to_string()],
            )
        }
        DnsPolicy::Default => {
            // Read from node's /etc/resolv.conf.
            let (ns, srch, opts) = parse_resolv_conf(&node_dns.resolv_conf_path);
            (ns, srch, opts)
        }
        DnsPolicy::None => (vec![], vec![], vec![]),
    };

    // Apply pod-level overrides.
    if let Some(pod_dns) = &pod.dns_config {
        if !pod_dns.nameservers.is_empty() {
            nameservers = pod_dns.nameservers.clone();
        }
        if !pod_dns.searches.is_empty() {
            searches = pod_dns.searches.clone();
        }
        for opt in &pod_dns.options {
            let opt_str = if let Some(v) = &opt.value {
                format!("{}:{}", opt.name, v)
            } else {
                opt.name.clone()
            };
            options.push(opt_str);
        }
    }

    // Max 3 nameservers, 6 search domains (Linux limits).
    nameservers.truncate(3);
    searches.truncate(6);

    DnsConfigSpec {
        servers: nameservers,
        searches,
        options,
    }
}

fn parse_resolv_conf(path: &str) -> (Vec<String>, Vec<String>, Vec<String>) {
    let (ns, srch, opts) = parse_resolv_conf_raw(path);
    // On systemd-resolved nodes /etc/resolv.conf contains only the local stub
    // listener (127.0.0.53 or similar 127.x).  Using that address inside a
    // container would loop back through CoreDNS.  Fall back to the
    // systemd-resolved upstream file which lists the real DNS servers.
    let stub_only = !ns.is_empty() && ns.iter().all(|s| s.starts_with("127."));
    if stub_only {
        const UPSTREAM: &str = "/run/systemd/resolve/resolv.conf";
        let (ns2, srch2, opts2) = parse_resolv_conf_raw(UPSTREAM);
        if !ns2.is_empty() {
            return (ns2, srch2, opts2);
        }
    }
    (ns, srch, opts)
}

fn parse_resolv_conf_raw(path: &str) -> (Vec<String>, Vec<String>, Vec<String>) {
    let content = std::fs::read_to_string(path).unwrap_or_default();
    let mut nameservers = vec![];
    let mut searches = vec![];
    let mut options = vec![];
    for line in content.lines() {
        let line = line.trim();
        if line.starts_with("nameserver ") {
            if let Some(ns) = line.split_whitespace().nth(1) {
                nameservers.push(ns.to_string());
            }
        } else if line.starts_with("search ") {
            searches.extend(line.split_whitespace().skip(1).map(|s| s.to_string()));
        } else if line.starts_with("options ") {
            options.extend(line.split_whitespace().skip(1).map(|s| s.to_string()));
        }
    }
    (nameservers, searches, options)
}

fn build_port_mappings(pod: &PodSpec) -> Vec<PortMappingSpec> {
    let mut mappings = vec![];
    for ctr in pod.containers.iter().chain(pod.init_containers.iter()) {
        for port in &ctr.ports {
            if port.host_port.is_some() {
                mappings.push(PortMappingSpec {
                    protocol: format!("{:?}", port.protocol),
                    container_port: port.container_port,
                    host_port: port.host_port,
                    host_ip: port.host_ip.clone(),
                });
            }
        }
    }
    mappings
}

fn build_cgroup_parent(pod: &PodSpec) -> String {
    use kubelet_core::qos::compute_qos_class;
    let qos = compute_qos_class(pod);
    let qos_str = match qos {
        kubelet_core::qos::QosClass::Guaranteed => "guaranteed",
        kubelet_core::qos::QosClass::Burstable => "burstable",
        kubelet_core::qos::QosClass::BestEffort => "besteffort",
    };
    format!("/kubepods/{}/pod{}", qos_str, pod.uid.0)
}

fn build_sysctls(pod: &PodSpec) -> HashMap<String, String> {
    pod.security_context
        .as_ref()
        .map(|sc| {
            sc.sysctls
                .iter()
                .map(|s| (s.name.clone(), s.value.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// Build the supplemental groups list for the pod sandbox.
///
/// Includes fsGroup (if set) followed by any explicit supplementalGroups.
/// The runtime uses these to set the GID list for all containers in the pod.
fn build_supplemental_groups(pod: &PodSpec) -> Vec<i64> {
    let Some(sc) = &pod.security_context else {
        return vec![];
    };
    let mut groups: Vec<i64> = Vec::new();
    if let Some(gid) = sc.fs_group {
        groups.push(gid as i64);
    }
    for g in &sc.supplemental_groups {
        let g = *g as i64;
        if !groups.contains(&g) {
            groups.push(g);
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::pod::{DnsConfig, DnsOption, DnsPolicy, RestartPolicy};
    use kubelet_core::types::{PodRef, PodUID};

    fn basic_pod(name: &str) -> PodSpec {
        PodSpec {
            uid: PodUID::new(format!("uid-{}", name)),
            pod_ref: PodRef {
                name: name.to_string(),
                namespace: "default".to_string(),
            },
            containers: vec![],
            init_containers: vec![],
            ephemeral_containers: vec![],
            volumes: vec![],
            node_name: "node1".to_string(),
            host_network: false,
            host_pid: false,
            host_ipc: false,
            dns_config: None,
            restart_policy: RestartPolicy::Always,
            termination_grace_period_seconds: 30,
            service_account_name: "default".to_string(),
            priority: None,
            tolerations: vec![],
            node_selector: Default::default(),
            annotations: Default::default(),
            labels: Default::default(),
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

    #[test]
    fn test_cluster_first_dns_has_cluster_dns() {
        let pod = basic_pod("nginx");
        let node_dns = NodeDnsConfig::default();
        let cfg = build_sandbox_config(
            &pod,
            &node_dns,
            "runc",
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
        );
        let dns = cfg.dns_config.unwrap();
        assert!(dns.servers.contains(&"10.96.0.10".to_string()));
        assert!(dns.searches.iter().any(|s| s.contains("default.svc")));
    }

    #[test]
    fn test_dns_none_policy_empty() {
        let mut pod = basic_pod("nginx");
        pod.dns_config = Some(DnsConfig {
            nameservers: vec![],
            searches: vec![],
            options: vec![],
            policy: DnsPolicy::None,
        });
        let node_dns = NodeDnsConfig::default();
        let cfg = build_sandbox_config(
            &pod,
            &node_dns,
            "runc",
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
        );
        let dns = cfg.dns_config.unwrap();
        assert!(dns.servers.is_empty());
    }

    #[test]
    fn test_dns_override_nameservers() {
        let mut pod = basic_pod("nginx");
        pod.dns_config = Some(DnsConfig {
            nameservers: vec!["8.8.8.8".to_string()],
            searches: vec![],
            options: vec![],
            policy: DnsPolicy::ClusterFirst,
        });
        let node_dns = NodeDnsConfig::default();
        let cfg = build_sandbox_config(
            &pod,
            &node_dns,
            "runc",
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
        );
        let dns = cfg.dns_config.unwrap();
        assert_eq!(dns.servers, vec!["8.8.8.8"]);
    }

    #[test]
    fn test_sandbox_labels_include_pod_metadata() {
        let pod = basic_pod("my-pod");
        let node_dns = NodeDnsConfig::default();
        let cfg = build_sandbox_config(
            &pod,
            &node_dns,
            "runc",
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
        );
        assert_eq!(cfg.labels.get("io.kubernetes.pod.name").unwrap(), "my-pod");
    }

    #[test]
    fn test_log_directory_includes_namespace_and_uid() {
        let pod = basic_pod("my-pod");
        let node_dns = NodeDnsConfig::default();
        let cfg = build_sandbox_config(
            &pod,
            &node_dns,
            "runc",
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
        );
        assert!(cfg.log_directory.contains("default"));
        assert!(cfg.log_directory.contains("my-pod"));
    }

    // ── DNS policy routing ────────────────────────────────────────────────────
    //
    // Regression: pods with dnsPolicy:Default (e.g. CoreDNS) must never
    // receive 10.96.0.10 as a nameserver. That would cause CoreDNS to forward
    // DNS queries to itself, triggering the loop plugin FATAL crash-loop.

    #[test]
    fn test_dns_default_policy_does_not_use_cluster_dns() {
        // DnsPolicy::Default must use the node-level resolv.conf, not
        // cluster_dns (10.96.0.10). Verify 10.96.0.10 is absent.
        let mut pod = basic_pod("coredns");
        pod.dns_config = Some(DnsConfig {
            nameservers: vec![],
            searches: vec![],
            options: vec![],
            policy: DnsPolicy::Default,
        });
        let node_dns = NodeDnsConfig {
            cluster_dns: vec!["10.96.0.10".to_string()],
            cluster_domain: "cluster.local".to_string(),
            resolv_conf_path: "/etc/resolv.conf".to_string(),
        };
        let cfg = build_sandbox_config(
            &pod,
            &node_dns,
            "runc",
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
        );
        let dns = cfg.dns_config.unwrap();
        assert!(
            !dns.servers.contains(&"10.96.0.10".to_string()),
            "DnsPolicy::Default must never inject cluster DNS (10.96.0.10), \
             got servers: {:?}",
            dns.servers
        );
    }

    #[test]
    fn test_dns_default_policy_ndots5_not_added() {
        // ClusterFirst adds ndots:5 which amplifies DNS queries 5x.
        // Default policy must not add ndots:5 unless the node resolv.conf has it.
        let mut pod = basic_pod("coredns");
        pod.dns_config = Some(DnsConfig {
            nameservers: vec![],
            searches: vec![],
            options: vec![],
            policy: DnsPolicy::Default,
        });
        let node_dns = NodeDnsConfig {
            cluster_dns: vec!["10.96.0.10".to_string()],
            cluster_domain: "cluster.local".to_string(),
            // Non-existent path → empty result, no ndots:5.
            resolv_conf_path: "/dev/null".to_string(),
        };
        let cfg = build_sandbox_config(
            &pod,
            &node_dns,
            "runc",
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
        );
        let dns = cfg.dns_config.unwrap();
        assert!(
            !dns.options.contains(&"ndots:5".to_string()),
            "DnsPolicy::Default must not add ndots:5, got options: {:?}",
            dns.options
        );
    }

    #[test]
    fn test_dns_cluster_first_with_host_net_uses_cluster_dns() {
        let mut pod = basic_pod("hostnet-pod");
        pod.dns_config = Some(DnsConfig {
            nameservers: vec![],
            searches: vec![],
            options: vec![],
            policy: DnsPolicy::ClusterFirstWithHostNet,
        });
        let node_dns = NodeDnsConfig::default();
        let cfg = build_sandbox_config(
            &pod,
            &node_dns,
            "runc",
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
        );
        let dns = cfg.dns_config.unwrap();
        assert!(
            dns.servers.contains(&"10.96.0.10".to_string()),
            "ClusterFirstWithHostNet must use cluster DNS"
        );
        assert!(dns.searches.iter().any(|s| s.contains("svc.cluster.local")));
    }
}
