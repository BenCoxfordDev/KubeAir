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

//! CRI container configuration builder.
//!
//! Translates a `ContainerSpec` + pod context into a complete CRI
//! `CreateContainerConfig` including security context, env vars, mounts,
//! resource limits, and seccomp/AppArmor profiles.
//!
//! Mirrors pkg/kubelet/kuberuntime/kuberuntime_container.go.

use kubelet_core::pod::{
    AppArmorSpec, ContainerSpec, LifecycleHandler, PodSecurityContext, PodSpec, SeccompSpec,
    SecurityContext,
};
use kubelet_ports::driven::container_runtime::{CreateContainerConfig, LinuxContainerSecurity};
use std::collections::HashMap;
use tracing::debug;

/// Env vars resolved from ConfigMaps, Secrets, and DownwardAPI.
pub struct ResolvedEnv {
    pub vars: Vec<(String, String)>,
}

impl Default for ResolvedEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl ResolvedEnv {
    pub fn new() -> Self {
        Self { vars: vec![] }
    }

    pub fn add(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.vars.push((key.into(), value.into()));
    }
}

/// Build a complete `CreateContainerConfig` from a container spec.
pub fn build_container_config(
    pod: &PodSpec,
    container: &ContainerSpec,
    sandbox_id: &str,
    resolved_env: &ResolvedEnv,
    log_dir: &str,
) -> CreateContainerConfig {
    // Merge pod-level + container-level security context.
    let security = build_linux_security(
        pod.security_context.as_ref(),
        container.security_context.as_ref(),
    );

    // Env: static + resolved (ConfigMap/Secret/DownwardAPI).
    let mut env: Vec<(String, String)> = container
        .env
        .iter()
        .filter_map(|e| e.value.as_ref().map(|v| (e.name.clone(), v.clone())))
        .collect();
    env.extend(resolved_env.vars.iter().cloned());

    CreateContainerConfig {
        pod_name: pod.pod_ref.name.clone(),
        pod_uid: pod.uid.0.clone(),
        pod_namespace: pod.pod_ref.namespace.clone(),
        attempt: 0,
        sandbox_id: sandbox_id.to_string(),
        container: container.clone(),
        log_directory: format!(
            "{}/{}_{}_{}/{}/",
            log_dir, pod.pod_ref.namespace, pod.pod_ref.name, pod.uid.0, container.name
        ),
        security,
        extra_env: env,
        image_id: String::new(),
        env_overrides: std::collections::HashMap::new(),
        linux_cgroup_parent: String::new(),
        extra_devices: vec![],
        extra_mounts: vec![],
        extra_device_envs: vec![],
        share_process_namespace: pod.share_process_namespace.unwrap_or(false),
        pod_hostname: pod.effective_hostname(),
    }
}

fn build_linux_security(
    pod_sc: Option<&PodSecurityContext>,
    ctr_sc: Option<&SecurityContext>,
) -> LinuxContainerSecurity {
    let run_as_user = ctr_sc
        .and_then(|s| s.run_as_user)
        .or_else(|| pod_sc.and_then(|s| s.run_as_user));

    let run_as_group = ctr_sc
        .and_then(|s| s.run_as_group)
        .or_else(|| pod_sc.and_then(|s| s.run_as_group));

    let supplemental_groups = pod_sc
        .map(|s| s.supplemental_groups.clone())
        .unwrap_or_default();

    let privileged = ctr_sc.and_then(|s| s.privileged).unwrap_or(false);
    let read_only_root = ctr_sc
        .and_then(|s| s.read_only_root_filesystem)
        .unwrap_or(false);
    let allow_priv_esc = ctr_sc.and_then(|s| s.allow_privilege_escalation);

    let caps_add = ctr_sc
        .and_then(|s| s.capabilities.as_ref())
        .map(|c| c.add.clone())
        .unwrap_or_default();
    let caps_drop = ctr_sc
        .and_then(|s| s.capabilities.as_ref())
        .map(|c| c.drop.clone())
        .unwrap_or_default();

    // Seccomp: container-level takes precedence over pod-level.
    let seccomp = ctr_sc
        .and_then(|s| s.seccomp_profile.as_ref())
        .or_else(|| pod_sc.and_then(|s| s.seccomp_profile.as_ref()))
        .cloned();

    let apparmor = ctr_sc.and_then(|s| s.apparmor_profile.as_ref()).cloned();

    LinuxContainerSecurity {
        run_as_user,
        run_as_group,
        supplemental_groups,
        privileged,
        read_only_root_filesystem: read_only_root,
        allow_privilege_escalation: allow_priv_esc,
        capabilities_add: caps_add,
        capabilities_drop: caps_drop,
        seccomp_profile_type: seccomp.as_ref().map(|s| s.type_.clone()),
        seccomp_localhost_path: seccomp.as_ref().and_then(|s| s.localhost_profile.clone()),
        apparmor_profile: apparmor.map(|a| match a.type_.as_str() {
            "Unconfined" => "unconfined".to_string(),
            "Localhost" => format!("localhost/{}", a.localhost_profile.as_deref().unwrap_or("")),
            _ => "runtime/default".to_string(),
        }),
    }
}

/// Resolve env vars from DownwardAPI field references.
pub fn resolve_downward_api_env(pod: &PodSpec, container: &ContainerSpec) -> Vec<(String, String)> {
    let mut resolved = vec![];
    for env in &container.env {
        if let Some(kubelet_core::pod::EnvVarSource::FieldRef { field_path }) = &env.value_from {
            let value = match field_path.as_str() {
                "metadata.name" => pod.pod_ref.name.clone(),
                "metadata.namespace" => pod.pod_ref.namespace.clone(),
                "metadata.uid" => pod.uid.0.clone(),
                "spec.nodeName" => pod.node_name.clone(),
                "spec.serviceAccountName" => pod.service_account_name.clone(),
                // status.hostIP is the node's outbound IP, always available.
                "status.hostIP" => detect_node_ip_for_downward_api(),
                // status.podIP: for host-network pods, pod IP equals host IP.
                // For regular pods it is filled in later (after sandbox creation).
                "status.podIP" => {
                    if pod.host_network {
                        detect_node_ip_for_downward_api()
                    } else {
                        String::new()
                    }
                }
                _ => {
                    // Check labels/annotations: "metadata.labels['key']", "metadata.annotations['key']"
                    if let Some(key) = field_path
                        .strip_prefix("metadata.labels['")
                        .and_then(|s| s.strip_suffix("']"))
                    {
                        pod.labels.get(key).cloned().unwrap_or_default()
                    } else if let Some(key) = field_path
                        .strip_prefix("metadata.annotations['")
                        .and_then(|s| s.strip_suffix("']"))
                    {
                        pod.annotations.get(key).cloned().unwrap_or_default()
                    } else {
                        String::new()
                    }
                }
            };
            resolved.push((env.name.clone(), value));
        } else if let Some(kubelet_core::pod::EnvVarSource::ResourceFieldRef { resource, .. }) =
            &env.value_from
        {
            let value = match resource.as_str() {
                "limits.cpu" | "requests.cpu" => container
                    .resources
                    .requests
                    .get("cpu")
                    .map(|q| q.value.to_string())
                    .unwrap_or_else(|| "0".to_string()),
                "limits.memory" | "requests.memory" => container
                    .resources
                    .requests
                    .get("memory")
                    .map(|q| q.value.to_string())
                    .unwrap_or_else(|| "0".to_string()),
                _ => String::new(),
            };
            resolved.push((env.name.clone(), value));
        }
    }
    resolved
}

/// Resolve env vars from ConfigMap refs (passed as resolved data).
pub fn resolve_configmap_env(
    container: &ContainerSpec,
    configmaps: &HashMap<String, HashMap<String, String>>,
) -> Vec<(String, String)> {
    let mut resolved = vec![];

    // EnvFrom ConfigMap sources.
    for env_from in &container.env_from {
        if let Some(cm_ref) = &env_from.config_map_ref
            && let Some(cm_data) = configmaps.get(&cm_ref.name)
        {
            let prefix = env_from.prefix.as_deref().unwrap_or("");
            for (k, v) in cm_data {
                resolved.push((format!("{}{}", prefix, k), v.clone()));
            }
        }
    }

    // Individual ConfigMapKeyRef entries.
    for env in &container.env {
        if let Some(kubelet_core::pod::EnvVarSource::ConfigMapKeyRef {
            name,
            key,
            optional,
        }) = &env.value_from
            && let Some(cm_data) = configmaps.get(name)
        {
            if let Some(v) = cm_data.get(key) {
                resolved.push((env.name.clone(), v.clone()));
            } else if !optional {
                tracing::warn!(configmap = %name, key = %key, "Required ConfigMap key missing");
            }
        }
    }

    resolved
}

/// Resolve env vars from Secret refs.
pub fn resolve_secret_env(
    container: &ContainerSpec,
    secrets: &HashMap<String, HashMap<String, Vec<u8>>>,
) -> Vec<(String, String)> {
    let mut resolved = vec![];

    // EnvFrom Secret sources.
    for env_from in &container.env_from {
        if let Some(secret_ref) = &env_from.secret_ref
            && let Some(secret_data) = secrets.get(&secret_ref.name)
        {
            let prefix = env_from.prefix.as_deref().unwrap_or("");
            for (k, v) in secret_data {
                if let Ok(s) = String::from_utf8(v.clone()) {
                    resolved.push((format!("{}{}", prefix, k), s));
                }
            }
        }
    }

    // Individual SecretKeyRef entries.
    for env in &container.env {
        if let Some(kubelet_core::pod::EnvVarSource::SecretKeyRef {
            name,
            key,
            optional,
        }) = &env.value_from
            && let Some(secret_data) = secrets.get(name)
        {
            if let Some(v) = secret_data.get(key) {
                if let Ok(s) = String::from_utf8(v.clone()) {
                    resolved.push((env.name.clone(), s));
                }
            } else if !optional {
                tracing::warn!(secret = %name, key = %key, "Required Secret key missing");
            }
        }
    }

    resolved
}

/// Return the node's outbound IP address for Downward API `status.hostIP` resolution.
///
/// Uses a non-blocking UDP connect trick to find the local address the OS would
/// route to the internet, then falls back to `127.0.0.1` in sandboxed environments.
fn detect_node_ip_for_downward_api() -> String {
    if let Ok(sock) = std::net::UdpSocket::bind("0.0.0.0:0")
        && sock.connect("1.1.1.1:80").is_ok()
        && let Ok(addr) = sock.local_addr()
    {
        if let std::net::IpAddr::V4(v4) = addr.ip()
            && !v4.is_loopback()
        {
            return v4.to_string();
        }
        let s = addr.ip().to_string();
        if !s.starts_with("::1") && s != "::1" {
            return s;
        }
    }
    "127.0.0.1".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::pod::{
        ContainerSpec, EnvFromRef, EnvFromSource, EnvVar, EnvVarSource, ImagePullPolicy,
        RestartPolicy,
    };
    use kubelet_core::types::{PodRef, PodUID};

    fn base_pod() -> PodSpec {
        PodSpec {
            uid: PodUID::new("uid-test"),
            pod_ref: PodRef {
                name: "my-pod".to_string(),
                namespace: "prod".to_string(),
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
            service_account_name: "my-sa".to_string(),
            priority: None,
            tolerations: vec![],
            node_selector: Default::default(),
            annotations: [("env".to_string(), "production".to_string())]
                .into_iter()
                .collect(),
            labels: [("app".to_string(), "backend".to_string())]
                .into_iter()
                .collect(),
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

    fn base_container(name: &str) -> ContainerSpec {
        ContainerSpec {
            name: name.to_string(),
            image: "nginx:latest".to_string(),
            command: vec![],
            args: vec![],
            working_dir: None,
            ports: vec![],
            env: vec![],
            env_from: vec![],
            resources: Default::default(),
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

    #[test]
    fn test_resolve_downward_api_metadata_name() {
        let pod = base_pod();
        let mut ctr = base_container("app");
        ctr.env = vec![EnvVar {
            name: "POD_NAME".to_string(),
            value: None,
            value_from: Some(EnvVarSource::FieldRef {
                field_path: "metadata.name".to_string(),
            }),
        }];
        let resolved = resolve_downward_api_env(&pod, &ctr);
        assert_eq!(
            resolved,
            vec![("POD_NAME".to_string(), "my-pod".to_string())]
        );
    }

    #[test]
    fn test_resolve_downward_api_label() {
        let pod = base_pod();
        let mut ctr = base_container("app");
        ctr.env = vec![EnvVar {
            name: "APP_LABEL".to_string(),
            value: None,
            value_from: Some(EnvVarSource::FieldRef {
                field_path: "metadata.labels['app']".to_string(),
            }),
        }];
        let resolved = resolve_downward_api_env(&pod, &ctr);
        assert_eq!(resolved[0].1, "backend");
    }

    #[test]
    fn test_resolve_configmap_env_from() {
        let mut ctr = base_container("app");
        ctr.env_from = vec![EnvFromSource {
            prefix: Some("CM_".to_string()),
            config_map_ref: Some(EnvFromRef {
                name: "my-cm".to_string(),
                optional: false,
            }),
            secret_ref: None,
        }];
        let mut cms = HashMap::new();
        cms.insert(
            "my-cm".to_string(),
            [("KEY".to_string(), "VALUE".to_string())]
                .into_iter()
                .collect(),
        );
        let resolved = resolve_configmap_env(&ctr, &cms);
        assert_eq!(resolved, vec![("CM_KEY".to_string(), "VALUE".to_string())]);
    }

    #[test]
    fn test_resolve_secret_env_from() {
        let mut ctr = base_container("app");
        ctr.env_from = vec![EnvFromSource {
            prefix: None,
            config_map_ref: None,
            secret_ref: Some(EnvFromRef {
                name: "my-secret".to_string(),
                optional: false,
            }),
        }];
        let mut secrets = HashMap::new();
        secrets.insert(
            "my-secret".to_string(),
            [("PASSWORD".to_string(), b"s3cr3t".to_vec())]
                .into_iter()
                .collect(),
        );
        let resolved = resolve_secret_env(&ctr, &secrets);
        assert_eq!(
            resolved,
            vec![("PASSWORD".to_string(), "s3cr3t".to_string())]
        );
    }

    #[test]
    fn test_build_linux_security_merges_pod_and_container() {
        let pod_sc = PodSecurityContext {
            run_as_user: Some(1000),
            run_as_group: Some(3000),
            run_as_non_root: None,
            fs_group: Some(2000),
            supplemental_groups: vec![4000],
            sysctls: vec![],
            seccomp_profile: None,
            fs_group_change_policy: None,
        };
        let ctr_sc = SecurityContext {
            run_as_user: Some(2000), // overrides pod
            run_as_group: None,
            run_as_non_root: None,
            privileged: None,
            read_only_root_filesystem: Some(true),
            allow_privilege_escalation: Some(false),
            capabilities: None,
            seccomp_profile: None,
            apparmor_profile: None,
            proc_mount: None,
        };
        let sec = build_linux_security(Some(&pod_sc), Some(&ctr_sc));
        assert_eq!(sec.run_as_user, Some(2000)); // container wins
        assert_eq!(sec.run_as_group, Some(3000)); // falls back to pod
        assert_eq!(sec.supplemental_groups, vec![4000]);
        assert!(sec.read_only_root_filesystem);
    }
}
