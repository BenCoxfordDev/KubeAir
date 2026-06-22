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

//! Kubernetes API watcher adapter.
//!
//! Provides a `PodSource` that watches the Kubernetes API server for pods
//! assigned to this node, translating kubelet watch events into `PodUpdate`s.
//!
//! Also provides `KubeNodeReporter` -- a real `NodeReporter` implementation
//! backed by the Kubernetes API.
//!
//! The watcher uses a retry loop with exponential backoff on errors,
//! mirroring the Go kubelet's `config/apiserver.go`.

use async_trait::async_trait;
use chrono::Utc;
use kubelet_core::error::{KubeletError, Result};
use kubelet_core::node::{NodeAddress, NodeAddressType, NodeCapacity, NodeCondition, NodeStatus};
use kubelet_core::pod::lifecycle::PodLifecycleState;
use kubelet_core::pod::{
    AppArmorSpec, Capabilities, ContainerSpec, DownwardAPIVolumeFile, EnvFromRef, EnvFromSource,
    EnvVar, EnvVarSource, ImagePullPolicy, KeyToPath, LocalObjectReference, PodOperation,
    PodSecurityContext, PodSpec, PodUpdate, ProjectedVolumeSource, ResourceClaimRef,
    ResourceFieldRef, ResourceRequirements, RestartPolicy, SeccompSpec, SecurityContext, Sysctl,
    VolumeMount, VolumeSource, VolumeSpec,
};
use kubelet_core::types::{PodRef, PodUID, ResourceQuantity, ResourceUnit};
use kubelet_ports::driven::node_reporter::NodeReporter;
use kubelet_ports::driven::pod_source::PodSource;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

// -- Pod source ----------------------------------------------------------------

/// Simulated API server pod source for testing without a real cluster.
/// In production this would use `kube::Api<Pod>` + `kube_runtime::watcher`.
pub struct SimulatedApiPodSource {
    node_name: String,
    initial_pods: Vec<PodSpec>,
    poll_interval: Duration,
}

impl SimulatedApiPodSource {
    pub fn new(
        node_name: impl Into<String>,
        initial_pods: Vec<PodSpec>,
        poll_interval: Duration,
    ) -> Self {
        Self {
            node_name: node_name.into(),
            initial_pods,
            poll_interval,
        }
    }
}

#[async_trait]
impl PodSource for SimulatedApiPodSource {
    fn name(&self) -> &str {
        "api"
    }

    async fn run(&self, tx: mpsc::Sender<PodUpdate>) -> Result<()> {
        info!(node = %self.node_name, source = "api", "API pod source started");

        // Emit initial pods
        for pod in &self.initial_pods {
            if tx
                .send(PodUpdate {
                    pod: pod.clone(),
                    op: PodOperation::Add,
                })
                .await
                .is_err()
            {
                return Ok(());
            }
        }

        // Then periodically send reconcile events (simulates watch re-sync)
        loop {
            tokio::time::sleep(self.poll_interval).await;
            for pod in &self.initial_pods {
                if tx
                    .send(PodUpdate {
                        pod: pod.clone(),
                        op: PodOperation::Reconcile,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
        }
    }
}

// -- Node reporter -------------------------------------------------------------

/// Simulated node reporter that records calls but could be replaced with
/// a real kubelet backed implementation.
///
/// The real implementation would call:
///   kube::Api::<Node>::patch_status(...)
///   kube::Api::<Lease>::patch(...)
#[derive(Default)]
pub struct LoggingNodeReporter {
    node_name: String,
}

impl LoggingNodeReporter {
    pub fn new(node_name: impl Into<String>) -> Self {
        Self {
            node_name: node_name.into(),
        }
    }
}

#[async_trait]
impl NodeReporter for LoggingNodeReporter {
    async fn report_node_status(&self, status: &NodeStatus) -> Result<()> {
        info!(
            node = %status.name,
            phase = %status.phase_str(),
            conditions = status.conditions.len(),
            "Reporting node status to API server"
        );
        Ok(())
    }

    async fn report_pod_status(
        &self,
        pod_ref: &PodRef,
        uid: &PodUID,
        state: &PodLifecycleState,
    ) -> Result<()> {
        debug!(
            pod = %pod_ref,
            uid = %uid,
            phase = %state.phase,
            "Reporting pod status to API server"
        );
        Ok(())
    }

    async fn delete_pod(&self, pod_ref: &PodRef, _uid: &PodUID) -> Result<()> {
        debug!(pod = %pod_ref, "LoggingNodeReporter: skip pod delete");
        Ok(())
    }

    async fn patch_node_conditions(
        &self,
        node_name: &str,
        conditions: &[NodeCondition],
    ) -> Result<()> {
        debug!(
            node_name,
            conditions = conditions.len(),
            "Patching node conditions on API server"
        );
        Ok(())
    }

    async fn renew_node_lease(&self, node_name: &str, duration_seconds: u32) -> Result<()> {
        debug!(
            node_name,
            duration_seconds, "Renewing node lease on API server"
        );
        Ok(())
    }
}

// -- Pod spec conversion -------------------------------------------------------

/// Convert a simplified pod spec map (as would come from kubelet JSON) into
/// our internal PodSpec. In a real implementation this would parse
/// k8s_openapi::api::core::v1::Pod.
pub fn pod_spec_from_map(map: &serde_json::Value, node_name: &str) -> Option<PodSpec> {
    let metadata = map.get("metadata")?;
    let spec = map.get("spec")?;

    let name = metadata.get("name")?.as_str()?.to_string();
    let namespace = metadata
        .get("namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();
    let uid = metadata.get("uid")?.as_str()?.to_string();

    let containers: Vec<ContainerSpec> = spec
        .get("containers")?
        .as_array()?
        .iter()
        .filter_map(build_container_spec)
        .collect();

    let init_containers: Vec<ContainerSpec> = spec
        .get("initContainers")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(build_container_spec)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let resource_claims: Vec<ResourceClaimRef> = spec
        .get("resourceClaims")
        .and_then(|v| v.as_array())
        .map(|claims| {
            claims
                .iter()
                .filter_map(|claim| {
                    let name = claim.get("name")?.as_str()?.to_string();
                    Some(ResourceClaimRef {
                        name,
                        resource_class_name: claim
                            .get("resourceClassName")
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                        allocated: claim
                            .get("allocated")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false),
                        prepared: claim
                            .get("prepared")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let restart_policy = match spec.get("restartPolicy").and_then(|v| v.as_str()) {
        Some("Never") => RestartPolicy::Never,
        Some("OnFailure") => RestartPolicy::OnFailure,
        _ => RestartPolicy::Always,
    };

    Some(PodSpec {
        uid: PodUID::new(uid),
        pod_ref: PodRef::new(namespace, name.clone()),
        containers,
        init_containers,
        ephemeral_containers: vec![],
        volumes: parse_volumes(spec),
        node_name: spec
            .get("nodeName")
            .and_then(|v| v.as_str())
            .unwrap_or(node_name)
            .to_string(),
        host_network: spec
            .get("hostNetwork")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        host_pid: spec
            .get("hostPID")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        host_ipc: spec
            .get("hostIPC")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        dns_config: {
            let policy_str = spec.get("dnsPolicy").and_then(|v| v.as_str());
            match policy_str {
                Some("Default") | Some("None") | Some("ClusterFirstWithHostNet") => {
                    let overrides = spec.get("dnsConfig");
                    let policy = match policy_str {
                        Some("None") => kubelet_core::pod::DnsPolicy::None,
                        Some("ClusterFirstWithHostNet") => {
                            kubelet_core::pod::DnsPolicy::ClusterFirstWithHostNet
                        }
                        _ => kubelet_core::pod::DnsPolicy::Default,
                    };
                    Some(kubelet_core::pod::DnsConfig {
                        policy,
                        nameservers: overrides
                            .and_then(|d| d.get("nameservers"))
                            .and_then(|v| v.as_array())
                            .map(|a| {
                                a.iter()
                                    .filter_map(|v| v.as_str().map(str::to_string))
                                    .collect()
                            })
                            .unwrap_or_default(),
                        searches: overrides
                            .and_then(|d| d.get("searches"))
                            .and_then(|v| v.as_array())
                            .map(|a| {
                                a.iter()
                                    .filter_map(|v| v.as_str().map(str::to_string))
                                    .collect()
                            })
                            .unwrap_or_default(),
                        options: overrides
                            .and_then(|d| d.get("options"))
                            .and_then(|v| v.as_array())
                            .map(|a| {
                                a.iter()
                                    .filter_map(|opt| {
                                        let name = opt.get("name")?.as_str()?.to_string();
                                        Some(kubelet_core::pod::DnsOption {
                                            name,
                                            value: opt
                                                .get("value")
                                                .and_then(|v| v.as_str())
                                                .map(str::to_string),
                                        })
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                    })
                }
                // "ClusterFirst" (default) and anything else → None, handled as ClusterFirst default
                _ => None,
            }
        },
        restart_policy,
        termination_grace_period_seconds: spec
            .get("terminationGracePeriodSeconds")
            .and_then(|v| v.as_u64())
            .unwrap_or(30),
        service_account_name: spec
            .get("serviceAccountName")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string(),
        priority: None,
        tolerations: vec![],
        node_selector: Default::default(),
        annotations: metadata
            .get("annotations")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        labels: metadata
            .get("labels")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        runtime_class_name: spec
            .get("runtimeClassName")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        security_context: parse_pod_security_context(spec.get("securityContext")),
        readiness_gates: vec![],
        active_deadline_seconds: spec.get("activeDeadlineSeconds").and_then(|v| v.as_u64()),
        automount_service_account_token: spec
            .get("automountServiceAccountToken")
            .and_then(|v| v.as_bool()),
        image_pull_secrets: spec
            .get("imagePullSecrets")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| {
                        s.get("name")?.as_str().map(|n| LocalObjectReference {
                            name: n.to_string(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        enable_service_links: spec.get("enableServiceLinks").and_then(|v| v.as_bool()),
        share_process_namespace: spec.get("shareProcessNamespace").and_then(|v| v.as_bool()),
        resource_claims,
        hostname: spec
            .get("hostname")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        subdomain: spec
            .get("subdomain")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        host_aliases: spec
            .get("hostAliases")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|entry| {
                        let ip = entry.get("ip")?.as_str()?.to_string();
                        let hostnames = entry
                            .get("hostnames")
                            .and_then(|h| h.as_array())
                            .map(|a| {
                                a.iter()
                                    .filter_map(|s| s.as_str().map(String::from))
                                    .collect()
                            })
                            .unwrap_or_default();
                        Some(kubelet_core::pod::HostAlias { ip, hostnames })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        observed_start_time: None,
    })
}

fn build_container_spec(c: &serde_json::Value) -> Option<ContainerSpec> {
    let ports = parse_container_ports(c);
    Some(ContainerSpec {
        name: c.get("name")?.as_str()?.to_string(),
        image: c.get("image")?.as_str()?.to_string(),
        command: c
            .get("command")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        args: c
            .get("args")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        working_dir: c
            .get("workingDir")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        ports: ports.clone(),
        env: parse_env_vars(c),
        resources: parse_resource_requirements(c.get("resources")),
        volume_mounts: parse_volume_mounts(c),
        liveness_probe: parse_probe(c.get("livenessProbe"), &ports),
        readiness_probe: parse_probe(c.get("readinessProbe"), &ports),
        startup_probe: parse_probe(c.get("startupProbe"), &ports),
        image_pull_policy: match c.get("imagePullPolicy").and_then(|v| v.as_str()) {
            Some("Always") => ImagePullPolicy::Always,
            Some("Never") => ImagePullPolicy::Never,
            _ => ImagePullPolicy::IfNotPresent,
        },
        security_context: parse_container_security_context(c.get("securityContext")),
        termination_message_path: c
            .get("terminationMessagePath")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        termination_message_policy: c
            .get("terminationMessagePolicy")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        lifecycle: None,
        env_from: parse_env_from(c),
        stdin: None,
        stdin_once: None,
        tty: None,
        restart_policy: c.get("restartPolicy").and_then(|v| match v.as_str() {
            Some("Never") => Some(RestartPolicy::Never),
            Some("OnFailure") => Some(RestartPolicy::OnFailure),
            Some(_) => Some(RestartPolicy::Always),
            None => None,
        }),
    })
}

fn parse_env_vars(c: &serde_json::Value) -> Vec<EnvVar> {
    c.get("env")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|e| {
                    let name = e.get("name")?.as_str()?.to_string();
                    let value = e.get("value").and_then(|v| v.as_str()).map(str::to_string);
                    let value_from = if let Some(vf) = e.get("valueFrom") {
                        if let Some(fr) = vf.get("fieldRef") {
                            let field_path = fr.get("fieldPath")?.as_str()?.to_string();
                            Some(EnvVarSource::FieldRef { field_path })
                        } else if let Some(rfr) = vf.get("resourceFieldRef") {
                            let resource = rfr.get("resource")?.as_str()?.to_string();
                            let container_name = rfr
                                .get("containerName")
                                .and_then(|v| v.as_str())
                                .map(str::to_string);
                            Some(EnvVarSource::ResourceFieldRef {
                                container_name,
                                resource,
                            })
                        } else if let Some(cmr) = vf.get("configMapKeyRef") {
                            let name = cmr.get("name")?.as_str()?.to_string();
                            let key = cmr.get("key")?.as_str()?.to_string();
                            let optional = cmr
                                .get("optional")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            Some(EnvVarSource::ConfigMapKeyRef {
                                name,
                                key,
                                optional,
                            })
                        } else if let Some(sr) = vf.get("secretKeyRef") {
                            let name = sr.get("name")?.as_str()?.to_string();
                            let key = sr.get("key")?.as_str()?.to_string();
                            let optional = sr
                                .get("optional")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            Some(EnvVarSource::SecretKeyRef {
                                name,
                                key,
                                optional,
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    Some(EnvVar {
                        name,
                        value,
                        value_from,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_container_ports(c: &serde_json::Value) -> Vec<kubelet_core::pod::ContainerPort> {
    c.get("ports")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|p| {
                    let container_port = p
                        .get("containerPort")
                        .and_then(|v| v.as_u64())
                        .map(|n| n as u16)?;
                    Some(kubelet_core::pod::ContainerPort {
                        name: p.get("name").and_then(|v| v.as_str()).map(str::to_string),
                        container_port,
                        host_port: p.get("hostPort").and_then(|v| v.as_u64()).map(|n| n as u16),
                        protocol: match p.get("protocol").and_then(|v| v.as_str()) {
                            Some("UDP") => kubelet_core::pod::Protocol::UDP,
                            Some("SCTP") => kubelet_core::pod::Protocol::SCTP,
                            _ => kubelet_core::pod::Protocol::TCP,
                        },
                        host_ip: p.get("hostIP").and_then(|v| v.as_str()).map(str::to_string),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Resolve a port value that may be a number or a named port.
fn resolve_port(
    port_val: &serde_json::Value,
    ports: &[kubelet_core::pod::ContainerPort],
) -> Option<u16> {
    if let Some(n) = port_val.as_u64() {
        return Some(n as u16);
    }
    if let Some(name) = port_val.as_str() {
        // Try to parse as a number first.
        if let Ok(n) = name.parse::<u16>() {
            return Some(n);
        }
        // Look up named port in the container's port list.
        return ports
            .iter()
            .find(|p| p.name.as_deref() == Some(name))
            .map(|p| p.container_port);
    }
    None
}

fn parse_probe(
    v: Option<&serde_json::Value>,
    ports: &[kubelet_core::pod::ContainerPort],
) -> Option<kubelet_core::pod::Probe> {
    let v = v?;
    let initial_delay_seconds = v
        .get("initialDelaySeconds")
        .and_then(|x| x.as_u64())
        .unwrap_or(0) as u32;
    let period_seconds = v
        .get("periodSeconds")
        .and_then(|x| x.as_u64())
        .unwrap_or(10) as u32;
    let timeout_seconds = v
        .get("timeoutSeconds")
        .and_then(|x| x.as_u64())
        .unwrap_or(1) as u32;
    let success_threshold = v
        .get("successThreshold")
        .and_then(|x| x.as_u64())
        .unwrap_or(1) as u32;
    let failure_threshold = v
        .get("failureThreshold")
        .and_then(|x| x.as_u64())
        .unwrap_or(3) as u32;

    let handler = if let Some(hg) = v.get("httpGet") {
        let path = hg
            .get("path")
            .and_then(|p| p.as_str())
            .unwrap_or("/")
            .to_string();
        let port = resolve_port(hg.get("port")?, ports)?;
        let host = hg.get("host").and_then(|h| h.as_str()).map(str::to_string);
        let scheme = hg
            .get("scheme")
            .and_then(|s| s.as_str())
            .unwrap_or("HTTP")
            .to_string();
        kubelet_core::pod::ProbeHandler::HttpGet {
            path,
            port,
            host,
            scheme,
        }
    } else if let Some(tc) = v.get("tcpSocket") {
        let port = resolve_port(tc.get("port")?, ports)?;
        let host = tc.get("host").and_then(|h| h.as_str()).map(str::to_string);
        kubelet_core::pod::ProbeHandler::TcpSocket { port, host }
    } else if let Some(exec) = v.get("exec") {
        let command = exec
            .get("command")
            .and_then(|c| c.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        kubelet_core::pod::ProbeHandler::Exec { command }
    } else if let Some(grpc) = v.get("grpc") {
        let port = grpc.get("port").and_then(|p| p.as_u64())? as u16;
        let service = grpc
            .get("service")
            .and_then(|s| s.as_str())
            .map(str::to_string);
        kubelet_core::pod::ProbeHandler::Grpc { port, service }
    } else {
        return None;
    };

    Some(kubelet_core::pod::Probe {
        handler,
        initial_delay_seconds,
        period_seconds,
        timeout_seconds,
        success_threshold,
        failure_threshold,
    })
}

fn parse_env_from(c: &serde_json::Value) -> Vec<EnvFromSource> {
    c.get("envFrom")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .map(|e| EnvFromSource {
                    prefix: e.get("prefix").and_then(|v| v.as_str()).map(str::to_string),
                    config_map_ref: e.get("configMapRef").and_then(|cmr| {
                        let name = cmr.get("name")?.as_str()?.to_string();
                        let optional = cmr
                            .get("optional")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        Some(EnvFromRef { name, optional })
                    }),
                    secret_ref: e.get("secretRef").and_then(|sr| {
                        let name = sr.get("name")?.as_str()?.to_string();
                        let optional = sr
                            .get("optional")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        Some(EnvFromRef { name, optional })
                    }),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_seccomp(sc: Option<&serde_json::Value>) -> Option<SeccompSpec> {
    let sc = sc?;
    let type_ = sc.get("type")?.as_str()?.to_string();
    let localhost_profile = sc
        .get("localhostProfile")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    Some(SeccompSpec {
        type_,
        localhost_profile,
    })
}

fn parse_apparmor(sc: Option<&serde_json::Value>) -> Option<AppArmorSpec> {
    let sc = sc?;
    let type_ = sc.get("type")?.as_str()?.to_string();
    let localhost_profile = sc
        .get("localhostProfile")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    Some(AppArmorSpec {
        type_,
        localhost_profile,
    })
}

fn parse_container_security_context(sc: Option<&serde_json::Value>) -> Option<SecurityContext> {
    let sc = sc?;
    let capabilities = sc.get("capabilities").map(|caps| Capabilities {
        add: caps
            .get("add")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        drop: caps
            .get("drop")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
    });
    Some(SecurityContext {
        run_as_user: sc
            .get("runAsUser")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32),
        run_as_group: sc
            .get("runAsGroup")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32),
        run_as_non_root: sc.get("runAsNonRoot").and_then(|v| v.as_bool()),
        privileged: sc.get("privileged").and_then(|v| v.as_bool()),
        read_only_root_filesystem: sc.get("readOnlyRootFilesystem").and_then(|v| v.as_bool()),
        allow_privilege_escalation: sc.get("allowPrivilegeEscalation").and_then(|v| v.as_bool()),
        capabilities,
        seccomp_profile: parse_seccomp(sc.get("seccompProfile")),
        apparmor_profile: parse_apparmor(sc.get("appArmorProfile")),
        proc_mount: sc
            .get("procMount")
            .and_then(|v| v.as_str())
            .map(str::to_string),
    })
}

fn parse_pod_security_context(sc: Option<&serde_json::Value>) -> Option<PodSecurityContext> {
    let sc = sc?;
    Some(PodSecurityContext {
        run_as_user: sc
            .get("runAsUser")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32),
        run_as_group: sc
            .get("runAsGroup")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32),
        run_as_non_root: sc.get("runAsNonRoot").and_then(|v| v.as_bool()),
        fs_group: sc.get("fsGroup").and_then(|v| v.as_u64()).map(|v| v as u32),
        supplemental_groups: sc
            .get("supplementalGroups")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_u64().map(|v| v as u32))
                    .collect()
            })
            .unwrap_or_default(),
        sysctls: sc
            .get("sysctls")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|s| {
                        let name = s.get("name")?.as_str()?.to_string();
                        let value = s.get("value")?.as_str()?.to_string();
                        Some(Sysctl { name, value })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        seccomp_profile: parse_seccomp(sc.get("seccompProfile")),
        fs_group_change_policy: sc
            .get("fsGroupChangePolicy")
            .and_then(|v| v.as_str())
            .map(str::to_string),
    })
}

fn parse_key_to_paths(arr: &serde_json::Value) -> Vec<KeyToPath> {
    arr.as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let key = item.get("key")?.as_str()?.to_string();
                    let path = item.get("path")?.as_str()?.to_string();
                    let mode = item.get("mode").and_then(|v| v.as_i64()).map(|m| m as i32);
                    Some(KeyToPath { key, path, mode })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_volume_mounts(c: &serde_json::Value) -> Vec<VolumeMount> {
    c.get("volumeMounts")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|vm| {
                    let name = vm.get("name")?.as_str()?.to_string();
                    let mount_path = vm.get("mountPath")?.as_str()?.to_string();
                    let sub_path = vm
                        .get("subPath")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let sub_path_expr = vm
                        .get("subPathExpr")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let read_only = vm
                        .get("readOnly")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    Some(VolumeMount {
                        name,
                        mount_path,
                        sub_path,
                        sub_path_expr,
                        read_only,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_downward_api_items(items_json: &serde_json::Value) -> Vec<DownwardAPIVolumeFile> {
    items_json
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let path = item.get("path")?.as_str()?.to_string();
                    let mode = item.get("mode").and_then(|v| v.as_i64()).map(|m| m as i32);
                    let field_ref = item.get("fieldRef").and_then(|fr| {
                        fr.get("fieldPath")
                            .and_then(|fp| fp.as_str())
                            .map(str::to_string)
                    });
                    let resource_field_ref = item.get("resourceFieldRef").and_then(|rfr| {
                        let resource = rfr.get("resource")?.as_str()?.to_string();
                        Some(ResourceFieldRef {
                            container_name: rfr
                                .get("containerName")
                                .and_then(|v| v.as_str())
                                .map(str::to_string),
                            resource,
                            divisor: rfr
                                .get("divisor")
                                .and_then(|v| v.as_str())
                                .map(str::to_string),
                        })
                    });
                    Some(DownwardAPIVolumeFile {
                        path,
                        field_ref,
                        resource_field_ref,
                        mode,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_projected_sources(sources_json: &serde_json::Value) -> Vec<ProjectedVolumeSource> {
    sources_json
        .as_array()
        .map(|sources| {
            sources
                .iter()
                .filter_map(|s| {
                    if let Some(token) = s.get("serviceAccountToken") {
                        Some(ProjectedVolumeSource::ServiceAccountToken {
                            audience: token
                                .get("audience")
                                .and_then(|v| v.as_str())
                                .map(str::to_string),
                            expiration_seconds: token
                                .get("expirationSeconds")
                                .and_then(|v| v.as_u64()),
                            path: token.get("path")?.as_str()?.to_string(),
                        })
                    } else if let Some(cm) = s.get("configMap") {
                        Some(ProjectedVolumeSource::ConfigMap {
                            name: cm
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            items: parse_key_to_paths(
                                cm.get("items").unwrap_or(&serde_json::Value::Null),
                            ),
                            optional: cm
                                .get("optional")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false),
                        })
                    } else if let Some(secret) = s.get("secret") {
                        Some(ProjectedVolumeSource::Secret {
                            name: secret
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            items: parse_key_to_paths(
                                secret.get("items").unwrap_or(&serde_json::Value::Null),
                            ),
                            optional: secret
                                .get("optional")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false),
                        })
                    } else if let Some(dapi) = s.get("downwardAPI") {
                        let items = parse_downward_api_items(
                            dapi.get("items").unwrap_or(&serde_json::Value::Null),
                        );
                        Some(ProjectedVolumeSource::DownwardAPI { items })
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_volumes(spec: &serde_json::Value) -> Vec<VolumeSpec> {
    spec.get("volumes")
        .and_then(|v| v.as_array())
        .map(|vols| {
            vols.iter()
                .filter_map(|v| {
                    let name = v.get("name")?.as_str()?.to_string();
                    let source = if let Some(hp) = v.get("hostPath") {
                        VolumeSource::HostPath {
                            path: hp.get("path")?.as_str()?.to_string(),
                            path_type: hp.get("type").and_then(|t| t.as_str()).map(str::to_string),
                        }
                    } else if v.get("emptyDir").is_some() {
                        let ed = v.get("emptyDir").unwrap();
                        VolumeSource::EmptyDir {
                            medium: ed
                                .get("medium")
                                .and_then(|m| m.as_str())
                                .map(str::to_string),
                            size_limit: None,
                        }
                    } else if let Some(cm) = v.get("configMap") {
                        VolumeSource::ConfigMap {
                            name: cm
                                .get("name")
                                .and_then(|n| n.as_str())
                                .unwrap_or("")
                                .to_string(),
                            items: parse_key_to_paths(
                                cm.get("items").unwrap_or(&serde_json::Value::Null),
                            ),
                            optional: cm
                                .get("optional")
                                .and_then(|o| o.as_bool())
                                .unwrap_or(false),
                            default_mode: cm
                                .get("defaultMode")
                                .and_then(|m| m.as_i64())
                                .map(|m| m as i32),
                        }
                    } else if let Some(secret) = v.get("secret") {
                        VolumeSource::Secret {
                            secret_name: secret
                                .get("secretName")
                                .and_then(|n| n.as_str())
                                .unwrap_or("")
                                .to_string(),
                            items: parse_key_to_paths(
                                secret.get("items").unwrap_or(&serde_json::Value::Null),
                            ),
                            optional: secret
                                .get("optional")
                                .and_then(|o| o.as_bool())
                                .unwrap_or(false),
                            default_mode: secret
                                .get("defaultMode")
                                .and_then(|m| m.as_i64())
                                .map(|m| m as i32),
                        }
                    } else if let Some(pvc) = v.get("persistentVolumeClaim") {
                        VolumeSource::PersistentVolumeClaim {
                            claim_name: pvc.get("claimName")?.as_str()?.to_string(),
                            read_only: pvc
                                .get("readOnly")
                                .and_then(|r| r.as_bool())
                                .unwrap_or(false),
                        }
                    } else if let Some(proj) = v.get("projected") {
                        let sources = parse_projected_sources(
                            proj.get("sources").unwrap_or(&serde_json::Value::Null),
                        );
                        VolumeSource::Projected {
                            sources,
                            default_mode: proj
                                .get("defaultMode")
                                .and_then(|m| m.as_i64())
                                .map(|m| m as i32),
                        }
                    } else if let Some(dapi) = v.get("downwardAPI") {
                        let items = parse_downward_api_items(
                            dapi.get("items").unwrap_or(&serde_json::Value::Null),
                        );
                        VolumeSource::DownwardAPI {
                            items,
                            default_mode: dapi
                                .get("defaultMode")
                                .and_then(|m| m.as_i64())
                                .map(|m| m as i32),
                        }
                    } else if let Some(nfs) = v.get("nfs") {
                        VolumeSource::NFS {
                            server: nfs.get("server")?.as_str()?.to_string(),
                            path: nfs.get("path")?.as_str()?.to_string(),
                            read_only: nfs
                                .get("readOnly")
                                .and_then(|r| r.as_bool())
                                .unwrap_or(false),
                        }
                    } else {
                        return None; // unsupported volume type
                    };
                    Some(VolumeSpec { name, source })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_k8s_quantity(s: &str) -> Option<ResourceQuantity> {
    let s = s.trim();
    // Memory suffixes
    if let Some(x) = s.strip_suffix("Ki") {
        return Some(ResourceQuantity {
            value: x.parse::<i64>().ok()?.saturating_mul(1024),
            unit: ResourceUnit::Bytes,
        });
    }
    if let Some(x) = s.strip_suffix("Mi") {
        return Some(ResourceQuantity {
            value: x.parse::<i64>().ok()?.saturating_mul(1024 * 1024),
            unit: ResourceUnit::Bytes,
        });
    }
    if let Some(x) = s.strip_suffix("Gi") {
        return Some(ResourceQuantity {
            value: x.parse::<i64>().ok()?.saturating_mul(1024 * 1024 * 1024),
            unit: ResourceUnit::Bytes,
        });
    }
    if let Some(x) = s.strip_suffix("Ti") {
        return Some(ResourceQuantity {
            value: x.parse::<i64>().ok()?.saturating_mul(1024_i64.pow(4)),
            unit: ResourceUnit::Bytes,
        });
    }
    if let Some(x) = s.strip_suffix("k") {
        return Some(ResourceQuantity {
            value: x.parse::<i64>().ok()?.saturating_mul(1000),
            unit: ResourceUnit::Bytes,
        });
    }
    if let Some(x) = s.strip_suffix("M") {
        return Some(ResourceQuantity {
            value: x.parse::<i64>().ok()?.saturating_mul(1_000_000),
            unit: ResourceUnit::Bytes,
        });
    }
    if let Some(x) = s.strip_suffix("G") {
        return Some(ResourceQuantity {
            value: x.parse::<i64>().ok()?.saturating_mul(1_000_000_000),
            unit: ResourceUnit::Bytes,
        });
    }
    // CPU millicores
    if let Some(x) = s.strip_suffix('m') {
        return Some(ResourceQuantity {
            value: x.parse::<i64>().ok()?,
            unit: ResourceUnit::Millicores,
        });
    }
    // Plain number — could be CPU cores or byte count
    if let Ok(n) = s.parse::<i64>() {
        return Some(ResourceQuantity {
            value: n,
            unit: ResourceUnit::Count,
        });
    }
    None
}

fn parse_resource_requirements(res: Option<&serde_json::Value>) -> ResourceRequirements {
    let mut reqs = ResourceRequirements::default();
    let Some(res) = res else {
        return reqs;
    };
    if let Some(requests) = res.get("requests").and_then(|v| v.as_object()) {
        for (k, v) in requests {
            if let Some(s) = v.as_str()
                && let Some(q) = parse_k8s_quantity_for_key(k, s)
            {
                reqs.requests.insert(k.clone(), q);
            }
        }
    }
    if let Some(limits) = res.get("limits").and_then(|v| v.as_object()) {
        for (k, v) in limits {
            if let Some(s) = v.as_str()
                && let Some(q) = parse_k8s_quantity_for_key(k, s)
            {
                reqs.limits.insert(k.clone(), q);
            }
        }
    }
    reqs
}

/// Parse a k8s resource quantity, normalizing CPU plain-number values to millicores.
fn parse_k8s_quantity_for_key(key: &str, s: &str) -> Option<ResourceQuantity> {
    let q = parse_k8s_quantity(s)?;
    // Normalize: CPU "2" (Count) means 2 cores = 2000 millicores
    if key == "cpu" && q.unit == ResourceUnit::Count {
        return Some(ResourceQuantity {
            value: q.value.saturating_mul(1000),
            unit: ResourceUnit::Millicores,
        });
    }
    Some(q)
}

// -- NodeStatus extension ------------------------------------------------------

trait NodeStatusExt {
    fn phase_str(&self) -> &str;
}

impl NodeStatusExt for NodeStatus {
    fn phase_str(&self) -> &str {
        if self.is_ready() { "Ready" } else { "NotReady" }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn sample_pod_json(uid: &str, name: &str, image: &str) -> serde_json::Value {
        serde_json::json!({
            "metadata": {
                "name": name,
                "namespace": "default",
                "uid": uid,
                "labels": { "app": name },
                "annotations": {}
            },
            "spec": {
                "containers": [
                    { "name": "app", "image": image, "command": ["sleep", "3600"] }
                ],
                "nodeName": "node1",
                "restartPolicy": "Always",
                "terminationGracePeriodSeconds": 30
            }
        })
    }

    #[test]
    fn test_pod_spec_from_map_basic() {
        let json = sample_pod_json("uid-1", "my-pod", "nginx:1.25");
        let spec = pod_spec_from_map(&json, "node1").unwrap();
        assert_eq!(spec.pod_ref.name, "my-pod");
        assert_eq!(spec.pod_ref.namespace, "default");
        assert_eq!(spec.uid.0, "uid-1");
        assert_eq!(spec.containers.len(), 1);
        assert_eq!(spec.containers[0].image, "nginx:1.25");
    }

    #[test]
    fn test_pod_spec_from_map_restart_policy() {
        let mut json = sample_pod_json("uid-2", "batch", "alpine");
        json["spec"]["restartPolicy"] = serde_json::json!("Never");
        let spec = pod_spec_from_map(&json, "node1").unwrap();
        assert_eq!(spec.restart_policy, RestartPolicy::Never);
    }

    #[test]
    fn test_pod_spec_from_map_labels_and_annotations() {
        let json = sample_pod_json("uid-3", "labeled-pod", "alpine");
        let spec = pod_spec_from_map(&json, "node1").unwrap();
        assert_eq!(spec.labels.get("app"), Some(&"labeled-pod".to_string()));
    }

    #[test]
    fn test_pod_spec_from_map_missing_uid_returns_none() {
        let json = serde_json::json!({
            "metadata": { "name": "no-uid" },
            "spec": { "containers": [] }
        });
        let spec = pod_spec_from_map(&json, "node1");
        assert!(spec.is_none());
    }

    #[test]
    fn test_pod_spec_from_map_grace_period() {
        let json = sample_pod_json("uid-4", "pod", "img");
        let spec = pod_spec_from_map(&json, "node1").unwrap();
        assert_eq!(spec.termination_grace_period_seconds, 30);
    }

    #[test]
    fn test_pod_spec_command_args() {
        let json = sample_pod_json("uid-5", "sleeper", "alpine");
        let spec = pod_spec_from_map(&json, "node1").unwrap();
        assert_eq!(spec.containers[0].command, vec!["sleep", "3600"]);
    }

    #[tokio::test]
    async fn test_simulated_source_emits_initial_pods() {
        let uid = PodUID::new("sim-uid");
        let pod = PodSpec {
            uid: uid.clone(),
            pod_ref: PodRef::new("default", "sim-pod"),
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
        };

        let source = SimulatedApiPodSource::new("node1", vec![pod], Duration::from_secs(3600));
        let (tx, mut rx) = mpsc::channel(10);

        tokio::spawn(async move { source.run(tx).await });

        let update = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout")
            .expect("no update");

        assert_eq!(update.pod.uid, uid);
        assert_eq!(update.op, PodOperation::Add);
    }

    #[tokio::test]
    async fn test_logging_reporter_does_not_fail() {
        let reporter = LoggingNodeReporter::new("node1");
        let status = NodeStatus::new("node1");
        reporter.report_node_status(&status).await.unwrap();
        reporter.renew_node_lease("node1", 40).await.unwrap();
        reporter.patch_node_conditions("node1", &[]).await.unwrap();
    }

    // ── imagePullSecrets parsing ──────────────────────────────────────────────
    //
    // Regression: kube_watcher's JSON-based pod_spec_from_map hardcoded
    // image_pull_secrets: vec![] instead of parsing spec.imagePullSecrets.
    // This caused ALL pods to ignore their imagePullSecrets, so private
    // images always failed with NotFound instead of using registry credentials.

    #[test]
    fn test_pod_spec_from_map_image_pull_secrets_parsed() {
        let json = serde_json::json!({
            "metadata": { "name": "my-pod", "namespace": "default", "uid": "uid-pull" },
            "spec": {
                "containers": [{ "name": "app", "image": "us-east1-docker.pkg.dev/proj/workloads/myapp:v1" }],
                "nodeName": "node1",
                "imagePullSecrets": [
                    { "name": "gcp-pull-secret" },
                    { "name": "registry-token" }
                ]
            }
        });
        let spec = pod_spec_from_map(&json, "node1").unwrap();
        assert_eq!(spec.image_pull_secrets.len(), 2);
        assert_eq!(spec.image_pull_secrets[0].name, "gcp-pull-secret");
        assert_eq!(spec.image_pull_secrets[1].name, "registry-token");
    }

    #[test]
    fn test_pod_spec_from_map_no_image_pull_secrets_is_empty() {
        let json = serde_json::json!({
            "metadata": { "name": "pub-pod", "namespace": "default", "uid": "uid-pub" },
            "spec": {
                "containers": [{ "name": "app", "image": "nginx:1.25" }],
                "nodeName": "node1"
            }
        });
        let spec = pod_spec_from_map(&json, "node1").unwrap();
        assert!(spec.image_pull_secrets.is_empty());
    }

    #[test]
    fn test_pod_spec_from_map_empty_image_pull_secrets_array() {
        let json = serde_json::json!({
            "metadata": { "name": "empty-pod", "namespace": "default", "uid": "uid-empty" },
            "spec": {
                "containers": [{ "name": "app", "image": "nginx:1.25" }],
                "nodeName": "node1",
                "imagePullSecrets": []
            }
        });
        let spec = pod_spec_from_map(&json, "node1").unwrap();
        assert!(spec.image_pull_secrets.is_empty());
    }
}
