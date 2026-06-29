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

//! Real Kubernetes API node reporter and pod source.
//!
//! `KubeNodeReporter` -- PATCHes Node and Pod status to the API server via kubelet.
//! `KubePodSource`    -- watches the API server for pod assignments via kube_runtime::watcher.
//!
//! In standalone mode (no cluster reachable), both fall back to logging-only.

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use kubelet_core::error::{KubeletError, Result};
use kubelet_core::node::{NodeCondition, NodeConditionStatus, NodeStatus};
use kubelet_core::pod::lifecycle::{ContainerState, PodCondition, PodLifecycleState};
use kubelet_core::pod::{
    ContainerPort, ContainerSpec, DnsConfig, DnsOption, DnsPolicy, DownwardAPIVolumeFile,
    EnvFromRef, EnvFromSource, EnvVar, EnvVarSource, ImagePullPolicy, KeyToPath, Lifecycle,
    LifecycleHandler, PodOperation, PodSpec, PodUpdate, Probe, ProbeHandler, ProjectedVolumeSource,
    ReadinessGate, ResourceFieldRef, ResourceRequirements, RestartPolicy, SecurityContext,
    VolumeMount, VolumeSource, VolumeSpec,
};
use kubelet_core::types::{PodRef, PodUID, ResourceQuantity, ResourceUnit};
use kubelet_ports::driven::node_reporter::NodeReporter;
use kubelet_ports::driven::pod_source::PodSource;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use futures::StreamExt;
use k8s_openapi::api::core::v1::{Node, NodeCondition as K8sNodeCondition, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::{
    Client, Config, Error as KubeError,
    api::{Api, ListParams, Patch, PatchParams, PostParams},
    config::{KubeConfigOptions, Kubeconfig},
    runtime::watcher::{self, Event},
};

fn format_lease_renew_time(now: chrono::DateTime<Utc>) -> String {
    now.to_rfc3339_opts(SecondsFormat::Micros, true)
}

// -- Connection mode ----------------------------------------------------------

#[derive(Debug, Clone)]
pub enum KubeConnectMode {
    InCluster,
    Kubeconfig { path: std::path::PathBuf },
    Standalone,
}

impl KubeConnectMode {
    pub fn detect() -> Self {
        if std::path::Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists() {
            return Self::InCluster;
        }
        if let Ok(kc) = std::env::var("KUBECONFIG") {
            return Self::Kubeconfig { path: kc.into() };
        }
        if let Ok(home) = std::env::var("HOME") {
            let p = std::path::PathBuf::from(home).join(".kube/config");
            if p.exists() {
                return Self::Kubeconfig { path: p };
            }
        }
        Self::Standalone
    }
}

// -- KubeNodeReporter ---------------------------------------------------------

pub struct KubeNodeReporter {
    node_name: String,
    mode: KubeConnectMode,
    client: Option<Client>,
}

impl KubeNodeReporter {
    pub async fn new(node_name: impl Into<String>) -> Self {
        let node_name = node_name.into();
        let mode = KubeConnectMode::detect();
        let client = Self::try_connect(&mode).await;
        Self {
            node_name,
            mode,
            client,
        }
    }

    pub async fn with_mode(node_name: impl Into<String>, mode: KubeConnectMode) -> Self {
        let node_name = node_name.into();
        let client = Self::try_connect(&mode).await;
        Self {
            node_name,
            mode,
            client,
        }
    }

    pub async fn try_connect(mode: &KubeConnectMode) -> Option<Client> {
        match mode {
            KubeConnectMode::Standalone => None,
            KubeConnectMode::InCluster => Client::try_default().await.ok().inspect(|c| {
                info!("kubelet: in-cluster client connected");
            }),
            KubeConnectMode::Kubeconfig { path } => {
                let kubeconfig = match Kubeconfig::read_from(path) {
                    Ok(cfg) => cfg,
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "Failed to read kubeconfig file");
                        return None;
                    }
                };

                let options = KubeConfigOptions::default();
                let config = match Config::from_custom_kubeconfig(kubeconfig, &options).await {
                    Ok(cfg) => cfg,
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "Failed to build Kubernetes client config from kubeconfig");
                        return None;
                    }
                };

                match Client::try_from(config) {
                    Ok(client) => {
                        info!(path = %path.display(), "kubelet: kubeconfig client connected");
                        Some(client)
                    }
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "Failed to create Kubernetes client from kubeconfig");
                        None
                    }
                }
            }
        }
    }

    pub fn is_connected(&self) -> bool {
        self.client.is_some()
    }
    pub fn connection_mode(&self) -> &KubeConnectMode {
        &self.mode
    }
}

#[async_trait]
impl NodeReporter for KubeNodeReporter {
    async fn report_node_status(&self, status: &NodeStatus) -> Result<()> {
        let Some(client) = &self.client else {
            info!(node = %status.name, ready = status.is_ready(), "Standalone: node status");
            return Ok(());
        };
        let nodes: Api<Node> = Api::all(client.clone());
        let patch_params = PatchParams::apply("kube-air").force();
        let patch = build_node_status_patch(status);
        if let Err(e) = nodes
            .patch_status(&self.node_name, &patch_params, &Patch::Apply(patch.clone()))
            .await
        {
            if is_not_found(&e) {
                // Upstream node e2e expects kubelet to self-register a Node object.
                let apply_patch = build_node_apply_patch(status);
                nodes
                    .patch(&self.node_name, &patch_params, &Patch::Apply(apply_patch))
                    .await
                    .map_err(|create_err| {
                        KubeletError::NodeStatus(format!(
                            "CREATE node before status patch: {}",
                            create_err
                        ))
                    })?;

                nodes
                    .patch_status(&self.node_name, &patch_params, &Patch::Apply(patch))
                    .await
                    .map_err(|retry_err| {
                        KubeletError::NodeStatus(format!(
                            "PATCH node status after create: {}",
                            retry_err
                        ))
                    })?;
            } else {
                return Err(KubeletError::NodeStatus(format!(
                    "PATCH node status: {}",
                    e
                )));
            }
        }
        debug!(node = %status.name, "Node status patched");
        Ok(())
    }

    async fn report_pod_status(
        &self,
        pod_ref: &PodRef,
        uid: &PodUID,
        state: &PodLifecycleState,
    ) -> Result<()> {
        let Some(client) = &self.client else {
            debug!(pod = %pod_ref, state = ?state, "Standalone: pod status");
            return Ok(());
        };
        let phase = lifecycle_to_phase(state);
        let pods: Api<Pod> = Api::namespaced(client.clone(), &pod_ref.namespace);

        // Build container statuses from lifecycle state
        let container_statuses: Vec<k8s_openapi::api::core::v1::ContainerStatus> = state
            .container_statuses
            .iter()
            .map(lifecycle_container_status_to_k8s)
            .collect();

        let init_container_statuses: Vec<k8s_openapi::api::core::v1::ContainerStatus> = state
            .init_container_statuses
            .iter()
            .map(lifecycle_container_status_to_k8s)
            .collect();

        let ephemeral_container_statuses: Vec<k8s_openapi::api::core::v1::ContainerStatus> = state
            .ephemeral_container_statuses
            .iter()
            .map(lifecycle_container_status_to_k8s)
            .collect();

        // Build conditions
        let conditions: Vec<k8s_openapi::api::core::v1::PodCondition> =
            state.conditions.iter().map(pod_condition_to_k8s).collect();

        let start_time = state.start_time.map(Time);
        let pod_ip = state.pod_ip.clone();
        let host_ip = state.host_ip.clone();
        let reason = state.reason.clone();
        let observed_generation = state.observed_generation;

        let pod_ips = pod_ip
            .clone()
            .filter(|ip| !ip.is_empty())
            .map(|ip| vec![k8s_openapi::api::core::v1::PodIP { ip: Some(ip) }])
            .unwrap_or_default();

        let host_ips = host_ip
            .clone()
            .map(|ip| vec![k8s_openapi::api::core::v1::HostIP { ip: Some(ip) }])
            .unwrap_or_default();

        // Use Apply patch with field manager to avoid conflicts with other controllers
        let patch_params = PatchParams::apply("kubelet").force();
        pods.patch_status(
            &pod_ref.name,
            &patch_params,
            &Patch::Apply(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_ref.name,
                    "namespace": pod_ref.namespace,
                },
                "status": {
                    "phase": phase,
                    "reason": reason,
                    "conditions": conditions,
                    "containerStatuses": container_statuses,
                    "initContainerStatuses": init_container_statuses,
                    "ephemeralContainerStatuses": ephemeral_container_statuses,
                    "startTime": start_time,
                    "podIP": pod_ip,
                    "podIPs": pod_ips,
                    "hostIP": host_ip,
                    "hostIPs": host_ips,
                    "observedGeneration": observed_generation,
                }
            })),
        )
        .await
        .map_err(|e| KubeletError::PodStatus(format!("PATCH pod status: {}", e)))?;
        Ok(())
    }

    async fn delete_pod(&self, pod_ref: &PodRef, _uid: &PodUID) -> Result<()> {
        let Some(client) = &self.client else {
            debug!(pod = %pod_ref, "Standalone: skip pod delete");
            return Ok(());
        };
        let pods: Api<Pod> = Api::namespaced(client.clone(), &pod_ref.namespace);
        let dp = kube::api::DeleteParams {
            grace_period_seconds: Some(0),
            ..Default::default()
        };
        match pods.delete(&pod_ref.name, &dp).await {
            Ok(_) => {
                info!(pod = %pod_ref, "Force-deleted pod from API server");
                Ok(())
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                // Pod already gone, that's fine
                Ok(())
            }
            Err(e) => Err(KubeletError::PodStatus(format!(
                "DELETE pod {}: {}",
                pod_ref.name, e
            ))),
        }
    }

    async fn patch_node_conditions(
        &self,
        node_name: &str,
        conditions: &[NodeCondition],
    ) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        let nodes: Api<Node> = Api::all(client.clone());
        let k8s_conditions: Vec<K8sNodeCondition> =
            conditions.iter().map(node_condition_to_k8s).collect();
        let patch = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": { "name": node_name },
            "status": { "conditions": k8s_conditions }
        });
        nodes
            .patch_status(
                node_name,
                &PatchParams::apply("kube-air").force(),
                &Patch::Apply(patch),
            )
            .await
            .map_err(|e| KubeletError::NodeStatus(format!("PATCH node conditions: {}", e)))?;
        Ok(())
    }

    async fn renew_node_lease(&self, node_name: &str, duration_seconds: u32) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        use k8s_openapi::api::coordination::v1::Lease;
        let leases: Api<Lease> = Api::namespaced(client.clone(), "kube-node-lease");
        let patch = serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": { "name": node_name, "namespace": "kube-node-lease" },
            "spec": {
                "holderIdentity": node_name,
                "leaseDurationSeconds": duration_seconds,
                "renewTime": format_lease_renew_time(Utc::now())
            }
        });
        leases
            .patch(
                node_name,
                &PatchParams::apply("kube-air").force(),
                &Patch::Apply(patch),
            )
            .await
            .map_err(|e| KubeletError::NodeStatus(format!("renew lease: {}", e)))?;
        debug!(node = %node_name, "Node lease renewed");
        Ok(())
    }

    async fn emit_container_event(
        &self,
        pod_ref: &PodRef,
        uid: &PodUID,
        container_name: &str,
        event_type: &str,
        reason: &str,
        message: &str,
    ) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        use k8s_openapi::api::core::v1::Event as K8sEvent;
        use k8s_openapi::api::core::v1::ObjectReference;
        let events: Api<K8sEvent> = Api::namespaced(client.clone(), &pod_ref.namespace);
        let now = Utc::now();
        let now_time = Time(now);
        let event_name = format!("{}.{}", pod_ref.name, reason.to_lowercase());
        let event = K8sEvent {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                name: Some(event_name),
                namespace: Some(pod_ref.namespace.clone()),
                ..Default::default()
            },
            involved_object: ObjectReference {
                api_version: Some("v1".to_string()),
                kind: Some("Pod".to_string()),
                name: Some(pod_ref.name.clone()),
                namespace: Some(pod_ref.namespace.clone()),
                uid: Some(uid.0.clone()),
                field_path: Some(format!("spec.containers{{{}}}", container_name)),
                ..Default::default()
            },
            reason: Some(reason.to_string()),
            message: Some(message.to_string()),
            type_: Some(event_type.to_string()),
            count: Some(1),
            first_timestamp: Some(now_time.clone()),
            last_timestamp: Some(now_time),
            // Do NOT set event_time: mixing old-style (firstTimestamp/lastTimestamp)
            // and new-style (eventTime) fields causes a 422 Unprocessable Entity.
            event_time: None,
            reporting_component: Some("kubelet".to_string()),
            reporting_instance: Some(self.node_name.clone()),
            source: Some(k8s_openapi::api::core::v1::EventSource {
                component: Some("kubelet".to_string()),
                host: Some(self.node_name.clone()),
            }),
            action: None,
            related: None,
            series: None,
        };
        match events.create(&PostParams::default(), &event).await {
            Ok(_) => {
                debug!(
                    pod = %pod_ref,
                    container = container_name,
                    reason = reason,
                    "Emitted container event"
                );
            }
            Err(e) => {
                // Non-fatal: log but do not propagate
                warn!(
                    pod = %pod_ref,
                    container = container_name,
                    reason = reason,
                    error = %e,
                    "Failed to emit container event"
                );
            }
        }
        Ok(())
    }
}

// -- Patch builders -----------------------------------------------------------

pub fn build_node_status_patch(status: &NodeStatus) -> serde_json::Value {
    let conditions: Vec<serde_json::Value> = status
        .conditions
        .iter()
        .map(|c| {
            serde_json::json!({
                "type": c.condition_type.to_string(),
                "status": if c.status == NodeConditionStatus::True { "True" } else { "False" },
                "reason": c.reason,
                "message": c.message,
                "lastHeartbeatTime": Utc::now().to_rfc3339(),
                "lastTransitionTime": Utc::now().to_rfc3339()
            })
        })
        .collect();

    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": { "name": status.name },
        "status": {
            "conditions": conditions,
            "daemonEndpoints": {
                "kubeletEndpoint": {
                    "Port": 10250
                }
            },
            "addresses": status.addresses.iter().map(|a| serde_json::json!({
                "type": format!("{:?}", a.address_type),
                "address": a.address
            })).collect::<Vec<_>>(),
            "capacity": {
                "cpu": status.capacity.cpu_cores.to_string(),
                "memory": format!("{}Ki", status.capacity.memory_bytes / 1024),
                "pods": status.capacity.pods.to_string()
            },
            "allocatable": {
                "cpu": status.capacity.cpu_cores.to_string(),
                "memory": format!("{}Ki", status.capacity.memory_bytes * 9 / (1024 * 10)),
                "pods": status.capacity.pods.to_string()
            }
        }
    })
}

fn build_node_apply_patch(status: &NodeStatus) -> serde_json::Value {
    // Detect OS and architecture for the standard well-known node labels.
    // These are required for scheduling system pods (coredns, etc.) which use
    // nodeAffinity on kubernetes.io/os and kubernetes.io/arch.
    let os = std::env::consts::OS; // "linux", "windows", "macos"
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        a => a,
    };
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": status.name,
            "labels": {
                "kubernetes.io/hostname": status.name,
                "kubernetes.io/os": os,
                "kubernetes.io/arch": arch,
                "beta.kubernetes.io/os": os,
                "beta.kubernetes.io/arch": arch
            }
        },
        "spec": {}
    })
}

fn is_not_found(error: &KubeError) -> bool {
    matches!(error, KubeError::Api(ae) if ae.code == 404)
}

fn lifecycle_to_phase(state: &PodLifecycleState) -> &'static str {
    match &state.phase {
        kubelet_core::pod::lifecycle::PodPhase::Pending => "Pending",
        kubelet_core::pod::lifecycle::PodPhase::Running => "Running",
        kubelet_core::pod::lifecycle::PodPhase::Succeeded => "Succeeded",
        kubelet_core::pod::lifecycle::PodPhase::Failed => "Failed",
        kubelet_core::pod::lifecycle::PodPhase::Unknown => "Unknown",
    }
}

fn node_condition_to_k8s(c: &NodeCondition) -> K8sNodeCondition {
    K8sNodeCondition {
        type_: c.condition_type.to_string(),
        status: if c.status == NodeConditionStatus::True {
            "True".to_string()
        } else {
            "False".to_string()
        },
        reason: Some(c.reason.clone()),
        message: Some(c.message.clone()),
        last_heartbeat_time: Some(Time(Utc::now())),
        last_transition_time: Some(Time(Utc::now())),
    }
}

fn pod_condition_to_k8s(c: &PodCondition) -> k8s_openapi::api::core::v1::PodCondition {
    use kubelet_core::pod::lifecycle::ConditionStatus;
    k8s_openapi::api::core::v1::PodCondition {
        type_: match &c.condition_type {
            kubelet_core::pod::lifecycle::PodConditionType::PodScheduled => {
                "PodScheduled".to_string()
            }
            kubelet_core::pod::lifecycle::PodConditionType::ContainersReady => {
                "ContainersReady".to_string()
            }
            kubelet_core::pod::lifecycle::PodConditionType::Initialized => {
                "Initialized".to_string()
            }
            kubelet_core::pod::lifecycle::PodConditionType::Ready => "Ready".to_string(),
            kubelet_core::pod::lifecycle::PodConditionType::PodReadyToStartContainers => {
                "PodReadyToStartContainers".to_string()
            }
            kubelet_core::pod::lifecycle::PodConditionType::DisruptionTarget => {
                "DisruptionTarget".to_string()
            }
        },
        status: match &c.status {
            ConditionStatus::True => "True".to_string(),
            ConditionStatus::False => "False".to_string(),
            ConditionStatus::Unknown => "Unknown".to_string(),
        },
        reason: c.reason.clone(),
        message: c.message.clone(),
        last_probe_time: c.last_probe_time.map(Time),
        last_transition_time: c.last_transition_time.map(Time),
    }
}

fn container_state_to_k8s(s: &ContainerState) -> k8s_openapi::api::core::v1::ContainerState {
    use k8s_openapi::api::core::v1::{
        ContainerState as K8sContainerState, ContainerStateRunning, ContainerStateTerminated,
        ContainerStateWaiting,
    };
    match s {
        ContainerState::Waiting { reason, message } => K8sContainerState {
            waiting: Some(ContainerStateWaiting {
                reason: Some(reason.clone()),
                message: message.clone(),
            }),
            ..Default::default()
        },
        ContainerState::Running { started_at } => K8sContainerState {
            running: Some(ContainerStateRunning {
                started_at: Some(Time(*started_at)),
            }),
            ..Default::default()
        },
        ContainerState::Terminated {
            exit_code,
            reason,
            message,
            started_at,
            finished_at,
        } => K8sContainerState {
            terminated: Some(ContainerStateTerminated {
                exit_code: *exit_code,
                reason: Some(reason.clone()),
                message: message.clone(),
                started_at: Some(Time(*started_at)),
                finished_at: Some(Time(*finished_at)),
                ..Default::default()
            }),
            ..Default::default()
        },
    }
}

fn lifecycle_container_status_to_k8s(
    cs: &kubelet_core::pod::lifecycle::ContainerStatus,
) -> k8s_openapi::api::core::v1::ContainerStatus {
    let (state_ready, started) = match &cs.state {
        ContainerState::Waiting { .. } => (false, Some(false)),
        ContainerState::Running { .. } => (true, Some(true)),
        ContainerState::Terminated { .. } => (false, Some(true)),
    };
    // Use cs.ready rather than state_ready for the final value: the caller
    // (update_pod_status) sets cs.ready=true for successfully completed init
    // containers, which overrides the state-derived value.
    let ready = cs.ready || state_ready;
    k8s_openapi::api::core::v1::ContainerStatus {
        name: cs.name.clone(),
        state: Some(container_state_to_k8s(&cs.state)),
        last_state: cs.last_state.as_ref().map(container_state_to_k8s),
        ready,
        restart_count: cs.restart_count as i32,
        image: cs.image.clone(),
        image_id: cs.image_id.clone(),
        container_id: cs.container_id.clone(),
        started,
        ..Default::default()
    }
}

// -- KubePodSource ------------------------------------------------------------

pub struct KubePodSource {
    node_name: String,
    client: Option<Client>,
}

impl KubePodSource {
    pub async fn new(node_name: impl Into<String>) -> Self {
        let mode = KubeConnectMode::detect();
        let client = KubeNodeReporter::try_connect(&mode).await;
        Self {
            node_name: node_name.into(),
            client,
        }
    }

    pub async fn with_mode(node_name: impl Into<String>, mode: KubeConnectMode) -> Self {
        let client = KubeNodeReporter::try_connect(&mode).await;
        Self {
            node_name: node_name.into(),
            client,
        }
    }
}

#[async_trait]
impl PodSource for KubePodSource {
    fn name(&self) -> &str {
        "kube-api-watcher"
    }

    async fn run(&self, tx: mpsc::Sender<PodUpdate>) -> Result<()> {
        let Some(client) = &self.client else {
            info!(node = %self.node_name, "Standalone: no pod watch");
            std::future::pending::<()>().await;
            return Ok(());
        };

        let pods: Api<Pod> = Api::all(client.clone());
        let node_name = self.node_name.clone();

        let field_selector = format!("spec.nodeName={}", node_name);
        let list_params = ListParams::default().fields(&field_selector);

        loop {
            let mut stream = watcher::watcher(
                pods.clone(),
                watcher::Config::default()
                    .fields(&field_selector)
                    .timeout(290),
            )
            .boxed();
            let mut relist_tick = tokio::time::interval(Duration::from_secs(30));

            info!(node = %node_name, "Pod watch stream started");

            loop {
                tokio::select! {
                    // biased: relist tick is checked first so it can never be
                    // starved by a high-frequency stream (e.g. during a burst
                    // of Applied/Restarted events from kubelet).
                    biased;
                    _ = relist_tick.tick() => {
                        // Wrap the list call in a timeout so a hung API-server
                        // connection cannot freeze the entire watcher loop.
                        match tokio::time::timeout(
                            Duration::from_secs(15),
                            pods.list(&list_params),
                        )
                        .await
                        {
                            Ok(Ok(list)) => {
                                debug!(node = %node_name, pods = list.items.len(), "Pod relist tick");
                                for pod in list.items {
                                    // Skip pods being deleted; they are handled via Applied events.
                                    if pod.metadata.deletion_timestamp.is_some() {
                                        continue;
                                    }
                                    if let Some(spec) = pod_to_spec(&pod, &node_name)
                                        && tx.send(PodUpdate { pod: spec, op: PodOperation::Reconcile }).await.is_err() {
                                            warn!("Pod source channel closed during relist");
                                            return Ok(());
                                        }
                                }
                            }
                            Ok(Err(e)) => {
                                warn!("Pod relist failed: {}", e);
                            }
                            Err(_) => {
                                warn!(node = %node_name, "Pod relist timed out; reconnecting watcher");
                                break;
                            }
                        }
                    }
                    maybe_event = stream.next() => {
                        match maybe_event {
                            None => {
                                warn!("Pod watch stream ended; reconnecting");
                                break;
                            }
                            Some(Err(e)) => {
                                warn!("Pod watch error: {}; reconnecting", e);
                                break;
                            }
                            Some(Ok(event)) => {
                                for update in pod_event_to_updates(event, &node_name) {
                                    if tx.send(update).await.is_err() {
                                        warn!("Pod source channel closed");
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

fn pod_event_to_updates(event: Event<Pod>, node_name: &str) -> Vec<PodUpdate> {
    match event {
        Event::Applied(pod) => {
            // If the pod has a deletionTimestamp, treat it as a removal
            if pod.metadata.deletion_timestamp.is_some() {
                let uid = pod.metadata.uid.as_deref().unwrap_or("").to_string();
                let name = pod.metadata.name.as_deref().unwrap_or("").to_string();
                let ns = pod
                    .metadata
                    .namespace
                    .as_deref()
                    .unwrap_or("default")
                    .to_string();
                if uid.is_empty() || name.is_empty() {
                    return vec![];
                }
                return vec![PodUpdate {
                    op: PodOperation::Remove,
                    pod: PodSpec {
                        uid: PodUID::new(&uid),
                        pod_ref: PodRef {
                            name,
                            namespace: ns,
                        },
                        node_name: node_name.to_string(),
                        ..Default::default()
                    },
                }];
            }
            pod_to_spec(&pod, node_name)
                .map(|spec| {
                    vec![PodUpdate {
                        pod: spec,
                        op: PodOperation::Add,
                    }]
                })
                .unwrap_or_default()
        }
        Event::Deleted(pod) => {
            let uid = pod.metadata.uid.as_deref().unwrap_or("").to_string();
            let name = pod.metadata.name.as_deref().unwrap_or("").to_string();
            let ns = pod
                .metadata
                .namespace
                .as_deref()
                .unwrap_or("default")
                .to_string();
            if uid.is_empty() || name.is_empty() {
                return vec![];
            }
            vec![PodUpdate {
                op: PodOperation::Remove,
                pod: PodSpec {
                    uid: PodUID::new(&uid),
                    pod_ref: PodRef {
                        name,
                        namespace: ns,
                    },
                    node_name: node_name.to_string(),
                    ..Default::default()
                },
            }]
        }
        Event::Restarted(pods) => pods
            .iter()
            .flat_map(|p| {
                // Pods being deleted should be treated as Remove events on reconnect.
                if p.metadata.deletion_timestamp.is_some() {
                    let uid = p.metadata.uid.as_deref().unwrap_or("").to_string();
                    let name = p.metadata.name.as_deref().unwrap_or("").to_string();
                    let ns = p
                        .metadata
                        .namespace
                        .as_deref()
                        .unwrap_or("default")
                        .to_string();
                    if uid.is_empty() || name.is_empty() {
                        return vec![];
                    }
                    return vec![PodUpdate {
                        op: PodOperation::Remove,
                        pod: PodSpec {
                            uid: PodUID::new(&uid),
                            pod_ref: PodRef {
                                name,
                                namespace: ns,
                            },
                            node_name: node_name.to_string(),
                            ..Default::default()
                        },
                    }];
                }
                pod_to_spec(p, node_name)
                    .map(|spec| {
                        vec![PodUpdate {
                            pod: spec,
                            op: PodOperation::Add,
                        }]
                    })
                    .unwrap_or_default()
            })
            .collect(),
    }
}

fn pod_to_spec(pod: &Pod, node_name: &str) -> Option<PodSpec> {
    let uid = pod.metadata.uid.as_deref()?.to_string();
    let name = pod.metadata.name.as_deref()?.to_string();
    let ns = pod
        .metadata
        .namespace
        .as_deref()
        .unwrap_or("default")
        .to_string();

    // Skip static pod mirrors — these are managed exclusively by the file/URL
    // pod source. The API server stores a mirror copy with the annotation
    // "kubernetes.io/config.mirror". If we also emit them here, the pod worker
    // sees two Add events for the same pod with different UIDs (the file/URL
    // source uses a deterministic synthetic UID, while the mirror has its own
    // etcd UID) which causes the pod_worker to destroy and recreate the sandbox
    // on every kubelet restart.
    if let Some(annotations) = &pod.metadata.annotations
        && annotations.contains_key("kubernetes.io/config.mirror")
    {
        debug!(
            pod = %name,
            namespace = %ns,
            "Skipping static pod mirror from API server watcher"
        );
        return None;
    }

    let k8s_spec = pod.spec.as_ref()?;

    let containers = k8s_spec
        .containers
        .iter()
        .map(k8s_container_to_spec)
        .collect();
    let init_containers = k8s_spec
        .init_containers
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(k8s_container_to_spec)
        .collect();
    let ephemeral_containers = k8s_spec
        .ephemeral_containers
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(k8s_ephemeral_container_to_spec)
        .collect();
    let volumes = k8s_spec
        .volumes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(k8s_volume_to_spec)
        .collect();

    let restart_policy = match k8s_spec.restart_policy.as_deref() {
        Some("Never") => RestartPolicy::Never,
        Some("OnFailure") => RestartPolicy::OnFailure,
        _ => RestartPolicy::Always,
    };

    let node_selector: HashMap<String, String> = k8s_spec
        .node_selector
        .as_ref()
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();

    let readiness_gates = k8s_spec
        .readiness_gates
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|rg| ReadinessGate {
            condition_type: rg.condition_type.clone(),
        })
        .collect();

    Some(PodSpec {
        uid: PodUID::new(&uid),
        pod_ref: PodRef {
            name,
            namespace: ns,
        },
        node_name: k8s_spec
            .node_name
            .clone()
            .unwrap_or_else(|| node_name.to_string()),
        containers,
        init_containers,
        ephemeral_containers,
        volumes,
        restart_policy,
        active_deadline_seconds: k8s_spec.active_deadline_seconds.map(|s| s as u64),
        service_account_name: k8s_spec.service_account_name.clone().unwrap_or_default(),
        automount_service_account_token: k8s_spec.automount_service_account_token,
        termination_grace_period_seconds: k8s_spec
            .termination_grace_period_seconds
            .map(|s| s as u64)
            .unwrap_or(30),
        node_selector,
        priority: k8s_spec.priority,
        runtime_class_name: k8s_spec.runtime_class_name.clone(),
        readiness_gates,
        labels: pod
            .metadata
            .labels
            .as_ref()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default(),
        annotations: pod
            .metadata
            .annotations
            .as_ref()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default(),
        security_context: k8s_spec.security_context.as_ref().map(|sc| {
            use kubelet_core::pod::{PodSecurityContext, Sysctl};
            PodSecurityContext {
                run_as_user: sc.run_as_user.map(|u| u as u32),
                run_as_group: sc.run_as_group.map(|g| g as u32),
                run_as_non_root: sc.run_as_non_root,
                fs_group: sc.fs_group.map(|g| g as u32),
                supplemental_groups: sc
                    .supplemental_groups
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|g| *g as u32)
                    .collect(),
                fs_group_change_policy: sc.fs_group_change_policy.clone(),
                sysctls: sc
                    .sysctls
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|s| Sysctl {
                        name: s.name.clone(),
                        value: s.value.clone(),
                    })
                    .collect(),
                seccomp_profile: sc.seccomp_profile.as_ref().map(|sp| {
                    use kubelet_core::pod::SeccompSpec;
                    SeccompSpec {
                        type_: sp.type_.clone(),
                        localhost_profile: sp.localhost_profile.clone(),
                    }
                }),
            }
        }),
        host_network: k8s_spec.host_network.unwrap_or(false),
        host_pid: k8s_spec.host_pid.unwrap_or(false),
        host_ipc: k8s_spec.host_ipc.unwrap_or(false),
        host_aliases: k8s_spec
            .host_aliases
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|ha| kubelet_core::pod::HostAlias {
                ip: ha.ip.clone().unwrap_or_default(),
                hostnames: ha.hostnames.clone().unwrap_or_default(),
            })
            .collect(),
        observed_start_time: pod
            .status
            .as_ref()
            .and_then(|s| s.start_time.as_ref())
            .map(|t| t.0),
        image_pull_secrets: k8s_spec
            .image_pull_secrets
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|r| kubelet_core::pod::LocalObjectReference {
                name: r.name.clone().unwrap_or_default(),
            })
            .collect(),
        enable_service_links: k8s_spec.enable_service_links,
        share_process_namespace: k8s_spec.share_process_namespace,
        dns_config: {
            let policy = match k8s_spec.dns_policy.as_deref() {
                Some("Default") => DnsPolicy::Default,
                Some("None") => DnsPolicy::None,
                Some("ClusterFirstWithHostNet") => DnsPolicy::ClusterFirstWithHostNet,
                _ => DnsPolicy::ClusterFirst,
            };
            let extra = k8s_spec.dns_config.as_ref();
            let nameservers = extra
                .and_then(|d| d.nameservers.clone())
                .unwrap_or_default();
            let searches = extra.and_then(|d| d.searches.clone()).unwrap_or_default();
            let options = extra
                .and_then(|d| d.options.clone())
                .unwrap_or_default()
                .into_iter()
                .filter_map(|o| {
                    o.name.map(|name| DnsOption {
                        name,
                        value: o.value,
                    })
                })
                .collect();
            Some(DnsConfig {
                policy,
                nameservers,
                searches,
                options,
            })
        },
        hostname: k8s_spec
            .hostname
            .as_deref()
            .filter(|h| !h.is_empty())
            .map(str::to_string),
        subdomain: k8s_spec
            .subdomain
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        generation: pod.metadata.generation,
        ..Default::default()
    })
}

fn k8s_volume_to_spec(v: &k8s_openapi::api::core::v1::Volume) -> Option<VolumeSpec> {
    let source = if let Some(host_path) = &v.host_path {
        VolumeSource::HostPath {
            path: host_path.path.clone(),
            path_type: host_path.type_.clone(),
        }
    } else if let Some(empty_dir) = &v.empty_dir {
        VolumeSource::EmptyDir {
            medium: empty_dir.medium.clone(),
            size_limit: None,
        }
    } else if let Some(config_map) = &v.config_map {
        VolumeSource::ConfigMap {
            name: config_map.name.clone().unwrap_or_default(),
            items: config_map
                .items
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(k8s_key_to_path)
                .collect(),
            optional: config_map.optional.unwrap_or(false),
            default_mode: config_map.default_mode,
        }
    } else if let Some(secret) = &v.secret {
        VolumeSource::Secret {
            secret_name: secret.secret_name.clone().unwrap_or_default(),
            items: secret
                .items
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(k8s_key_to_path)
                .collect(),
            optional: secret.optional.unwrap_or(false),
            default_mode: secret.default_mode,
        }
    } else if let Some(pvc) = &v.persistent_volume_claim {
        VolumeSource::PersistentVolumeClaim {
            claim_name: pvc.claim_name.clone(),
            read_only: pvc.read_only.unwrap_or(false),
        }
    } else if let Some(projected) = &v.projected {
        let sources = projected
            .sources
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter_map(|s| {
                if let Some(token) = &s.service_account_token {
                    Some(ProjectedVolumeSource::ServiceAccountToken {
                        audience: token.audience.clone(),
                        expiration_seconds: token.expiration_seconds.map(|n| n as u64),
                        path: token.path.clone(),
                    })
                } else if let Some(cm) = &s.config_map {
                    Some(ProjectedVolumeSource::ConfigMap {
                        name: cm.name.clone().unwrap_or_default(),
                        items: cm
                            .items
                            .as_deref()
                            .unwrap_or_default()
                            .iter()
                            .map(k8s_key_to_path)
                            .collect(),
                        optional: cm.optional.unwrap_or(false),
                    })
                } else if let Some(secret) = &s.secret {
                    Some(ProjectedVolumeSource::Secret {
                        name: secret.name.clone().unwrap_or_default(),
                        items: secret
                            .items
                            .as_deref()
                            .unwrap_or_default()
                            .iter()
                            .map(k8s_key_to_path)
                            .collect(),
                        optional: secret.optional.unwrap_or(false),
                    })
                } else {
                    s.downward_api
                        .as_ref()
                        .map(|d| ProjectedVolumeSource::DownwardAPI {
                            items: d
                                .items
                                .as_deref()
                                .unwrap_or_default()
                                .iter()
                                .map(k8s_downward_api_file)
                                .collect(),
                        })
                }
            })
            .collect();

        VolumeSource::Projected {
            sources,
            default_mode: projected.default_mode,
        }
    } else if let Some(downward) = &v.downward_api {
        VolumeSource::DownwardAPI {
            items: downward
                .items
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(k8s_downward_api_file)
                .collect(),
            default_mode: downward.default_mode,
        }
    } else {
        warn!(volume = %v.name, "Skipping unsupported volume source in watched pod");
        return None;
    };

    Some(VolumeSpec {
        name: v.name.clone(),
        source,
    })
}

fn parse_k8s_quantity(s: &str) -> Option<ResourceQuantity> {
    let s = s.trim();
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
    if let Some(x) = s.strip_suffix('k') {
        return Some(ResourceQuantity {
            value: x.parse::<i64>().ok()?.saturating_mul(1000),
            unit: ResourceUnit::Bytes,
        });
    }
    if let Some(x) = s.strip_suffix('M') {
        return Some(ResourceQuantity {
            value: x.parse::<i64>().ok()?.saturating_mul(1_000_000),
            unit: ResourceUnit::Bytes,
        });
    }
    if let Some(x) = s.strip_suffix('G') {
        return Some(ResourceQuantity {
            value: x.parse::<i64>().ok()?.saturating_mul(1_000_000_000),
            unit: ResourceUnit::Bytes,
        });
    }
    if let Some(x) = s.strip_suffix('m') {
        return Some(ResourceQuantity {
            value: x.parse::<i64>().ok()?,
            unit: ResourceUnit::Millicores,
        });
    }
    if let Ok(n) = s.parse::<i64>() {
        return Some(ResourceQuantity {
            value: n,
            unit: ResourceUnit::Count,
        });
    }
    None
}

fn parse_k8s_quantity_for_key(key: &str, s: &str) -> Option<ResourceQuantity> {
    let q = parse_k8s_quantity(s)?;
    if key == "cpu" && q.unit == ResourceUnit::Count {
        return Some(ResourceQuantity {
            value: q.value.saturating_mul(1000),
            unit: ResourceUnit::Millicores,
        });
    }
    Some(q)
}

fn k8s_key_to_path(item: &k8s_openapi::api::core::v1::KeyToPath) -> KeyToPath {
    KeyToPath {
        key: item.key.clone(),
        path: item.path.clone(),
        mode: item.mode,
    }
}

fn k8s_downward_api_file(
    item: &k8s_openapi::api::core::v1::DownwardAPIVolumeFile,
) -> DownwardAPIVolumeFile {
    DownwardAPIVolumeFile {
        path: item.path.clone(),
        field_ref: item.field_ref.as_ref().map(|f| f.field_path.clone()),
        resource_field_ref: item.resource_field_ref.as_ref().map(|f| ResourceFieldRef {
            container_name: f.container_name.clone(),
            resource: f.resource.clone(),
            // k8s_openapi Quantity wraps a String
            divisor: f.divisor.as_ref().map(|q| q.0.clone()),
        }),
        mode: item.mode,
    }
}

fn k8s_lifecycle_handler_to_spec(
    h: &k8s_openapi::api::core::v1::LifecycleHandler,
) -> Option<LifecycleHandler> {
    if let Some(exec) = &h.exec {
        Some(LifecycleHandler::Exec {
            command: exec.command.clone().unwrap_or_default(),
        })
    } else if let Some(http) = &h.http_get {
        let port = match &http.port {
            k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(n) => *n as u16,
            k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(_) => 80,
        };
        Some(LifecycleHandler::HttpGet {
            path: http.path.clone().unwrap_or_else(|| "/".to_string()),
            port,
            host: http.host.clone(),
            scheme: http.scheme.clone().unwrap_or_else(|| "HTTP".to_string()),
        })
    } else if let Some(tcp) = &h.tcp_socket {
        let port = match &tcp.port {
            k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(n) => *n as u16,
            k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(_) => 0,
        };
        Some(LifecycleHandler::TcpSocket {
            port,
            host: tcp.host.clone(),
        })
    } else {
        h.sleep.as_ref().map(|sleep| LifecycleHandler::Sleep {
            seconds: sleep.seconds.max(0) as u64,
        })
    }
}

fn resolve_intorstring_port(
    port: &k8s_openapi::apimachinery::pkg::util::intstr::IntOrString,
    container_ports: &[ContainerPort],
) -> u16 {
    match port {
        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(n) => *n as u16,
        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(name) => {
            // Resolve named port against the container's declared port list.
            container_ports
                .iter()
                .find(|p| p.name.as_deref() == Some(name.as_str()))
                .map(|p| p.container_port)
                .unwrap_or(0)
        }
    }
}

fn k8s_probe_to_spec(
    p: &k8s_openapi::api::core::v1::Probe,
    container_ports: &[ContainerPort],
) -> Option<Probe> {
    let handler = if let Some(exec) = &p.exec {
        ProbeHandler::Exec {
            command: exec.command.clone().unwrap_or_default(),
        }
    } else if let Some(http) = &p.http_get {
        let port = resolve_intorstring_port(&http.port, container_ports);
        ProbeHandler::HttpGet {
            path: http.path.clone().unwrap_or_else(|| "/".to_string()),
            port,
            host: http.host.clone(),
            scheme: http.scheme.clone().unwrap_or_else(|| "HTTP".to_string()),
        }
    } else if let Some(tcp) = &p.tcp_socket {
        let port = resolve_intorstring_port(&tcp.port, container_ports);
        ProbeHandler::TcpSocket {
            port,
            host: tcp.host.clone(),
        }
    } else if let Some(grpc) = &p.grpc {
        ProbeHandler::Grpc {
            port: grpc.port as u16,
            service: grpc.service.clone(),
        }
    } else {
        return None;
    };
    Some(Probe {
        handler,
        initial_delay_seconds: p.initial_delay_seconds.unwrap_or(0) as u32,
        period_seconds: p.period_seconds.unwrap_or(10) as u32,
        timeout_seconds: p.timeout_seconds.unwrap_or(1) as u32,
        success_threshold: p.success_threshold.unwrap_or(1) as u32,
        failure_threshold: p.failure_threshold.unwrap_or(3) as u32,
    })
}

/// Convert a k8s EphemeralContainer to our ContainerSpec.
/// EphemeralContainer has the same fields as Container (without resource/port restrictions).
fn k8s_ephemeral_container_to_spec(
    c: &k8s_openapi::api::core::v1::EphemeralContainer,
) -> ContainerSpec {
    // Reuse the container conversion by remapping fields
    use kubelet_core::pod::{
        AppArmorSpec, Capabilities, EnvFromRef, EnvFromSource, EnvVar, EnvVarSource,
        ImagePullPolicy, Lifecycle, ResourceRequirements, SeccompSpec, SecurityContext,
        VolumeMount,
    };

    let image_pull_policy = match c.image_pull_policy.as_deref() {
        Some("Never") => ImagePullPolicy::Never,
        Some("Always") => ImagePullPolicy::Always,
        _ => ImagePullPolicy::IfNotPresent,
    };

    let env: Vec<EnvVar> = c
        .env
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|e| EnvVar {
            name: e.name.clone(),
            value: e.value.clone(),
            value_from: e.value_from.as_ref().and_then(|vf| {
                if let Some(f) = &vf.field_ref {
                    Some(EnvVarSource::FieldRef {
                        field_path: f.field_path.clone(),
                    })
                } else if let Some(r) = &vf.resource_field_ref {
                    Some(EnvVarSource::ResourceFieldRef {
                        container_name: r.container_name.clone(),
                        resource: r.resource.clone(),
                    })
                } else if let Some(cm) = &vf.config_map_key_ref {
                    Some(EnvVarSource::ConfigMapKeyRef {
                        name: cm.name.clone().unwrap_or_default(),
                        key: cm.key.clone(),
                        optional: cm.optional.unwrap_or(false),
                    })
                } else {
                    vf.secret_key_ref
                        .as_ref()
                        .map(|s| EnvVarSource::SecretKeyRef {
                            name: s.name.clone().unwrap_or_default(),
                            key: s.key.clone(),
                            optional: s.optional.unwrap_or(false),
                        })
                }
            }),
        })
        .collect();

    let env_from: Vec<EnvFromSource> = c
        .env_from
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|ef| EnvFromSource {
            prefix: ef.prefix.clone(),
            config_map_ref: ef.config_map_ref.as_ref().map(|r| EnvFromRef {
                name: r.name.clone().unwrap_or_default(),
                optional: r.optional.unwrap_or(false),
            }),
            secret_ref: ef.secret_ref.as_ref().map(|r| EnvFromRef {
                name: r.name.clone().unwrap_or_default(),
                optional: r.optional.unwrap_or(false),
            }),
        })
        .collect();

    let volume_mounts: Vec<VolumeMount> = c
        .volume_mounts
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|m| VolumeMount {
            name: m.name.clone(),
            mount_path: m.mount_path.clone(),
            sub_path: m.sub_path.clone(),
            sub_path_expr: m.sub_path_expr.clone(),
            read_only: m.read_only.unwrap_or(false),
        })
        .collect();

    ContainerSpec {
        name: c.name.clone(),
        image: c.image.clone().unwrap_or_default(),
        image_pull_policy,
        command: c.command.clone().unwrap_or_default(),
        args: c.args.clone().unwrap_or_default(),
        env,
        env_from,
        volume_mounts,
        resources: ResourceRequirements::default(),
        working_dir: c.working_dir.clone(),
        stdin: c.stdin,
        tty: c.tty,
        termination_message_path: c.termination_message_path.clone(),
        termination_message_policy: c.termination_message_policy.clone(),
        ports: vec![],
        liveness_probe: None,
        readiness_probe: None,
        startup_probe: None,
        security_context: c.security_context.as_ref().map(|sc| SecurityContext {
            run_as_user: sc.run_as_user.map(|n| n as u32),
            run_as_group: sc.run_as_group.map(|n| n as u32),
            run_as_non_root: sc.run_as_non_root,
            privileged: sc.privileged,
            read_only_root_filesystem: sc.read_only_root_filesystem,
            allow_privilege_escalation: sc.allow_privilege_escalation,
            capabilities: sc.capabilities.as_ref().map(|caps| Capabilities {
                add: caps.add.as_deref().unwrap_or_default().to_vec(),
                drop: caps.drop.as_deref().unwrap_or_default().to_vec(),
            }),
            seccomp_profile: sc.seccomp_profile.as_ref().map(|sp| SeccompSpec {
                type_: sp.type_.clone(),
                localhost_profile: sp.localhost_profile.clone(),
            }),
            apparmor_profile: sc.app_armor_profile.as_ref().map(|ap| AppArmorSpec {
                type_: ap.type_.clone(),
                localhost_profile: ap.localhost_profile.clone(),
            }),
            proc_mount: sc.proc_mount.clone(),
        }),
        lifecycle: None,
        restart_policy: None,
        ..Default::default()
    }
}

fn k8s_container_to_spec(c: &k8s_openapi::api::core::v1::Container) -> ContainerSpec {
    let image_pull_policy = match c.image_pull_policy.as_deref() {
        Some("Never") => ImagePullPolicy::Never,
        Some("Always") => ImagePullPolicy::Always,
        _ => ImagePullPolicy::IfNotPresent,
    };

    let env: Vec<EnvVar> = c
        .env
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|e| EnvVar {
            name: e.name.clone(),
            value: e.value.clone(),
            value_from: e.value_from.as_ref().and_then(|vf| {
                if let Some(f) = &vf.field_ref {
                    Some(EnvVarSource::FieldRef {
                        field_path: f.field_path.clone(),
                    })
                } else if let Some(r) = &vf.resource_field_ref {
                    Some(EnvVarSource::ResourceFieldRef {
                        container_name: r.container_name.clone(),
                        resource: r.resource.clone(),
                    })
                } else if let Some(cm) = &vf.config_map_key_ref {
                    Some(EnvVarSource::ConfigMapKeyRef {
                        name: cm.name.clone().unwrap_or_default(),
                        key: cm.key.clone(),
                        optional: cm.optional.unwrap_or(false),
                    })
                } else {
                    vf.secret_key_ref
                        .as_ref()
                        .map(|s| EnvVarSource::SecretKeyRef {
                            name: s.name.clone().unwrap_or_default(),
                            key: s.key.clone(),
                            optional: s.optional.unwrap_or(false),
                        })
                }
            }),
        })
        .collect();

    let env_from: Vec<EnvFromSource> = c
        .env_from
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|ef| EnvFromSource {
            prefix: ef.prefix.clone(),
            config_map_ref: ef.config_map_ref.as_ref().map(|r| EnvFromRef {
                name: r.name.clone().unwrap_or_default(),
                optional: r.optional.unwrap_or(false),
            }),
            secret_ref: ef.secret_ref.as_ref().map(|r| EnvFromRef {
                name: r.name.clone().unwrap_or_default(),
                optional: r.optional.unwrap_or(false),
            }),
        })
        .collect();

    let volume_mounts: Vec<VolumeMount> = c
        .volume_mounts
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|m| VolumeMount {
            name: m.name.clone(),
            mount_path: m.mount_path.clone(),
            sub_path: m.sub_path.clone(),
            sub_path_expr: m.sub_path_expr.clone(),
            read_only: m.read_only.unwrap_or(false),
        })
        .collect();

    let resources = c
        .resources
        .as_ref()
        .map(|r| ResourceRequirements {
            requests: r
                .requests
                .as_ref()
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| {
                            parse_k8s_quantity_for_key(k, &v.0).map(|q| (k.clone(), q))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            limits: r
                .limits
                .as_ref()
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| {
                            parse_k8s_quantity_for_key(k, &v.0).map(|q| (k.clone(), q))
                        })
                        .collect()
                })
                .unwrap_or_default(),
        })
        .unwrap_or_default();

    let ports: Vec<ContainerPort> = c
        .ports
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|p| ContainerPort {
            name: p.name.clone(),
            container_port: p.container_port as u16,
            host_port: p.host_port.map(|n| n as u16),
            protocol: match p.protocol.as_deref() {
                Some("UDP") => kubelet_core::pod::Protocol::UDP,
                Some("SCTP") => kubelet_core::pod::Protocol::SCTP,
                _ => kubelet_core::pod::Protocol::TCP,
            },
            host_ip: p.host_ip.clone(),
        })
        .collect();

    ContainerSpec {
        name: c.name.clone(),
        image: c.image.clone().unwrap_or_default(),
        image_pull_policy,
        command: c.command.clone().unwrap_or_default(),
        args: c.args.clone().unwrap_or_default(),
        env,
        env_from,
        volume_mounts,
        resources,
        working_dir: c.working_dir.clone(),
        stdin: c.stdin,
        tty: c.tty,
        termination_message_path: c.termination_message_path.clone(),
        termination_message_policy: c.termination_message_policy.clone(),
        ports: ports.clone(),
        liveness_probe: c
            .liveness_probe
            .as_ref()
            .and_then(|p| k8s_probe_to_spec(p, &ports)),
        readiness_probe: c
            .readiness_probe
            .as_ref()
            .and_then(|p| k8s_probe_to_spec(p, &ports)),
        startup_probe: c
            .startup_probe
            .as_ref()
            .and_then(|p| k8s_probe_to_spec(p, &ports)),
        security_context: c.security_context.as_ref().map(|sc| SecurityContext {
            run_as_user: sc.run_as_user.map(|n| n as u32),
            run_as_group: sc.run_as_group.map(|n| n as u32),
            run_as_non_root: sc.run_as_non_root,
            privileged: sc.privileged,
            read_only_root_filesystem: sc.read_only_root_filesystem,
            allow_privilege_escalation: sc.allow_privilege_escalation,
            capabilities: sc
                .capabilities
                .as_ref()
                .map(|caps| kubelet_core::pod::Capabilities {
                    add: caps.add.as_deref().unwrap_or_default().to_vec(),
                    drop: caps.drop.as_deref().unwrap_or_default().to_vec(),
                }),
            seccomp_profile: sc
                .seccomp_profile
                .as_ref()
                .map(|sp| kubelet_core::pod::SeccompSpec {
                    type_: sp.type_.clone(),
                    localhost_profile: sp.localhost_profile.clone(),
                }),
            apparmor_profile: sc.app_armor_profile.as_ref().map(|ap| {
                kubelet_core::pod::AppArmorSpec {
                    type_: ap.type_.clone(),
                    localhost_profile: ap.localhost_profile.clone(),
                }
            }),
            proc_mount: sc.proc_mount.clone(),
        }),
        restart_policy: match c.restart_policy.as_deref() {
            Some("Never") => Some(kubelet_core::pod::RestartPolicy::Never),
            Some("OnFailure") => Some(kubelet_core::pod::RestartPolicy::OnFailure),
            Some("Always") => Some(kubelet_core::pod::RestartPolicy::Always),
            _ => None,
        },
        lifecycle: c.lifecycle.as_ref().map(|lc| Lifecycle {
            post_start: lc
                .post_start
                .as_ref()
                .and_then(k8s_lifecycle_handler_to_spec),
            pre_stop: lc.pre_stop.as_ref().and_then(k8s_lifecycle_handler_to_spec),
        }),
        ..Default::default()
    }
}

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Timelike};
    use kubelet_core::pod::lifecycle::PodPhase;
    use std::path::PathBuf;

    #[test]
    fn test_build_node_status_patch_has_conditions() {
        use kubelet_core::node::{
            NodeCapacity, NodeCondition, NodeConditionStatus, NodeConditionType,
        };
        let status = NodeStatus {
            name: "node1".to_string(),
            conditions: vec![NodeCondition {
                condition_type: NodeConditionType::Ready,
                status: NodeConditionStatus::True,
                reason: "KubeletReady".to_string(),
                message: "kubelet is ready".to_string(),
                last_heartbeat_time: Utc::now(),
                last_transition_time: Utc::now(),
            }],
            capacity: NodeCapacity {
                cpu_cores: 4.0,
                memory_bytes: 8 * 1024 * 1024 * 1024,
                pods: 110,
                ephemeral_storage_bytes: 100 * 1024 * 1024 * 1024,
                hugepages: Default::default(),
                extended_resources: Default::default(),
            },
            allocatable: Default::default(),
            addresses: vec![],
            system_info: Default::default(),
            images: vec![],
            volumes_attached: vec![],
            volumes_in_use: vec![],
            last_updated: Utc::now(),
        };
        let patch = build_node_status_patch(&status);
        assert_eq!(patch["status"]["conditions"][0]["type"], "Ready");
        assert_eq!(patch["status"]["conditions"][0]["status"], "True");
        assert_eq!(
            patch["status"]["daemonEndpoints"]["kubeletEndpoint"]["Port"],
            10250
        );
    }

    #[test]
    fn test_lifecycle_to_phase() {
        let mut state = PodLifecycleState {
            phase: PodPhase::Running,
            ..Default::default()
        };
        assert_eq!(lifecycle_to_phase(&state), "Running");

        state.phase = PodPhase::Pending;
        assert_eq!(lifecycle_to_phase(&state), "Pending");

        state.phase = PodPhase::Succeeded;
        assert_eq!(lifecycle_to_phase(&state), "Succeeded");
    }

    #[test]
    fn test_kube_connect_mode_detect_no_panic() {
        let _ = KubeConnectMode::detect();
    }

    #[tokio::test]
    async fn test_try_connect_kubeconfig_missing_file_returns_none() {
        let mode = KubeConnectMode::Kubeconfig {
            path: PathBuf::from("/tmp/kube-air-does-not-exist-kubeconfig.yaml"),
        };

        let client = KubeNodeReporter::try_connect(&mode).await;
        assert!(client.is_none());
    }

    #[test]
    fn test_format_lease_renew_time_uses_microseconds() {
        let ts = Utc.with_ymd_and_hms(2026, 5, 19, 11, 46, 40).unwrap();
        let ts = ts.with_nanosecond(315_402_804).unwrap();

        assert_eq!(format_lease_renew_time(ts), "2026-05-19T11:46:40.315402Z");
    }

    // ── Named port resolution in liveness/readiness probes ───────────────────
    //
    // Regression: k8s_probe_to_spec used a hardcoded port 80 for named ports
    // (IntOrString::String). Probes like cert-manager's `http-healthz` (9403)
    // were hitting port 80 instead, causing liveness probe failures and
    // repeated container kills every 81 seconds.

    #[test]
    fn test_k8s_probe_named_port_resolved_from_container_ports() {
        use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
        let ports = vec![
            ContainerPort {
                name: Some("http-metrics".to_string()),
                container_port: 9402,
                host_port: None,
                protocol: kubelet_core::pod::Protocol::TCP,
                host_ip: None,
            },
            ContainerPort {
                name: Some("http-healthz".to_string()),
                container_port: 9403,
                host_port: None,
                protocol: kubelet_core::pod::Protocol::TCP,
                host_ip: None,
            },
        ];

        let k8s_probe = k8s_openapi::api::core::v1::Probe {
            http_get: Some(k8s_openapi::api::core::v1::HTTPGetAction {
                path: Some("/livez".to_string()),
                port: IntOrString::String("http-healthz".to_string()),
                scheme: Some("HTTP".to_string()),
                ..Default::default()
            }),
            initial_delay_seconds: Some(10),
            period_seconds: Some(10),
            timeout_seconds: Some(15),
            failure_threshold: Some(8),
            ..Default::default()
        };

        let probe = k8s_probe_to_spec(&k8s_probe, &ports).unwrap();
        match probe.handler {
            ProbeHandler::HttpGet { port, path, .. } => {
                assert_eq!(
                    port, 9403,
                    "Named port 'http-healthz' must resolve to 9403, not 80"
                );
                assert_eq!(path, "/livez");
            }
            other => panic!("Expected HttpGet, got {:?}", other),
        }
    }

    #[test]
    fn test_k8s_probe_numeric_port_unchanged() {
        use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
        let probe = k8s_openapi::api::core::v1::Probe {
            http_get: Some(k8s_openapi::api::core::v1::HTTPGetAction {
                path: Some("/healthz".to_string()),
                port: IntOrString::Int(8080),
                scheme: Some("HTTP".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let result = k8s_probe_to_spec(&probe, &[]).unwrap();
        match result.handler {
            ProbeHandler::HttpGet { port, .. } => assert_eq!(port, 8080),
            other => panic!("Expected HttpGet, got {:?}", other),
        }
    }

    // ── imagePullSecrets in pod_to_spec ──────────────────────────────────────
    //
    // Regression: pod_to_spec used ..Default::default() which zeroed out
    // image_pull_secrets. All pods ignored their imagePullSecrets, causing
    // private image pulls to fail with NotFound even when credentials existed.

    #[test]
    fn test_pod_to_spec_preserves_image_pull_secrets() {
        use k8s_openapi::api::core::v1::{LocalObjectReference, Pod, PodSpec as K8sPodSpec};
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("my-pod".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("uid-abc".to_string()),
                ..Default::default()
            },
            spec: Some(K8sPodSpec {
                containers: vec![k8s_openapi::api::core::v1::Container {
                    name: "app".to_string(),
                    image: Some("us-east1-docker.pkg.dev/proj/workloads/myapp:v1".to_string()),
                    ..Default::default()
                }],
                node_name: Some("node1".to_string()),
                image_pull_secrets: Some(vec![
                    LocalObjectReference {
                        name: Some("gcp-pull-secret".to_string()),
                    },
                    LocalObjectReference {
                        name: Some("fallback-secret".to_string()),
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let spec = pod_to_spec(&pod, "node1").unwrap();
        assert_eq!(spec.image_pull_secrets.len(), 2);
        assert_eq!(spec.image_pull_secrets[0].name, "gcp-pull-secret");
        assert_eq!(spec.image_pull_secrets[1].name, "fallback-secret");
    }

    #[test]
    fn test_pod_to_spec_no_image_pull_secrets_is_empty() {
        use k8s_openapi::api::core::v1::{Pod, PodSpec as K8sPodSpec};
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("public-pod".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("uid-pub".to_string()),
                ..Default::default()
            },
            spec: Some(K8sPodSpec {
                containers: vec![k8s_openapi::api::core::v1::Container {
                    name: "app".to_string(),
                    image: Some("nginx:1.25".to_string()),
                    ..Default::default()
                }],
                node_name: Some("node1".to_string()),
                image_pull_secrets: None,
                ..Default::default()
            }),
            ..Default::default()
        };

        let spec = pod_to_spec(&pod, "node1").unwrap();
        assert!(spec.image_pull_secrets.is_empty());
    }

    // ── dns_policy in pod_to_spec ────────────────────────────────────────────
    //
    // Regression: pod_to_spec used ..Default::default() and never set
    // dns_config, so every pod — including CoreDNS pods with dnsPolicy:Default
    // — received ClusterFirst semantics and had 10.96.0.10 injected as their
    // nameserver. CoreDNS would then forward to itself, triggering the loop
    // plugin's FATAL crash and entering a crash-loop.

    fn dns_test_pod(dns_policy: Option<&str>) -> Pod {
        use k8s_openapi::api::core::v1::{Pod, PodSpec as K8sPodSpec};
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        Pod {
            metadata: ObjectMeta {
                name: Some("dns-test-pod".to_string()),
                namespace: Some("kube-system".to_string()),
                uid: Some("uid-dns-test".to_string()),
                ..Default::default()
            },
            spec: Some(K8sPodSpec {
                containers: vec![k8s_openapi::api::core::v1::Container {
                    name: "coredns".to_string(),
                    image: Some("registry.k8s.io/coredns/coredns:v1.11.1".to_string()),
                    ..Default::default()
                }],
                node_name: Some("node1".to_string()),
                dns_policy: dns_policy.map(str::to_string),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_pod_to_spec_dns_policy_default() {
        // CoreDNS uses dnsPolicy:Default so it reads the node's /etc/resolv.conf
        // and never contacts itself (10.96.0.10). We must preserve this policy.
        let spec = pod_to_spec(&dns_test_pod(Some("Default")), "node1").unwrap();
        let dns = spec.dns_config.unwrap();
        assert!(
            matches!(dns.policy, kubelet_core::pod::DnsPolicy::Default),
            "dnsPolicy:Default must map to DnsPolicy::Default, got {:?}",
            dns.policy
        );
    }

    #[test]
    fn test_pod_to_spec_dns_policy_cluster_first() {
        let spec = pod_to_spec(&dns_test_pod(Some("ClusterFirst")), "node1").unwrap();
        let dns = spec.dns_config.unwrap();
        assert!(
            matches!(dns.policy, kubelet_core::pod::DnsPolicy::ClusterFirst),
            "dnsPolicy:ClusterFirst must map to DnsPolicy::ClusterFirst"
        );
    }

    #[test]
    fn test_pod_to_spec_dns_policy_none() {
        let spec = pod_to_spec(&dns_test_pod(Some("None")), "node1").unwrap();
        let dns = spec.dns_config.unwrap();
        assert!(matches!(dns.policy, kubelet_core::pod::DnsPolicy::None));
    }

    #[test]
    fn test_pod_to_spec_dns_policy_cluster_first_with_host_net() {
        let spec = pod_to_spec(&dns_test_pod(Some("ClusterFirstWithHostNet")), "node1").unwrap();
        let dns = spec.dns_config.unwrap();
        assert!(matches!(
            dns.policy,
            kubelet_core::pod::DnsPolicy::ClusterFirstWithHostNet
        ));
    }

    #[test]
    fn test_pod_to_spec_dns_policy_missing_defaults_to_cluster_first() {
        // When dnsPolicy is absent, Kubernetes defaults to ClusterFirst.
        let spec = pod_to_spec(&dns_test_pod(None), "node1").unwrap();
        let dns = spec.dns_config.unwrap();
        assert!(matches!(
            dns.policy,
            kubelet_core::pod::DnsPolicy::ClusterFirst
        ));
    }

    #[test]
    fn test_pod_to_spec_dns_config_extra_nameservers_preserved() {
        // dnsPolicy:None with explicit nameservers via spec.dnsConfig must be
        // forwarded through to PodSpec.dns_config.nameservers.
        use k8s_openapi::api::core::v1::{Pod, PodDNSConfig, PodSpec as K8sPodSpec};
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("custom-dns-pod".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("uid-custom-dns".to_string()),
                ..Default::default()
            },
            spec: Some(K8sPodSpec {
                containers: vec![k8s_openapi::api::core::v1::Container {
                    name: "app".to_string(),
                    image: Some("nginx:latest".to_string()),
                    ..Default::default()
                }],
                node_name: Some("node1".to_string()),
                dns_policy: Some("None".to_string()),
                dns_config: Some(PodDNSConfig {
                    nameservers: Some(vec!["1.1.1.1".to_string(), "8.8.8.8".to_string()]),
                    searches: Some(vec!["example.com".to_string()]),
                    options: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let spec = pod_to_spec(&pod, "node1").unwrap();
        let dns = spec.dns_config.unwrap();
        assert!(matches!(dns.policy, kubelet_core::pod::DnsPolicy::None));
        assert_eq!(dns.nameservers, vec!["1.1.1.1", "8.8.8.8"]);
        assert_eq!(dns.searches, vec!["example.com"]);
    }

    #[test]
    fn test_pod_to_spec_propagates_metadata_generation() {
        let pod = k8s_openapi::api::core::v1::Pod {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                name: Some("test-pod".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("abc-123".to_string()),
                generation: Some(3),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::PodSpec {
                containers: vec![k8s_openapi::api::core::v1::Container {
                    name: "c".to_string(),
                    image: Some("nginx:latest".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };

        let spec = pod_to_spec(&pod, "node1").unwrap();
        assert_eq!(spec.generation, Some(3));
    }

    #[test]
    fn test_pod_to_spec_generation_none_when_missing() {
        let pod = k8s_openapi::api::core::v1::Pod {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                name: Some("test-pod".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("abc-123".to_string()),
                generation: None,
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::PodSpec {
                containers: vec![k8s_openapi::api::core::v1::Container {
                    name: "c".to_string(),
                    image: Some("nginx:latest".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };

        let spec = pod_to_spec(&pod, "node1").unwrap();
        assert_eq!(spec.generation, None);
    }
}
