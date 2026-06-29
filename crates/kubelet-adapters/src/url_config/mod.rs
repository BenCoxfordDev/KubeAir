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

//! URL-based static pod source adapter.
//!
//! Polls a `staticPodURL` HTTP endpoint that returns a Kubernetes PodList JSON
//! and emits PodUpdate events for each pod in the list.
//!
//! Mirrors the Go kubelet's `pkg/kubelet/config/http.go` HTTP source.

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};
use kubelet_core::error::Result;
use kubelet_core::pod::{
    Capabilities, ContainerSpec, EnvVar, EnvVarSource, ImagePullPolicy, KeyToPath, PodOperation,
    PodSpec, PodUpdate, ProjectedVolumeSource, ResourceRequirements, RestartPolicy,
    SecurityContext, VolumeMount, VolumeSource, VolumeSpec,
};
use kubelet_core::types::{PodRef, PodUID};
use kubelet_ports::driven::pod_source::PodSource;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// ── Manifest deserialization types ─────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PodList {
    items: Option<Vec<PodManifest>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PodManifest {
    metadata: ManifestMetadata,
    spec: ManifestSpec,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestMetadata {
    name: Option<String>,
    namespace: Option<String>,
    uid: Option<String>,
    annotations: Option<HashMap<String, String>>,
    labels: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestSpec {
    containers: Option<Vec<ManifestContainer>>,
    #[serde(rename = "initContainers")]
    init_containers: Option<Vec<ManifestContainer>>,
    volumes: Option<Vec<ManifestVolume>>,
    #[serde(rename = "restartPolicy")]
    restart_policy: Option<String>,
    #[serde(rename = "hostNetwork")]
    host_network: Option<bool>,
    #[serde(rename = "hostPID")]
    host_pid: Option<bool>,
    #[serde(rename = "hostIPC")]
    host_ipc: Option<bool>,
    #[serde(rename = "nodeName")]
    node_name: Option<String>,
    #[serde(rename = "securityContext")]
    security_context: Option<ManifestPodSecurityContext>,
    #[serde(rename = "priorityClassName")]
    priority_class_name: Option<String>,
    #[serde(rename = "hostAliases")]
    host_aliases: Option<Vec<ManifestHostAlias>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestHostAlias {
    ip: Option<String>,
    hostnames: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestContainer {
    name: String,
    image: String,
    command: Option<Vec<String>>,
    args: Option<Vec<String>>,
    #[serde(rename = "volumeMounts")]
    volume_mounts: Option<Vec<ManifestVolumeMount>>,
    #[serde(rename = "imagePullPolicy")]
    image_pull_policy: Option<String>,
    env: Option<Vec<ManifestEnvVar>>,
    #[serde(rename = "securityContext")]
    security_context: Option<ManifestSecurityContext>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestEnvVar {
    name: String,
    value: Option<String>,
    #[serde(rename = "valueFrom")]
    value_from: Option<ManifestEnvVarSource>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestEnvVarSource {
    #[serde(rename = "fieldRef")]
    field_ref: Option<ManifestFieldRef>,
    #[serde(rename = "resourceFieldRef")]
    resource_field_ref: Option<ManifestResourceFieldRef>,
    #[serde(rename = "configMapKeyRef")]
    config_map_key_ref: Option<ManifestConfigMapKeyRef>,
    #[serde(rename = "secretKeyRef")]
    secret_key_ref: Option<ManifestSecretKeyRef>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestFieldRef {
    #[serde(rename = "fieldPath")]
    field_path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestResourceFieldRef {
    resource: String,
    #[serde(rename = "containerName")]
    container_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestConfigMapKeyRef {
    name: String,
    key: String,
    optional: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestSecretKeyRef {
    name: String,
    key: String,
    optional: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestSecurityContext {
    privileged: Option<bool>,
    capabilities: Option<ManifestCapabilities>,
    #[serde(rename = "runAsUser")]
    run_as_user: Option<u32>,
    #[serde(rename = "runAsGroup")]
    run_as_group: Option<u32>,
    #[serde(rename = "runAsNonRoot")]
    run_as_non_root: Option<bool>,
    #[serde(rename = "readOnlyRootFilesystem")]
    read_only_root_filesystem: Option<bool>,
    #[serde(rename = "allowPrivilegeEscalation")]
    allow_privilege_escalation: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestCapabilities {
    add: Option<Vec<String>>,
    drop: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestPodSecurityContext {
    #[serde(rename = "runAsUser")]
    run_as_user: Option<u32>,
    #[serde(rename = "runAsGroup")]
    run_as_group: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestVolumeMount {
    name: String,
    #[serde(rename = "mountPath")]
    mount_path: String,
    #[serde(rename = "subPath")]
    sub_path: Option<String>,
    #[serde(rename = "readOnly")]
    read_only: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestVolume {
    name: String,
    #[serde(rename = "hostPath")]
    host_path: Option<ManifestHostPath>,
    #[serde(rename = "emptyDir")]
    empty_dir: Option<ManifestEmptyDir>,
    #[serde(rename = "configMap")]
    config_map: Option<ManifestConfigMapVolume>,
    secret: Option<ManifestSecretVolume>,
    projected: Option<ManifestProjectedVolume>,
    #[serde(rename = "persistentVolumeClaim")]
    pvc: Option<ManifestPvc>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestHostPath {
    path: String,
    #[serde(rename = "type")]
    path_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestEmptyDir {
    medium: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestConfigMapVolume {
    name: Option<String>,
    items: Option<Vec<ManifestKeyToPath>>,
    optional: Option<bool>,
    #[serde(rename = "defaultMode")]
    default_mode: Option<i32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestSecretVolume {
    #[serde(rename = "secretName")]
    secret_name: Option<String>,
    items: Option<Vec<ManifestKeyToPath>>,
    optional: Option<bool>,
    #[serde(rename = "defaultMode")]
    default_mode: Option<i32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestProjectedVolume {
    sources: Option<Vec<ManifestProjectedSource>>,
    #[serde(rename = "defaultMode")]
    default_mode: Option<i32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestProjectedSource {
    #[serde(rename = "serviceAccountToken")]
    service_account_token: Option<ManifestSaTokenProjection>,
    #[serde(rename = "configMap")]
    config_map: Option<ManifestCmProjection>,
    secret: Option<ManifestSecretProjection>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestSaTokenProjection {
    audience: Option<String>,
    #[serde(rename = "expirationSeconds")]
    expiration_seconds: Option<u64>,
    path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestCmProjection {
    name: Option<String>,
    items: Option<Vec<ManifestKeyToPath>>,
    optional: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestSecretProjection {
    name: Option<String>,
    items: Option<Vec<ManifestKeyToPath>>,
    optional: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestPvc {
    #[serde(rename = "claimName")]
    claim_name: String,
    #[serde(rename = "readOnly")]
    read_only: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestKeyToPath {
    key: String,
    path: String,
    mode: Option<i32>,
}

// ── UrlPodSource ────────────────────────────────────────────────────────────

/// Polls a HTTP URL that returns a Kubernetes PodList JSON and emits updates.
pub struct UrlPodSource {
    url: String,
    node_name: String,
    poll_interval: Duration,
    client: reqwest::Client,
    /// Optional kube client used to look up mirror pod UIDs from the API
    /// server, so the URL source uses the same UID as the API server watcher.
    kube_client: Option<kube::Client>,
}

impl UrlPodSource {
    pub async fn new(
        url: impl Into<String>,
        node_name: impl Into<String>,
        poll_interval: Duration,
    ) -> Self {
        let kube_client = kube::Client::try_default().await.ok();
        if kube_client.is_some() {
            info!("URL pod source: kube client connected for mirror-pod UID resolution");
        } else {
            warn!(
                "URL pod source: no kube client; will use synthetic UIDs (risk of sandbox churn)"
            );
        }
        Self {
            url: url.into(),
            node_name: node_name.into(),
            poll_interval,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build HTTP client"),
            kube_client,
        }
    }

    fn parse_pod(&self, manifest: PodManifest) -> Option<PodSpec> {
        let name = manifest
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let namespace = manifest
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "kube-system".to_string());
        let uid = manifest.metadata.uid.clone().unwrap_or_else(|| {
            // Stable deterministic UID from name+namespace
            Uuid::new_v5(
                &Uuid::NAMESPACE_URL,
                format!("{}/{}", namespace, name).as_bytes(),
            )
            .to_string()
        });

        let restart_policy = match manifest.spec.restart_policy.as_deref() {
            Some("Never") => RestartPolicy::Never,
            Some("OnFailure") => RestartPolicy::OnFailure,
            _ => RestartPolicy::Always,
        };

        let volumes = parse_volumes(manifest.spec.volumes.unwrap_or_default());
        let containers = parse_containers(manifest.spec.containers.unwrap_or_default());
        let init_containers = parse_containers(manifest.spec.init_containers.unwrap_or_default());

        // Go kubelet appends -<nodename> to static pod names so they are
        // unique per node and match CRI sandbox labels created by any kubelet.
        let static_name = format!("{}-{}", name, self.node_name);

        Some(PodSpec {
            uid: PodUID::new(uid),
            pod_ref: PodRef::new(namespace, static_name),
            containers,
            init_containers,
            ephemeral_containers: vec![],
            volumes,
            node_name: manifest
                .spec
                .node_name
                .unwrap_or_else(|| self.node_name.clone()),
            host_network: manifest.spec.host_network.unwrap_or(false),
            host_pid: manifest.spec.host_pid.unwrap_or(false),
            host_ipc: manifest.spec.host_ipc.unwrap_or(false),
            dns_config: None,
            restart_policy,
            termination_grace_period_seconds: 30,
            service_account_name: "default".to_string(),
            priority: None,
            tolerations: vec![],
            node_selector: Default::default(),
            annotations: manifest.metadata.annotations.unwrap_or_default(),
            labels: manifest.metadata.labels.unwrap_or_default(),
            runtime_class_name: None,
            security_context: None,
            readiness_gates: vec![],
            active_deadline_seconds: None,
            automount_service_account_token: None,
            image_pull_secrets: vec![],
            enable_service_links: None,
            share_process_namespace: None,
            resource_claims: vec![],
            host_aliases: manifest
                .spec
                .host_aliases
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|ha| kubelet_core::pod::HostAlias {
                    ip: ha.ip.clone().unwrap_or_default(),
                    hostnames: ha.hostnames.clone().unwrap_or_default(),
                })
                .collect(),
            hostname: None,
            subdomain: None,
            observed_start_time: None,
            generation: None,
        })
    }
}

fn parse_containers(containers: Vec<ManifestContainer>) -> Vec<ContainerSpec> {
    containers
        .into_iter()
        .map(|c| {
            let env: Vec<EnvVar> = c
                .env
                .unwrap_or_default()
                .into_iter()
                .map(|e| EnvVar {
                    name: e.name,
                    value: e.value,
                    value_from: e.value_from.and_then(|vf| {
                        if let Some(fr) = vf.field_ref {
                            Some(EnvVarSource::FieldRef {
                                field_path: fr.field_path,
                            })
                        } else if let Some(rf) = vf.resource_field_ref {
                            Some(EnvVarSource::ResourceFieldRef {
                                container_name: rf.container_name,
                                resource: rf.resource,
                            })
                        } else if let Some(cm) = vf.config_map_key_ref {
                            Some(EnvVarSource::ConfigMapKeyRef {
                                name: cm.name,
                                key: cm.key,
                                optional: cm.optional.unwrap_or(false),
                            })
                        } else if let Some(s) = vf.secret_key_ref {
                            Some(EnvVarSource::SecretKeyRef {
                                name: s.name,
                                key: s.key,
                                optional: s.optional.unwrap_or(false),
                            })
                        } else {
                            None
                        }
                    }),
                })
                .collect();

            let security_context = c.security_context.map(|sc| SecurityContext {
                privileged: sc.privileged,
                capabilities: sc.capabilities.map(|caps| Capabilities {
                    add: caps.add.unwrap_or_default(),
                    drop: caps.drop.unwrap_or_default(),
                }),
                run_as_user: sc.run_as_user,
                run_as_group: sc.run_as_group,
                run_as_non_root: sc.run_as_non_root,
                read_only_root_filesystem: sc.read_only_root_filesystem,
                allow_privilege_escalation: sc.allow_privilege_escalation,
                seccomp_profile: None,
                apparmor_profile: None,
                proc_mount: None,
            });

            ContainerSpec {
                name: c.name,
                image: c.image,
                command: c.command.unwrap_or_default(),
                args: c.args.unwrap_or_default(),
                working_dir: None,
                ports: vec![],
                env,
                resources: ResourceRequirements::default(),
                volume_mounts: c
                    .volume_mounts
                    .unwrap_or_default()
                    .into_iter()
                    .map(|m| VolumeMount {
                        name: m.name,
                        mount_path: m.mount_path,
                        sub_path: m.sub_path,
                        sub_path_expr: None,
                        read_only: m.read_only.unwrap_or(false),
                    })
                    .collect(),
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                image_pull_policy: match c.image_pull_policy.as_deref() {
                    Some("Always") => ImagePullPolicy::Always,
                    Some("Never") => ImagePullPolicy::Never,
                    _ => ImagePullPolicy::IfNotPresent,
                },
                security_context,
                termination_message_path: None,
                termination_message_policy: None,
                lifecycle: None,
                env_from: vec![],
                stdin: None,
                stdin_once: None,
                tty: None,
                restart_policy: None,
            }
        })
        .collect()
}

fn parse_volumes(volumes: Vec<ManifestVolume>) -> Vec<VolumeSpec> {
    volumes
        .into_iter()
        .filter_map(|v| {
            let source = if let Some(hp) = v.host_path {
                VolumeSource::HostPath {
                    path: hp.path,
                    path_type: hp.path_type,
                }
            } else if let Some(ed) = v.empty_dir {
                VolumeSource::EmptyDir {
                    medium: ed.medium,
                    size_limit: None,
                }
            } else if let Some(cm) = v.config_map {
                VolumeSource::ConfigMap {
                    name: cm.name.unwrap_or_default(),
                    items: cm
                        .items
                        .unwrap_or_default()
                        .into_iter()
                        .map(|k| KeyToPath {
                            key: k.key,
                            path: k.path,
                            mode: k.mode,
                        })
                        .collect(),
                    optional: cm.optional.unwrap_or(false),
                    default_mode: cm.default_mode,
                }
            } else if let Some(s) = v.secret {
                VolumeSource::Secret {
                    secret_name: s.secret_name.unwrap_or_default(),
                    items: s
                        .items
                        .unwrap_or_default()
                        .into_iter()
                        .map(|k| KeyToPath {
                            key: k.key,
                            path: k.path,
                            mode: k.mode,
                        })
                        .collect(),
                    optional: s.optional.unwrap_or(false),
                    default_mode: s.default_mode,
                }
            } else if let Some(proj) = v.projected {
                let sources = proj
                    .sources
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|s| {
                        if let Some(sat) = s.service_account_token {
                            Some(ProjectedVolumeSource::ServiceAccountToken {
                                audience: sat.audience,
                                expiration_seconds: sat.expiration_seconds,
                                path: sat.path,
                            })
                        } else if let Some(cm) = s.config_map {
                            Some(ProjectedVolumeSource::ConfigMap {
                                name: cm.name.unwrap_or_default(),
                                items: cm
                                    .items
                                    .unwrap_or_default()
                                    .into_iter()
                                    .map(|k| KeyToPath {
                                        key: k.key,
                                        path: k.path,
                                        mode: k.mode,
                                    })
                                    .collect(),
                                optional: cm.optional.unwrap_or(false),
                            })
                        } else if let Some(sec) = s.secret {
                            Some(ProjectedVolumeSource::Secret {
                                name: sec.name.unwrap_or_default(),
                                items: sec
                                    .items
                                    .unwrap_or_default()
                                    .into_iter()
                                    .map(|k| KeyToPath {
                                        key: k.key,
                                        path: k.path,
                                        mode: k.mode,
                                    })
                                    .collect(),
                                optional: sec.optional.unwrap_or(false),
                            })
                        } else {
                            None
                        }
                    })
                    .collect();
                VolumeSource::Projected {
                    sources,
                    default_mode: proj.default_mode,
                }
            } else if let Some(pvc) = v.pvc {
                VolumeSource::PersistentVolumeClaim {
                    claim_name: pvc.claim_name,
                    read_only: pvc.read_only.unwrap_or(false),
                }
            } else {
                warn!(volume = %v.name, "Skipping unsupported volume source in URL static pod");
                return None;
            };

            Some(VolumeSpec {
                name: v.name,
                source,
            })
        })
        .collect()
}

#[async_trait]
impl PodSource for UrlPodSource {
    fn name(&self) -> &str {
        "url"
    }

    async fn run(&self, tx: mpsc::Sender<PodUpdate>) -> Result<()> {
        info!(url = %self.url, "Starting URL pod source");
        let mut known: HashMap<String, PodUID> = HashMap::new(); // uid_str → PodUID
        // Maps "namespace/name" → committed UID string. Once we commit a UID
        // (real from API server or synthetic fallback) we keep using it for the
        // lifetime of that pod entry. This prevents UID flip-flop when the API
        // server becomes reachable after an initial failure at startup, which
        // would otherwise cause sandbox churn. The entry is cleared when the pod
        // disappears from the URL endpoint.
        let mut committed_uids: HashMap<String, String> = HashMap::new();

        loop {
            match self.client.get(&self.url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<PodList>().await {
                        Ok(pod_list) => {
                            let mut current_uids: std::collections::HashSet<String> =
                                std::collections::HashSet::new();

                            for mut item in pod_list.items.unwrap_or_default() {
                                let ns = item
                                    .metadata
                                    .namespace
                                    .as_deref()
                                    .unwrap_or("kube-system")
                                    .to_string();
                                let name = item
                                    .metadata
                                    .name
                                    .as_deref()
                                    .unwrap_or("unknown")
                                    .to_string();
                                let pod_key = format!("{}/{}", ns, name);

                                // Resolve UID: honour any UID already in the manifest,
                                // or use the committed UID if we've already latched one,
                                // or query the API server once for the mirror pod UID.
                                if item.metadata.uid.is_none() {
                                    if let Some(committed) = committed_uids.get(&pod_key) {
                                        // Reuse the UID we already committed for this pod.
                                        item.metadata.uid = Some(committed.clone());
                                    } else if let Some(kube) = &self.kube_client {
                                        let mirror_name = format!("{}-{}", name, self.node_name);
                                        let api: Api<Pod> = Api::namespaced(kube.clone(), &ns);
                                        match api.get_opt(&mirror_name).await {
                                            Ok(Some(mirror_pod)) => {
                                                if let Some(uid) = mirror_pod.metadata.uid {
                                                    debug!(
                                                        pod = %mirror_name,
                                                        uid = %uid,
                                                        "URL source resolved mirror pod UID from API server"
                                                    );
                                                    item.metadata.uid = Some(uid);
                                                }
                                            }
                                            Ok(None) => {
                                                debug!(
                                                    pod = %mirror_name,
                                                    "Mirror pod not yet in API server; using synthetic UID"
                                                );
                                            }
                                            Err(e) => {
                                                warn!(
                                                    pod = %mirror_name,
                                                    error = %e,
                                                    "Failed to look up mirror pod UID; using synthetic UID"
                                                );
                                            }
                                        }
                                    }
                                }

                                let uid_str = item.metadata.uid.clone().unwrap_or_else(|| {
                                    Uuid::new_v5(&Uuid::NAMESPACE_URL, pod_key.as_bytes())
                                        .to_string()
                                });

                                // Commit the resolved UID so subsequent polls use the same UID
                                // even if the API server becomes reachable (avoids flip-flop).
                                committed_uids
                                    .entry(pod_key.clone())
                                    .or_insert_with(|| uid_str.clone());

                                current_uids.insert(uid_str.clone());

                                if let Some(pod) = self.parse_pod(item) {
                                    let pod_uid = pod.uid.clone();
                                    let op = if known.get(&uid_str) == Some(&pod_uid) {
                                        PodOperation::Reconcile
                                    } else if known.contains_key(&uid_str) {
                                        PodOperation::Update
                                    } else {
                                        PodOperation::Add
                                    };
                                    known.insert(uid_str, pod_uid);
                                    if tx.send(PodUpdate { pod, op }).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }

                            // Detect removed pods
                            let removed: Vec<String> = known
                                .keys()
                                .filter(|u| !current_uids.contains(*u))
                                .cloned()
                                .collect();

                            // Also clear committed_uids for pods that disappeared.
                            committed_uids.retain(|_key, uid| current_uids.contains(uid));

                            for uid_str in removed {
                                if let Some(uid) = known.remove(&uid_str) {
                                    debug!(uid = %uid_str, "URL static pod removed");
                                    let pod = PodSpec {
                                        uid: uid.clone(),
                                        pod_ref: PodRef::new("kube-system", "removed"),
                                        containers: vec![],
                                        init_containers: vec![],
                                        ephemeral_containers: vec![],
                                        volumes: vec![],
                                        node_name: self.node_name.clone(),
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
                                        generation: None,
                                    };
                                    if tx
                                        .send(PodUpdate {
                                            pod,
                                            op: PodOperation::Remove,
                                        })
                                        .await
                                        .is_err()
                                    {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(url = %self.url, error = %e, "Failed to parse PodList JSON from URL pod source");
                        }
                    }
                }
                Ok(resp) => {
                    warn!(url = %self.url, status = %resp.status(), "URL pod source returned non-success status");
                }
                Err(e) => {
                    debug!(url = %self.url, error = %e, "URL pod source fetch failed (will retry)");
                }
            }

            tokio::time::sleep(self.poll_interval).await;
        }
    }
}
