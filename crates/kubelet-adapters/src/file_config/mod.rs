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

//! File-based static pod source adapter.
//!
//! Watches a directory for pod manifest YAML/JSON files and emits PodUpdate events.

use async_trait::async_trait;
use kubelet_core::error::{KubeletError, Result};
use kubelet_core::pod::{
    ContainerSpec, ImagePullPolicy, KeyToPath, PodOperation, PodSpec, PodUpdate,
    ProjectedVolumeSource, ResourceRequirements, RestartPolicy, VolumeMount, VolumeSource,
    VolumeSpec,
};
use kubelet_core::types::{PodRef, PodUID};
use kubelet_ports::driven::pod_source::PodSource;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Minimal pod manifest schema for file-based static pods.
/// This matches the Kubernetes Pod API object subset we handle.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct PodManifest {
    #[serde(rename = "apiVersion")]
    api_version: Option<String>,
    kind: Option<String>,
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
    #[serde(rename = "nodeName")]
    node_name: Option<String>,
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
    host_path: Option<ManifestHostPathVolumeSource>,
    #[serde(rename = "emptyDir")]
    empty_dir: Option<ManifestEmptyDirVolumeSource>,
    #[serde(rename = "configMap")]
    config_map: Option<ManifestConfigMapVolumeSource>,
    secret: Option<ManifestSecretVolumeSource>,
    #[serde(rename = "persistentVolumeClaim")]
    persistent_volume_claim: Option<ManifestPersistentVolumeClaimVolumeSource>,
    projected: Option<ManifestProjectedVolumeSource>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestHostPathVolumeSource {
    path: String,
    #[serde(rename = "type")]
    path_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestEmptyDirVolumeSource {
    medium: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestConfigMapVolumeSource {
    name: Option<String>,
    items: Option<Vec<ManifestKeyToPath>>,
    optional: Option<bool>,
    #[serde(rename = "defaultMode")]
    default_mode: Option<i32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestSecretVolumeSource {
    #[serde(rename = "secretName")]
    secret_name: Option<String>,
    items: Option<Vec<ManifestKeyToPath>>,
    optional: Option<bool>,
    #[serde(rename = "defaultMode")]
    default_mode: Option<i32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestPersistentVolumeClaimVolumeSource {
    #[serde(rename = "claimName")]
    claim_name: String,
    #[serde(rename = "readOnly")]
    read_only: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestProjectedVolumeSource {
    sources: Option<Vec<ManifestProjectedSource>>,
    #[serde(rename = "defaultMode")]
    default_mode: Option<i32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestProjectedSource {
    #[serde(rename = "serviceAccountToken")]
    service_account_token: Option<ManifestServiceAccountTokenProjection>,
    #[serde(rename = "configMap")]
    config_map: Option<ManifestConfigMapProjection>,
    secret: Option<ManifestSecretProjection>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestServiceAccountTokenProjection {
    audience: Option<String>,
    #[serde(rename = "expirationSeconds")]
    expiration_seconds: Option<u64>,
    path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ManifestConfigMapProjection {
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
struct ManifestKeyToPath {
    key: String,
    path: String,
    mode: Option<i32>,
}

/// Polls a directory for static pod manifests and emits updates.
pub struct FilePodSource {
    path: PathBuf,
    node_name: String,
    poll_interval: Duration,
}

impl FilePodSource {
    pub fn new(
        path: impl Into<PathBuf>,
        node_name: impl Into<String>,
        poll_interval: Duration,
    ) -> Self {
        Self {
            path: path.into(),
            node_name: node_name.into(),
            poll_interval,
        }
    }

    fn parse_manifest(&self, content: &str, file_path: &Path) -> Option<PodSpec> {
        let manifest: PodManifest = if file_path.extension().map(|e| e == "json").unwrap_or(false) {
            match serde_json::from_str(content) {
                Ok(m) => m,
                Err(e) => {
                    error!(path = %file_path.display(), error = %e, "Failed to deserialize JSON pod manifest");
                    return None;
                }
            }
        } else {
            match serde_yaml::from_str(content) {
                Ok(m) => m,
                Err(e) => {
                    error!(path = %file_path.display(), error = %e, "Failed to deserialize YAML pod manifest");
                    return None;
                }
            }
        };

        let name = manifest
            .metadata
            .name
            .clone()
            .or_else(|| {
                file_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_else(|| "unknown".to_string());

        let namespace = manifest
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let uid = manifest.metadata.uid.clone().unwrap_or_else(|| {
            // Derive a stable deterministic UUID from the file path so that
            // pods without an explicit UID don't churn between poll cycles.
            let path_str = file_path.to_string_lossy();
            Uuid::new_v5(&Uuid::NAMESPACE_URL, path_str.as_bytes()).to_string()
        });

        let restart_policy = match manifest.spec.restart_policy.as_deref() {
            Some("Never") => RestartPolicy::Never,
            Some("OnFailure") => RestartPolicy::OnFailure,
            _ => RestartPolicy::Always,
        };

        let volumes: Vec<VolumeSpec> = manifest
            .spec
            .volumes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| {
                let source = if let Some(host_path) = v.host_path {
                    VolumeSource::HostPath {
                        path: host_path.path,
                        path_type: host_path.path_type,
                    }
                } else if let Some(empty_dir) = v.empty_dir {
                    VolumeSource::EmptyDir {
                        medium: empty_dir.medium,
                        size_limit: None,
                    }
                } else if let Some(config_map) = v.config_map {
                    VolumeSource::ConfigMap {
                        name: config_map.name.unwrap_or_default(),
                        items: config_map.items.unwrap_or_default().into_iter().map(manifest_key_to_path).collect(),
                        optional: config_map.optional.unwrap_or(false),
                        default_mode: config_map.default_mode,
                    }
                } else if let Some(secret) = v.secret {
                    VolumeSource::Secret {
                        secret_name: secret.secret_name.unwrap_or_default(),
                        items: secret.items.unwrap_or_default().into_iter().map(manifest_key_to_path).collect(),
                        optional: secret.optional.unwrap_or(false),
                        default_mode: secret.default_mode,
                    }
                } else if let Some(pvc) = v.persistent_volume_claim {
                    VolumeSource::PersistentVolumeClaim {
                        claim_name: pvc.claim_name,
                        read_only: pvc.read_only.unwrap_or(false),
                    }
                } else if let Some(projected) = v.projected {
                    let projected_sources = projected
                        .sources
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|s| {
                            if let Some(token) = s.service_account_token {
                                Some(ProjectedVolumeSource::ServiceAccountToken {
                                    audience: token.audience,
                                    expiration_seconds: token.expiration_seconds,
                                    path: token.path,
                                })
                            } else if let Some(cm) = s.config_map {
                                Some(ProjectedVolumeSource::ConfigMap {
                                    name: cm.name.unwrap_or_default(),
                                    items: cm.items.unwrap_or_default().into_iter().map(manifest_key_to_path).collect(),
                                    optional: cm.optional.unwrap_or(false),
                                })
                            } else if let Some(secret) = s.secret {
                                Some(ProjectedVolumeSource::Secret {
                                    name: secret.name.unwrap_or_default(),
                                    items: secret.items.unwrap_or_default().into_iter().map(manifest_key_to_path).collect(),
                                    optional: secret.optional.unwrap_or(false),
                                })
                            } else {
                                None
                            }
                        })
                        .collect();

                    VolumeSource::Projected {
                        sources: projected_sources,
                        default_mode: projected.default_mode,
                    }
                } else {
                    warn!(volume = %v.name, "Skipping unsupported volume source in static pod manifest");
                    return None;
                };

                Some(VolumeSpec { name: v.name, source })
            })
            .collect();

        let containers: Vec<ContainerSpec> = manifest
            .spec
            .containers
            .unwrap_or_default()
            .into_iter()
            .map(|c| ContainerSpec {
                name: c.name,
                image: c.image,
                command: c.command.unwrap_or_default(),
                args: c.args.unwrap_or_default(),
                working_dir: None,
                ports: vec![],
                env: vec![],
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
                security_context: None,
                termination_message_path: None,
                termination_message_policy: None,
                lifecycle: None,
                env_from: vec![],
                stdin: None,
                stdin_once: None,
                tty: None,
                restart_policy: None,
            })
            .collect();

        let init_containers: Vec<ContainerSpec> = manifest
            .spec
            .init_containers
            .unwrap_or_default()
            .into_iter()
            .map(|c| ContainerSpec {
                name: c.name,
                image: c.image,
                command: c.command.unwrap_or_default(),
                args: c.args.unwrap_or_default(),
                working_dir: None,
                ports: vec![],
                env: vec![],
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
                security_context: None,
                termination_message_path: None,
                termination_message_policy: None,
                lifecycle: None,
                env_from: vec![],
                stdin: None,
                stdin_once: None,
                tty: None,
                restart_policy: None,
            })
            .collect();

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
            host_pid: false,
            host_ipc: false,
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

fn manifest_key_to_path(item: ManifestKeyToPath) -> KeyToPath {
    KeyToPath {
        key: item.key,
        path: item.path,
        mode: item.mode,
    }
}

#[async_trait]
impl PodSource for FilePodSource {
    fn name(&self) -> &str {
        "file"
    }

    async fn run(&self, tx: mpsc::Sender<PodUpdate>) -> Result<()> {
        info!(path = %self.path.display(), "Starting file pod source");
        let mut known: HashMap<PathBuf, PodUID> = HashMap::new();

        loop {
            if !self.path.exists() {
                tokio::time::sleep(self.poll_interval).await;
                continue;
            }

            let mut current_paths: HashSet<PathBuf> = HashSet::new();

            let read_dir = tokio::fs::read_dir(&self.path).await;
            match read_dir {
                Ok(mut dir) => {
                    while let Ok(Some(entry)) = dir.next_entry().await {
                        let file_path = entry.path();
                        let ext = file_path.extension().and_then(|e| e.to_str()).unwrap_or("");
                        if !["yaml", "yml", "json"].contains(&ext) {
                            continue;
                        }

                        current_paths.insert(file_path.clone());

                        match tokio::fs::read_to_string(&file_path).await {
                            Ok(content) => {
                                if let Some(pod) = self.parse_manifest(&content, &file_path) {
                                    let uid = pod.uid.clone();
                                    let op = if known.get(&file_path) == Some(&uid) {
                                        PodOperation::Reconcile
                                    } else if known.contains_key(&file_path) {
                                        PodOperation::Update
                                    } else {
                                        PodOperation::Add
                                    };
                                    known.insert(file_path.clone(), uid);
                                    if tx.send(PodUpdate { pod, op }).await.is_err() {
                                        return Ok(());
                                    }
                                } else {
                                    warn!(path = %file_path.display(), "Failed to parse pod manifest");
                                }
                            }
                            Err(e) => {
                                error!(path = %file_path.display(), error = %e, "Failed to read pod manifest");
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(path = %self.path.display(), error = %e, "Failed to read static pod dir");
                }
            }

            // Detect removed files
            let removed: Vec<PathBuf> = known
                .keys()
                .filter(|p| !current_paths.contains(*p))
                .cloned()
                .collect();

            for path in removed {
                if let Some(uid) = known.remove(&path) {
                    debug!(path = %path.display(), "Static pod manifest removed");
                    // We need to synthesize a minimal pod to send the Remove event.
                    // Use the same -<nodename> suffix convention so the pod manager
                    // matches the entry that was originally inserted.
                    let stem = path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("unknown");
                    let remove_name = format!("{}-{}", stem, self.node_name);
                    let pod = PodSpec {
                        uid: uid.clone(),
                        pod_ref: PodRef::new("default", remove_name),
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

            tokio::time::sleep(self.poll_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    fn source(dir: &Path) -> FilePodSource {
        FilePodSource::new(dir, "test-node", Duration::from_millis(100))
    }

    #[test]
    fn test_parse_yaml_manifest() {
        let dir = TempDir::new().unwrap();
        let src = source(dir.path());

        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
  namespace: kube-system
spec:
  containers:
    - name: nginx
      image: nginx:1.25
"#;
        let pod = src.parse_manifest(yaml, Path::new("test-pod.yaml"));
        assert!(pod.is_some());
        let pod = pod.unwrap();
        assert_eq!(pod.pod_ref.name, "test-pod-test-node");
        assert_eq!(pod.pod_ref.namespace, "kube-system");
        assert_eq!(pod.containers.len(), 1);
        assert_eq!(pod.containers[0].image, "nginx:1.25");
    }

    #[test]
    fn test_parse_json_manifest() {
        let dir = TempDir::new().unwrap();
        let src = source(dir.path());

        let json = r#"{
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "json-pod", "namespace": "default" },
            "spec": {
                "containers": [{"name": "app", "image": "alpine:3.18"}]
            }
        }"#;
        let pod = src.parse_manifest(json, Path::new("pod.json"));
        assert!(pod.is_some());
        let pod = pod.unwrap();
        assert_eq!(pod.pod_ref.name, "json-pod-test-node");
        assert_eq!(pod.containers[0].image, "alpine:3.18");
    }

    #[test]
    fn test_parse_manifest_preserves_host_network_true() {
        let dir = TempDir::new().unwrap();
        let src = source(dir.path());

        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: kube-apiserver
  namespace: kube-system
spec:
  hostNetwork: true
  containers:
    - name: kube-apiserver
      image: registry.k8s.io/kube-apiserver:v1.32.0
"#;

        let pod = src.parse_manifest(yaml, Path::new("kube-apiserver.yaml"));
        assert!(pod.is_some());
        assert!(pod.unwrap().host_network);
    }

    #[test]
    fn test_parse_manifest_preserves_hostpath_volumes_and_mounts() {
        let dir = TempDir::new().unwrap();
        let src = source(dir.path());

        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: kube-scheduler
  namespace: kube-system
spec:
  containers:
    - name: kube-scheduler
      image: registry.k8s.io/kube-scheduler:v1.32.0
      command:
        - kube-scheduler
        - --config=/etc/kubernetes/scheduler.conf
      volumeMounts:
        - name: kubeconfig
          mountPath: /etc/kubernetes/scheduler.conf
          readOnly: true
  volumes:
    - name: kubeconfig
      hostPath:
        path: /etc/kubernetes/scheduler.conf
        type: File
"#;

        let pod = src
            .parse_manifest(yaml, Path::new("kube-scheduler.yaml"))
            .expect("manifest should parse");

        assert_eq!(pod.volumes.len(), 1);
        assert_eq!(pod.volumes[0].name, "kubeconfig");
        match &pod.volumes[0].source {
            VolumeSource::HostPath { path, path_type } => {
                assert_eq!(path, "/etc/kubernetes/scheduler.conf");
                assert_eq!(path_type.as_deref(), Some("File"));
            }
            other => panic!("expected HostPath volume source, got: {:?}", other),
        }

        assert_eq!(pod.containers.len(), 1);
        assert_eq!(pod.containers[0].volume_mounts.len(), 1);
        let vm = &pod.containers[0].volume_mounts[0];
        assert_eq!(vm.name, "kubeconfig");
        assert_eq!(vm.mount_path, "/etc/kubernetes/scheduler.conf");
        assert!(vm.read_only);
    }

    #[test]
    fn test_parse_invalid_yaml_returns_none() {
        let dir = TempDir::new().unwrap();
        let src = source(dir.path());
        let result = src.parse_manifest("not: valid: yaml: {{{", Path::new("bad.yaml"));
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_uses_filename_as_fallback_name() {
        let dir = TempDir::new().unwrap();
        let src = source(dir.path());
        let yaml = r#"
metadata:
  namespace: default
spec:
  containers: []
"#;
        let pod = src.parse_manifest(yaml, Path::new("my-static-pod.yaml"));
        assert!(pod.is_some());
        assert_eq!(pod.unwrap().pod_ref.name, "my-static-pod-test-node");
    }

    #[tokio::test]
    async fn test_run_picks_up_manifest_file() {
        let dir = TempDir::new().unwrap();
        let manifest_path = dir.path().join("nginx.yaml");
        tokio::fs::write(
            &manifest_path,
            r#"
apiVersion: v1
kind: Pod
metadata:
  name: nginx
  namespace: default
spec:
  containers:
    - name: nginx
      image: nginx:latest
"#,
        )
        .await
        .unwrap();

        let src = FilePodSource::new(dir.path(), "node1", Duration::from_millis(50));
        let (tx, mut rx) = mpsc::channel(10);

        tokio::spawn(async move {
            src.run(tx).await.unwrap();
        });

        let update = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout")
            .expect("no update");

        assert_eq!(update.pod.pod_ref.name, "nginx-node1");
        assert_eq!(update.op, PodOperation::Add);
    }
}
