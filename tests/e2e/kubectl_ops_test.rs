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

//! kubectl operations e2e tests.
//!
//! Verifies that common kubectl operations work correctly against the kube-air
//! kubelet: logs, exec, rollout restart, and pod deletion.
//!
//! All tests are `#[ignore]` — run with `-- --ignored` from the e2e harness.
//!
//! Requirements:
//!   KUBECONFIG set (or /etc/kubernetes/admin.conf present).
//!   `kubectl` available on PATH (installed by setup-node.sh).

use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::core::v1::{Container, Pod, PodSpec, PodTemplateSpec, Toleration};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use kube::{
    api::{Api, DeleteParams, PostParams},
    Client, Config,
};
use std::collections::BTreeMap;
use std::process::Command;
use std::time::Duration;

// ── helpers ───────────────────────────────────────────────────────────────────

async fn cluster_client() -> Client {
    let _ = rustls::crypto::ring::default_provider().install_default();
    if std::env::var("KUBECONFIG").is_err() {
        let default = "/etc/kubernetes/admin.conf";
        if std::path::Path::new(default).exists() {
            unsafe { std::env::set_var("KUBECONFIG", default) };
        }
    }
    let config = Config::infer().await.expect("Failed to infer kubeconfig");
    Client::try_from(config).expect("Failed to build kube client")
}

fn kubectl(args: &[&str]) -> std::process::Output {
    Command::new("kubectl")
        .args(args)
        .output()
        .expect("kubectl not found")
}

/// Wait up to `timeout` for the pod to reach one of the given phases.
async fn wait_for_pod_phase(
    pods: &Api<Pod>,
    name: &str,
    phases: &[&str],
    timeout: Duration,
) -> Option<String> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(pod) = pods.get(name).await {
            let phase = pod
                .status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .unwrap_or("Unknown")
                .to_string();
            if phases.contains(&phase.as_str()) {
                return Some(phase);
            }
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Wait up to `timeout` for a pod with the given label to be Running.
async fn wait_for_pod_running_by_label(
    pods: &Api<Pod>,
    label: &str,
    timeout: Duration,
) -> Option<String> {
    use kube::api::ListParams;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(list) = pods.list(&ListParams::default().labels(label)).await {
            for pod in &list.items {
                let phase = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.phase.as_deref())
                    .unwrap_or("");
                if phase == "Running" {
                    return pod.metadata.name.clone();
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

fn long_running_pod(name: &str) -> Pod {
    Pod {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("default".to_string()),
            labels: Some({
                let mut m = BTreeMap::new();
                m.insert("kubeair-e2e-ops".to_string(), name.to_string());
                m
            }),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "main".to_string(),
                image: Some("busybox:1.36".to_string()),
                command: Some(vec![
                    "sh".into(),
                    "-c".into(),
                    "echo 'hello from kube-air' && sleep 300".into(),
                ]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![Toleration {
                operator: Some("Exists".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn nginx_deployment(name: &str) -> Deployment {
    let mut labels = BTreeMap::new();
    labels.insert("app".to_string(), name.to_string());

    Deployment {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "nginx".to_string(),
                        image: Some("nginx:1.25-alpine".to_string()),
                        image_pull_policy: Some("IfNotPresent".to_string()),
                        ..Default::default()
                    }],
                    tolerations: Some(vec![Toleration {
                        operator: Some("Exists".to_string()),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn cleanup_pod(pods: &Api<Pod>, name: &str) {
    let _ = pods.delete(name, &DeleteParams::default()).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        match pods.get(name).await {
            Err(_) => break,
            Ok(_) => {
                if std::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn cleanup_deployment(deployments: &Api<Deployment>, name: &str) {
    use kube::api::DeleteParams;
    let dp = DeleteParams {
        grace_period_seconds: Some(0),
        ..Default::default()
    };
    let _ = deployments.delete(name, &dp).await;
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// `kubectl logs` must return the container's stdout output.
///
/// Creates a pod that prints a known string, waits for it to complete, then
/// asserts the log output contains that string.
#[tokio::test]
#[ignore]
async fn e2e_kubectl_logs() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-logs";
    cleanup_pod(&pods, pod_name).await;

    let manifest = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "main".to_string(),
                image: Some("busybox:1.36".to_string()),
                command: Some(vec![
                    "sh".into(),
                    "-c".into(),
                    "echo 'kubeair-log-marker'".into(),
                ]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![Toleration {
                operator: Some("Exists".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };

    pods.create(&PostParams::default(), &manifest)
        .await
        .expect("Failed to create pod");

    let phase = wait_for_pod_phase(
        &pods,
        pod_name,
        &["Succeeded", "Failed"],
        Duration::from_secs(120),
    )
    .await;

    assert_eq!(
        phase.as_deref(),
        Some("Succeeded"),
        "Pod did not reach Succeeded (got {:?})",
        phase
    );

    let out = kubectl(&["logs", "-n", "default", pod_name]);
    assert!(
        out.status.success(),
        "kubectl logs exited non-zero: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("kubeair-log-marker"),
        "kubectl logs output did not contain expected string.\nGot: {stdout}"
    );

    cleanup_pod(&pods, pod_name).await;
}

/// `kubectl exec` must be able to run a command inside a running container.
///
/// Creates a long-running pod, waits for it to be Running, then executes a
/// command via kubectl and asserts the output.
#[tokio::test]
#[ignore]
async fn e2e_kubectl_exec() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-exec";
    cleanup_pod(&pods, pod_name).await;

    pods.create(&PostParams::default(), &long_running_pod(pod_name))
        .await
        .expect("Failed to create pod");

    let phase = wait_for_pod_phase(&pods, pod_name, &["Running"], Duration::from_secs(120)).await;
    assert_eq!(
        phase.as_deref(),
        Some("Running"),
        "Pod did not reach Running (got {:?})",
        phase
    );

    let out = kubectl(&[
        "exec",
        "-n",
        "default",
        pod_name,
        "--",
        "echo",
        "kubeair-exec-marker",
    ]);

    assert!(
        out.status.success(),
        "kubectl exec exited non-zero: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("kubeair-exec-marker"),
        "kubectl exec output did not contain expected string.\nGot: {stdout}"
    );

    cleanup_pod(&pods, pod_name).await;
}

/// `kubectl delete pod` must remove the pod and have it leave the API server.
///
/// Creates a long-running pod, deletes it via kubectl, and verifies it is gone
/// from the API server within a reasonable timeout.
#[tokio::test]
#[ignore]
async fn e2e_kubectl_delete_pod() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-delete";
    cleanup_pod(&pods, pod_name).await;

    pods.create(&PostParams::default(), &long_running_pod(pod_name))
        .await
        .expect("Failed to create pod");

    wait_for_pod_phase(&pods, pod_name, &["Running"], Duration::from_secs(120)).await;

    let out = kubectl(&[
        "delete",
        "pod",
        "-n",
        "default",
        pod_name,
        "--grace-period=5",
    ]);
    assert!(
        out.status.success(),
        "kubectl delete pod exited non-zero: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Verify the pod is gone from the API server
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        match pods.get(pod_name).await {
            Err(_) => break, // gone
            Ok(_) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "Pod {pod_name} still exists in API server 60s after kubectl delete"
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

/// `kubectl rollout restart` must replace the Deployment pod and bring a new
/// one up healthy.
///
/// Creates a single-replica nginx Deployment, waits for it to be Available,
/// performs a rollout restart, then waits for the new pod to be Running.
#[tokio::test]
#[ignore]
async fn e2e_kubectl_rollout_restart() {
    let client = cluster_client().await;
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), "default");
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let deploy_name = "kubeair-e2e-rollout";
    cleanup_deployment(&deployments, deploy_name).await;
    // Also clean up any lingering pods from a previous run
    tokio::time::sleep(Duration::from_secs(3)).await;

    deployments
        .create(&PostParams::default(), &nginx_deployment(deploy_name))
        .await
        .expect("Failed to create Deployment");

    // Wait for the initial pod to be Running
    let label = format!("app={deploy_name}");
    let initial_pod = wait_for_pod_running_by_label(&pods, &label, Duration::from_secs(180)).await;
    assert!(
        initial_pod.is_some(),
        "Initial Deployment pod never reached Running within 180s"
    );
    let initial_pod_name = initial_pod.unwrap();

    // Trigger rollout restart
    let out = kubectl(&[
        "rollout",
        "restart",
        "deployment",
        "-n",
        "default",
        deploy_name,
    ]);
    assert!(
        out.status.success(),
        "kubectl rollout restart exited non-zero: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Wait for the old pod to be replaced by a new one
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    let mut new_pod_running = false;
    while std::time::Instant::now() < deadline {
        if let Ok(list) = pods
            .list(&kube::api::ListParams::default().labels(&label))
            .await
        {
            for pod in &list.items {
                let name = pod.metadata.name.as_deref().unwrap_or("");
                let phase = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.phase.as_deref())
                    .unwrap_or("");
                // A new pod (different name) is Running
                if name != initial_pod_name && phase == "Running" {
                    new_pod_running = true;
                    break;
                }
            }
        }
        if new_pod_running {
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    assert!(
        new_pod_running,
        "No new Running pod appeared after kubectl rollout restart within 180s \
         (initial pod was {initial_pod_name})"
    );

    cleanup_deployment(&deployments, deploy_name).await;
}
