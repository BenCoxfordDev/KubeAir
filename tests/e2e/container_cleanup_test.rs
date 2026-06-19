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

//! Container resource cleanup e2e tests.
//!
//! Verifies that the kubelet correctly removes containerd container and sandbox
//! records when pods are torn down or containers restart.
//!
//! Regression coverage for two production bugs:
//!
//!   1. **Pod teardown cleanup** — deleting a pod via the API must remove all
//!      associated containerd container records.  Before the fix, stale records
//!      accumulated until containerd consumed gigabytes of memory.
//!
//!   2. **Restart cleanup** — each container restart must remove the old
//!      container record from containerd before creating a new one.  Without
//!      this, every crash-loop cycle left a leaked record plus an orphaned
//!      overlayfs snapshot.
//!
//! All tests are `#[ignore]` — run with `-- --ignored` from the e2e harness.
//!
//! Requirements:
//!   KUBECONFIG set (or /etc/kubernetes/admin.conf present).
//!   `crictl` available on PATH (installed by setup-node.sh).
//!   `ctr`    available on PATH (installed with containerd).

use k8s_openapi::api::core::v1::{Container, EnvVar, Pod, PodSpec as KubePodSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::{
    api::{Api, DeleteParams, PostParams},
    Client, Config,
};
use serde_json::Value;
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

/// Return all container IDs currently registered in the containerd k8s.io
/// namespace (includes both running and stopped/exited records).
fn ctr_container_ids() -> Vec<String> {
    let out = Command::new("ctr")
        .args(["-n", "k8s.io", "containers", "ls", "-q"])
        .output()
        .expect("ctr not found — is containerd installed?");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

/// Return container IDs for a specific pod from `crictl ps -a` (all states).
/// Matches by pod sandbox ID, which is embedded in the crictl output.
fn crictl_containers_for_pod(pod_name: &str, namespace: &str) -> Vec<String> {
    // First find the sandbox (pod) ID
    let sandbox_out = Command::new("crictl")
        .args(["pods", "--name", pod_name, "--namespace", namespace, "-q"])
        .output()
        .expect("crictl not found");

    let sandbox_ids: Vec<String> = String::from_utf8_lossy(&sandbox_out.stdout)
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();

    if sandbox_ids.is_empty() {
        return vec![];
    }

    // Now list all containers and filter by sandbox ID
    let ps_out = Command::new("crictl")
        .args(["ps", "-a", "--output", "json"])
        .output()
        .expect("crictl not found");

    let json: Value = serde_json::from_slice(&ps_out.stdout).unwrap_or(Value::Null);
    json["containers"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter(|c| {
            let sid = c["podSandboxId"].as_str().unwrap_or("");
            sandbox_ids.iter().any(|s| sid.starts_with(s.as_str()))
        })
        .filter_map(|c| c["id"].as_str().map(|s| s.to_string()))
        .collect()
}

/// Build a test Pod manifest.
fn test_pod(name: &str, image: &str, command: Vec<String>, restart_policy: &str) -> Pod {
    let mut labels = BTreeMap::new();
    labels.insert("kubeair-e2e".to_string(), "container-cleanup".to_string());

    Pod {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("default".to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(KubePodSpec {
            containers: vec![Container {
                name: "main".to_string(),
                image: Some(image.to_string()),
                command: if command.is_empty() {
                    None
                } else {
                    Some(command)
                },
                env: Some(vec![EnvVar {
                    name: "KUBEAIR_E2E_TEST".to_string(),
                    value: Some("container-cleanup".to_string()),
                    ..Default::default()
                }]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ..Default::default()
            }],
            restart_policy: Some(restart_policy.to_string()),
            tolerations: Some(vec![k8s_openapi::api::core::v1::Toleration {
                operator: Some("Exists".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Wait up to `timeout` for the pod phase to reach one of `phases`.
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

/// Wait for the container restart count reported by the API to reach at least
/// `min_restarts`, up to `timeout`.
async fn wait_for_restart_count(
    pods: &Api<Pod>,
    name: &str,
    min_restarts: i32,
    timeout: Duration,
) -> i32 {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(pod) = pods.get(name).await {
            let count = pod
                .status
                .as_ref()
                .and_then(|s| s.container_statuses.as_deref())
                .and_then(|cs| cs.iter().find(|c| c.name == "main"))
                .map(|c| c.restart_count)
                .unwrap_or(0);
            if count >= min_restarts {
                return count;
            }
        }
        if std::time::Instant::now() >= deadline {
            return -1;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Delete a pod and wait for it to be fully removed (up to 90 s).
async fn cleanup_pod(pods: &Api<Pod>, name: &str) {
    let _ = pods.delete(name, &DeleteParams::default()).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
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

// ── tests ─────────────────────────────────────────────────────────────────────

/// Deleting a pod via the API must remove all associated container records from
/// containerd.
///
/// Regression: before the teardown fix, container records remained in
/// containerd after pod deletion, accumulating stale overlayfs snapshots and
/// consuming memory indefinitely.
#[tokio::test]
#[ignore]
async fn e2e_pod_teardown_removes_containerd_container_records() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-cleanup-teardown";
    cleanup_pod(&pods, pod_name).await;

    // Snapshot containerd state before the test.
    let before_ids: std::collections::HashSet<String> = ctr_container_ids().into_iter().collect();

    // Create a pod that runs briefly and exits.
    // terminationGracePeriodSeconds=0 ensures the pod is removed from the API
    // immediately on deletion and the kubelet uses SIGKILL (no 30-second grace
    // period race with Kubernetes GC).
    let mut manifest = test_pod(
        pod_name,
        "busybox:1.36",
        vec!["sh".into(), "-c".into(), "echo hello && sleep 10".into()],
        "Never",
    );
    if let Some(spec) = manifest.spec.as_mut() {
        spec.termination_grace_period_seconds = Some(0);
    }
    pods.create(&PostParams::default(), &manifest)
        .await
        .expect("Failed to create test pod");

    // Wait for the pod to reach Running so we know containers were created.
    wait_for_pod_phase(
        &pods,
        pod_name,
        &["Running", "Succeeded", "Failed"],
        Duration::from_secs(120),
    )
    .await
    .expect("Pod did not reach Running phase within 120s");

    // Confirm the pod's containers appear in containerd.
    let pod_ctr_ids = crictl_containers_for_pod(pod_name, "default");
    assert!(
        !pod_ctr_ids.is_empty(),
        "Expected at least one container record in containerd after pod start"
    );

    // Verify those IDs also show up in ctr (raw containerd level).
    let mid_ids: std::collections::HashSet<String> = ctr_container_ids().into_iter().collect();
    let new_ids: Vec<&String> = pod_ctr_ids
        .iter()
        .filter(|id| mid_ids.contains(*id) && !before_ids.contains(*id))
        .collect();
    assert!(
        !new_ids.is_empty(),
        "Expected new container records in containerd k8s.io namespace after pod start"
    );

    // Delete the pod.
    pods.delete(pod_name, &DeleteParams::default())
        .await
        .expect("Failed to delete pod");

    // Wait for deletion to be acknowledged by the API.
    let api_gone_deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        if pods.get(pod_name).await.is_err() {
            break;
        }
        if std::time::Instant::now() >= api_gone_deadline {
            panic!(
                "Pod {} was not removed from API server within 90s",
                pod_name
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Poll until the kubelet has cleaned up the containerd records, or until
    // a generous timeout expires.  A fixed sleep is unreliable in CI because
    // stop_container with the default 30-second grace period can run right up
    // to the point where Kubernetes GC removes the pod from the API server,
    // leaving remove_container still in flight when the test's 5-second sleep
    // expires.
    let containerd_cleanup_deadline = std::time::Instant::now() + Duration::from_secs(60);
    let after_ids: std::collections::HashSet<String> = loop {
        let current_ids: std::collections::HashSet<String> =
            ctr_container_ids().into_iter().collect();
        let still_present: bool = pod_ctr_ids.iter().any(|id| current_ids.contains(id));
        if !still_present || std::time::Instant::now() >= containerd_cleanup_deadline {
            break current_ids;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    let leaked: Vec<&String> = pod_ctr_ids
        .iter()
        .filter(|id| after_ids.contains(*id))
        .collect();

    assert!(
        leaked.is_empty(),
        "Leaked containerd container records after pod deletion: {:?}. \
         The kubelet must call remove_container when tearing down a pod.",
        leaked
    );
}

/// Each container restart must remove the previous container record from
/// containerd so that records do not accumulate.
///
/// Regression: before the fix, every crash-loop restart left behind a stale
/// containerd container record with an orphaned overlayfs snapshot.  After
/// ~7000 restarts (observed in production) containerd consumed 3.3 GB of RSS.
///
/// This test verifies that after N observed restarts, the total number of
/// containerd records for the pod is bounded — not growing with each restart.
#[tokio::test]
#[ignore]
async fn e2e_container_restart_does_not_leak_containerd_records() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-cleanup-restarts";
    cleanup_pod(&pods, pod_name).await;

    // Pod with RestartPolicy=Always that exits immediately — will crash-loop.
    let manifest = test_pod(
        pod_name,
        "busybox:1.36",
        vec!["sh".into(), "-c".into(), "exit 1".into()],
        "Always",
    );
    pods.create(&PostParams::default(), &manifest)
        .await
        .expect("Failed to create test pod");

    // Wait for at least 3 restarts to accumulate.
    let target_restarts = 3_i32;
    let restart_count =
        wait_for_restart_count(&pods, pod_name, target_restarts, Duration::from_secs(300)).await;

    assert!(
        restart_count >= target_restarts,
        "Pod did not reach {} restarts within 300s (got {})",
        target_restarts,
        restart_count
    );

    // Fetch the container records for this pod from crictl.
    let pod_ctr_ids = crictl_containers_for_pod(pod_name, "default");

    // With the fix: there must be at most one container record per container
    // slot (plus the pause sandbox).  Without the fix there would be one
    // leaked record per restart cycle.
    //
    // We allow up to 2 to account for a container that has just been started
    // and the previous one not yet cleaned up during the transition window,
    // but never more than that.
    assert!(
        pod_ctr_ids.len() <= 2,
        "Expected at most 2 container records (current + possible in-flight transition) \
         for pod {} after {} restarts, got {}. \
         Old records are leaking — remove_container must be called on restart.",
        pod_name,
        restart_count,
        pod_ctr_ids.len()
    );

    // Tear down.
    cleanup_pod(&pods, pod_name).await;

    // After teardown, all records for this pod must be gone.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let remaining = crictl_containers_for_pod(pod_name, "default");
    assert!(
        remaining.is_empty(),
        "Expected no container records after pod deletion, found: {:?}",
        remaining
    );
}

/// Verify that the kubelet reports a sha256 image ID (not the registry reference)
/// in `pod.status.containerStatuses[].imageID`.
///
/// Regression: the Rust kubelet used to pass the image reference string (e.g.
/// `registry.example.com/org/app:tag`) as the CRI `image.image` field instead
/// of the resolved sha256 digest.  This caused containerd to store the reference
/// as `image_ref`, which the kubelet then surfaced as `imageID` in pod status —
/// breaking tooling and conformance tests that expect a content-addressable ID.
#[tokio::test]
#[ignore]
async fn e2e_container_image_id_is_sha256_digest() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client, "default");

    let pod_name = "kubeair-e2e-image-id-check";
    cleanup_pod(&pods, pod_name).await;

    // Use busybox which will always be available on the node.
    let manifest = test_pod(
        pod_name,
        "busybox:1.36",
        vec!["sh".into(), "-c".into(), "sleep 60".into()],
        "Never",
    );
    pods.create(&PostParams::default(), &manifest)
        .await
        .expect("Failed to create test pod");

    // Wait for Running so the container has actually been created.
    let phase = wait_for_pod_phase(&pods, pod_name, &["Running"], Duration::from_secs(120)).await;
    assert_eq!(
        phase.as_deref(),
        Some("Running"),
        "Pod did not reach Running phase within 120s"
    );

    // Read back pod status and inspect imageID.
    let pod = pods.get(pod_name).await.expect("Failed to fetch pod");
    let image_id = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_deref())
        .and_then(|cs| cs.iter().find(|c| c.name == "main"))
        .map(|c| c.image_id.clone())
        .unwrap_or_default();

    assert!(
        image_id.starts_with("sha256:"),
        "Expected imageID to be a sha256 digest (e.g. sha256:abc…), got: {:?}. \
         The kubelet must pass the resolved image ID to CRI CreateContainer, not \
         the original reference string.",
        image_id
    );

    cleanup_pod(&pods, pod_name).await;
}
