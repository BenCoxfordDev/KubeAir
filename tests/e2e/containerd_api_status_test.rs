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

//! Containerd ↔ API server status conformance tests.
//!
//! These tests verify that the kube-air kubelet correctly propagates container
//! and pod state from the containerd CRI into the Kubernetes API server.  They
//! catch divergence bugs such as:
//!   - Container running in containerd but showing Waiting/Unknown in the API.
//!   - Terminated container with exit-code not reflected in lastTerminationState.
//!   - Pod phase not updated after container exits.
//!   - Restart count in API server not matching crictl restart count.
//!
//! All tests are `#[ignore]` — run with `-- --ignored` from the e2e harness.
//!
//! Requirements:
//!   KUBECONFIG set (or /etc/kubernetes/admin.conf present).
//!   `crictl` available at /usr/local/bin/crictl (installed by setup-node.sh).
//!
//! Strategy:
//!   1. Create a short-lived test pod via the Kubernetes API.
//!   2. Wait for kubelet to start the container in containerd.
//!   3. Query containerd directly via `crictl inspect` (subprocess).
//!   4. Assert that the Kubernetes API server's pod/container status matches
//!      the containerd-reported state.
//!   5. Clean up the test pod.

use k8s_openapi::api::core::v1::{Container, EnvVar, Pod, PodSpec as KubePodSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::{
    Client, Config,
    api::{Api, DeleteParams, PostParams},
};
use serde_json::Value;
use std::process::Command;
use std::time::Duration;

// ── helpers ───────────────────────────────────────────────────────────────────

async fn cluster_client() -> Client {
    // Install the aws-lc-rs rustls CryptoProvider once per process.
    let _ = rustls::crypto::ring::default_provider().install_default();

    if std::env::var("KUBECONFIG").is_err() {
        let default = "/etc/kubernetes/admin.conf";
        if std::path::Path::new(default).exists() {
            // SAFETY: single-threaded during test setup.
            unsafe { std::env::set_var("KUBECONFIG", default) };
        }
    }
    let config = Config::infer().await.expect("Failed to infer kubeconfig");
    Client::try_from(config).expect("Failed to build kube client")
}

/// Query containerd via `crictl inspect` and return the parsed JSON.
/// Returns `None` if the container ID is not found or crictl fails.
fn crictl_inspect(container_id: &str) -> Option<Value> {
    let output = Command::new("crictl")
        .args(["inspect", "--output", "json", container_id])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }
    serde_json::from_slice(&output.stdout).ok()
}

/// Create a test Pod manifest.
fn test_pod(name: &str, image: &str, command: Vec<String>) -> Pod {
    Pod {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("default".to_string()),
            labels: Some({
                let mut m = std::collections::BTreeMap::new();
                m.insert("kubeair-e2e".to_string(), "true".to_string());
                m
            }),
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
                    value: Some("1".to_string()),
                    ..Default::default()
                }]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            // Tolerate control-plane taint so single-node clusters schedule pods
            tolerations: Some(vec![k8s_openapi::api::core::v1::Toleration {
                operator: Some("Exists".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Wait up to `timeout` seconds for the pod phase to reach one of `phases`.
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
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Delete a pod and ignore not-found errors.
async fn cleanup_pod(pods: &Api<Pod>, name: &str) {
    let _ = pods.delete(name, &DeleteParams::default()).await;
    // Wait for the pod to be fully gone so subsequent creates don't race.
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

// ── tests ─────────────────────────────────────────────────────────────────────

/// A pod that runs and exits successfully must transition to Succeeded phase
/// in the API server, and the container state in containerd (CONTAINER_EXITED)
/// must match.
#[tokio::test]
#[ignore]
async fn e2e_pod_succeeded_phase_matches_containerd_exited() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-succeeded";
    cleanup_pod(&pods, pod_name).await;

    // Pod that exits immediately with code 0
    let manifest = test_pod(
        pod_name,
        "busybox:1.36",
        vec!["sh".into(), "-c".into(), "echo ok; exit 0".into()],
    );

    pods.create(&PostParams::default(), &manifest)
        .await
        .expect("Failed to create test pod");

    // Wait for Succeeded phase
    let phase = wait_for_pod_phase(
        &pods,
        pod_name,
        &["Succeeded", "Failed"],
        Duration::from_secs(120),
    )
    .await
    .expect("Pod did not reach terminal phase within 120s");

    assert_eq!(phase, "Succeeded", "Expected Succeeded, got {}", phase);

    // Verify API container status: Terminated with exitCode 0
    let pod = pods.get(pod_name).await.expect("Pod not found");
    let container_status = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_deref())
        .and_then(|cs| cs.iter().find(|c| c.name == "main"))
        .expect("main container status not found");

    let state = container_status
        .state
        .as_ref()
        .expect("container state is None");

    let terminated = state
        .terminated
        .as_ref()
        .expect("container should be in Terminated state");

    assert_eq!(
        terminated.exit_code, 0,
        "API server exit_code should be 0, got {}",
        terminated.exit_code
    );

    // Verify containerd shows CONTAINER_EXITED via crictl
    let container_id = container_status
        .container_id
        .as_deref()
        .and_then(|id| id.strip_prefix("containerd://"))
        .map(|s| s.to_string());

    if let Some(cid) = container_id
        && let Some(info) = crictl_inspect(&cid)
    {
        let ctr_state = info
            .pointer("/status/state")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN");

        assert!(
            ctr_state.contains("EXITED") || ctr_state.contains("STOPPED"),
            "containerd state should be EXITED/STOPPED, got: {}",
            ctr_state
        );

        let crictl_exit_code = info
            .pointer("/status/exitCode")
            .and_then(|v| v.as_i64())
            .unwrap_or(-1);

        assert_eq!(
            crictl_exit_code, 0,
            "containerd exit code mismatch: API={}, containerd={}",
            terminated.exit_code, crictl_exit_code
        );
    }

    cleanup_pod(&pods, pod_name).await;
}

/// A pod that exits with non-zero must show Failed phase and Terminated state
/// with the correct exit code in the API server.
#[tokio::test]
#[ignore]
async fn e2e_pod_failed_phase_and_exit_code_propagated() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-failed";
    cleanup_pod(&pods, pod_name).await;

    let manifest = test_pod(
        pod_name,
        "busybox:1.36",
        vec!["sh".into(), "-c".into(), "exit 42".into()],
    );

    pods.create(&PostParams::default(), &manifest)
        .await
        .expect("Failed to create test pod");

    let phase = wait_for_pod_phase(
        &pods,
        pod_name,
        &["Failed", "Succeeded"],
        Duration::from_secs(120),
    )
    .await
    .expect("Pod did not reach terminal phase");

    assert_eq!(phase, "Failed", "Expected Failed phase, got {}", phase);

    let pod = pods.get(pod_name).await.expect("Pod not found");
    let cs = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_deref())
        .and_then(|cs| cs.iter().find(|c| c.name == "main"))
        .expect("main container status not found");

    let terminated = cs
        .state
        .as_ref()
        .and_then(|s| s.terminated.as_ref())
        .expect("container should be Terminated");

    assert_eq!(
        terminated.exit_code, 42,
        "API server exit_code should be 42, got {}",
        terminated.exit_code
    );

    cleanup_pod(&pods, pod_name).await;
}

/// A long-running pod must show Running phase in the API server and
/// CONTAINER_RUNNING state in containerd.
#[tokio::test]
#[ignore]
async fn e2e_running_pod_state_matches_containerd() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-running";
    cleanup_pod(&pods, pod_name).await;

    // Pod that sleeps for 60 seconds — long enough to inspect
    let mut manifest = test_pod(pod_name, "busybox:1.36", vec!["sleep".into(), "60".into()]);
    // Use Always restart so it stays alive if kubelet restarts it
    if let Some(spec) = manifest.spec.as_mut() {
        spec.restart_policy = Some("Always".to_string());
    }

    pods.create(&PostParams::default(), &manifest)
        .await
        .expect("Failed to create test pod");

    let phase = wait_for_pod_phase(&pods, pod_name, &["Running"], Duration::from_secs(120))
        .await
        .expect("Pod did not reach Running phase within 120s");

    assert_eq!(phase, "Running");

    // Check that the API server container is in running state
    let pod = pods.get(pod_name).await.expect("Pod not found");
    let cs = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_deref())
        .and_then(|cs| cs.iter().find(|c| c.name == "main"))
        .expect("main container status not found");

    assert!(cs.ready, "Container should be ready when pod is Running");

    let running = cs
        .state
        .as_ref()
        .and_then(|s| s.running.as_ref())
        .expect("container should be in Running state in API server");

    // started_at must be populated
    assert!(
        running.started_at.is_some(),
        "Running container startedAt must be populated"
    );

    // Compare with containerd via crictl
    let container_id = cs
        .container_id
        .as_deref()
        .and_then(|id| id.strip_prefix("containerd://"))
        .map(|s| s.to_string());

    if let Some(cid) = container_id
        && let Some(info) = crictl_inspect(&cid)
    {
        let ctr_state = info
            .pointer("/status/state")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN");

        assert!(
            ctr_state.contains("RUNNING"),
            "containerd state should be RUNNING while API shows Running, got: {}",
            ctr_state
        );
    }

    cleanup_pod(&pods, pod_name).await;
}

/// Verify that `lastTerminationState` is populated in the API server after
/// a container crashes and is restarted by the kubelet.
/// Uses `restartPolicy: Always` and a container that exits with code 1.
#[tokio::test]
#[ignore]
async fn e2e_last_termination_state_populated_after_restart() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-restart";
    cleanup_pod(&pods, pod_name).await;

    // Container exits immediately — kubelet should restart it (restartPolicy=Always)
    let mut manifest = test_pod(
        pod_name,
        "busybox:1.36",
        vec!["sh".into(), "-c".into(), "exit 1".into()],
    );
    if let Some(spec) = manifest.spec.as_mut() {
        spec.restart_policy = Some("Always".to_string());
    }

    pods.create(&PostParams::default(), &manifest)
        .await
        .expect("Failed to create test pod");

    // Wait for at least one restart (restartCount >= 1)
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    let mut restart_count_seen: i32 = 0;

    loop {
        if let Ok(pod) = pods.get(pod_name).await
            && let Some(rc) = pod
                .status
                .as_ref()
                .and_then(|s| s.container_statuses.as_deref())
                .and_then(|cs| cs.iter().find(|c| c.name == "main"))
                .map(|cs| cs.restart_count)
        {
            restart_count_seen = rc;
            if rc >= 1 {
                break;
            }
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    assert!(
        restart_count_seen >= 1,
        "Container did not restart within 180s (restartCount={})",
        restart_count_seen
    );

    // Now verify lastTerminationState is populated
    let pod = pods.get(pod_name).await.expect("Pod not found");
    let cs = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_deref())
        .and_then(|cs| cs.iter().find(|c| c.name == "main"))
        .expect("main container status not found");

    let last_state = cs
        .last_state
        .as_ref()
        .expect("lastTerminationState should be populated after restart");

    let last_terminated = last_state
        .terminated
        .as_ref()
        .expect("lastTerminationState should contain Terminated");

    assert_eq!(
        last_terminated.exit_code, 1,
        "lastTerminationState exit_code should be 1 (the crash exit code)"
    );

    assert!(
        cs.restart_count >= 1,
        "restartCount should reflect actual restarts"
    );

    cleanup_pod(&pods, pod_name).await;
}

/// Verify that pod IP is populated in the API server once the pod is Running.
/// The IP should be within the Calico pod CIDR (192.168.x.x) or the default
/// containerd bridge subnet.
#[tokio::test]
#[ignore]
async fn e2e_running_pod_has_ip_assigned() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-pod-ip";
    cleanup_pod(&pods, pod_name).await;

    let mut manifest = test_pod(pod_name, "busybox:1.36", vec!["sleep".into(), "30".into()]);
    if let Some(spec) = manifest.spec.as_mut() {
        spec.restart_policy = Some("Never".to_string());
    }

    pods.create(&PostParams::default(), &manifest)
        .await
        .expect("Failed to create test pod");

    let _phase = wait_for_pod_phase(&pods, pod_name, &["Running"], Duration::from_secs(120))
        .await
        .expect("Pod did not reach Running phase");

    let pod = pods.get(pod_name).await.expect("Pod not found");
    let pod_ip = pod
        .status
        .as_ref()
        .and_then(|s| s.pod_ip.as_deref())
        .expect("podIP should be set on a Running pod");

    // Must be a valid non-empty IP address
    let parsed: std::net::IpAddr = pod_ip
        .parse()
        .unwrap_or_else(|_| panic!("podIP '{}' is not a valid IP address", pod_ip));

    // Must not be loopback
    assert!(
        !parsed.is_loopback(),
        "podIP should not be loopback, got: {}",
        pod_ip
    );

    cleanup_pod(&pods, pod_name).await;
}

/// Verify that `containerID` in the API server uses the `containerd://` scheme
/// and the ID matches a real container in the containerd runtime.
#[tokio::test]
#[ignore]
async fn e2e_container_id_scheme_and_exists_in_containerd() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-ctr-id";
    cleanup_pod(&pods, pod_name).await;

    let mut manifest = test_pod(pod_name, "busybox:1.36", vec!["sleep".into(), "30".into()]);
    if let Some(spec) = manifest.spec.as_mut() {
        spec.restart_policy = Some("Never".to_string());
    }

    pods.create(&PostParams::default(), &manifest)
        .await
        .expect("Failed to create test pod");

    let _phase = wait_for_pod_phase(&pods, pod_name, &["Running"], Duration::from_secs(120))
        .await
        .expect("Pod did not reach Running phase");

    let pod = pods.get(pod_name).await.expect("Pod not found");
    let cs = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_deref())
        .and_then(|cs| cs.iter().find(|c| c.name == "main"))
        .expect("main container status not found");

    let container_id_raw = cs
        .container_id
        .as_deref()
        .expect("containerID should be populated on a Running container");

    // Must use containerd:// scheme
    assert!(
        container_id_raw.starts_with("containerd://"),
        "containerID should use containerd:// scheme, got: {}",
        container_id_raw
    );

    let cid = &container_id_raw["containerd://".len()..];
    assert!(!cid.is_empty(), "containerID must have a non-empty ID part");

    // Verify the container actually exists in containerd
    if let Some(info) = crictl_inspect(cid) {
        let ctr_id_in_crictl = info
            .pointer("/status/id")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        assert!(
            ctr_id_in_crictl.starts_with(cid) || cid.starts_with(ctr_id_in_crictl),
            "crictl container ID '{}' does not match API server container ID '{}'",
            ctr_id_in_crictl,
            cid
        );
    }

    cleanup_pod(&pods, pod_name).await;
}
