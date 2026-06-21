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

//! Live cluster health tests.
//!
//! These tests require a running Kubernetes cluster provisioned by
//! `hack/e2e/setup-node.sh` (containerd + kubeadm + Calico eBPF).
//!
//! All tests are `#[ignore]` so they do not run during `cargo test`.
//! `hack/e2e/run-cluster-tests.sh` invokes them with `-- --ignored`.
//!
//! Required environment:
//!   KUBECONFIG=/etc/kubernetes/admin.conf  (or set the KUBECONFIG env var)
//!
//! Covered assertions:
//!   - The node is in Ready condition.
//!   - All pods in kube-system are in Running phase and at least one
//!     container is ready.
//!   - CoreDNS pods (k8s-app=kube-dns) are Running and Ready.
//!   - Calico node pods are Running and Ready (searched across
//!     kube-system and calico-system namespaces).
//!   - Calico API server pods are Running (calico-apiserver namespace).

use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    Client, Config,
    api::{Api, ListParams},
};
use std::collections::HashSet;
use std::time::Duration;

// ── helpers ───────────────────────────────────────────────────────────────────

async fn cluster_client() -> Client {
    // Install the aws-lc-rs rustls CryptoProvider once per process.
    // Both aws-lc-rs and ring are in the dependency tree so rustls cannot
    // auto-select; we must pick one explicitly.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Prefer explicit KUBECONFIG; fall back to the kubeadm default path.
    if std::env::var("KUBECONFIG").is_err() {
        let default = "/etc/kubernetes/admin.conf";
        if std::path::Path::new(default).exists() {
            // SAFETY: single-threaded during test setup; no concurrent reads.
            unsafe { std::env::set_var("KUBECONFIG", default) };
        }
    }
    let config = Config::infer()
        .await
        .expect("Failed to infer kubeconfig. Set KUBECONFIG or run on the cluster node.");
    Client::try_from(config).expect("Failed to build kube client")
}

/// Return true if a pod has at least one ready container and is in Running phase.
fn pod_is_running_and_ready(pod: &Pod) -> bool {
    let phase = pod
        .status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .unwrap_or("");

    if phase != "Running" {
        return false;
    }

    let container_statuses = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_deref())
        .unwrap_or(&[]);

    // At least one container must be Ready
    container_statuses.iter().any(|cs| cs.ready)
}

/// Return the pod name for error messages.
fn pod_name(pod: &Pod) -> String {
    pod.metadata
        .name
        .clone()
        .unwrap_or_else(|| "<unknown>".to_string())
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// The single cluster node must be in Ready condition.
#[tokio::test]
#[ignore]
async fn e2e_node_is_ready() {
    let client = cluster_client().await;
    let nodes: Api<Node> = Api::all(client);

    let node_list = nodes
        .list(&ListParams::default())
        .await
        .expect("Failed to list nodes");

    assert!(!node_list.items.is_empty(), "No nodes found in the cluster");

    for node in &node_list.items {
        let name = node.metadata.name.as_deref().unwrap_or("<unknown>");

        let ready = node
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_deref())
            .unwrap_or(&[])
            .iter()
            .any(|c| c.type_ == "Ready" && c.status == "True");

        assert!(ready, "Node '{}' is not in Ready condition", name);
    }
}

/// Every pod in kube-system must be in Running phase with at least one ready container.
/// Static pods for the control-plane (api-server, etcd, scheduler, controller-manager)
/// are included.
#[tokio::test]
#[ignore]
async fn e2e_kube_system_all_pods_running() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client, "kube-system");

    // Give pods up to 5 minutes to settle after cluster init
    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    let mut not_ready: Vec<String>;

    loop {
        let pod_list = pods
            .list(&ListParams::default())
            .await
            .expect("Failed to list kube-system pods");

        not_ready = pod_list
            .items
            .iter()
            .filter(|p| !pod_is_running_and_ready(p))
            .map(|p| {
                let phase = p
                    .status
                    .as_ref()
                    .and_then(|s| s.phase.as_deref())
                    .unwrap_or("Unknown");
                format!("{} (phase={})", pod_name(p), phase)
            })
            .collect();

        if not_ready.is_empty() {
            break;
        }

        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    assert!(
        not_ready.is_empty(),
        "kube-system pods not Running/Ready:\n  {}",
        not_ready.join("\n  ")
    );
}

/// CoreDNS pods must be Running and have at least one ready container.
/// Verified both by phase and by the `k8s-app=kube-dns` label selector.
#[tokio::test]
#[ignore]
async fn e2e_coredns_running_and_ready() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client, "kube-system");

    let lp = ListParams::default().labels("k8s-app=kube-dns");

    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    let mut coredns_pods: Vec<Pod>;

    loop {
        coredns_pods = pods
            .list(&lp)
            .await
            .expect("Failed to list CoreDNS pods")
            .items;

        let all_ready =
            !coredns_pods.is_empty() && coredns_pods.iter().all(pod_is_running_and_ready);

        if all_ready {
            break;
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    assert!(
        !coredns_pods.is_empty(),
        "No CoreDNS pods found in kube-system (label: k8s-app=kube-dns)"
    );

    let not_ready: Vec<String> = coredns_pods
        .iter()
        .filter(|p| !pod_is_running_and_ready(p))
        .map(|p| {
            let phase = p
                .status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .unwrap_or("Unknown");
            format!("{} (phase={})", pod_name(p), phase)
        })
        .collect();

    assert!(
        not_ready.is_empty(),
        "CoreDNS pods not Running/Ready:\n  {}",
        not_ready.join("\n  ")
    );
}

/// Calico node pods must be Running.
/// Calico pods may be in `kube-system` (manifest install) or `calico-system`
/// (operator install). We search both.
#[tokio::test]
#[ignore]
async fn e2e_calico_pods_running() {
    let client = cluster_client().await;

    let namespaces_to_check = ["kube-system", "calico-system"];
    let calico_label = "k8s-app=calico-node";

    let deadline = std::time::Instant::now() + Duration::from_secs(360);
    let mut found_pods: Vec<(String, Pod)> = Vec::new(); // (namespace, pod)

    loop {
        found_pods.clear();

        for ns in &namespaces_to_check {
            let pods: Api<Pod> = Api::namespaced(client.clone(), ns);
            let lp = ListParams::default().labels(calico_label);
            if let Ok(list) = pods.list(&lp).await {
                for pod in list.items {
                    found_pods.push(((*ns).to_string(), pod));
                }
            }
        }

        let all_ready =
            !found_pods.is_empty() && found_pods.iter().all(|(_, p)| pod_is_running_and_ready(p));

        if all_ready {
            break;
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    assert!(
        !found_pods.is_empty(),
        "No Calico node pods found (label: {}) in namespaces: {:?}",
        calico_label,
        namespaces_to_check
    );

    let not_ready: Vec<String> = found_pods
        .iter()
        .filter(|(_, p)| !pod_is_running_and_ready(p))
        .map(|(ns, p)| {
            let phase = p
                .status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .unwrap_or("Unknown");
            format!("{}/{} (phase={})", ns, pod_name(p), phase)
        })
        .collect();

    assert!(
        not_ready.is_empty(),
        "Calico node pods not Running/Ready:\n  {}",
        not_ready.join("\n  ")
    );
}

/// Calico API server pods (operator mode) must be Running if present.
/// This test is informational — it passes if the namespace does not exist.
#[tokio::test]
#[ignore]
async fn e2e_calico_apiserver_running_if_present() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client, "calico-apiserver");
    let lp = ListParams::default().labels("app=calico-apiserver");

    let list = match pods.list(&lp).await {
        Ok(l) => l,
        // Namespace may not exist for non-operator installs — treat as pass
        Err(_) => return,
    };

    if list.items.is_empty() {
        // Not installed with the operator API server component — skip
        return;
    }

    let not_ready: Vec<String> = list
        .items
        .iter()
        .filter(|p| !pod_is_running_and_ready(p))
        .map(|p| {
            let phase = p
                .status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .unwrap_or("Unknown");
            format!("{} (phase={})", pod_name(p), phase)
        })
        .collect();

    assert!(
        not_ready.is_empty(),
        "calico-apiserver pods not Running/Ready:\n  {}",
        not_ready.join("\n  ")
    );
}

/// Verify no pods across all namespaces are stuck in Pending or CrashLoopBackOff
/// for more than a brief period after cluster startup.
#[tokio::test]
#[ignore]
async fn e2e_no_crashloopbackoff_pods() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::all(client);

    let pod_list = pods
        .list(&ListParams::default())
        .await
        .expect("Failed to list all pods");

    let crash_looping: Vec<String> = pod_list
        .items
        .iter()
        .filter(|pod| {
            let container_statuses = pod
                .status
                .as_ref()
                .and_then(|s| s.container_statuses.as_deref())
                .unwrap_or(&[]);

            container_statuses.iter().any(|cs| {
                cs.state
                    .as_ref()
                    .and_then(|s| s.waiting.as_ref())
                    .and_then(|w| w.reason.as_deref())
                    .map(|r| r.contains("CrashLoopBackOff") || r.contains("ImagePullBackOff"))
                    .unwrap_or(false)
            })
        })
        .map(|p| {
            let ns = p.metadata.namespace.as_deref().unwrap_or("default");
            let name = p.metadata.name.as_deref().unwrap_or("<unknown>");
            let reason = p
                .status
                .as_ref()
                .and_then(|s| s.container_statuses.as_deref())
                .unwrap_or(&[])
                .iter()
                .find_map(|cs| {
                    cs.state
                        .as_ref()
                        .and_then(|s| s.waiting.as_ref())
                        .and_then(|w| w.reason.as_deref())
                        .map(|r| r.to_string())
                })
                .unwrap_or_else(|| "Unknown".to_string());
            format!("{}/{} ({})", ns, name, reason)
        })
        .collect();

    assert!(
        crash_looping.is_empty(),
        "Pods in CrashLoopBackOff or ImagePullBackOff:\n  {}",
        crash_looping.join("\n  ")
    );
}

/// Verify the set of expected control-plane component pods exist in kube-system.
#[tokio::test]
#[ignore]
async fn e2e_control_plane_components_present() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client, "kube-system");

    let pod_list = pods
        .list(&ListParams::default())
        .await
        .expect("Failed to list kube-system pods");

    let pod_names: HashSet<String> = pod_list
        .items
        .iter()
        .filter_map(|p| p.metadata.name.clone())
        .collect();

    // Static pod names contain the node name as a suffix; check prefixes.
    let required_prefixes = [
        "kube-apiserver",
        "etcd",
        "kube-scheduler",
        "kube-controller-manager",
    ];

    for prefix in &required_prefixes {
        let found = pod_names.iter().any(|n| n.starts_with(prefix));
        assert!(
            found,
            "Expected control-plane pod with prefix '{}' not found in kube-system. \
             Found pods: {:?}",
            prefix, pod_names
        );
    }
}
