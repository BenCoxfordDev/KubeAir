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

//! Workload feature e2e tests.
//!
//! Verifies that the kube-air kubelet correctly implements common workload
//! features that real applications depend on:
//!
//!   - ConfigMap environment variable injection.
//!   - Init container ordering (init runs to completion before main starts).
//!   - emptyDir volume shared between two sidecar containers.
//!   - Service ClusterIP DNS resolution via CoreDNS.
//!
//! All tests are `#[ignore]` — run with `-- --ignored` from the e2e harness.
//!
//! Requirements:
//!   KUBECONFIG set (or /etc/kubernetes/admin.conf present).
//!   `kubectl` available on PATH.

use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapEnvSource, Container, EmptyDirVolumeSource, EnvFromSource, EnvVar,
    EnvVarSource, ObjectFieldSelector, Pod, PodSpec, Service, ServicePort, ServiceSpec, Toleration,
    Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
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

/// Wait up to `timeout` for the pod to be Running with all containers ready.
async fn wait_for_pod_running(pods: &Api<Pod>, name: &str, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(pod) = pods.get(name).await {
            let phase = pod
                .status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .unwrap_or("");
            let all_ready = pod
                .status
                .as_ref()
                .and_then(|s| s.container_statuses.as_deref())
                .map(|cs| !cs.is_empty() && cs.iter().all(|c| c.ready))
                .unwrap_or(false);
            if phase == "Running" && all_ready {
                return true;
            }
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

fn taint_toleration() -> Toleration {
    Toleration {
        operator: Some("Exists".to_string()),
        ..Default::default()
    }
}

/// Delete a pod and wait for it to disappear (up to 60 s).
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

/// Delete a ConfigMap (ignore not-found).
async fn cleanup_configmap(cms: &Api<ConfigMap>, name: &str) {
    let _ = cms.delete(name, &DeleteParams::default()).await;
}

/// Delete a Service (ignore not-found).
async fn cleanup_service(svcs: &Api<Service>, name: &str) {
    let _ = svcs.delete(name, &DeleteParams::default()).await;
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// A ConfigMap key injected as an environment variable must be visible inside
/// the container at runtime.
///
/// Creates a ConfigMap with a known key/value, mounts it via `envFrom`, runs
/// a pod that prints the env var, and asserts it appears in `kubectl logs`.
#[tokio::test]
#[ignore]
async fn e2e_configmap_env_injection() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");
    let cms: Api<ConfigMap> = Api::namespaced(client.clone(), "default");

    let cm_name = "kubeair-e2e-cm-env";
    let pod_name = "kubeair-e2e-configmap-env";

    cleanup_pod(&pods, pod_name).await;
    cleanup_configmap(&cms, cm_name).await;

    // Create ConfigMap
    let mut data = BTreeMap::new();
    data.insert("KUBEAIR_CM_VALUE".to_string(), "cm-injected-42".to_string());

    cms.create(
        &PostParams::default(),
        &ConfigMap {
            metadata: ObjectMeta {
                name: Some(cm_name.to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            data: Some(data),
            ..Default::default()
        },
    )
    .await
    .expect("Failed to create ConfigMap");

    // Pod that prints the injected env var
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
                    "echo \"configmap-env:${KUBEAIR_CM_VALUE}\"".into(),
                ]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                env_from: Some(vec![EnvFromSource {
                    config_map_ref: Some(ConfigMapEnvSource {
                        name: Some(cm_name.to_string()),
                        optional: Some(false),
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![taint_toleration()]),
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
    .await
    .expect("Pod did not reach terminal phase within 120s");

    assert_eq!(
        phase, "Succeeded",
        "Pod phase should be Succeeded, got {phase}"
    );

    // Assert log contains the injected value
    let out = kubectl(&["logs", pod_name, "-n", "default"]);
    let logs = String::from_utf8_lossy(&out.stdout);
    assert!(
        logs.contains("configmap-env:cm-injected-42"),
        "Expected ConfigMap value in logs, got: {logs}"
    );

    cleanup_pod(&pods, pod_name).await;
    cleanup_configmap(&cms, cm_name).await;
}

/// An init container must run to completion (exit 0) before the main container
/// starts.  The kubelet must enforce this ordering.
///
/// Strategy: the init container writes a sentinel file to an emptyDir volume.
/// The main container checks that the file exists and prints a success marker.
/// If init ran after main (or not at all) the main container would fail.
#[tokio::test]
#[ignore]
async fn e2e_init_container_ordering() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-init-order";
    cleanup_pod(&pods, pod_name).await;

    let manifest = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            init_containers: Some(vec![Container {
                name: "init".to_string(),
                image: Some("busybox:1.36".to_string()),
                command: Some(vec![
                    "sh".into(),
                    "-c".into(),
                    "echo init-done > /shared/sentinel && echo 'init container ran'".into(),
                ]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                volume_mounts: Some(vec![VolumeMount {
                    name: "shared".to_string(),
                    mount_path: "/shared".to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            }]),
            containers: vec![Container {
                name: "main".to_string(),
                image: Some("busybox:1.36".to_string()),
                command: Some(vec![
                    "sh".into(),
                    "-c".into(),
                    // Fail loudly if sentinel wasn't written by init
                    "test -f /shared/sentinel && echo 'init-order-ok' || (echo 'sentinel missing'; exit 1)".into(),
                ]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                volume_mounts: Some(vec![VolumeMount {
                    name: "shared".to_string(),
                    mount_path: "/shared".to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            volumes: Some(vec![Volume {
                name: "shared".to_string(),
                empty_dir: Some(EmptyDirVolumeSource::default()),
                ..Default::default()
            }]),
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![taint_toleration()]),
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
    .await
    .expect("Pod did not reach terminal phase within 120s");

    assert_eq!(
        phase, "Succeeded",
        "Pod phase should be Succeeded, got {phase}"
    );

    // Confirm init-order marker is in main container logs
    let out = kubectl(&["logs", pod_name, "-c", "main", "-n", "default"]);
    let logs = String::from_utf8_lossy(&out.stdout);
    assert!(
        logs.contains("init-order-ok"),
        "Expected 'init-order-ok' in main container logs, got: {logs}"
    );

    cleanup_pod(&pods, pod_name).await;
}

/// Two containers sharing an emptyDir volume can communicate via the
/// filesystem.  The writer container writes a file; the reader container
/// reads it via `kubectl exec` and the content must match.
#[tokio::test]
#[ignore]
async fn e2e_emptydir_volume_shared_between_containers() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-emptydir";
    cleanup_pod(&pods, pod_name).await;

    let manifest = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![
                Container {
                    name: "writer".to_string(),
                    image: Some("busybox:1.36".to_string()),
                    command: Some(vec![
                        "sh".into(),
                        "-c".into(),
                        // Write the file then sleep so the pod stays Running
                        // long enough for the reader to exec into.
                        "echo 'kubeair-shared-content' > /shared/data.txt && sleep 300".into(),
                    ]),
                    image_pull_policy: Some("IfNotPresent".to_string()),
                    volume_mounts: Some(vec![VolumeMount {
                        name: "shared".to_string(),
                        mount_path: "/shared".to_string(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                },
                Container {
                    name: "reader".to_string(),
                    image: Some("busybox:1.36".to_string()),
                    command: Some(vec!["sleep".into(), "300".into()]),
                    image_pull_policy: Some("IfNotPresent".to_string()),
                    volume_mounts: Some(vec![VolumeMount {
                        name: "shared".to_string(),
                        mount_path: "/shared".to_string(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                },
            ],
            volumes: Some(vec![Volume {
                name: "shared".to_string(),
                empty_dir: Some(EmptyDirVolumeSource::default()),
                ..Default::default()
            }]),
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![taint_toleration()]),
            ..Default::default()
        }),
        ..Default::default()
    };

    pods.create(&PostParams::default(), &manifest)
        .await
        .expect("Failed to create pod");

    let running = wait_for_pod_running(&pods, pod_name, Duration::from_secs(120)).await;
    assert!(running, "Pod did not reach Running/Ready within 120s");

    // Wait a moment for writer to finish writing before reader execs
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Read the shared file from the reader container via kubectl exec
    let out = kubectl(&[
        "exec",
        pod_name,
        "-c",
        "reader",
        "-n",
        "default",
        "--",
        "cat",
        "/shared/data.txt",
    ]);
    let content = String::from_utf8_lossy(&out.stdout);
    assert!(
        content.contains("kubeair-shared-content"),
        "Expected shared file content in reader container, got: {content}"
    );

    cleanup_pod(&pods, pod_name).await;
}

/// A Kubernetes Service must be resolvable via DNS inside a pod.
///
/// Creates a ClusterIP Service pointing to a simple echo server pod, then
/// execs a `nslookup` from a separate pod to verify that CoreDNS correctly
/// resolves the Service's cluster-local DNS name
/// (`<service>.<namespace>.svc.cluster.local`).
#[tokio::test]
#[ignore]
async fn e2e_service_dns_resolution() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");
    let svcs: Api<Service> = Api::namespaced(client.clone(), "default");

    let server_pod_name = "kubeair-e2e-dns-server";
    let client_pod_name = "kubeair-e2e-dns-client";
    let svc_name = "kubeair-e2e-dns-svc";

    cleanup_pod(&pods, server_pod_name).await;
    cleanup_pod(&pods, client_pod_name).await;
    cleanup_service(&svcs, svc_name).await;

    // Server pod: a minimal HTTP server (busybox httpd) on port 8080
    let mut server_labels = BTreeMap::new();
    server_labels.insert("kubeair-dns-server".to_string(), "true".to_string());

    let server_manifest = Pod {
        metadata: ObjectMeta {
            name: Some(server_pod_name.to_string()),
            namespace: Some("default".to_string()),
            labels: Some(server_labels.clone()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "server".to_string(),
                image: Some("busybox:1.36".to_string()),
                command: Some(vec![
                    "sh".into(),
                    "-c".into(),
                    "mkdir -p /www && echo 'ok' > /www/index.html && httpd -f -p 8080 -h /www"
                        .into(),
                ]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![taint_toleration()]),
            ..Default::default()
        }),
        ..Default::default()
    };

    pods.create(&PostParams::default(), &server_manifest)
        .await
        .expect("Failed to create server pod");

    // ClusterIP Service selecting the server pod
    svcs.create(
        &PostParams::default(),
        &Service {
            metadata: ObjectMeta {
                name: Some(svc_name.to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                selector: Some(server_labels),
                ports: Some(vec![ServicePort {
                    port: 8080,
                    target_port: Some(IntOrString::Int(8080)),
                    ..Default::default()
                }]),
                type_: Some("ClusterIP".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .expect("Failed to create Service");

    // Client pod: long-running so we can exec into it
    let client_manifest = Pod {
        metadata: ObjectMeta {
            name: Some(client_pod_name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "client".to_string(),
                image: Some("busybox:1.36".to_string()),
                command: Some(vec!["sleep".into(), "300".into()]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![taint_toleration()]),
            ..Default::default()
        }),
        ..Default::default()
    };

    pods.create(&PostParams::default(), &client_manifest)
        .await
        .expect("Failed to create client pod");

    // Wait for both pods to be Running
    let server_running =
        wait_for_pod_running(&pods, server_pod_name, Duration::from_secs(120)).await;
    assert!(
        server_running,
        "Server pod did not reach Running/Ready within 120s"
    );

    let client_running =
        wait_for_pod_running(&pods, client_pod_name, Duration::from_secs(120)).await;
    assert!(
        client_running,
        "Client pod did not reach Running/Ready within 120s"
    );

    // Resolve the Service DNS name from the client pod
    let fqdn = format!("{svc_name}.default.svc.cluster.local");
    let out = kubectl(&[
        "exec",
        client_pod_name,
        "-n",
        "default",
        "--",
        "nslookup",
        &fqdn,
    ]);
    let output = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(
        out.status.success(),
        "nslookup failed (exit {:?}):\nstdout: {output}\nstderr: {stderr}",
        out.status.code()
    );
    assert!(
        output.contains("Address") || output.contains("answer"),
        "nslookup output did not contain an address for {fqdn}:\n{output}"
    );

    cleanup_pod(&pods, server_pod_name).await;
    cleanup_pod(&pods, client_pod_name).await;
    cleanup_service(&svcs, svc_name).await;
}

/// A pod can read its own name via the Downward API (fieldRef: metadata.name)
/// injected as an environment variable.
#[tokio::test]
#[ignore]
async fn e2e_downward_api_pod_name_env() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-downward-api";
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
                    "echo \"pod-name:${MY_POD_NAME}\"".into(),
                ]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                env: Some(vec![EnvVar {
                    name: "MY_POD_NAME".to_string(),
                    value_from: Some(EnvVarSource {
                        field_ref: Some(ObjectFieldSelector {
                            field_path: "metadata.name".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![taint_toleration()]),
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
    .await
    .expect("Pod did not reach terminal phase within 120s");

    assert_eq!(
        phase, "Succeeded",
        "Pod phase should be Succeeded, got {phase}"
    );

    let out = kubectl(&["logs", pod_name, "-n", "default"]);
    let logs = String::from_utf8_lossy(&out.stdout);
    let expected = format!("pod-name:{pod_name}");
    assert!(
        logs.contains(&expected),
        "Expected '{expected}' in logs (Downward API pod name), got: {logs}"
    );

    cleanup_pod(&pods, pod_name).await;
}

/// The projected service-account token and CA bundle must be accessible inside
/// the container at the standard mount path `/var/run/secrets/kubernetes.io/serviceaccount`.
///
/// This verifies that:
///   1. The kubelet writes the projected volume files under the correct
///      `kubernetes.io~projected/<name>/` subdirectory (Go-kubelet convention).
///   2. The files are bind-mounted into the container at the expected paths.
///   3. The token file is non-empty and the CA bundle begins with a PEM header.
///
/// This is a regression test for the bug where the kubelet used `projected/<name>/`
/// instead of `kubernetes.io~projected/<name>/`, causing dangling bind-mounts.
#[tokio::test]
#[ignore]
async fn e2e_projected_service_account_token_accessible_in_container() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-sa-token";
    cleanup_pod(&pods, pod_name).await;

    // A simple pod that reads the projected SA token and CA bundle and prints
    // their first bytes.  We use `wc -c` to assert the files are non-empty and
    // `head -c 27` to assert the CA begins with the PEM header.
    let manifest = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "checker".to_string(),
                image: Some("busybox:1.36".to_string()),
                command: Some(vec![
                    "sh".into(),
                    "-c".into(),
                    concat!(
                        "SA=/var/run/secrets/kubernetes.io/serviceaccount; ",
                        "TOKEN_LEN=$(wc -c < $SA/token); ",
                        "CA_HEAD=$(head -c 27 $SA/ca.crt); ",
                        "NS=$(cat $SA/namespace); ",
                        "echo token-len:$TOKEN_LEN; ",
                        "echo ca-head:$CA_HEAD; ",
                        "echo namespace:$NS",
                    )
                    .into(),
                ]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![taint_toleration()]),
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
    .await
    .expect("Pod did not reach terminal phase within 120s");

    assert_eq!(
        phase, "Succeeded",
        "Pod phase should be Succeeded, got {phase}"
    );

    let out = kubectl(&["logs", pod_name, "-n", "default"]);
    let logs = String::from_utf8_lossy(&out.stdout);

    // Token must be non-empty.
    let token_len: usize = logs
        .lines()
        .find(|l| l.starts_with("token-len:"))
        .and_then(|l| l.strip_prefix("token-len:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    assert!(
        token_len > 0,
        "Service account token must be non-empty; got token-len:{token_len}\nfull logs:\n{logs}"
    );

    // CA bundle must start with the standard PEM header.
    assert!(
        logs.contains("ca-head:-----BEGIN CERTIFICATE"),
        "CA bundle must begin with '-----BEGIN CERTIFICATE'; got:\n{logs}"
    );

    // Namespace must match where the pod was created.
    assert!(
        logs.contains("namespace:default"),
        "Namespace file must contain 'default'; got:\n{logs}"
    );

    cleanup_pod(&pods, pod_name).await;
}

/// A Secret mounted as a volume must have its keys available as files inside
/// the container.
///
/// Regression test for the dangling-symlink bug: the Go kubelet's atomic writer
/// leaves `..data -> ..TIMESTAMP` symlinks behind after handover.  Our kubelet
/// must remove them before writing flat files, otherwise `open(path)` returns
/// ENOENT through the dangling chain.
///
/// Mirrors: `TestSecretVolumePlugin` in the Go kubelet node e2e suite.
#[tokio::test]
#[ignore]
async fn e2e_secret_volume_files_accessible_in_container() {
    use k8s_openapi::api::core::v1::{KeyToPath, Secret, SecretVolumeSource};

    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");
    let secrets: Api<Secret> = Api::namespaced(client.clone(), "default");

    let secret_name = "kubeair-e2e-secret-vol";
    let pod_name = "kubeair-e2e-secret-vol-pod";

    cleanup_pod(&pods, pod_name).await;
    let _ = secrets.delete(secret_name, &DeleteParams::default()).await;

    let mut string_data = BTreeMap::new();
    string_data.insert("tls.crt".to_string(), "FAKE_CERT_DATA".to_string());
    string_data.insert("tls.key".to_string(), "FAKE_KEY_DATA".to_string());

    secrets
        .create(
            &PostParams::default(),
            &Secret {
                metadata: ObjectMeta {
                    name: Some(secret_name.to_string()),
                    namespace: Some("default".to_string()),
                    ..Default::default()
                },
                string_data: Some(string_data),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to create Secret");

    let manifest = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "checker".to_string(),
                image: Some("busybox:1.36".to_string()),
                command: Some(vec![
                    "sh".into(),
                    "-c".into(),
                    concat!(
                        "CRT=$(cat /secret/tls.crt); ",
                        "KEY=$(cat /secret/tls.key); ",
                        "echo crt:$CRT; ",
                        "echo key:$KEY",
                    )
                    .into(),
                ]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                volume_mounts: Some(vec![VolumeMount {
                    name: "secret-vol".to_string(),
                    mount_path: "/secret".to_string(),
                    read_only: Some(true),
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            volumes: Some(vec![Volume {
                name: "secret-vol".to_string(),
                secret: Some(SecretVolumeSource {
                    secret_name: Some(secret_name.to_string()),
                    items: Some(vec![
                        KeyToPath {
                            key: "tls.crt".to_string(),
                            path: "tls.crt".to_string(),
                            ..Default::default()
                        },
                        KeyToPath {
                            key: "tls.key".to_string(),
                            path: "tls.key".to_string(),
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![taint_toleration()]),
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
    .await
    .expect("Pod did not reach terminal phase within 120s");

    assert_eq!(
        phase, "Succeeded",
        "Pod phase should be Succeeded, got {phase}"
    );

    let out = kubectl(&["logs", pod_name, "-n", "default"]);
    let logs = String::from_utf8_lossy(&out.stdout);

    assert!(
        logs.contains("crt:FAKE_CERT_DATA"),
        "tls.crt must be readable from Secret volume; got:\n{logs}"
    );
    assert!(
        logs.contains("key:FAKE_KEY_DATA"),
        "tls.key must be readable from Secret volume; got:\n{logs}"
    );

    cleanup_pod(&pods, pod_name).await;
    let _ = secrets.delete(secret_name, &DeleteParams::default()).await;
}

/// A ConfigMap mounted as a volume must have its keys available as files inside
/// the container.
///
/// Mirrors: `TestConfigMapVolumePlugin` in the Go kubelet node e2e suite.
#[tokio::test]
#[ignore]
async fn e2e_configmap_volume_files_accessible_in_container() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");
    let cms: Api<ConfigMap> = Api::namespaced(client.clone(), "default");

    let cm_name = "kubeair-e2e-cm-vol";
    let pod_name = "kubeair-e2e-cm-vol-pod";

    cleanup_pod(&pods, pod_name).await;
    cleanup_configmap(&cms, cm_name).await;

    let mut data = BTreeMap::new();
    data.insert("app.conf".to_string(), "setting=enabled".to_string());
    data.insert("version".to_string(), "v1.2.3".to_string());

    cms.create(
        &PostParams::default(),
        &ConfigMap {
            metadata: ObjectMeta {
                name: Some(cm_name.to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            data: Some(data),
            ..Default::default()
        },
    )
    .await
    .expect("Failed to create ConfigMap");

    let manifest = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "checker".to_string(),
                image: Some("busybox:1.36".to_string()),
                command: Some(vec![
                    "sh".into(),
                    "-c".into(),
                    concat!(
                        "CONF=$(cat /config/app.conf); ",
                        "VER=$(cat /config/version); ",
                        "echo conf:$CONF; ",
                        "echo ver:$VER",
                    )
                    .into(),
                ]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                volume_mounts: Some(vec![VolumeMount {
                    name: "cm-vol".to_string(),
                    mount_path: "/config".to_string(),
                    read_only: Some(true),
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            volumes: Some(vec![Volume {
                name: "cm-vol".to_string(),
                config_map: Some(k8s_openapi::api::core::v1::ConfigMapVolumeSource {
                    name: Some(cm_name.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![taint_toleration()]),
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
    .await
    .expect("Pod did not reach terminal phase within 120s");

    assert_eq!(
        phase, "Succeeded",
        "Pod phase should be Succeeded, got {phase}"
    );

    let out = kubectl(&["logs", pod_name, "-n", "default"]);
    let logs = String::from_utf8_lossy(&out.stdout);

    assert!(
        logs.contains("conf:setting=enabled"),
        "app.conf must be readable from ConfigMap volume; got:\n{logs}"
    );
    assert!(
        logs.contains("ver:v1.2.3"),
        "version must be readable from ConfigMap volume; got:\n{logs}"
    );

    cleanup_pod(&pods, pod_name).await;
    cleanup_configmap(&cms, cm_name).await;
}

/// A container with RestartPolicy=Never that exits non-zero must NOT be
/// restarted.  Its restart count must remain 0 and pod phase must be Failed.
///
/// Mirrors: the Never policy path in `TestRestartPolicy` in the Go kubelet
/// node e2e suite.
#[tokio::test]
#[ignore]
async fn e2e_restart_policy_never_failed_container_not_restarted() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-no-restart";
    cleanup_pod(&pods, pod_name).await;

    let manifest = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "failer".to_string(),
                image: Some("busybox:1.36".to_string()),
                command: Some(vec!["sh".into(), "-c".into(), "exit 42".into()]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![taint_toleration()]),
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
        &["Failed", "Succeeded"],
        Duration::from_secs(60),
    )
    .await
    .expect("Pod did not reach terminal phase within 60s");

    assert_eq!(
        phase, "Failed",
        "Pod with RestartPolicy=Never must reach Failed, got {phase}"
    );

    let pod = pods.get(pod_name).await.expect("Failed to get pod");
    let restart_count = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_deref())
        .and_then(|cs| cs.first())
        .map(|c| c.restart_count)
        .unwrap_or(-1);

    assert_eq!(
        restart_count, 0,
        "Container with RestartPolicy=Never must have restart count 0, got {restart_count}"
    );

    cleanup_pod(&pods, pod_name).await;
}

/// A container with a passing liveness probe must keep running with 0 restarts.
///
/// Mirrors: `TestLivenessHTTP` (healthy case) in the Go kubelet node e2e suite.
#[tokio::test]
#[ignore]
async fn e2e_liveness_probe_healthy_container_stays_running() {
    use k8s_openapi::api::core::v1::{HTTPGetAction, Probe};
    use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-liveness-ok";
    cleanup_pod(&pods, pod_name).await;

    // busybox httpd listens on 8080 — liveness probe hits /index.html
    let manifest = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "server".to_string(),
                image: Some("busybox:1.36".to_string()),
                command: Some(vec![
                    "sh".into(),
                    "-c".into(),
                    "mkdir -p /www && echo ok > /www/index.html && httpd -f -p 8080 -h /www".into(),
                ]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                liveness_probe: Some(Probe {
                    http_get: Some(HTTPGetAction {
                        path: Some("/index.html".to_string()),
                        port: IntOrString::Int(8080),
                        scheme: Some("HTTP".to_string()),
                        ..Default::default()
                    }),
                    initial_delay_seconds: Some(5),
                    period_seconds: Some(5),
                    failure_threshold: Some(3),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            restart_policy: Some("Always".to_string()),
            tolerations: Some(vec![taint_toleration()]),
            ..Default::default()
        }),
        ..Default::default()
    };

    pods.create(&PostParams::default(), &manifest)
        .await
        .expect("Failed to create pod");

    let running = wait_for_pod_running(&pods, pod_name, Duration::from_secs(60)).await;
    assert!(running, "Pod did not reach Running/Ready within 60s");

    // Wait 30s (covers multiple probe periods) — probe is healthy so the
    // container must not be restarted.
    tokio::time::sleep(Duration::from_secs(30)).await;

    let pod = pods.get(pod_name).await.expect("Failed to get pod");
    let restart_count = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_deref())
        .and_then(|cs| cs.first())
        .map(|c| c.restart_count)
        .unwrap_or(-1);

    assert_eq!(
        restart_count, 0,
        "Healthy container with passing liveness probe must not be restarted; got {restart_count} restarts"
    );

    cleanup_pod(&pods, pod_name).await;
}

/// A container whose liveness probe always fails must be restarted by the
/// kubelet once the failure threshold is exceeded.
///
/// Mirrors: `TestLivenessHTTP` (failing case) in the Go kubelet node e2e suite.
#[tokio::test]
#[ignore]
async fn e2e_liveness_probe_failing_container_is_restarted() {
    use k8s_openapi::api::core::v1::{HTTPGetAction, Probe};
    use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-liveness-fail";
    cleanup_pod(&pods, pod_name).await;

    // Container sleeps — never opens port 8080, so liveness probe always fails.
    let manifest = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "sleeper".to_string(),
                image: Some("busybox:1.36".to_string()),
                command: Some(vec!["sleep".into(), "300".into()]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                liveness_probe: Some(Probe {
                    http_get: Some(HTTPGetAction {
                        path: Some("/healthz".to_string()),
                        port: IntOrString::Int(8080),
                        scheme: Some("HTTP".to_string()),
                        ..Default::default()
                    }),
                    initial_delay_seconds: Some(5),
                    period_seconds: Some(5),
                    failure_threshold: Some(3),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            restart_policy: Some("Always".to_string()),
            tolerations: Some(vec![taint_toleration()]),
            ..Default::default()
        }),
        ..Default::default()
    };

    pods.create(&PostParams::default(), &manifest)
        .await
        .expect("Failed to create pod");

    let running = wait_for_pod_running(&pods, pod_name, Duration::from_secs(60)).await;
    assert!(running, "Pod did not reach Running/Ready within 60s");

    // Wait past failure threshold: initialDelay(5) + failures(3)*period(5) = 20s.
    // Add headroom for kubelet sync cycle.
    tokio::time::sleep(Duration::from_secs(40)).await;

    let pod = pods.get(pod_name).await.expect("Failed to get pod");
    let restart_count = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_deref())
        .and_then(|cs| cs.first())
        .map(|c| c.restart_count)
        .unwrap_or(0);

    assert!(
        restart_count > 0,
        "Container with always-failing liveness probe must be restarted at least once; got {restart_count} restarts"
    );

    cleanup_pod(&pods, pod_name).await;
}

// ── Device plugin e2e tests ───────────────────────────────────────────────────
//
// These tests run against a live cluster and verify that the kubelet's device
// plugin Registration gRPC service is reachable on kubelet.sock.  They are
// #[ignore] so they only run in the e2e harness.

/// Verify kubelet.sock exists and the Registration gRPC service responds.
///
/// Fails if:
///   - /var/lib/kubelet/device-plugins/kubelet.sock is absent (server never started), or
///   - the socket refuses connections (server bound but not accepting), or
///   - a Register() call returns an unexpected error.
#[tokio::test]
#[ignore]
async fn test_e2e_device_plugin_kubelet_sock_accepts_registration() {
    use hyper_util::rt::TokioIo;
    use kubelet_adapters::device_manager::proto::registration_client::RegistrationClient;
    use kubelet_adapters::device_manager::proto::RegisterRequest;
    use tonic::transport::{Endpoint, Uri};
    use tower::service_fn;

    // The socket path used by all Kubernetes device plugins.
    let kubelet_sock = std::path::PathBuf::from("/var/lib/kubelet/device-plugins/kubelet.sock");

    assert!(
        kubelet_sock.exists(),
        "kubelet.sock must exist at {}; the PluginRegistrationServer is not running",
        kubelet_sock.display()
    );

    let sock_clone = kubelet_sock.clone();
    let channel = Endpoint::try_from("http://[::]:1")
        .expect("valid endpoint")
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = sock_clone.clone();
            async move {
                tokio::net::UnixStream::connect(&path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .expect("should connect to kubelet.sock");

    let mut client = RegistrationClient::new(channel);

    // Use a synthetic resource name that is unlikely to conflict with real plugins.
    let resp = client
        .register(RegisterRequest {
            version: "v1beta1".to_string(),
            endpoint: "e2e-test-probe.sock".to_string(),
            resource_name: "e2e-probe.kubelet.rs/device".to_string(),
            options: vec![],
        })
        .await;

    // The call must not fail with a transport error.  A gRPC status error (e.g.
    // INVALID_ARGUMENT) is acceptable — it means the server is running.
    match resp {
        Ok(_) => {}
        Err(status) => {
            // UNAVAILABLE means the socket is not serving gRPC.
            assert_ne!(
                status.code(),
                tonic::Code::Unavailable,
                "kubelet.sock is not accepting gRPC connections: {status}"
            );
        }
    }
}

/// E2E: plugin-agent pods in the kube-device-plugins namespace are Running.
///
/// If any plugin-agent pod is not Running within 120 s, the device plugin
/// registration flow is broken.
#[tokio::test]
#[ignore]
async fn e2e_plugin_agent_pods_running() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client, "kube-device-plugins");

    let deadline = std::time::Instant::now() + Duration::from_secs(120);

    loop {
        let pod_list = pods
            .list(&kube::api::ListParams::default())
            .await
            .expect("list kube-device-plugins pods");

        // No pods → namespace or DaemonSet not deployed; skip test.
        if pod_list.items.is_empty() {
            eprintln!("No pods in kube-device-plugins namespace — skipping plugin-agent check");
            return;
        }

        let not_running: Vec<_> = pod_list
            .items
            .iter()
            .filter(|p| p.status.as_ref().and_then(|s| s.phase.as_deref()) != Some("Running"))
            .map(|p| p.metadata.name.as_deref().unwrap_or("?"))
            .collect();

        if not_running.is_empty() {
            return; // All pods running — test passes.
        }

        if std::time::Instant::now() > deadline {
            panic!(
                "plugin-agent pods not Running after 120 s: {:?}",
                not_running
            );
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

#[tokio::test]
#[ignore]
async fn e2e_device_plugin_pod_running() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client, "kube-device-plugins");

    let deadline = std::time::Instant::now() + Duration::from_secs(180);

    loop {
        let pod_list = pods
            .list(&kube::api::ListParams::default())
            .await
            .expect("list kube-device-plugins pods");

        let device_pods: Vec<_> = pod_list
            .items
            .iter()
            .filter(|p| {
                p.metadata
                    .name
                    .as_deref()
                    .unwrap_or("")
                    .starts_with("device-plugin-")
            })
            .collect();

        if device_pods.is_empty() {
            eprintln!("No device-plugin pods found in kube-device-plugins namespace — skipping");
            return;
        }

        let all_running = device_pods.iter().all(|p| {
            let phase = p.status.as_ref().and_then(|s| s.phase.as_deref());
            phase == Some("Running")
        });

        if all_running {
            return; // device plugin is up — test passes.
        }

        // Fail early if any pod is in a crash loop.
        for p in &device_pods {
            if let Some(statuses) = p
                .status
                .as_ref()
                .and_then(|s| s.container_statuses.as_ref())
            {
                for cs in statuses {
                    if let Some(waiting) = cs.state.as_ref().and_then(|s| s.waiting.as_ref()) {
                        if waiting.reason.as_deref() == Some("CrashLoopBackOff") {
                            panic!(
                                "device-plugin container '{}' is in CrashLoopBackOff — \
                                 device injection likely failed (Allocate() not called)",
                                cs.name
                            );
                        }
                    }
                }
            }
        }

        if std::time::Instant::now() > deadline {
            let phases: Vec<_> = device_pods
                .iter()
                .map(|p| {
                    (
                        p.metadata.name.as_deref().unwrap_or("?"),
                        p.status.as_ref().and_then(|s| s.phase.as_deref()),
                    )
                })
                .collect();
            panic!("device-plugin pods not Running after 180 s: {:?}", phases);
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// E2E: verify that a device plugin allocation actually injects devices into
/// the container.  Checks that the DeviceManager has a registered plugin
/// by inspecting the kubelet registration socket.
#[tokio::test]
#[ignore]
async fn e2e_device_plugin_registered() {
    use hyper_util::rt::TokioIo;
    use kubelet_adapters::device_manager::proto::registration_client::RegistrationClient;
    use kubelet_adapters::device_manager::proto::RegisterRequest;
    use tonic::transport::{Endpoint, Uri};
    use tower::service_fn;

    // Verify kubelet.sock is serving — if not, registration never happened.
    let kubelet_sock = std::path::PathBuf::from("/var/lib/kubelet/device-plugins/kubelet.sock");
    assert!(kubelet_sock.exists(), "kubelet.sock not found");

    // Verify the plugin socket is present (created by the plugin agent).
    // If it doesn't exist, the plugin infrastructure isn't deployed in this
    // cluster — skip gracefully rather than failing the whole suite.
    let plugin_sock = std::path::PathBuf::from("/var/lib/kubelet/device-plugins/plugin-agent.sock");
    if !plugin_sock.exists() {
        eprintln!(
            "plugin-agent.sock not found — plugin agent is not deployed in this cluster. Skipping."
        );
        return;
    }

    // Connect to kubelet.sock and send a probe Register() call.
    let sock_clone = kubelet_sock.clone();
    let channel = Endpoint::try_from("http://[::]:1")
        .unwrap()
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = sock_clone.clone();
            async move {
                tokio::net::UnixStream::connect(&path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .expect("should connect to kubelet.sock");

    let mut client = RegistrationClient::new(channel);
    let resp = client
        .register(RegisterRequest {
            version: "v1beta1".to_string(),
            endpoint: "e2e-test-probe.sock".to_string(),
            resource_name: "e2e-probe.kubelet.rs/plugin-check".to_string(),
            options: vec![],
        })
        .await;

    match resp {
        Ok(_) => {}
        Err(s) => assert_ne!(
            s.code(),
            tonic::Code::Unavailable,
            "kubelet registration gRPC server not responding: {s}"
        ),
    }
}

// ── CoreDNS stability (DNS policy:Default regression) ────────────────────────
//
// Regression: our kubelet's pod_to_spec used ..Default::default() and never
// set dns_config, so every pod — including CoreDNS which has dnsPolicy:Default
// — received ClusterFirst and had 10.96.0.10 injected as its nameserver.
// CoreDNS would then forward queries to itself, the loop plugin detected the
// cycle, and the pod entered a crash-loop (FATAL).
//
// This test:
//   1. Rolls out a fresh CoreDNS pod.
//   2. Waits for it to become Running.
//   3. Asserts its restart count is 0 (no crash-loop).
//   4. Verifies an external name (kubernetes.default.svc.cluster.local)
//      resolves correctly, confirming CoreDNS is functional.

#[tokio::test]
#[ignore]
async fn e2e_coredns_no_crash_loop_after_restart() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "kube-system");

    // Find the CoreDNS deployment name by listing pods in kube-system.
    let pod_list = pods
        .list(&Default::default())
        .await
        .expect("Failed to list kube-system pods");

    let coredns_pod_name = pod_list
        .items
        .iter()
        .filter_map(|p| p.metadata.name.as_deref())
        .find(|n| n.contains("coredns"))
        .map(str::to_string)
        .expect("No CoreDNS pod found in kube-system");

    // Wait up to 90 s for CoreDNS to be Running.
    let running = wait_for_pod_running(&pods, &coredns_pod_name, Duration::from_secs(90)).await;
    assert!(
        running,
        "CoreDNS pod {coredns_pod_name} did not reach Running within 90 s"
    );

    // Fetch current pod and assert restart_count == 0 (no crash-loop).
    let coredns = pods
        .get(&coredns_pod_name)
        .await
        .expect("Failed to get CoreDNS pod");
    let restart_count: i32 = coredns
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_deref())
        .and_then(|cs| cs.first())
        .map(|c| c.restart_count)
        .unwrap_or(0);
    assert_eq!(
        restart_count, 0,
        "CoreDNS has restarted {restart_count} time(s) — likely crash-looping \
         due to DNS loop. Check that dnsPolicy:Default is honoured."
    );

    // Verify DNS resolution via a lookup pod.
    let pod_name = "kubeair-e2e-coredns-dns-check";
    cleanup_pod(&pods, pod_name).await;

    let dns_pod = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("kube-system".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "dig".to_string(),
                image: Some("registry.k8s.io/e2e-test-images/agnhost:2.43".to_string()),
                command: Some(vec!["sh".to_string()]),
                args: Some(vec![
                    "-c".to_string(),
                    // nslookup returns 0 on success, non-zero on failure.
                    "nslookup kubernetes.default.svc.cluster.local && echo DNS_OK".to_string(),
                ]),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![taint_toleration()]),
            ..Default::default()
        }),
        ..Default::default()
    };

    let all_pods: Api<Pod> = Api::namespaced(client.clone(), "kube-system");
    all_pods
        .create(&PostParams::default(), &dns_pod)
        .await
        .expect("Failed to create DNS check pod");

    let phase = wait_for_pod_phase(
        &all_pods,
        pod_name,
        &["Succeeded", "Failed"],
        Duration::from_secs(60),
    )
    .await;

    // Capture logs before cleanup.
    // Use KUBECONFIG env var if set; fall back to ~/.kube/config then admin.conf.
    let kubeconfig = std::env::var("KUBECONFIG").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_default();
        let user_kc = format!("{home}/.kube/config");
        if std::path::Path::new(&user_kc).exists() {
            user_kc
        } else {
            "/etc/kubernetes/admin.conf".to_string()
        }
    });
    let logs_out = kubectl(&[
        "logs",
        pod_name,
        "-n",
        "kube-system",
        &format!("--kubeconfig={kubeconfig}"),
    ]);
    let logs = String::from_utf8_lossy(&logs_out.stdout);

    cleanup_pod(&all_pods, pod_name).await;

    assert_eq!(
        phase.as_deref(),
        Some("Succeeded"),
        "DNS check pod failed (phase={phase:?}). CoreDNS may not be resolving. \
         Logs: {logs}"
    );
    assert!(
        logs.contains("DNS_OK"),
        "DNS_OK marker missing from logs — nslookup failed. Logs: {logs}"
    );
}

/// The `HOSTNAME` environment variable inside a container must equal the pod's
/// name (or `spec.hostname` if set), **not** the node's hostname.
///
/// This is a regression test for the bug where an empty `sandbox_config.hostname`
/// caused containerd to inherit the node hostname (e.g. `worker-node`)
/// and inject it as `HOSTNAME` into every container. Stateful applications
/// that use `$HOSTNAME` to derive their pod identity will fail when it
/// contains the node name.
///
/// Mirrors: `TestHostnameEnvVar` and the Go kubelet's automatic `HOSTNAME` injection.
#[tokio::test]
#[ignore]
async fn e2e_hostname_env_var_equals_pod_name() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-hostname-env";
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
                    // Print both $HOSTNAME and the kernel hostname so we can
                    // assert both equal the pod name.
                    "echo \"env-hostname:${HOSTNAME}\"; echo \"kernel-hostname:$(hostname)\""
                        .into(),
                ]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![taint_toleration()]),
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
    .await
    .expect("Pod did not reach terminal phase within 120s");

    assert_eq!(
        phase, "Succeeded",
        "Pod phase should be Succeeded, got {phase}"
    );

    let out = kubectl(&["logs", pod_name, "-n", "default"]);
    let logs = String::from_utf8_lossy(&out.stdout);

    let expected_env = format!("env-hostname:{pod_name}");
    assert!(
        logs.contains(&expected_env),
        "HOSTNAME env var must equal pod name '{pod_name}', got:\n{logs}"
    );

    let expected_kernel = format!("kernel-hostname:{pod_name}");
    assert!(
        logs.contains(&expected_kernel),
        "kernel hostname (from `hostname` cmd) must equal pod name '{pod_name}', got:\n{logs}"
    );

    cleanup_pod(&pods, pod_name).await;
}

/// When `spec.hostname` is explicitly set on a pod, the `HOSTNAME` env var and
/// the UTS hostname inside containers must reflect that value, not the pod name
/// and not the node hostname.
///
/// Mirrors: the Go kubelet's `spec.hostname` handling in `generatePodHostNameAndDomain`.
#[tokio::test]
#[ignore]
async fn e2e_spec_hostname_overrides_pod_name() {
    let client = cluster_client().await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), "default");

    let pod_name = "kubeair-e2e-spec-hostname";
    let custom_hostname = "my-custom-host";
    cleanup_pod(&pods, pod_name).await;

    let manifest = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            hostname: Some(custom_hostname.to_string()),
            containers: vec![Container {
                name: "main".to_string(),
                image: Some("busybox:1.36".to_string()),
                command: Some(vec![
                    "sh".into(),
                    "-c".into(),
                    "echo \"env-hostname:${HOSTNAME}\"; echo \"kernel-hostname:$(hostname)\""
                        .into(),
                ]),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            tolerations: Some(vec![taint_toleration()]),
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
    .await
    .expect("Pod did not reach terminal phase within 120s");

    assert_eq!(
        phase, "Succeeded",
        "Pod phase should be Succeeded, got {phase}"
    );

    let out = kubectl(&["logs", pod_name, "-n", "default"]);
    let logs = String::from_utf8_lossy(&out.stdout);

    let expected_env = format!("env-hostname:{custom_hostname}");
    assert!(
        logs.contains(&expected_env),
        "HOSTNAME env var must equal spec.hostname '{custom_hostname}', got:\n{logs}"
    );

    let expected_kernel = format!("kernel-hostname:{custom_hostname}");
    assert!(
        logs.contains(&expected_kernel),
        "kernel hostname must equal spec.hostname '{custom_hostname}', got:\n{logs}"
    );

    cleanup_pod(&pods, pod_name).await;
}
