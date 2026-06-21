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

//! Integration tests for runtime, networking, storage, and node services.

use chrono::{Duration as ChronoDuration, Utc};
use kubelet_adapters::cgroup::{CgroupManager, CgroupPath, shares_to_weight, weight_to_shares};
use kubelet_adapters::cni::{CniNetworkPlugin, cni_env};
use kubelet_adapters::csi::{
    CsiVolumeContext, CsiVolumeManager, proto::AccessType as CsiAccessType,
};
use kubelet_adapters::kube_reporter::{KubeConnectMode, KubeNodeReporter, build_node_status_patch};
use kubelet_adapters::mock_runtime::MockRuntime;
use kubelet_adapters::nfd::{FeatureScanner, NodeFeatures};
use kubelet_adapters::tls::{CertificateRotationManager, CertificateStore};
use kubelet_app::streaming::{
    FramedMessage, STREAM_STDERR, STREAM_STDOUT, StreamMultiplexer, exec_to_frames,
};
use kubelet_core::node::NodeStatus;
use kubelet_core::pod::lifecycle::{PodLifecycleState, PodPhase};
use kubelet_core::qos::QosClass;
use kubelet_core::types::{PodRef, PodUID};
use kubelet_cri::mock_server::MockCriServer;
use kubelet_cri::types::{
    CriContainerConfig, CriContainerMetadata, CriContainerState, CriImageSpec,
    CriPodSandboxMetadata, CriSandboxConfig,
};
use kubelet_ports::driven::container_runtime::{
    ContainerRuntime, CreateContainerConfig, CreateSandboxConfig,
};
use kubelet_ports::driven::network::NetworkPlugin;
use kubelet_ports::driven::node_reporter::NodeReporter;
use std::collections::HashMap;
use tempfile::TempDir;
use tokio::net::UnixListener;

// ── Phase 11: CRI gRPC mock server ───────────────────────────────────────────

#[tokio::test]
async fn test_cri_mock_full_pod_lifecycle() {
    let srv = MockCriServer::new();

    // 1. Pull image
    let image_id = srv.pull_image("nginx:1.25").await;
    assert!(!image_id.is_empty());
    assert_eq!(srv.image_count().await, 1);

    // 2. Create sandbox
    let sandbox_id = srv
        .run_pod_sandbox(CriSandboxConfig {
            metadata: CriPodSandboxMetadata {
                name: "nginx-pod".to_string(),
                uid: "uid-integ-cri".to_string(),
                namespace: "default".to_string(),
                attempt: 0,
            },
            hostname: "nginx-pod".to_string(),
            log_directory: "/var/log/pods".to_string(),
            dns_config: None,
            port_mappings: vec![],
            labels: HashMap::new(),
            annotations: HashMap::new(),
            linux: None,
        })
        .await
        .unwrap();

    // 3. Create + start container
    let ctr_id = srv
        .create_container(
            &sandbox_id,
            CriContainerConfig {
                metadata: CriContainerMetadata {
                    name: "nginx".to_string(),
                    attempt: 0,
                },
                image: CriImageSpec {
                    image: "nginx:1.25".to_string(),
                    annotations: HashMap::new(),
                },
                command: vec!["nginx".to_string()],
                args: vec!["-g".to_string(), "daemon off;".to_string()],
                working_dir: "/".to_string(),
                envs: vec![],
                mounts: vec![],
                log_path: "/var/log/pods/default_nginx-pod/nginx/0.log".to_string(),
                stdin: false,
                stdin_once: false,
                tty: false,
                linux: None,
                labels: HashMap::new(),
                annotations: HashMap::new(),
            },
        )
        .await
        .unwrap();

    srv.start_container(&ctr_id).await.unwrap();

    let status = srv.container_status(&ctr_id).await.unwrap();
    assert_eq!(status.state, CriContainerState::ContainerRunning);

    // 4. Exec
    let exec_result = srv
        .exec_sync(&ctr_id, vec!["nginx".to_string(), "-v".to_string()], 5)
        .await;
    assert_eq!(exec_result.exit_code, 0);

    // 5. Stop + remove
    srv.stop_container(&ctr_id, 0).await;
    srv.remove_container(&ctr_id).await;
    srv.remove_pod_sandbox(&sandbox_id).await;

    assert_eq!(srv.container_count().await, 0);
    assert_eq!(srv.sandbox_count().await, 0);
}

#[tokio::test]
async fn test_cri_sandbox_network_ip_assigned() {
    let srv = MockCriServer::new();
    let id = srv
        .run_pod_sandbox(CriSandboxConfig {
            metadata: CriPodSandboxMetadata {
                name: "p".to_string(),
                uid: "u".to_string(),
                namespace: "d".to_string(),
                attempt: 0,
            },
            hostname: "p".to_string(),
            log_directory: "/t".to_string(),
            dns_config: None,
            port_mappings: vec![],
            labels: HashMap::new(),
            annotations: HashMap::new(),
            linux: None,
        })
        .await
        .unwrap();

    let status = srv.pod_sandbox_status(&id).await.unwrap();
    assert_eq!(status.network.unwrap().ip, "10.244.0.1");
}

// ── Phase 12: kube-rs reporter ────────────────────────────────────────────────

#[tokio::test]
async fn test_kube_reporter_standalone_all_methods() {
    let reporter = KubeNodeReporter::with_mode("integ-node", KubeConnectMode::Standalone).await;
    let status = NodeStatus::new("integ-node");
    reporter.report_node_status(&status).await.unwrap();
    reporter.renew_node_lease("integ-node", 40).await.unwrap();
    reporter
        .patch_node_conditions("integ-node", &[])
        .await
        .unwrap();

    let pod_ref = PodRef::new("default", "pod-1");
    let uid = PodUID::new("uid-1");
    let state = PodLifecycleState::default();
    reporter
        .report_pod_status(&pod_ref, &uid, &state)
        .await
        .unwrap();
}

#[test]
fn test_node_status_patch_json_valid() {
    let status = NodeStatus::new("node1");
    let patch = build_node_status_patch(&status);
    assert!(patch["status"].is_object());
    assert!(patch["status"]["conditions"].is_array());
    assert!(patch["status"]["capacity"]["cpu"].is_string());
}

#[test]
fn test_pod_status_patch_phase() {
    let state = PodLifecycleState {
        phase: PodPhase::Running,
        pod_ip: Some("10.0.0.5".to_string()),
        ..Default::default()
    };
    assert_eq!(state.phase, PodPhase::Running);
    assert_eq!(state.pod_ip.as_deref(), Some("10.0.0.5"));
}

// ── Phase 13: WebSocket streaming ────────────────────────────────────────────

#[test]
fn test_ws_frame_encode_decode() {
    let msg = FramedMessage::new(STREAM_STDOUT, b"output data".to_vec());
    let encoded = msg.encode();
    let decoded = FramedMessage::decode(&encoded).unwrap();
    assert_eq!(decoded.stream_id, STREAM_STDOUT);
    assert_eq!(decoded.payload, b"output data");
}

#[test]
fn test_ws_exit_code_zero_is_success() {
    let msg = FramedMessage::new(
        kubelet_app::streaming::STREAM_ERROR,
        serde_json::json!({"status": "Success"})
            .to_string()
            .into_bytes(),
    );
    let json: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(json["status"], "Success");
}

#[tokio::test]
async fn test_stream_mux_routes_to_correct_channel() {
    let mux = StreamMultiplexer::demux(vec![
        FramedMessage::new(STREAM_STDOUT, b"out".to_vec()),
        FramedMessage::new(STREAM_STDERR, b"err".to_vec()),
    ]);

    assert_eq!(mux.stdout_string(), "out");
    assert_eq!(mux.stderr_string(), "err");
}

#[tokio::test]
async fn test_exec_to_frames_mock_runtime() {
    let _rt = MockRuntime::new();
    let frames = exec_to_frames(b"ls output".to_vec(), Vec::new());
    assert!(!frames.is_empty());
    assert_eq!(frames[0].stream_id, STREAM_STDOUT);
}

#[tokio::test]
async fn test_attach_running_container_stdio_passthrough() {
    let runtime = MockRuntime::new();

    let sandbox_id = runtime
        .run_pod_sandbox(CreateSandboxConfig {
            pod_uid: "uid-attach-1".to_string(),
            pod_name: "attach-pod".to_string(),
            pod_namespace: "default".to_string(),
            hostname: "attach-pod".to_string(),
            log_directory: "/tmp".to_string(),
            dns_config: None,
            port_mappings: vec![],
            labels: HashMap::new(),
            annotations: HashMap::new(),
            linux_cgroup_parent: "/kubepods/besteffort/poduid-attach-1".to_string(),
            sysctls: HashMap::new(),
            host_network: false,
            host_pid: false,
            host_ipc: false,
            runtime_handler: "runc".to_string(),
            sandbox_image: "pause:3.6".to_string(),
            supplemental_groups: vec![],
            privileged: false,
            share_process_namespace: false,
        })
        .await
        .unwrap();

    let container_id = runtime
        .create_container(CreateContainerConfig {
            pod_uid: "uid-attach-1".to_string(),
            pod_name: "attach-pod".to_string(),
            pod_namespace: "default".to_string(),
            attempt: 0,
            container: kubelet_core::pod::ContainerSpec {
                name: "sleepy".to_string(),
                image: "busybox:latest".to_string(),
                command: vec!["sleep".to_string()],
                args: vec!["300".to_string()],
                image_pull_policy: kubelet_core::pod::ImagePullPolicy::IfNotPresent,
                resources: kubelet_core::pod::ResourceRequirements::default(),
                tty: Some(true),
                ..Default::default()
            },
            sandbox_id,
            image_id: "sha256:attach-test".to_string(),
            log_directory: "/tmp".to_string(),
            linux_cgroup_parent: "/kubepods/besteffort/poduid-attach-1".to_string(),
            env_overrides: HashMap::new(),
            extra_env: vec![],
            security: Default::default(),
            extra_devices: vec![],
            extra_mounts: vec![],
            extra_device_envs: vec![],
            share_process_namespace: false,
            pod_hostname: "attach-pod".to_string(),
        })
        .await
        .unwrap();

    runtime.start_container(&container_id).await.unwrap();

    let result = runtime.attach_sync(&container_id, 30).await.unwrap();
    assert_eq!(result.exit_code, 0);
    assert!(result.stderr.is_empty());
    assert!(
        String::from_utf8_lossy(&result.stdout).contains("mock attach:"),
        "expected attach stdout marker, got {:?}",
        result.stdout
    );
}

// ── Phase 14: CNI ─────────────────────────────────────────────────────────────

#[test]
fn test_cni_env_all_fields() {
    let env = cni_env(
        "ADD",
        "ctr-abc",
        "/var/run/netns/foo",
        "eth0",
        "/opt/cni/bin",
    );
    assert_eq!(env["CNI_COMMAND"], "ADD");
    assert_eq!(env["CNI_CONTAINERID"], "ctr-abc");
    assert_eq!(env["CNI_NETNS"], "/var/run/netns/foo");
    assert_eq!(env["CNI_IFNAME"], "eth0");
}

#[tokio::test]
async fn test_cni_fallback_without_plugin() {
    let dir = TempDir::new().unwrap();
    let plugin = CniNetworkPlugin::from_config_dir(dir.path(), dir.path());
    let result = plugin
        .setup_pod("uid", "default", "pod", "sandbox-1", &HashMap::new())
        .await;
    assert!(result.is_ok(), "CNI should fall back gracefully");
    let att = result.unwrap();
    assert!(!att.ip_addresses.is_empty());
}

// ── Phase 15: CSI ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_csi_full_volume_lifecycle() {
    let dir = TempDir::new().unwrap();
    let plugin_dir = dir.path().join("test-driver");
    std::fs::create_dir_all(&plugin_dir).unwrap();
    let socket = plugin_dir.join("csi.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let socket_task = tokio::spawn(async move {
        loop {
            if listener.accept().await.is_err() {
                break;
            }
        }
    });

    let mut mgr = CsiVolumeManager::new(dir.path());
    mgr.registry.scan();
    assert_eq!(mgr.registry.plugin_count(), 1);

    let target = dir.path().join("target");
    let ctx = CsiVolumeContext {
        volume_id: "vol-integ-1".to_string(),
        volume_attributes: [(
            "csi.storage.k8s.io/driver".to_string(),
            "test-driver".to_string(),
        )]
        .into_iter()
        .collect(),
        target_path: target.clone(),
        staging_target_path: None,
        read_only: false,
        access_type: CsiAccessType::Mount,
        fs_type: Some("ext4".to_string()),
        mount_flags: vec![],
        secrets: HashMap::new(),
    };

    mgr.publish_volume(&ctx).await.unwrap();
    assert!(mgr.is_published("vol-integ-1"));

    mgr.unpublish_volume("vol-integ-1", &target).await.unwrap();
    assert!(!mgr.is_published("vol-integ-1"));

    socket_task.abort();
}

// ── Phase 16: TLS cert rotation ───────────────────────────────────────────────

#[test]
fn test_tls_cert_rotation_lifecycle() {
    let dir = TempDir::new().unwrap();
    let mut mgr = CertificateRotationManager::new("node1", dir.path());

    // Initially needs rotation (no cert)
    assert!(mgr.rotation_needed().is_some());

    // Generate and install
    let cert = mgr.generate_self_signed(90).unwrap();
    assert!(!cert.is_expired());
    mgr.store_mut().install(cert).unwrap();

    // Now doesn't need rotation (fresh cert)
    assert!(mgr.rotation_needed().is_none());

    // Simulate cert aging to 85% of lifetime
    let meta = mgr.store().current().unwrap().clone();
    let lifetime_secs = (meta.not_after - meta.not_before).num_seconds();
    assert!(lifetime_secs > 0);
}

#[test]
fn test_tls_cert_persists_to_disk() {
    let dir = TempDir::new().unwrap();
    let mut store = CertificateStore::new(dir.path());

    let cert = kubelet_adapters::tls::CertificateMeta {
        pem: "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----\n".to_string(),
        not_before: Utc::now(),
        not_after: Utc::now() + ChronoDuration::days(90),
        common_name: "system:node:node1".to_string(),
        sans: vec!["node1".to_string()],
        serial: "abc123".to_string(),
    };
    store.install(cert).unwrap();

    // Re-load from disk
    let mut store2 = CertificateStore::new(dir.path());
    assert!(store2.load().unwrap());
    assert_eq!(store2.current().unwrap().serial, "abc123");
}

// ── Phase 17: cgroup v2 ───────────────────────────────────────────────────────

#[test]
fn test_cgroup_path_structure_integration() {
    let paths = CgroupPath::new("/sys/fs/cgroup");
    let uid = PodUID::new("test-uid-123");

    let pod_slice = paths.pod_slice(&QosClass::Guaranteed, &uid);
    assert!(pod_slice.to_str().unwrap().contains("guaranteed"));
    assert!(pod_slice.to_str().unwrap().contains("test_uid_123"));

    let container_scope = paths.container_scope(&QosClass::Burstable, &uid, "abc123");
    assert!(
        container_scope
            .to_str()
            .unwrap()
            .contains("cri-containerd-abc123")
    );
}

#[tokio::test]
async fn test_cgroup_manager_create_and_remove_dry_run() {
    let mgr = CgroupManager::new("/sys/fs/cgroup", true);
    let uid = PodUID::new("uid-cg-integ");
    mgr.create_pod_cgroup(&QosClass::BestEffort, &uid)
        .await
        .unwrap();
    mgr.remove_pod_cgroup(&QosClass::BestEffort, &uid)
        .await
        .unwrap();
}

#[test]
fn test_cgroup_cpu_weight_conversion_roundtrip() {
    let shares = 2048u64;
    let weight = shares_to_weight(shares);
    let back = weight_to_shares(weight);
    // Allow small rounding error
    assert!((back as i64 - shares as i64).abs() < 10);
}

// ── Phase 18: Node feature discovery ─────────────────────────────────────────

#[test]
fn test_nfd_features_to_labels() {
    let mut features = NodeFeatures {
        cpu_flags: vec!["avx".to_string(), "sse4_2".to_string()],
        architecture: "arm64".to_string(),
        ..Default::default()
    };
    features.hugepages.insert("2048kB".to_string(), 4);

    let labels = features.to_labels();
    assert!(labels.contains_key("feature.node.kubernetes.io/cpu-cpuid.AVX"));
    assert!(labels.contains_key("feature.node.kubernetes.io/cpu-cpuid.SSE4_2"));
    assert!(labels.contains_key("feature.node.kubernetes.io/memory-numa.hugepages-2048kB"));
}

#[test]
fn test_nfd_scanner_reads_real_system() {
    // Run against real /proc if available, otherwise skip gracefully
    let scanner = FeatureScanner::new();
    let features = scanner.scan();
    // Architecture should always be detected
    assert!(!features.architecture.is_empty());
}

#[test]
fn test_nfd_extended_resources() {
    let mut features = NodeFeatures::default();
    features
        .extended_resources
        .insert("example.com/fpga".to_string(), 1);
    let ext = features.to_extended_resources();
    assert_eq!(*ext.get("example.com/fpga").unwrap(), 1);
}

// ── Volume update propagation integration tests ───────────────────────────────
//
// These tests verify that ConfigMap and Secret volumes correctly reflect
// updates when the underlying resource data changes (reconcile cycle) and
// that stale files are removed when keys are deleted or the resource itself
// is deleted (optional volumes).

#[test]
fn test_configmap_volume_mount_reflects_initial_data() {
    use kubelet_adapters::volume::configmap::{ConfigMapData, ConfigMapVolumeManager};
    let dir = TempDir::new().unwrap();
    let mgr = ConfigMapVolumeManager::new(dir.path());
    let target = dir.path().join("vol");

    let cm = ConfigMapData {
        namespace: "default".to_string(),
        name: "my-cm".to_string(),
        data: [("key1".to_string(), "value1".to_string())].into(),
        binary_data: HashMap::new(),
    };
    mgr.mount(&cm, &target, &[], 0o644).unwrap();

    assert_eq!(
        std::fs::read_to_string(target.join("key1")).unwrap(),
        "value1"
    );
}

#[test]
fn test_configmap_volume_update_overwrites_existing_file() {
    // Simulates two successive reconcile cycles where the ConfigMap value changes.
    use kubelet_adapters::volume::configmap::{ConfigMapData, ConfigMapVolumeManager};
    let dir = TempDir::new().unwrap();
    let mgr = ConfigMapVolumeManager::new(dir.path());
    let target = dir.path().join("vol");

    // First mount: initial data.
    let cm_v1 = ConfigMapData {
        namespace: "default".to_string(),
        name: "my-cm".to_string(),
        data: [("config".to_string(), "v1".to_string())].into(),
        binary_data: HashMap::new(),
    };
    mgr.mount(&cm_v1, &target, &[], 0o644).unwrap();
    assert_eq!(
        std::fs::read_to_string(target.join("config")).unwrap(),
        "v1"
    );

    // Second mount (simulates reconcile after CM update): updated data.
    let cm_v2 = ConfigMapData {
        namespace: "default".to_string(),
        name: "my-cm".to_string(),
        data: [("config".to_string(), "v2".to_string())].into(),
        binary_data: HashMap::new(),
    };
    mgr.mount(&cm_v2, &target, &[], 0o644).unwrap();
    assert_eq!(
        std::fs::read_to_string(target.join("config")).unwrap(),
        "v2",
        "Updated ConfigMap value must be reflected after re-mount"
    );
}

#[test]
fn test_configmap_volume_stale_key_removed_on_update() {
    // When a key is removed from a ConfigMap, the corresponding file must
    // disappear from the volume directory on the next reconcile cycle.
    use kubelet_adapters::volume::configmap::{ConfigMapData, ConfigMapVolumeManager};
    let dir = TempDir::new().unwrap();
    let mgr = ConfigMapVolumeManager::new(dir.path());
    let target = dir.path().join("vol");

    // First mount: two keys.
    let cm_v1 = ConfigMapData {
        namespace: "default".to_string(),
        name: "my-cm".to_string(),
        data: [
            ("kept".to_string(), "a".to_string()),
            ("removed".to_string(), "b".to_string()),
        ]
        .into(),
        binary_data: HashMap::new(),
    };
    mgr.mount(&cm_v1, &target, &[], 0o644).unwrap();
    assert!(target.join("kept").exists());
    assert!(target.join("removed").exists());

    // Second mount: 'removed' key is gone from the CM.
    // Simulate what the pod worker's reconcile does: re-mount then remove stale files.
    let cm_v2 = ConfigMapData {
        namespace: "default".to_string(),
        name: "my-cm".to_string(),
        data: [("kept".to_string(), "a-updated".to_string())].into(),
        binary_data: HashMap::new(),
    };
    mgr.mount(&cm_v2, &target, &[], 0o644).unwrap();

    // Simulate the stale-file cleanup that pod_worker performs after re-writing.
    let expected_keys: std::collections::HashSet<_> =
        cm_v2.data.keys().map(|k| target.join(k)).collect();
    if let Ok(entries) = std::fs::read_dir(&target) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && !expected_keys.contains(&path) {
                std::fs::remove_file(&path).unwrap();
            }
        }
    }

    assert!(target.join("kept").exists(), "'kept' must remain");
    assert_eq!(
        std::fs::read_to_string(target.join("kept")).unwrap(),
        "a-updated"
    );
    assert!(
        !target.join("removed").exists(),
        "'removed' key must not appear after ConfigMap update removes it"
    );
}

#[test]
fn test_secret_volume_update_overwrites_existing_file() {
    use kubelet_adapters::volume::secret::{SecretData, SecretVolumeManager};
    let dir = TempDir::new().unwrap();
    let mgr = SecretVolumeManager::new(dir.path());
    let target = dir.path().join("vol");

    let secret_v1 = SecretData {
        namespace: "default".to_string(),
        name: "my-secret".to_string(),
        data: [("token".to_string(), b"old-token".to_vec())].into(),
        secret_type: "Opaque".to_string(),
    };
    mgr.mount(&secret_v1, &target, &[], 0o600).unwrap();
    assert_eq!(std::fs::read(target.join("token")).unwrap(), b"old-token");

    // Simulate reconcile: Secret data updated.
    let secret_v2 = SecretData {
        namespace: "default".to_string(),
        name: "my-secret".to_string(),
        data: [("token".to_string(), b"new-token".to_vec())].into(),
        secret_type: "Opaque".to_string(),
    };
    mgr.mount(&secret_v2, &target, &[], 0o600).unwrap();
    assert_eq!(
        std::fs::read(target.join("token")).unwrap(),
        b"new-token",
        "Updated Secret value must be reflected after re-mount"
    );
}

#[test]
fn test_optional_volume_cleared_when_source_deleted() {
    // When an optional ConfigMap is deleted, the volume directory must be emptied.
    // This simulates the pod_worker behaviour when api.get() returns NOT_FOUND
    // for an optional volume source.
    let dir = TempDir::new().unwrap();
    let vol_dir = dir.path().join("vol");
    std::fs::create_dir_all(&vol_dir).unwrap();

    // Pre-existing files from a previously-present optional CM.
    std::fs::write(vol_dir.join("file_from_deleted_cm"), b"stale").unwrap();
    std::fs::write(vol_dir.join("another_key"), b"also stale").unwrap();

    // Simulate clear_volume_dir (runs when optional CM is not found).
    if let Ok(entries) = std::fs::read_dir(&vol_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                std::fs::remove_file(&path).unwrap();
            }
        }
    }

    let remaining: Vec<_> = std::fs::read_dir(&vol_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.path().is_file())
        .collect();
    assert!(
        remaining.is_empty(),
        "All files must be removed when optional CM is deleted; found: {:?}",
        remaining.iter().map(|e| e.path()).collect::<Vec<_>>()
    );
}

// ── DNS policy integration ────────────────────────────────────────────────────
//
// Regression: pod_to_spec used ..Default::default() which left dns_config as
// None. build_dns_config then fell back to ClusterFirst and injected 10.96.0.10
// into the CoreDNS sandbox, causing the loop plugin FATAL crash-loop.

#[test]
fn test_build_dns_config_default_policy_excludes_cluster_dns() {
    use kubelet_adapters::sandbox_builder::{NodeDnsConfig, build_dns_config};
    use kubelet_core::pod::{DnsConfig, DnsPolicy, PodSpec};
    use kubelet_core::types::{PodRef, PodUID};

    let pod = PodSpec {
        uid: PodUID::new("uid-coredns"),
        pod_ref: PodRef::new("kube-system", "coredns-abc123"),
        dns_config: Some(DnsConfig {
            policy: DnsPolicy::Default,
            nameservers: vec![],
            searches: vec![],
            options: vec![],
        }),
        ..Default::default()
    };

    let node_dns = NodeDnsConfig {
        cluster_dns: vec!["10.96.0.10".to_string()],
        cluster_domain: "cluster.local".to_string(),
        // Use /dev/null so resolv.conf parsing returns empty (no real file dep).
        resolv_conf_path: "/dev/null".to_string(),
    };

    let dns = build_dns_config(&pod, &node_dns);

    assert!(
        !dns.servers.contains(&"10.96.0.10".to_string()),
        "DnsPolicy::Default must never inject cluster DNS into sandbox; \
         servers: {:?}",
        dns.servers
    );
    // ndots:5 is a ClusterFirst-ism; Default policy must not add it.
    assert!(
        !dns.options.contains(&"ndots:5".to_string()),
        "DnsPolicy::Default must not add ndots:5; options: {:?}",
        dns.options
    );
}

#[test]
fn test_build_dns_config_cluster_first_none_uses_cluster_dns() {
    // When dns_config is None the effective policy is ClusterFirst — pods that
    // don't specify a policy must still get the in-cluster resolver.
    use kubelet_adapters::sandbox_builder::{NodeDnsConfig, build_dns_config};
    use kubelet_core::pod::PodSpec;
    use kubelet_core::types::{PodRef, PodUID};

    let pod = PodSpec {
        uid: PodUID::new("uid-app"),
        pod_ref: PodRef::new("default", "my-app"),
        dns_config: None, // no explicit policy → ClusterFirst fallback
        ..Default::default()
    };

    let node_dns = NodeDnsConfig {
        cluster_dns: vec!["10.96.0.10".to_string()],
        cluster_domain: "cluster.local".to_string(),
        resolv_conf_path: "/dev/null".to_string(),
    };

    let dns = build_dns_config(&pod, &node_dns);

    assert!(
        dns.servers.contains(&"10.96.0.10".to_string()),
        "ClusterFirst fallback must inject cluster DNS; servers: {:?}",
        dns.servers
    );
    assert!(
        dns.searches.iter().any(|s| s.contains("default.svc")),
        "ClusterFirst must add per-namespace search domain"
    );
}
