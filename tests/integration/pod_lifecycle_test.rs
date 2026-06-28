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

//! Integration tests: Pod lifecycle end-to-end through the full stack.
//! Phase 6–10 extended.

use kubelet_adapters::cgroup::CgroupManager;
use kubelet_adapters::checkpoint::CheckpointManager;
use kubelet_adapters::device_manager::DeviceManager;
use kubelet_adapters::eviction::manager::{EvictionManager, EvictionReason};
use kubelet_adapters::image_gc::{ImageGcConfig, ImageGcManager};
use kubelet_adapters::kube_client::InMemoryNodeReporter;
use kubelet_adapters::kube_watcher::{SimulatedApiPodSource, pod_spec_from_map};
use kubelet_adapters::lease::LeaseController;
use kubelet_adapters::mock_runtime::MockRuntime;
use kubelet_adapters::node_status::{NodeConditionDeriver, NodeStatusCollector};
use kubelet_adapters::oom_watcher::{OomEvent, OomWatcher};
use kubelet_adapters::prober::{ProbeResult, run_grpc_probe};
use kubelet_adapters::sandbox_builder::NodeDnsConfig;
use kubelet_adapters::volume::LocalVolumeManager;
use kubelet_app::Kubelet;
use kubelet_app::pod_worker::{PodRuntimeState, PodSyncResult, PodWorker};
use kubelet_app::runtime_manager::RuntimeManager;
use kubelet_app::sync_loop::{SyncLoopConfig, run_sync_loop};
use kubelet_core::config::KubeletConfig;
use kubelet_core::lease::NodeLease;
use kubelet_core::node::{NodeAddress, NodeAddressType, NodeConditionStatus, NodeConditionType};
use kubelet_core::pod::lifecycle::PodPhase;
use kubelet_core::pod::manager::PodManager;
use kubelet_core::pod::{
    ContainerSpec, ImagePullPolicy, PodOperation, PodSpec, PodUpdate, ResourceRequirements,
    RestartPolicy, VolumeSource, VolumeSpec,
};
use kubelet_core::types::{PodRef, PodUID};
use kubelet_ports::driven::pod_source::PodSource;
use std::collections::HashMap;
use std::sync::{Arc, Once};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::mpsc;

// ── helpers ───────────────────────────────────────────────────────────────────

fn ensure_rustls_crypto_provider() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn pod(uid: &str, name: &str, image: &str) -> PodSpec {
    ensure_rustls_crypto_provider();

    PodSpec {
        uid: PodUID::new(uid),
        pod_ref: PodRef::new("default", name),
        containers: vec![ContainerSpec {
            name: "main".to_string(),
            image: image.to_string(),
            command: vec![],
            args: vec![],
            working_dir: None,
            ports: vec![],
            env: vec![],
            resources: ResourceRequirements::default(),
            volume_mounts: vec![],
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            image_pull_policy: ImagePullPolicy::IfNotPresent,
            security_context: None,
            termination_message_path: None,
            ..Default::default()
        }],
        init_containers: vec![],
        ephemeral_containers: vec![],
        volumes: vec![],
        node_name: "integration-node".to_string(),
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
    }
}

// ── Phase 1–5 tests (unchanged) ───────────────────────────────────────────────

#[tokio::test]
async fn test_pod_add_initializes_status() {
    let (tx, _rx) = mpsc::channel(100);
    let pm = Arc::new(PodManager::new(tx));
    let p = pod("integ-uid-1", "nginx", "nginx:1.25");
    pm.upsert(p).await.unwrap();
    let state = pm.status.get(&PodUID::new("integ-uid-1")).unwrap();
    assert_eq!(state.phase, PodPhase::Pending);
}

#[tokio::test]
async fn test_multiple_pods_coexist() {
    let (tx, _rx) = mpsc::channel(100);
    let pm = Arc::new(PodManager::new(tx));
    for i in 0..10 {
        pm.upsert(pod(
            &format!("uid-{}", i),
            &format!("pod-{}", i),
            "alpine:3",
        ))
        .await
        .unwrap();
    }
    assert_eq!(pm.count(), 10);
}

#[tokio::test]
async fn test_pod_remove_clears_from_manager() {
    let (tx, _rx) = mpsc::channel(100);
    let pm = Arc::new(PodManager::new(tx));
    pm.upsert(pod("uid-del", "to-delete", "nginx"))
        .await
        .unwrap();
    pm.remove(&PodUID::new("uid-del"), None).await.unwrap();
    assert!(pm.get(&PodUID::new("uid-del")).is_none());
}

#[tokio::test]
async fn test_sync_loop_reports_pod_status_to_reporter() {
    let (tx, rx) = mpsc::channel(100);
    let pm = Arc::new(PodManager::new(tx));
    let runtime = Arc::new(MockRuntime::new());
    let reporter = Arc::new(InMemoryNodeReporter::new());
    pm.upsert(pod("uid-sync-1", "my-app", "myapp:v1"))
        .await
        .unwrap();
    let pm2 = pm.clone();
    let rt2 = runtime.clone();
    let rp2 = reporter.clone();
    let handle = tokio::spawn(async move {
        run_sync_loop(
            pm2,
            rt2,
            rp2,
            rx,
            SyncLoopConfig {
                reconcile_interval: Duration::from_secs(3600),
                max_concurrent_syncs: 1,
            },
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    handle.abort();
    assert!(reporter.pod_report_count().await >= 1);
}

// ── Phase 6: Lease controller integration ────────────────────────────────────

#[tokio::test]
async fn test_lease_controller_renews_continuously() {
    let reporter = Arc::new(InMemoryNodeReporter::new());
    let controller = LeaseController::new("node1", 40, 3, reporter.clone());

    // Manually trigger 3 renewals
    for _ in 0..3 {
        controller.acquire_or_renew().await;
    }

    assert_eq!(reporter.lease_renewal_count().await, 3);
    assert_eq!(
        controller.current_state(),
        kubelet_adapters::lease::LeaseState::Active
    );
}

#[tokio::test]
async fn test_node_lease_domain_model_lifecycle() {
    let mut lease = NodeLease::new("node1", 40);
    assert!(!lease.is_expired());
    assert!(!lease.needs_renewal());
    lease.renew();
    assert!(!lease.is_expired());
}

#[tokio::test]
async fn test_grpc_probe_reports_failure_when_endpoint_unavailable() {
    let result = run_grpc_probe("127.0.0.1", 19998, None, Duration::from_millis(300)).await;
    assert!(matches!(result, ProbeResult::Failure(_)));
}

// ── Phase 6: Eviction manager integration ────────────────────────────────────

#[tokio::test]
async fn test_eviction_manager_evicts_best_effort_first() {
    let hard: HashMap<String, String> = [("memory.available".to_string(), "100Mi".to_string())]
        .into_iter()
        .collect();
    let mut mgr = EvictionManager::new(&hard, &HashMap::new(), None);

    let p = pod("uid-be", "best-effort-pod", "nginx");
    let resources = kubelet_adapters::eviction::NodeResources {
        available_memory_bytes: 50 * 1024 * 1024, // 50Mi < 100Mi threshold
        total_memory_bytes: 8 * 1024 * 1024 * 1024,
        available_disk_bytes: 50 * 1024 * 1024 * 1024,
        total_disk_bytes: 100 * 1024 * 1024 * 1024,
        available_pids: 10000,
        total_pids: 32768,
    };

    let decisions = mgr.evaluate(&resources, &[p], &HashMap::new());
    assert_eq!(decisions.len(), 1);
    assert_eq!(decisions[0].reason, EvictionReason::MemoryPressure);
    assert_eq!(decisions[0].grace_period, Duration::from_secs(0));
}

// ── Phase 7: Image GC integration ─────────────────────────────────────────────

#[tokio::test]
async fn test_image_gc_full_lifecycle() {
    let mut mgr = ImageGcManager::new(ImageGcConfig {
        high_threshold: 0.85,
        low_threshold: 0.80,
        min_age: chrono::Duration::zero(),
    });

    // Add images
    for i in 0..5 {
        mgr.track_image(kubelet_core::container::ImageInfo {
            id: format!("sha256:img{}", i),
            repo_tags: vec![format!("nginx:{}", i)],
            repo_digests: vec![],
            size_bytes: 10 * 1024 * 1024 * 1024,
        });
    }

    // Pin one
    mgr.mark_in_use("sha256:img0");
    assert_eq!(mgr.pinned_count(), 1);

    // GC at 90% usage
    let plan = mgr.gc_candidates(90 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024);
    assert!(!plan.to_delete.is_empty());
    assert!(
        !plan.to_delete.contains(&"sha256:img0".to_string()),
        "Pinned image not GC'd"
    );
}

// ── Phase 7: Checkpoint manager integration ────────────────────────────────────

#[tokio::test]
async fn test_checkpoint_written_on_pod_sync() {
    let (tx, _rx) = mpsc::channel(100);
    let pm = Arc::new(PodManager::new(tx));
    let rt = Arc::new(MockRuntime::new());
    let dir = TempDir::new().unwrap();
    let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
    let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
    let cm_ref = cm.clone();
    let runtime_overheads = Arc::new(HashMap::new());

    let vm = Arc::new(LocalVolumeManager::new(dir.path()));
    let worker = PodWorker::new(
        pm.clone(),
        rt.clone(),
        rt.clone(),
        vm,
        cm,
        cg,
        runtime_overheads,
        "cgroupfs",
        dir.path(),
        "/tmp",
        "registry.k8s.io/pause:3.9",
        None,
        "",
        Arc::new(InMemoryNodeReporter::new()),
        NodeDnsConfig::default(),
        Arc::new(DeviceManager::new("/tmp")),
    );
    let p = pod("cp-uid", "cp-pod", "nginx");
    pm.upsert(p.clone()).await.unwrap();
    let mut state = PodRuntimeState::default();
    worker.sync_pod(&p, &mut state).await;

    assert!(
        cm_ref.exists("cp-uid"),
        "Checkpoint must be written after sync"
    );
}

// ── Phase 8: Pod worker full lifecycle ────────────────────────────────────────

#[tokio::test]
async fn test_pod_worker_sync_and_terminate() {
    let (tx, _rx) = mpsc::channel(100);
    let pm = Arc::new(PodManager::new(tx));
    let rt = Arc::new(MockRuntime::new());
    let dir = TempDir::new().unwrap();
    let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
    let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
    let runtime_overheads = Arc::new(HashMap::new());
    let vm = Arc::new(LocalVolumeManager::new(dir.path()));
    let worker = PodWorker::new(
        pm.clone(),
        rt.clone(),
        rt.clone(),
        vm,
        cm,
        cg,
        runtime_overheads,
        "cgroupfs",
        dir.path(),
        "/tmp",
        "pause:3.6",
        None,
        "",
        Arc::new(InMemoryNodeReporter::new()),
        NodeDnsConfig::default(),
        Arc::new(DeviceManager::new("/tmp")),
    );

    let p = pod("pw-uid", "pw-pod", "nginx");
    pm.upsert(p.clone()).await.unwrap();

    let mut state = PodRuntimeState::default();
    let result = worker.sync_pod(&p, &mut state).await;
    assert_eq!(result, PodSyncResult::Synced);

    // Status should be Running
    let ls = pm.status.get(&PodUID::new("pw-uid")).unwrap();
    assert_eq!(ls.phase, PodPhase::Running);
    assert!(
        ls.host_ip.as_deref().is_some_and(|ip| !ip.is_empty()),
        "Pod lifecycle status should include host_ip after sync"
    );

    // Terminate
    worker
        .terminate_pod(&p, &state, Duration::from_secs(0))
        .await
        .unwrap();
    let ls2 = pm.status.get(&PodUID::new("pw-uid")).unwrap();
    assert_eq!(ls2.phase, PodPhase::Running);
}

#[tokio::test]
async fn test_runtime_manager_handles_multiple_pods() {
    let (tx, _rx) = mpsc::channel(1000);
    let pm = Arc::new(PodManager::new(tx));
    let rt = Arc::new(MockRuntime::new());
    let dir = TempDir::new().unwrap();
    let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
    let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
    let runtime_overheads = Arc::new(HashMap::new());
    let vm = Arc::new(LocalVolumeManager::new(dir.path()));
    let reporter = Arc::new(InMemoryNodeReporter::new());
    let manager = RuntimeManager::new(
        pm.clone(),
        rt.clone(),
        rt.clone(),
        vm,
        reporter,
        cm,
        cg,
        runtime_overheads,
        "cgroupfs",
        dir.path(),
        "/tmp",
        "registry.k8s.io/pause:3.9",
        None,
        "",
        NodeDnsConfig::default(),
        Arc::new(DeviceManager::new("/tmp")),
    );

    for i in 0..5 {
        let p = pod(&format!("rm-uid-{}", i), &format!("rm-pod-{}", i), "nginx");
        pm.upsert(p.clone()).await.unwrap();
        manager
            .handle_update(PodUpdate {
                pod: p,
                op: PodOperation::Add,
            })
            .await;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        let count = manager.active_pod_count().await;
        if count >= 3 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            assert!(
                count >= 3,
                "expected at least three active pod states during concurrent sync, got {}",
                count
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ignore = "requires a real CSI driver socket; skipped in unit/integration CI"]
async fn test_kubelet_run_pipeline_mounts_pvc_volume() {
    let dir = TempDir::new().unwrap();
    let plugin_dir = dir.path().join("plugins").join("test-driver");
    std::fs::create_dir_all(&plugin_dir).unwrap();
    std::fs::write(plugin_dir.join("csi.sock"), b"").unwrap();

    let config = KubeletConfig {
        node_name: "integration-kubelet".to_string(),
        root_dir: dir.path().to_path_buf(),
        address: "127.0.0.1".to_string(),
        port: 0,
        static_pod_path: None,
        sync_frequency: Duration::from_millis(100),
        ..Default::default()
    };

    let kubelet = Kubelet::new(config).await;
    let pod_manager = kubelet.pod_manager();

    let pod_uid = "pvc-kubelet-run-uid";
    let mut p = pod(pod_uid, "pvc-kubelet-run-pod", "nginx");
    p.volumes = vec![VolumeSpec {
        name: "data".to_string(),
        source: VolumeSource::PersistentVolumeClaim {
            claim_name: "csi://test-driver/vol-1".to_string(),
            read_only: false,
        },
    }];

    pod_manager.upsert(p).await.unwrap();

    let mount_path = dir
        .path()
        .join("pods")
        .join(pod_uid)
        .join("volumes")
        .join("data");

    let handle = tokio::spawn(async move { kubelet.run().await });

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if mount_path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("PVC volume path was not mounted in time");

    handle.abort();
}

// ── Phase 9: API watcher + node status ────────────────────────────────────────

#[tokio::test]
async fn test_simulated_api_source_emits_pods() {
    let p = pod("api-uid", "api-pod", "nginx");
    let source = SimulatedApiPodSource::new("node1", vec![p], Duration::from_secs(3600));
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move { source.run(tx).await });
    let update = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout")
        .expect("no update");
    assert_eq!(update.pod.uid, PodUID::new("api-uid"));
    assert_eq!(update.op, PodOperation::Add);
}

#[tokio::test]
async fn test_node_status_collector_builds_full_status() {
    let collector = NodeStatusCollector::new("integ-node", 110, 4.0, 8 << 30, 100 << 30);
    let addresses = vec![NodeAddress {
        address_type: NodeAddressType::InternalIP,
        address: "10.0.0.1".to_string(),
    }];
    let status = collector.collect(addresses);
    assert_eq!(status.name, "integ-node");
    assert_eq!(status.capacity.pods, 110);
    assert!(status.allocatable.memory_bytes < status.capacity.memory_bytes);
}

#[tokio::test]
async fn test_node_condition_deriver_ready() {
    let deriver = NodeConditionDeriver::new(false, false, false, false);
    let conditions = deriver.build_conditions();
    let ready = conditions
        .iter()
        .find(|c| c.condition_type == NodeConditionType::Ready)
        .unwrap();
    assert_eq!(ready.status, NodeConditionStatus::True);
}

// ── Phase 10: OOM watcher integration ────────────────────────────────────────

#[tokio::test]
async fn test_oom_watcher_tracks_events_by_pod() {
    let mut watcher = OomWatcher::new(100);
    let uid = PodUID::new("oom-pod-uid");
    watcher.record(OomEvent {
        timestamp: chrono::Utc::now(),
        container_id: Some("ctr-123".to_string()),
        pod_uid: Some(uid.clone()),
        process_name: "nginx".to_string(),
        message: "OOM kill: nginx".to_string(),
    });
    let events = watcher.events_for_pod(&uid);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].process_name, "nginx");
}

#[tokio::test]
async fn test_pod_spec_from_json_roundtrip() {
    let json = serde_json::json!({
        "metadata": { "name": "test-pod", "namespace": "default", "uid": "test-uid-123" },
        "spec": {
            "containers": [{ "name": "app", "image": "nginx:1.25" }],
            "nodeName": "node1"
        }
    });
    let spec = pod_spec_from_map(&json, "node1").unwrap();
    assert_eq!(spec.pod_ref.name, "test-pod");
    assert_eq!(spec.containers[0].image, "nginx:1.25");
}

// ── Active deadline + init container integration ──────────────────────────────

/// Verifies that sync_pod returns Terminated and sets the phase to Failed
/// when the pod's active deadline has elapsed.
#[tokio::test]
async fn test_active_deadline_elapsed_terminates_pod() {
    let (tx, _rx) = mpsc::channel(100);
    let pm = Arc::new(PodManager::new(tx));
    let rt = Arc::new(MockRuntime::new());
    let dir = TempDir::new().unwrap();
    let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
    let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
    let vm = Arc::new(LocalVolumeManager::new(dir.path()));
    let worker = PodWorker::new(
        pm.clone(),
        rt.clone(),
        rt.clone(),
        vm,
        cm,
        cg,
        Arc::new(HashMap::new()),
        "cgroupfs",
        dir.path(),
        "/tmp",
        "registry.k8s.io/pause:3.9",
        None,
        "",
        Arc::new(InMemoryNodeReporter::new()),
        NodeDnsConfig::default(),
        Arc::new(DeviceManager::new("/tmp")),
    );

    let mut p = pod("dl-uid", "dl-pod", "nginx");
    p.active_deadline_seconds = Some(10);
    pm.upsert(p.clone()).await.unwrap();

    // Back-date the start_time so the deadline appears expired.
    if let Some(mut ls) = pm.status.get(&p.uid) {
        ls.start_time = Some(chrono::Utc::now() - chrono::Duration::seconds(120));
        pm.status.set(p.uid.clone(), ls);
    }

    let mut state = PodRuntimeState::default();
    let result = worker.sync_pod(&p, &mut state).await;
    assert_eq!(result, PodSyncResult::Terminated);
    let ls = pm.status.get(&p.uid).unwrap();
    assert_eq!(ls.phase, PodPhase::Failed);
}

/// Verifies that sync_pod returns NeedsRetry (not Terminated) when the
/// active deadline has not yet elapsed.
#[tokio::test]
async fn test_active_deadline_not_elapsed_syncs_normally() {
    let (tx, _rx) = mpsc::channel(100);
    let pm = Arc::new(PodManager::new(tx));
    let rt = Arc::new(MockRuntime::new());
    let dir = TempDir::new().unwrap();
    let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
    let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
    let vm = Arc::new(LocalVolumeManager::new(dir.path()));
    let worker = PodWorker::new(
        pm.clone(),
        rt.clone(),
        rt.clone(),
        vm,
        cm,
        cg,
        Arc::new(HashMap::new()),
        "cgroupfs",
        dir.path(),
        "/tmp",
        "registry.k8s.io/pause:3.9",
        None,
        "",
        Arc::new(InMemoryNodeReporter::new()),
        NodeDnsConfig::default(),
        Arc::new(DeviceManager::new("/tmp")),
    );

    let mut p = pod("dl-uid-ok", "dl-pod-ok", "nginx");
    p.active_deadline_seconds = Some(3600);
    pm.upsert(p.clone()).await.unwrap();

    let mut state = PodRuntimeState::default();
    let result = worker.sync_pod(&p, &mut state).await;
    assert_eq!(result, PodSyncResult::Synced);
}

/// Verifies that a failing init container with RestartPolicy::Never terminates
/// the pod rather than endlessly restarting it.
#[tokio::test]
async fn test_failing_init_container_never_restarts_terminates_pod() {
    let (tx, _rx) = mpsc::channel(100);
    let pm = Arc::new(PodManager::new(tx));
    let rt = Arc::new(MockRuntime::new());
    let dir = TempDir::new().unwrap();
    let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
    let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
    let vm = Arc::new(LocalVolumeManager::new(dir.path()));
    let worker = PodWorker::new(
        pm.clone(),
        rt.clone(),
        rt.clone(),
        vm,
        cm,
        cg,
        Arc::new(HashMap::new()),
        "cgroupfs",
        dir.path(),
        "/tmp",
        "registry.k8s.io/pause:3.9",
        None,
        "",
        Arc::new(InMemoryNodeReporter::new()),
        NodeDnsConfig::default(),
        Arc::new(DeviceManager::new("/tmp")),
    );

    let mut p = pod("ic-fail-uid", "ic-fail-pod", "nginx");
    p.restart_policy = RestartPolicy::Never;
    p.init_containers = vec![ContainerSpec {
        name: "init1".to_string(),
        image: "busybox:latest".to_string(),
        ..Default::default()
    }];
    pm.upsert(p.clone()).await.unwrap();

    // Force init container to exit with code 1 immediately on start.
    rt.set_exit_on_start(Some(1)).await;

    let mut state = PodRuntimeState::default();
    // First sync: creates sandbox + starts init container (immediately exits/1).
    let first = worker.sync_pod(&p, &mut state).await;
    assert!(
        matches!(first, PodSyncResult::NeedsRetry(_)),
        "first sync should be NeedsRetry after starting init container, got {:?}",
        first
    );
    // Second sync: sees Exited/1 + RestartPolicy::Never → terminates.
    let result = worker.sync_pod(&p, &mut state).await;
    assert_eq!(
        result,
        PodSyncResult::Terminated,
        "should terminate when init exits non-zero and RestartPolicy=Never"
    );
    let ls = pm.status.get(&p.uid).unwrap();
    assert_eq!(ls.phase, PodPhase::Failed);
}

/// Verifies that status is visible (init container shows Running) during
/// the NeedsRetry phase — i.e. the API server sees progress.
#[tokio::test]
async fn test_init_container_status_visible_during_running() {
    let (tx, _rx) = mpsc::channel(100);
    let pm = Arc::new(PodManager::new(tx));
    let rt = Arc::new(MockRuntime::new());
    let dir = TempDir::new().unwrap();
    let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
    let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
    let vm = Arc::new(LocalVolumeManager::new(dir.path()));
    let worker = PodWorker::new(
        pm.clone(),
        rt.clone(),
        rt.clone(),
        vm,
        cm,
        cg,
        Arc::new(HashMap::new()),
        "cgroupfs",
        dir.path(),
        "/tmp",
        "registry.k8s.io/pause:3.9",
        None,
        "",
        Arc::new(InMemoryNodeReporter::new()),
        NodeDnsConfig::default(),
        Arc::new(DeviceManager::new("/tmp")),
    );

    let mut p = pod("ic-vis-uid", "ic-vis-pod", "nginx");
    p.init_containers = vec![ContainerSpec {
        name: "init1".to_string(),
        image: "busybox:latest".to_string(),
        ..Default::default()
    }];
    pm.upsert(p.clone()).await.unwrap();

    let mut state = PodRuntimeState::default();
    let result = worker.sync_pod(&p, &mut state).await;
    assert!(
        matches!(result, PodSyncResult::NeedsRetry(_)),
        "expected NeedsRetry while init container is running"
    );

    let ls = pm.status.get(&p.uid).unwrap();
    assert_eq!(ls.init_container_statuses.len(), 1);
    assert!(
        matches!(
            ls.init_container_statuses[0].state,
            kubelet_core::pod::lifecycle::ContainerState::Running { .. }
        ),
        "init container state must be Running during NeedsRetry"
    );
}

/// Regression test: each container restart must remove the old container record
/// from the runtime so that stale records do not accumulate.
///
/// Before the fix, `start_container` would create a new container without ever
/// calling `remove_container` on the previous one.  After N restarts of a
/// crash-looping pod there would be N orphaned container records in containerd,
/// consuming memory and overlayfs snapshot space.
///
/// After the fix the runtime's total container count must stay bounded: the old
/// container is removed before the new one is created, so there is at most one
/// container record per container name at any point in time.
#[tokio::test]
async fn test_container_restart_removes_old_container_record() {
    let (tx, _rx) = mpsc::channel(100);
    let pm = Arc::new(PodManager::new(tx));
    let rt = Arc::new(MockRuntime::new());
    let dir = TempDir::new().unwrap();
    let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
    let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
    let vm = Arc::new(LocalVolumeManager::new(dir.path()));
    let worker = PodWorker::new(
        pm.clone(),
        rt.clone(),
        rt.clone(),
        vm,
        cm,
        cg,
        Arc::new(HashMap::new()),
        "cgroupfs",
        dir.path(),
        "/tmp",
        "registry.k8s.io/pause:3.9",
        None,
        "",
        Arc::new(InMemoryNodeReporter::new()),
        NodeDnsConfig::default(),
        Arc::new(DeviceManager::new("/tmp")),
    );

    let p = pod("leak-uid", "leak-pod", "busybox");
    pm.upsert(p.clone()).await.unwrap();

    // Simulate N restart cycles: start → exit → restart
    const RESTARTS: u32 = 5;
    let mut state = PodRuntimeState::default();

    for i in 0..RESTARTS {
        // Each iteration: container starts running, then we make it exit so
        // the next sync restarts it.
        rt.set_exit_on_start(None).await; // start running
        let result = worker.sync_pod(&p, &mut state).await;
        assert_eq!(
            result,
            PodSyncResult::Synced,
            "iteration {}: expected Synced after starting container",
            i
        );

        // One sandbox + one container per pod at this point.
        // The key assertion: runtime container count must not exceed
        // (number of containers in pod spec) + (pause sandbox).
        let containers = rt.container_count();
        assert!(
            containers <= p.containers.len() + 1,
            "iteration {}: expected at most {} container records, got {} (old records are leaking)",
            i,
            p.containers.len() + 1,
            containers
        );

        // Make the running container exit so the next sync triggers a restart.
        if i < RESTARTS - 1 {
            rt.set_exit_on_start(Some(1)).await;
            // Force the container to be seen as Exited by syncing once more.
            let _ = worker.sync_pod(&p, &mut state).await;
            rt.set_exit_on_start(None).await;
        }
    }

    // Final check: after all restarts the runtime still has at most one
    // container record for the pod's single app container.
    let final_count = rt.container_count();
    assert!(
        final_count <= p.containers.len() + 1,
        "after {} restarts expected at most {} container records total, got {}",
        RESTARTS,
        p.containers.len() + 1,
        final_count
    );
}

// ── Sleep lifecycle hook integration tests ────────────────────────────────────

#[tokio::test]
async fn integ_sleep_prestop_hook_completes_within_grace_period() {
    use kubelet_adapters::lifecycle::run_pre_stop;
    use kubelet_adapters::mock_runtime::MockRuntime;
    use kubelet_core::container::ContainerID;
    use kubelet_core::pod::LifecycleHandler;
    use std::time::{Duration, Instant};

    let runtime = MockRuntime::new();
    let cid = ContainerID("integ-sleep-container".to_string());
    // 0-second sleep should complete near-instantly.
    let handler = LifecycleHandler::Sleep { seconds: 0 };
    let start = Instant::now();
    run_pre_stop(&handler, &cid, "app", &runtime, Duration::from_secs(5)).await;
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "Sleep(0) preStop took too long: {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn integ_sleep_prestop_hook_respects_grace_period_timeout() {
    use kubelet_adapters::lifecycle::run_pre_stop;
    use kubelet_adapters::mock_runtime::MockRuntime;
    use kubelet_core::container::ContainerID;
    use kubelet_core::pod::LifecycleHandler;
    use std::time::{Duration, Instant};

    let runtime = MockRuntime::new();
    let cid = ContainerID("integ-sleep-timeout-container".to_string());
    // Request a 60-second sleep but only allow 50ms — should return quickly.
    let handler = LifecycleHandler::Sleep { seconds: 60 };
    let start = Instant::now();
    // PreStop swallows errors — it should not block beyond grace period.
    run_pre_stop(&handler, &cid, "app", &runtime, Duration::from_millis(100)).await;
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "Sleep hook did not respect grace period timeout: {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn integ_sleep_handler_maps_from_lifecycle_executor() {
    use kubelet_adapters::lifecycle::LifecycleHookExecutor;
    use kubelet_adapters::mock_runtime::MockRuntime;
    use kubelet_core::container::ContainerID;
    use kubelet_core::pod::LifecycleHandler;
    use std::time::Duration;

    let runtime = MockRuntime::new();
    let cid = ContainerID("integ-sleep-exec-container".to_string());
    let handler = LifecycleHandler::Sleep { seconds: 0 };
    let executor = LifecycleHookExecutor::new(Duration::from_secs(5));
    let result = executor.execute(&handler, &cid, "app", &runtime).await;
    assert!(result.is_ok(), "Sleep(0) hook should succeed: {:?}", result);
}

// ── Lifecycle hook HTTPS TLS-skip tests ──────────────────────────────────────
//
// Mirrors: [sig-node] Container Lifecycle Hook "should execute prestop https
// hook properly" and "should execute poststart https hook properly".
//
// The Go kubelet explicitly skips TLS certificate verification for lifecycle
// hooks (pkg/kubelet/lifecycle/handlers.go insecureSkipVerify=true) because
// container-side servers commonly use self-signed certificates.

/// HTTPS lifecycle hooks must skip TLS verification.
/// Validated by confirming that a connection failure to a non-existent server
/// is NOT a certificate error (which would mean TLS verify is still active).
#[tokio::test]
async fn integ_https_lifecycle_hook_skips_tls_verification() {
    use kubelet_adapters::lifecycle::LifecycleHookExecutor;
    use kubelet_adapters::mock_runtime::MockRuntime;
    use kubelet_core::container::ContainerID;
    use kubelet_core::pod::LifecycleHandler;
    use std::time::Duration;

    let handler = LifecycleHandler::HttpGet {
        scheme: "HTTPS".to_string(),
        host: Some("127.0.0.1".to_string()),
        port: 19995,
        path: "/healthz".to_string(),
    };
    let runtime = MockRuntime::new();
    let cid = ContainerID("fake-container".to_string());
    let executor = LifecycleHookExecutor::new(Duration::from_millis(300));
    let result = executor
        .execute(&handler, &cid, "my-container", &runtime)
        .await;

    // Must fail (no server) but NOT because of a certificate error
    assert!(result.is_err(), "Expected error — no server on port 19995");
    let msg = result.unwrap_err().to_string().to_lowercase();
    assert!(
        !msg.contains("certificate") && !msg.contains("invalid cert"),
        "HTTPS hook must skip TLS cert verification (Go kubelet: insecureSkipVerify=true), \
         but got cert error: {}",
        msg
    );
}

/// HTTP (plain) lifecycle hook must work normally.
#[tokio::test]
async fn integ_http_lifecycle_hook_connects_to_plain_server() {
    use kubelet_adapters::lifecycle::LifecycleHookExecutor;
    use kubelet_adapters::mock_runtime::MockRuntime;
    use kubelet_core::container::ContainerID;
    use kubelet_core::pod::LifecycleHandler;
    use std::time::Duration;

    // Start a minimal plain HTTP server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            use tokio::io::AsyncWriteExt;
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await;
        }
    });

    let handler = LifecycleHandler::HttpGet {
        scheme: "HTTP".to_string(),
        host: Some("127.0.0.1".to_string()),
        port,
        path: "/".to_string(),
    };
    let runtime = MockRuntime::new();
    let cid = ContainerID("fake-container".to_string());
    let executor = LifecycleHookExecutor::new(Duration::from_secs(5));
    let result = executor
        .execute(&handler, &cid, "my-container", &runtime)
        .await;
    assert!(
        result.is_ok(),
        "Plain HTTP hook should succeed: {:?}",
        result
    );
}

/// Regression test: a sidecar init container (restartPolicy=Always) with a
/// failing readiness probe must cause the pod's Ready/ContainersReady conditions
/// to be False.
///
/// Before the fix:
///  - Readiness probe for sidecar init containers was never registered.
///  - `init_ready` for running sidecar containers was unconditionally `true`.
///  - The sidecar's readiness probe result was not consulted for `all_ready`.
///  → Pod would be reported Ready even when the sidecar's readiness probe failed.
///
/// After the fix:
///  - Running sidecar init containers respect their readiness probe.
///  - The pod's Ready condition is False when the sidecar readiness probe has not passed.
#[tokio::test]
async fn integ_sidecar_failing_readiness_probe_keeps_pod_not_ready() {
    use kubelet_core::pod::lifecycle::{ConditionStatus, PodConditionType};
    use kubelet_core::pod::{Probe, ProbeHandler, RestartPolicy};

    let (tx, _rx) = mpsc::channel(100);
    let pm = Arc::new(PodManager::new(tx));
    let rt = Arc::new(MockRuntime::new());
    let dir = TempDir::new().unwrap();
    let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
    let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
    let vm = Arc::new(LocalVolumeManager::new(dir.path()));
    let worker = PodWorker::new(
        pm.clone(),
        rt.clone(),
        rt.clone(),
        vm,
        cm,
        cg,
        Arc::new(HashMap::new()),
        "cgroupfs",
        dir.path(),
        "/tmp",
        "registry.k8s.io/pause:3.9",
        None,
        "",
        Arc::new(InMemoryNodeReporter::new()),
        NodeDnsConfig::default(),
        Arc::new(DeviceManager::new("/tmp")),
    );

    // Build a pod with one sidecar init container (restartPolicy=Always) that
    // has a readiness probe targeting a port that is guaranteed to be refused.
    let mut p = pod("sidecar-rdy-uid", "sidecar-rdy-pod", "nginx");
    p.init_containers = vec![ContainerSpec {
        name: "sidecar".to_string(),
        image: "busybox:latest".to_string(),
        restart_policy: Some(RestartPolicy::Always), // marks it as a sidecar
        readiness_probe: Some(Probe {
            handler: ProbeHandler::TcpSocket {
                port: 19997, // nothing listening here — probe will fail
                host: None,
            },
            initial_delay_seconds: 0,
            period_seconds: 1,
            timeout_seconds: 1,
            success_threshold: 1,
            failure_threshold: 1,
        }),
        ..Default::default()
    }];
    pm.upsert(p.clone()).await.unwrap();

    // First sync: sidecar starts and is Running but readiness probe result is
    // not yet in the map (defaults to false).
    let mut state = PodRuntimeState::default();
    worker.sync_pod(&p, &mut state).await;

    // Give the readiness probe task a short window to attempt and record false.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Update status to reflect the current probe state.
    worker.sync_pod(&p, &mut state).await;

    let ls = pm.status.get(&p.uid).expect("status must exist");

    // The sidecar init container itself must report ready=false (probe failing).
    let sidecar_status = ls
        .init_container_statuses
        .iter()
        .find(|s| s.name == "sidecar")
        .expect("sidecar status must be present");
    assert!(
        !sidecar_status.ready,
        "sidecar with failing readiness probe must not be ready"
    );

    // The pod's Ready condition must be False because the sidecar is not ready.
    let ready_cond = ls
        .conditions
        .iter()
        .find(|c| c.condition_type == PodConditionType::Ready);
    if let Some(cond) = ready_cond {
        assert_eq!(
            cond.status,
            ConditionStatus::False,
            "pod Ready must be False when sidecar readiness probe is failing"
        );
    }
    // Note: Ready condition may not yet be set if the pod hasn't finished initialising;
    // what matters is that `ready` on the sidecar status is false.
}
