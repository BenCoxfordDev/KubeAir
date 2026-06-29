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

//! CPU / speed benchmarks for kubelet pod lifecycle hot paths.
//!
//! Measures the throughput and latency of the most performance-critical
//! operations in the kubelet domain layer. Run with:
//!
//!   cargo bench --bench pod_operations
//!   cargo bench --bench pod_operations -- --save-baseline main
//!   cargo bench --bench pod_operations -- --baseline main   # compare

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kubelet_core::config::KubeletConfig;
use kubelet_core::pod::lifecycle::{ContainerState, ContainerStatus, compute_pod_phase};
use kubelet_core::pod::manager::PodManager;
use kubelet_core::pod::status::PodStatusManager;
use kubelet_core::pod::sync::validate_pod;
use kubelet_core::pod::{
    ContainerSpec, ImagePullPolicy, PodSpec, ResourceRequirements, RestartPolicy,
};
use kubelet_core::qos::compute_qos_class;
use kubelet_core::types::{PodRef, PodUID, ResourceQuantity};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

// ── helpers ──────────────────────────────────────────────────────────────────

fn make_pod(uid: &str, containers: usize) -> PodSpec {
    PodSpec {
        uid: PodUID::new(uid),
        pod_ref: PodRef::new("default", uid),
        containers: (0..containers)
            .map(|i| ContainerSpec {
                name: format!("c{}", i),
                image: "nginx:latest".to_string(),
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
            })
            .collect(),
        init_containers: vec![],
        ephemeral_containers: vec![],
        volumes: vec![],
        node_name: "bench-node".to_string(),
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
        ..Default::default()
    }
}

fn running_status(name: &str) -> ContainerStatus {
    ContainerStatus {
        name: name.to_string(),
        state: ContainerState::Running {
            started_at: chrono::Utc::now(),
        },
        last_state: None,
        ready: true,
        restart_count: 0,
        image: "nginx:latest".to_string(),
        image_id: "sha256:abc".to_string(),
        container_id: Some("ctr://abc".to_string()),
        started: Some(true),
        resources: None,
    }
}

fn terminated_status(name: &str, exit_code: i32) -> ContainerStatus {
    let now = chrono::Utc::now();
    ContainerStatus {
        name: name.to_string(),
        state: ContainerState::Terminated {
            exit_code,
            reason: "Completed".to_string(),
            message: None,
            started_at: now,
            finished_at: now,
        },
        last_state: None,
        ready: false,
        restart_count: 0,
        image: "nginx:latest".to_string(),
        image_id: "sha256:abc".to_string(),
        container_id: None,
        started: Some(false),
        resources: None,
    }
}

// ── 1. Pod Phase Computation ──────────────────────────────────────────────────

/// Benchmarks `compute_pod_phase` — called on every reconcile loop iteration
/// for every pod. This is one of the hottest paths in the kubelet.
fn bench_compute_pod_phase(c: &mut Criterion) {
    let mut group = c.benchmark_group("pod_phase_computation");

    // Scenario A: all containers running (common steady state)
    group.bench_function("all_running_1_container", |b| {
        let statuses = vec![running_status("app")];
        b.iter(|| compute_pod_phase(&[], std::hint::black_box(&statuses), &RestartPolicy::Always));
    });

    group.bench_function("all_running_10_containers", |b| {
        let statuses: Vec<ContainerStatus> = (0..10)
            .map(|i| running_status(&format!("c{}", i)))
            .collect();
        b.iter(|| compute_pod_phase(&[], std::hint::black_box(&statuses), &RestartPolicy::Always));
    });

    // Scenario B: all terminated (batch job completion)
    group.bench_function("all_terminated_success_5_containers", |b| {
        let statuses: Vec<ContainerStatus> = (0..5)
            .map(|i| terminated_status(&format!("c{}", i), 0))
            .collect();
        b.iter(|| compute_pod_phase(&[], std::hint::black_box(&statuses), &RestartPolicy::Never));
    });

    // Scenario C: init containers in progress
    group.bench_function("init_containers_running", |b| {
        let init = vec![running_status("init-1")];
        let statuses: Vec<ContainerStatus> = vec![];
        b.iter(|| {
            compute_pod_phase(
                std::hint::black_box(&init),
                std::hint::black_box(&statuses),
                &RestartPolicy::Always,
            )
        });
    });

    group.finish();
}

// ── 2. Pod Validation ─────────────────────────────────────────────────────────

/// Benchmarks `validate_pod` — called before every sync operation.
fn bench_pod_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("pod_validation");

    group.bench_function("valid_pod_1_container", |b| {
        let pod = make_pod("bench-uid", 1);
        b.iter(|| validate_pod(std::hint::black_box(&pod)));
    });

    group.bench_function("valid_pod_10_containers", |b| {
        let pod = make_pod("bench-uid", 10);
        b.iter(|| validate_pod(std::hint::black_box(&pod)));
    });

    group.bench_function("invalid_pod_empty_name", |b| {
        let mut pod = make_pod("bench-uid", 1);
        pod.pod_ref.name = String::new();
        b.iter(|| validate_pod(std::hint::black_box(&pod)));
    });

    group.finish();
}

// ── 3. QoS Classification ────────────────────────────────────────────────────

/// Benchmarks QoS class computation — called during pod admission and eviction.
fn bench_qos_classification(c: &mut Criterion) {
    let mut group = c.benchmark_group("qos_classification");

    let make_qos_pod =
        |cpu_req: Option<i64>, cpu_lim: Option<i64>, mem_req: Option<i64>, mem_lim: Option<i64>| {
            let mut requests = HashMap::new();
            let mut limits = HashMap::new();
            if let Some(v) = cpu_req {
                requests.insert("cpu".to_string(), ResourceQuantity::cpu_millicores(v));
            }
            if let Some(v) = cpu_lim {
                limits.insert("cpu".to_string(), ResourceQuantity::cpu_millicores(v));
            }
            if let Some(v) = mem_req {
                requests.insert("memory".to_string(), ResourceQuantity::memory_bytes(v));
            }
            if let Some(v) = mem_lim {
                limits.insert("memory".to_string(), ResourceQuantity::memory_bytes(v));
            }
            let mut pod = make_pod("qos-uid", 1);
            pod.containers[0].resources = ResourceRequirements { requests, limits };
            pod
        };

    group.bench_function("best_effort", |b| {
        let pod = make_pod("uid", 1); // no resources
        b.iter(|| compute_qos_class(std::hint::black_box(&pod)));
    });

    group.bench_function("guaranteed", |b| {
        let pod = make_qos_pod(Some(500), Some(500), Some(128_000_000), Some(128_000_000));
        b.iter(|| compute_qos_class(std::hint::black_box(&pod)));
    });

    group.bench_function("burstable", |b| {
        let pod = make_qos_pod(Some(250), Some(500), Some(64_000_000), Some(128_000_000));
        b.iter(|| compute_qos_class(std::hint::black_box(&pod)));
    });

    group.finish();
}

// ── 4. Pod Manager Throughput ─────────────────────────────────────────────────

/// Benchmarks pod upsert/remove throughput at scale.
fn bench_pod_manager(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("pod_manager");

    // Upsert throughput at various pod counts
    for count in [10, 100, 1_000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(
            BenchmarkId::new("upsert_pods", count),
            &count,
            |b, &count| {
                b.to_async(&rt).iter(|| async move {
                    let (tx, _rx) = mpsc::channel(count as usize + 100);
                    let manager = PodManager::new(tx);
                    for i in 0..count {
                        manager
                            .upsert(make_pod(&format!("uid-{}", i), 1))
                            .await
                            .unwrap();
                    }
                    manager.count()
                });
            },
        );
    }

    // List throughput with large pod registry (pre-populated outside loop)
    let (list_tx, _list_rx) = mpsc::channel(2000);
    let list_manager: Arc<PodManager> = {
        let m = Arc::new(PodManager::new(list_tx));
        rt.block_on(async {
            for i in 0..1000 {
                m.upsert(make_pod(&format!("uid-{}", i), 1)).await.unwrap();
            }
        });
        m
    };
    group.bench_function("list_1000_pods", |b| {
        let m = list_manager.clone();
        b.to_async(&rt).iter(move || {
            let m = m.clone();
            async move { m.list() }
        });
    });

    // Reconcile all — drain the channel continuously so it never fills up
    let (reconcile_tx, mut reconcile_rx) = mpsc::channel(10_000);
    let reconcile_manager: Arc<PodManager> = {
        let m = Arc::new(PodManager::new(reconcile_tx));
        rt.block_on(async {
            for i in 0..100 {
                m.upsert(make_pod(&format!("uid-{}", i), 1)).await.unwrap();
            }
        });
        m
    };
    // Background drainer
    rt.spawn(async move { while reconcile_rx.recv().await.is_some() {} });
    group.bench_function("reconcile_all_100_pods", |b| {
        let m = reconcile_manager.clone();
        b.to_async(&rt).iter(move || {
            let m = m.clone();
            async move { m.reconcile_all().await.unwrap() }
        });
    });

    group.finish();
}

// ── 5. Pod Status Manager ─────────────────────────────────────────────────────

/// Benchmarks the status manager — hit on every reconcile to track container states.
fn bench_pod_status_manager(c: &mut Criterion) {
    let mut group = c.benchmark_group("pod_status_manager");

    group.bench_function("initialize_pod_1_container", |b| {
        let pod = make_pod("uid", 1);
        let manager = PodStatusManager::new();
        b.iter(|| {
            manager.initialize(std::hint::black_box(&pod));
        });
    });

    group.bench_function("initialize_pod_10_containers", |b| {
        let pod = make_pod("uid", 10);
        let manager = PodStatusManager::new();
        b.iter(|| {
            manager.initialize(std::hint::black_box(&pod));
        });
    });

    group.bench_function("get_pod_status", |b| {
        let manager = PodStatusManager::new();
        let pod = make_pod("uid", 1);
        manager.initialize(&pod);
        let uid = pod.uid.clone();
        b.iter(|| manager.get(std::hint::black_box(&uid)));
    });

    group.bench_function("get_all_statuses_1000_pods", |b| {
        let manager = PodStatusManager::new();
        for i in 0..1000 {
            manager.initialize(&make_pod(&format!("uid-{}", i), 1));
        }
        b.iter(|| manager.all());
    });

    group.finish();
}

// ── 6. Kubelet Config Validation ──────────────────────────────────────────────

fn bench_config_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("kubelet_config");

    group.bench_function("validate_default_config", |b| {
        let config = KubeletConfig {
            node_name: "bench-node".to_string(),
            ..Default::default()
        };
        b.iter(|| config.validate());
    });

    group.finish();
}

// ── 7. Concurrent Pod Upserts ─────────────────────────────────────────────────

/// Benchmarks concurrent pod upserts from multiple tasks — simulates the
/// kubelet receiving many pod assignments simultaneously.
fn bench_concurrent_upserts(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrent_pod_operations");

    for tasks in [4, 8, 16] {
        group.throughput(Throughput::Elements(tasks * 10));
        group.bench_with_input(
            BenchmarkId::new("concurrent_upsert_tasks", tasks),
            &tasks,
            |b, &tasks| {
                b.to_async(&rt).iter(|| async move {
                    let (tx, _rx) = mpsc::channel(10000);
                    let manager = Arc::new(PodManager::new(tx));
                    let mut handles = vec![];
                    for t in 0..tasks {
                        let m = manager.clone();
                        handles.push(tokio::spawn(async move {
                            for i in 0..10u64 {
                                m.upsert(make_pod(&format!("uid-{}-{}", t, i), 1))
                                    .await
                                    .unwrap();
                            }
                        }));
                    }
                    for h in handles {
                        h.await.unwrap();
                    }
                    manager.count()
                });
            },
        );
    }

    group.finish();
}

// ── 8. Mock Runtime Operations ────────────────────────────────────────────────

/// Benchmarks the mock runtime adapter — reflects realistic CRI adapter overhead.
fn bench_mock_runtime(c: &mut Criterion) {
    use kubelet_adapters::mock_runtime::MockRuntime;
    use kubelet_ports::driven::container_runtime::{
        ContainerRuntime, CreateContainerConfig, CreateSandboxConfig, LinuxContainerSecurity,
    };

    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mock_runtime");

    group.bench_function("run_pod_sandbox", |b| {
        b.to_async(&rt).iter(|| async {
            let runtime = MockRuntime::new();
            runtime
                .run_pod_sandbox(CreateSandboxConfig {
                    pod_uid: "bench-uid".to_string(),
                    pod_name: "bench-pod".to_string(),
                    pod_namespace: "default".to_string(),
                    hostname: "bench".to_string(),
                    log_directory: "/tmp".to_string(),
                    dns_config: None,
                    port_mappings: vec![],
                    labels: HashMap::new(),
                    annotations: HashMap::new(),
                    linux_cgroup_parent: "/kubepods".to_string(),
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
                .unwrap()
        });
    });

    group.bench_function("create_and_start_container", |b| {
        let runtime_ref = Arc::new(MockRuntime::new());
        let sandbox_id = rt.block_on(async {
            runtime_ref
                .run_pod_sandbox(CreateSandboxConfig {
                    pod_uid: "bench-uid".to_string(),
                    pod_name: "bench".to_string(),
                    pod_namespace: "default".to_string(),
                    hostname: "bench".to_string(),
                    log_directory: "/tmp".to_string(),
                    dns_config: None,
                    port_mappings: vec![],
                    labels: HashMap::new(),
                    annotations: HashMap::new(),
                    linux_cgroup_parent: "/kubepods".to_string(),
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
                .unwrap()
        });
        b.to_async(&rt).iter(|| {
            let rt_clone = runtime_ref.clone();
            let sid = sandbox_id.clone();
            async move {
                let cid = rt_clone
                    .create_container(CreateContainerConfig {
                        pod_uid: "bench-uid".to_string(),
                        pod_name: "bench".to_string(),
                        pod_namespace: "default".to_string(),
                        attempt: 0,
                        container: make_pod("uid", 1).containers.remove(0),
                        sandbox_id: sid,
                        image_id: "sha256:bench".to_string(),
                        log_directory: "/tmp".to_string(),
                        linux_cgroup_parent: "/kubepods".to_string(),
                        env_overrides: HashMap::new(),
                        extra_env: vec![],
                        security: LinuxContainerSecurity::default(),
                        extra_devices: vec![],
                        extra_mounts: vec![],
                        extra_device_envs: vec![],
                        share_process_namespace: false,
                        pod_hostname: "bench-pod".to_string(),
                    })
                    .await
                    .unwrap();
                rt_clone.start_container(&cid).await.unwrap();
            }
        });
    });

    // Pre-populate 1000 containers for the list benchmark
    let list_runtime = Arc::new(MockRuntime::new());
    rt.block_on(async {
        let sandbox_id = list_runtime
            .run_pod_sandbox(CreateSandboxConfig {
                pod_uid: "uid".to_string(),
                pod_name: "pod".to_string(),
                pod_namespace: "default".to_string(),
                hostname: "node".to_string(),
                log_directory: "/tmp".to_string(),
                dns_config: None,
                port_mappings: vec![],
                labels: HashMap::new(),
                annotations: HashMap::new(),
                linux_cgroup_parent: "/kubepods".to_string(),
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
        for i in 0..1000 {
            list_runtime
                .create_container(CreateContainerConfig {
                    pod_uid: format!("uid-{}", i),
                    pod_name: format!("pod-{}", i),
                    pod_namespace: "default".to_string(),
                    attempt: 0,
                    container: make_pod("uid", 1).containers.remove(0),
                    sandbox_id: sandbox_id.clone(),
                    image_id: "sha256:bench".to_string(),
                    log_directory: "/tmp".to_string(),
                    linux_cgroup_parent: "/kubepods".to_string(),
                    env_overrides: HashMap::new(),
                    extra_env: vec![],
                    security: LinuxContainerSecurity::default(),
                    extra_devices: vec![],
                    extra_mounts: vec![],
                    extra_device_envs: vec![],
                    share_process_namespace: false,
                    pod_hostname: "bench-pod".to_string(),
                })
                .await
                .unwrap();
        }
    });
    group.bench_function("list_containers_1000", |b| {
        let lr = list_runtime.clone();
        b.to_async(&rt).iter(move || {
            let lr = lr.clone();
            async move { lr.list_containers().await.unwrap() }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_compute_pod_phase,
    bench_pod_validation,
    bench_qos_classification,
    bench_pod_manager,
    bench_pod_status_manager,
    bench_config_validation,
    bench_concurrent_upserts,
    bench_mock_runtime,
);
criterion_main!(benches);
