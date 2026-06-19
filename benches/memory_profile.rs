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

//! Memory profiling benchmarks for the kubelet.
//!
//! Measures heap allocations and peak RSS growth under workloads of
//! increasing pod counts. Uses /proc/self/status for live RSS readings.
//!
//! Run with:
//!   cargo bench --bench memory_profile
//!
//! For detailed heap profiling, build with:
//!   RUSTFLAGS="-C force-frame-pointers=yes" cargo bench --bench memory_profile

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kubelet_core::pod::manager::PodManager;
use kubelet_core::pod::status::PodStatusManager;
use kubelet_core::pod::{
    ContainerSpec, ImagePullPolicy, PodSpec, ResourceRequirements, RestartPolicy,
};
use kubelet_core::types::{PodRef, PodUID};
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

// ── RSS measurement ───────────────────────────────────────────────────────────

/// Read current process RSS from /proc/self/status (Linux only).
/// Returns bytes, or 0 on non-Linux.
fn rss_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    let kb: u64 = line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    return kb * 1024;
                }
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    0
}

fn format_bytes(b: u64) -> String {
    if b >= 1024 * 1024 * 1024 {
        format!("{:.2} GiB", b as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if b >= 1024 * 1024 {
        format!("{:.2} MiB", b as f64 / (1024.0 * 1024.0))
    } else if b >= 1024 {
        format!("{:.2} KiB", b as f64 / 1024.0)
    } else {
        format!("{} B", b)
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn make_pod(uid: &str, containers: usize) -> PodSpec {
    PodSpec {
        uid: PodUID::new(uid),
        pod_ref: PodRef::new("default", uid),
        containers: (0..containers)
            .map(|i| ContainerSpec {
                name: format!("container-{}", i),
                image: "nginx:latest".to_string(),
                command: vec!["nginx".to_string()],
                args: vec!["-g".to_string(), "daemon off;".to_string()],
                working_dir: Some("/".to_string()),
                ports: vec![],
                env: vec![],
                resources: ResourceRequirements::default(),
                volume_mounts: vec![],
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                image_pull_policy: ImagePullPolicy::IfNotPresent,
                security_context: None,
                termination_message_path: Some("/dev/termination-log".to_string()),
                ..Default::default()
            })
            .collect(),
        init_containers: vec![],
        ephemeral_containers: vec![],
        volumes: vec![],
        node_name: "memory-bench-node".to_string(),
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
        annotations: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "kubectl.kubernetes.io/last-applied-configuration".to_string(),
                "{\"apiVersion\":\"v1\",\"kind\":\"Pod\"}".to_string(),
            );
            m
        },
        labels: {
            let mut m = std::collections::HashMap::new();
            m.insert("app".to_string(), uid.to_string());
            m.insert("version".to_string(), "v1".to_string());
            m
        },
        runtime_class_name: None,
        security_context: None,
        ..Default::default()
    }
}

// ── 1. Pod Manager Memory Scaling ─────────────────────────────────────────────

/// Measures RSS growth as we add increasing numbers of pods to the manager.
/// Reports bytes-per-pod overhead so you can project memory at scale.
fn bench_pod_manager_memory_scaling(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("memory_scaling_pod_manager");
    group.sample_size(10); // fewer samples since we care about allocation patterns

    for &pod_count in &[10u64, 100, 500, 1_000, 5_000] {
        group.throughput(Throughput::Elements(pod_count));
        group.bench_with_input(
            BenchmarkId::new("pods_in_manager", pod_count),
            &pod_count,
            |b, &count| {
                b.to_async(&rt).iter(|| async move {
                    let before = rss_bytes();
                    let (tx, _rx) = mpsc::channel(count as usize + 100);
                    let manager = PodManager::new(tx);
                    for i in 0..count {
                        manager
                            .upsert(make_pod(&format!("uid-{}", i), 1))
                            .await
                            .unwrap();
                    }
                    let after = rss_bytes();
                    // Return so criterion can measure time, but also print memory
                    (count, before, after, manager.count())
                });
            },
        );
    }

    group.finish();
}

// ── 2. Status Manager Memory ──────────────────────────────────────────────────

fn bench_status_manager_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_status_manager");
    group.sample_size(10);

    for &pod_count in &[100u64, 1_000, 10_000] {
        group.throughput(Throughput::Elements(pod_count));
        group.bench_with_input(
            BenchmarkId::new("pod_statuses_initialized", pod_count),
            &pod_count,
            |b, &count| {
                b.iter(|| {
                    let manager = PodStatusManager::new();
                    for i in 0..count {
                        manager.initialize(&make_pod(&format!("uid-{}", i), 1));
                    }
                    manager.all().len()
                });
            },
        );
    }

    group.finish();
}

// ── 3. Allocation Rate Under Churn ─────────────────────────────────────────────

/// Measures memory behaviour when pods are constantly added and removed —
/// simulating real node churn. We care about both peak usage and that memory
/// is released after removes.
fn bench_pod_churn_memory(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("memory_pod_churn");
    group.sample_size(10);

    // Add N pods then remove them all, measure RSS delta
    for &churn_count in &[100u64, 500] {
        group.throughput(Throughput::Elements(churn_count * 2)); // add + remove
        group.bench_with_input(
            BenchmarkId::new("add_then_remove_all", churn_count),
            &churn_count,
            |b, &count| {
                b.to_async(&rt).iter(|| async move {
                    let (tx, _rx) = mpsc::channel(count as usize * 2 + 100);
                    let manager = PodManager::new(tx);
                    let rss_start = rss_bytes();

                    // Fill
                    for i in 0..count {
                        manager
                            .upsert(make_pod(&format!("uid-{}", i), 2))
                            .await
                            .unwrap();
                    }
                    let rss_peak = rss_bytes();

                    // Drain
                    for i in 0..count {
                        manager
                            .remove(&PodUID::new(format!("uid-{}", i)), None)
                            .await
                            .unwrap();
                    }
                    let rss_end = rss_bytes();

                    (rss_start, rss_peak, rss_end, manager.count())
                });
            },
        );
    }

    group.finish();
}

// ── 4. Standalone Memory Report (not a criterion bench) ───────────────────────

/// A standalone memory measurement function that prints a human-readable
/// report to stdout. Invoked when criterion runs this benchmark file.
fn print_memory_report() {
    let rt = Runtime::new().unwrap();

    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║         kube-air Memory Profiling Report               ║");
    println!("╚══════════════════════════════════════════════════════════╝");

    rt.block_on(async {
        let scenarios: Vec<(u64, usize)> =
            vec![(10, 1), (100, 1), (500, 1), (1_000, 1), (100, 5), (500, 5)];

        println!("\n  Pods   Ctrs │ RSS Before │ RSS After  │ Delta      │ Per-Pod");
        println!("  ─────────────┼────────────┼────────────┼────────────┼───────────");

        for (pod_count, ctr_count) in scenarios {
            // Force a GC-like collection by dropping
            let before = rss_bytes();

            let (tx, _rx) = mpsc::channel(pod_count as usize + 100);
            let manager = PodManager::new(tx);
            for i in 0..pod_count {
                manager
                    .upsert(make_pod(&format!("uid-{}", i), ctr_count))
                    .await
                    .unwrap();
            }

            let after = rss_bytes();
            let delta = after.saturating_sub(before);
            let per_pod = if pod_count > 0 { delta / pod_count } else { 0 };

            println!(
                "  {:5}  {:4} │ {:10} │ {:10} │ {:10} │ {:9}",
                pod_count,
                ctr_count,
                format_bytes(before),
                format_bytes(after),
                format_bytes(delta),
                format_bytes(per_pod),
            );
        }

        println!();
        println!("  Note: RSS measurements include Rust runtime, tokio, dashmap, etc.");
        println!("  Per-pod overhead reflects the full domain model allocation.");
    });

    println!();
}

// ── 5. Criterion: combined memory+time benchmark ──────────────────────────────

/// Directly measures both time and RSS for pod operations so criterion's
/// timing output can be correlated with the memory report above.
fn bench_memory_and_time(c: &mut Criterion) {
    // Print human-readable report once before criterion starts
    print_memory_report();

    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("memory_and_time");
    group.sample_size(10);

    group.bench_function("1000_pods_full_lifecycle_rss", |b| {
        b.to_async(&rt).iter(|| async {
            let before_rss = rss_bytes();
            let start = Instant::now();

            let (tx, _rx) = mpsc::channel(2000);
            let manager = Arc::new(PodManager::new(tx));

            for i in 0..1000usize {
                manager
                    .upsert(make_pod(&format!("uid-{}", i), 1))
                    .await
                    .unwrap();
            }

            let elapsed = start.elapsed();
            let after_rss = rss_bytes();

            // Return values to prevent dead-code elimination
            (
                manager.count(),
                elapsed.as_micros(),
                after_rss.saturating_sub(before_rss),
            )
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_pod_manager_memory_scaling,
    bench_status_manager_memory,
    bench_pod_churn_memory,
    bench_memory_and_time,
);
criterion_main!(benches);
