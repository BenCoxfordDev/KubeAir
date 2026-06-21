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

//! HTTP server throughput benchmarks.
//!
//! Measures requests-per-second and p50/p99 latency for the kubelet's HTTP
//! API endpoints. Spins up a real TCP listener and hits it with reqwest.
//!
//! Run with:
//!   cargo bench --bench server_throughput

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kubelet_adapters::mock_runtime::MockRuntime;
use kubelet_app::server::{ServerState, build_router};
use kubelet_core::pod::manager::PodManager;
use kubelet_core::pod::{
    ContainerSpec, ImagePullPolicy, PodSpec, ResourceRequirements, RestartPolicy,
};
use kubelet_core::types::{PodRef, PodUID};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

// ── Server fixture ─────────────────────────────────────────────────────────────

struct TestServer {
    base_url: String,
    _port: u16,
    _handle: tokio::task::JoinHandle<()>,
    client: reqwest::Client,
    _pod_manager: Arc<PodManager>,
}

impl TestServer {
    async fn start_empty() -> Self {
        let (tx, _rx) = mpsc::channel(1000);
        let pod_manager = Arc::new(PodManager::new(tx));
        Self::start_with(pod_manager).await
    }

    async fn start_with(pod_manager: Arc<PodManager>) -> Self {
        let state = ServerState {
            pod_manager: pod_manager.clone(),
            runtime: Arc::new(MockRuntime::new()),
            node_name: "bench-node".to_string(),
            anonymous_auth: true,
            always_allow: true,
            log_dir: "/tmp".to_string(),
            kube_client: None,
        };
        let app = build_router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        // Let server warm up
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(50)
            .build()
            .unwrap();

        Self {
            base_url: format!("http://127.0.0.1:{}", port),
            _port: port,
            _handle: handle,
            client,
            _pod_manager: pod_manager,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

fn make_pod(uid: &str) -> PodSpec {
    PodSpec {
        uid: PodUID::new(uid),
        pod_ref: PodRef::new("default", uid),
        containers: vec![ContainerSpec {
            name: "nginx".to_string(),
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
        }],
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

// ── 1. Healthz Latency & Throughput ───────────────────────────────────────────

/// The /healthz endpoint is polled by the API server every few seconds.
/// It must be extremely fast and never block.
fn bench_healthz(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let server = rt.block_on(TestServer::start_empty());

    let mut group = c.benchmark_group("server_healthz");
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_request_latency", |b| {
        b.to_async(&rt).iter(|| async {
            server
                .client
                .get(server.url("/healthz"))
                .send()
                .await
                .unwrap()
        });
    });

    // Batch: fire N requests sequentially and measure total
    for &batch in &[10u64, 50, 100] {
        group.throughput(Throughput::Elements(batch));
        group.bench_with_input(
            BenchmarkId::new("sequential_requests", batch),
            &batch,
            |b, &n| {
                b.to_async(&rt).iter(|| async {
                    for _ in 0..n {
                        server
                            .client
                            .get(server.url("/healthz"))
                            .send()
                            .await
                            .unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

// ── 2. Readyz Latency ─────────────────────────────────────────────────────────

fn bench_readyz(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let server = rt.block_on(TestServer::start_empty());
    let mut group = c.benchmark_group("server_readyz");
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_request_latency", |b| {
        b.to_async(&rt).iter(|| async {
            server
                .client
                .get(server.url("/readyz"))
                .send()
                .await
                .unwrap()
        });
    });

    group.finish();
}

// ── 3. Metrics Endpoint Throughput ────────────────────────────────────────────

/// /metrics is scraped by Prometheus every 15–30 seconds.
/// We benchmark the Prometheus exposition format serialization overhead.
fn bench_metrics(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let server = rt.block_on(TestServer::start_empty());
    let mut group = c.benchmark_group("server_metrics");
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_scrape_latency", |b| {
        b.to_async(&rt).iter(|| async {
            let resp = server
                .client
                .get(server.url("/metrics"))
                .send()
                .await
                .unwrap();
            resp.bytes().await.unwrap()
        });
    });

    group.finish();
}

// ── 4. Pod List Throughput at Scale ───────────────────────────────────────────

/// /pods is called by the API server and monitoring tools. Performance degrades
/// with pod count — this bench quantifies that relationship.
fn bench_pod_list(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("server_pod_list");

    for &pod_count in &[0u64, 10, 50, 110] {
        group.throughput(Throughput::Elements(1));

        // Pre-populate the manager
        let (tx, _rx) = mpsc::channel(1000);
        let pm = Arc::new(PodManager::new(tx));
        rt.block_on(async {
            for i in 0..pod_count {
                pm.upsert(make_pod(&format!("bench-uid-{}", i)))
                    .await
                    .unwrap();
            }
        });

        let server = rt.block_on(TestServer::start_with(pm));

        group.bench_with_input(
            BenchmarkId::new("list_pods_count", pod_count),
            &pod_count,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let resp = server.client.get(server.url("/pods")).send().await.unwrap();
                    resp.bytes().await.unwrap()
                });
            },
        );
    }

    group.finish();
}

// ── 5. Stats Summary ──────────────────────────────────────────────────────────

fn bench_stats_summary(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let server = rt.block_on(TestServer::start_empty());
    let mut group = c.benchmark_group("server_stats_summary");
    group.throughput(Throughput::Elements(1));

    group.bench_function("latency", |b| {
        b.to_async(&rt).iter(|| async {
            let resp = server
                .client
                .get(server.url("/stats/summary"))
                .send()
                .await
                .unwrap();
            resp.bytes().await.unwrap()
        });
    });

    group.finish();
}

// ── 6. Concurrent Client Throughput ──────────────────────────────────────────

/// Simulates multiple kubelet clients (API server, metrics-server, kubectl)
/// hitting the server concurrently.
fn bench_concurrent_clients(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let server = Arc::new(rt.block_on(TestServer::start_empty()));
    let mut group = c.benchmark_group("server_concurrent_clients");

    for &clients in &[2u64, 4, 8, 16] {
        group.throughput(Throughput::Elements(clients));
        let server_ref = server.clone();
        group.bench_with_input(
            BenchmarkId::new("concurrent_healthz_clients", clients),
            &clients,
            |b, &n| {
                b.to_async(&rt).iter(|| async {
                    let mut handles = vec![];
                    for _ in 0..n {
                        let url = server_ref.url("/healthz");
                        let client = server_ref.client.clone();
                        handles.push(tokio::spawn(async move {
                            client.get(url).send().await.unwrap()
                        }));
                    }
                    for h in handles {
                        h.await.unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

// ── 7. End-to-end: Full API Server Poll Simulation ────────────────────────────

/// Simulates what the Kubernetes API server does to poll a kubelet:
/// GET /healthz, then GET /pods if healthy.
fn bench_api_server_poll_simulation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let (tx, _rx) = mpsc::channel(200);
    let pm = Arc::new(PodManager::new(tx));
    rt.block_on(async {
        for i in 0..20 {
            pm.upsert(make_pod(&format!("uid-{}", i))).await.unwrap();
        }
    });

    let server = rt.block_on(TestServer::start_with(pm));
    let mut group = c.benchmark_group("server_api_server_poll");
    group.throughput(Throughput::Elements(2)); // healthz + pods

    group.bench_function("healthz_then_pods_20_pods", |b| {
        b.to_async(&rt).iter(|| async {
            let h = server
                .client
                .get(server.url("/healthz"))
                .send()
                .await
                .unwrap();
            if h.status() == 200 {
                server
                    .client
                    .get(server.url("/pods"))
                    .send()
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
            } else {
                bytes::Bytes::new()
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_healthz,
    bench_readyz,
    bench_metrics,
    bench_pod_list,
    bench_stats_summary,
    bench_concurrent_clients,
    bench_api_server_poll_simulation,
);
criterion_main!(benches);
