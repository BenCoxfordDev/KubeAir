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

//! Smoke tests: verify the kubelet binary and server start correctly.

use kubelet_adapters::mock_runtime::MockRuntime;
use kubelet_app::server::{ServerState, build_router};
use kubelet_core::pod::manager::PodManager;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Smoke test: server starts and responds to /healthz
#[tokio::test]
async fn smoke_server_healthz() {
    let (tx, _rx) = mpsc::channel(10);
    let state = ServerState {
        pod_manager: Arc::new(PodManager::new(tx)),
        runtime: Arc::new(MockRuntime::new()),
        node_name: "smoke-node".to_string(),
        anonymous_auth: true,
        always_allow: true,
        kube_client: None,
        log_dir: "/tmp".to_string(),
    };

    let app = build_router(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}/healthz", port);
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}

/// Smoke test: /readyz returns 200
#[tokio::test]
async fn smoke_server_readyz() {
    let (tx, _rx) = mpsc::channel(10);
    let state = ServerState {
        pod_manager: Arc::new(PodManager::new(tx)),
        runtime: Arc::new(MockRuntime::new()),
        node_name: "smoke-node".to_string(),
        anonymous_auth: true,
        always_allow: true,
        kube_client: None,
        log_dir: "/tmp".to_string(),
    };

    let app = build_router(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{}/readyz", port))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// Smoke test: /configz returns kubelet config JSON and the proxy form matches.
#[tokio::test]
async fn smoke_server_configz() {
    let (tx, _rx) = mpsc::channel(10);
    let state = ServerState {
        pod_manager: Arc::new(PodManager::new(tx)),
        runtime: Arc::new(MockRuntime::new()),
        node_name: "smoke-node".to_string(),
        anonymous_auth: true,
        always_allow: true,
        kube_client: None,
        log_dir: "/tmp".to_string(),
    };

    let app = build_router(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    for path in ["/configz", "/api/v1/nodes/smoke-node/proxy/configz"] {
        let resp = client
            .get(format!("http://127.0.0.1:{}{}", port, path))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200, "path: {}", path);

        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        assert!(content_type.contains("application/json"), "path: {}", path);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["kubeletconfig"]["nodeName"], "smoke-node",
            "path: {}",
            path
        );
    }
}

/// Smoke test: /metrics returns Prometheus-format text
#[tokio::test]
async fn smoke_server_metrics() {
    let (tx, _rx) = mpsc::channel(10);
    let state = ServerState {
        pod_manager: Arc::new(PodManager::new(tx)),
        runtime: Arc::new(MockRuntime::new()),
        node_name: "smoke-node".to_string(),
        anonymous_auth: true,
        always_allow: true,
        kube_client: None,
        log_dir: "/tmp".to_string(),
    };

    let app = build_router(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{}/metrics", port))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(content_type.contains("text/plain"));
}

/// Smoke test: /pods returns PodList JSON
#[tokio::test]
async fn smoke_server_pods_endpoint() {
    let (tx, _rx) = mpsc::channel(10);
    let state = ServerState {
        pod_manager: Arc::new(PodManager::new(tx)),
        runtime: Arc::new(MockRuntime::new()),
        node_name: "smoke-node".to_string(),
        anonymous_auth: true,
        always_allow: true,
        kube_client: None,
        log_dir: "/tmp".to_string(),
    };

    let app = build_router(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{}/pods", port))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["kind"], "PodList");
}

/// Smoke test: Kubelet can be instantiated with valid config
#[tokio::test]
async fn smoke_kubelet_instantiation() {
    let config = kubelet_core::config::KubeletConfig {
        node_name: "smoke-test-node".to_string(),
        root_dir: std::path::PathBuf::from("/tmp/kubelet-smoke"),
        ..Default::default()
    };

    let kubelet = kubelet_app::Kubelet::new(config).await;
    assert_eq!(kubelet.node_name(), "smoke-test-node");

    let status = kubelet.initial_node_status();
    assert!(status.is_ready());
    assert!(status.capacity.cpu_cores >= 1.0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional smoke tests: stats, spec, cadvisor, unknown routes
// Mirrors: TestKubeletServerEndpoints in k8s/pkg/kubelet/server/server_test.go
// ─────────────────────────────────────────────────────────────────────────────

/// Helper: spin up a fresh ServerState + router and return the bound port.
async fn start_server() -> u16 {
    let (tx, _rx) = mpsc::channel(10);
    let state = ServerState {
        pod_manager: Arc::new(PodManager::new(tx)),
        runtime: Arc::new(MockRuntime::new()),
        node_name: "smoke-node".to_string(),
        anonymous_auth: true,
        always_allow: true,
        kube_client: None,
        log_dir: "/tmp".to_string(),
    };
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    port
}

/// Smoke test: /stats/summary returns 200 with a JSON body containing "node" with nested "pods".
/// Mirrors TestKubeletServer_Stats in k8s/pkg/kubelet/server/server_test.go
#[tokio::test]
async fn smoke_stats_summary_returns_node_and_pods() {
    let port = start_server().await;
    let resp = reqwest::get(format!("http://127.0.0.1:{}/stats/summary", port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["node"].is_object(),
        "/stats/summary must contain a 'node' key"
    );
    // pods are nested under node.pods in this implementation
    assert!(
        body["node"]["pods"].is_array(),
        "/stats/summary node must contain a 'pods' array"
    );
}

/// Smoke test: /stats/summary node entry contains nodeName.
#[tokio::test]
async fn smoke_stats_summary_node_name_present() {
    let port = start_server().await;
    let body: serde_json::Value = reqwest::get(format!("http://127.0.0.1:{}/stats/summary", port))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        body["node"]["nodeName"].as_str().unwrap_or(""),
        "smoke-node"
    );
}

/// Smoke test: /spec/ returns 200 with a JSON body.
/// Mirrors TestKubeletServer_Spec in k8s/pkg/kubelet/server/server_test.go
#[tokio::test]
async fn smoke_node_spec_endpoint_returns_json() {
    let port = start_server().await;
    let resp = reqwest::get(format!("http://127.0.0.1:{}/spec/", port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("application/json"),
        "/spec/ must return application/json"
    );
}

/// Smoke test: /metrics/cadvisor returns 200 (compat alias).
#[tokio::test]
async fn smoke_metrics_cadvisor_returns_ok() {
    let port = start_server().await;
    let resp = reqwest::get(format!("http://127.0.0.1:{}/metrics/cadvisor", port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// Smoke test: /metrics/streaming returns 200.
#[tokio::test]
async fn smoke_metrics_streaming_returns_ok() {
    let port = start_server().await;
    let resp = reqwest::get(format!("http://127.0.0.1:{}/metrics/streaming", port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// Smoke test: unknown endpoint returns 404, not 500.
/// Mirrors TestKubeletServer_NotFound in k8s/pkg/kubelet/server/server_test.go
#[tokio::test]
async fn smoke_unknown_endpoint_returns_404() {
    let port = start_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{}/no-such-endpoint", port))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "Unknown endpoint must return 404, not 5xx"
    );
}

/// Smoke test: POST to /healthz returns 405 Method Not Allowed (GET-only route).
#[tokio::test]
async fn smoke_healthz_post_returns_405() {
    let port = start_server().await;
    let resp = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/healthz", port))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        405,
        "POST to GET-only /healthz must return 405"
    );
}

/// Smoke test: /pods returns an empty PodList when no pods are running.
#[tokio::test]
async fn smoke_pods_endpoint_empty_pod_list() {
    let port = start_server().await;
    let body: serde_json::Value = reqwest::get(format!("http://127.0.0.1:{}/pods", port))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["kind"], "PodList");
    assert_eq!(
        body["items"].as_array().map(|a| a.len()).unwrap_or(1),
        0,
        "Empty server must return zero items"
    );
}

/// Smoke test: /configz returns the same JSON via the proxy path.
#[tokio::test]
async fn smoke_configz_proxy_path_matches_direct() {
    let port = start_server().await;
    let client = reqwest::Client::new();

    let direct: serde_json::Value = client
        .get(format!("http://127.0.0.1:{}/configz", port))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let proxy: serde_json::Value = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/nodes/smoke-node/proxy/configz",
            port
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(
        direct["kubeletconfig"]["nodeName"], proxy["kubeletconfig"]["nodeName"],
        "Direct and proxy configz must return the same node name"
    );
}
