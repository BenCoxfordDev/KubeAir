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

//! Kubelet HTTP server (port 10250 / 10255).
//!
//! Exposes the kubelet API used by kubectl exec/logs/port-forward
//! and the API server health checks.
//!
//! Streaming endpoints (exec, attach, port-forward) are wired via WebSocket
//! using the `v4.channel.k8s.io` subprotocol (modern kubectl >= 1.29).

use axum::{
    Json, Router,
    extract::{Query, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{any, get},
};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, info};

use crate::metrics::{
    ContainerMetricLabels, ContainerMetricValues, RUNNING_CONTAINER_COUNT, RUNNING_POD_COUNT,
    gather_metrics, record_container_metrics,
};
use crate::streaming::{
    LogQuery, PortForwardQuery, StreamState, WsStreamParams, attach_handler_inner,
    exec_handler_inner, log_handler, log_websocket_handler, parse_exec_query, port_forward_handler,
    spdy_attach_handler_inner, spdy_exec_handler_inner, spdy_port_forward_handler,
};
use kubelet_core::container::ContainerID;
use kubelet_core::pod::manager::PodManager;
use kubelet_ports::driven::container_runtime::ContainerRuntime;

// -- Shared server state -------------------------------------------------------

/// Shared server state -- cloned into every handler.
#[derive(Clone)]
pub struct ServerState {
    pub pod_manager: Arc<PodManager>,
    pub runtime: Arc<dyn ContainerRuntime>,
    pub node_name: String,
    /// Requests without credentials are treated as anonymous.
    pub anonymous_auth: bool,
    /// All requests are authorized without a SAR check.
    pub always_allow: bool,
    /// Directory where pod/container logs are stored.
    pub log_dir: String,
    /// Pre-authenticated kube client for streaming auth (TokenReview/SAR).
    /// When set, streaming auth uses this instead of resolving a new client.
    pub kube_client: Option<kube::Client>,
}

impl ServerState {
    fn to_stream_state(&self) -> StreamState {
        StreamState {
            pod_manager: self.pod_manager.clone(),
            runtime: self.runtime.clone(),
            node_name: self.node_name.clone(),
            anonymous_auth: self.anonymous_auth,
            always_allow: self.always_allow,
            log_dir: self.log_dir.clone(),
            kube_client: self.kube_client.clone(),
        }
    }
}

// -- Router --------------------------------------------------------------------

/// Build the kubelet API router.
///
/// Routes mirror the real kubelet's serving.go endpoints.
pub fn build_router(state: ServerState) -> Router {
    Router::new()
        // -- Liveness / readiness -----------------------------------------
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        // -- Metrics ------------------------------------------------------
        .route("/metrics", get(metrics_handler))
        .route("/metrics/streaming", get(metrics_handler))
        .route("/metrics/cadvisor", get(metrics_handler)) // compat
        // -- Pod listing ---------------------------------------------------
        .route("/pods", get(list_pods))
        // -- Stats (metrics-server) ----------------------------------------
        .route("/stats/summary", get(stats_summary))
        .route(
            "/stats/:pod_name/:container_name",
            get(container_stats_handler),
        )
        .route(
            "/stats/:ns/:pod_name/:uid/:container_name",
            get(container_stats_handler_ns),
        )
        // -- Streaming: exec / logs / port-forward -------------------------
        // Note: kubectl addresses these via the API server proxy, which forwards
        // to the kubelet. We expose them directly here for node-level access.
        .route(
            "/api/v1/namespaces/:ns/pods/:pod_name/exec",
            get(exec_ws_handler).post(exec_ws_handler),
        )
        .route(
            "/api/v1/namespaces/:ns/pods/:pod_name/attach",
            get(attach_ws_handler).post(attach_ws_handler),
        )
        .route(
            "/api/v1/namespaces/:ns/pods/:pod_name/log",
            get(log_ws_handler),
        )
        // The K8s API server proxies `kubectl logs` to the kubelet at this path.
        // Some API server implementations forward WebSocket upgrades through here,
        // so we must handle both plain HTTP and WebSocket on this route.
        .route(
            "/containerLogs/:ns/:pod_name/:container_name",
            get(container_logs_ws_handler),
        )
        .route(
            "/api/v1/namespaces/:ns/pods/:pod_name/portforward",
            get(pf_ws_handler).post(pf_ws_handler),
        )
        // Kubelet-native paths: used by the API server when proxying exec/attach/
        // portforward directly to the kubelet (path format from pod/strategy.go).
        //   exec:        /exec/{ns}/{pod}/{container}
        //   attach:      /attach/{ns}/{pod}/{container}
        //   portForward: /portForward/{ns}/{pod}
        .route(
            "/exec/:ns/:pod_name/:container_name",
            get(kubelet_exec_handler).post(kubelet_exec_handler),
        )
        .route(
            "/attach/:ns/:pod_name/:container_name",
            get(kubelet_attach_handler).post(kubelet_attach_handler),
        )
        .route(
            "/portForward/:ns/:pod_name",
            get(pf_ws_handler).post(pf_ws_handler),
        )
        // -- Node-level info -----------------------------------------------
        .route("/spec/", get(node_spec_handler))
        .route("/configz", any(configz_handler))
        .route("/configz/", any(configz_handler))
        // Some proxy flows can forward the original apiserver proxy path
        // target rather than rewriting to a kubelet-local path.
        .route(
            "/api/v1/nodes/:node_name/proxy/configz",
            any(configz_proxy_path_handler),
        )
        .route(
            "/api/v1/nodes/:node_name/proxy/configz/",
            any(configz_proxy_path_handler),
        )
        // -- Per-state: main state vs stream state -------------------------
        .with_state(state)
    // Stream routes use a second state layer via nested routers.
    // (They extract StreamState from the ServerState via a layer.)
}

/// Start the kubelet server on the given address (plain HTTP).
pub async fn serve(addr: SocketAddr, router: Router) -> anyhow::Result<()> {
    info!("Kubelet server listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

// -- Liveness / readiness ------------------------------------------------------

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn readyz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

// -- Metrics -------------------------------------------------------------------

async fn metrics_handler(State(state): State<ServerState>) -> impl IntoResponse {
    refresh_cadvisor_metrics(&state).await;
    (
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        gather_metrics(),
    )
}

async fn refresh_cadvisor_metrics(state: &ServerState) {
    let pods = state.pod_manager.list();
    RUNNING_POD_COUNT.set(pods.len() as i64);
    let mut pod_index = std::collections::HashMap::new();
    for pod in &pods {
        pod_index.insert(
            pod.uid.0.clone(),
            (pod.pod_ref.namespace.clone(), pod.pod_ref.name.clone()),
        );
    }

    let Ok(containers) = state.runtime.list_containers().await else {
        return;
    };

    RUNNING_CONTAINER_COUNT.set(containers.len() as i64);

    for container in containers {
        let cid = ContainerID::new(container.id.0.clone());
        if let Ok(Some(stats)) = state.runtime.container_stats(&cid).await {
            let (namespace, pod_name) = pod_index
                .get(&container.pod_uid)
                .cloned()
                .unwrap_or_else(|| ("default".to_string(), container.pod_uid.clone()));
            record_container_metrics(
                &ContainerMetricLabels {
                    namespace: &namespace,
                    pod: &pod_name,
                    container: &container.name,
                    pod_uid: &container.pod_uid,
                },
                &ContainerMetricValues {
                    cpu_usage_core_nanos: stats.cpu_usage_nano_cores,
                    memory_working_set_bytes: stats.memory_usage_bytes,
                    network_rx_bytes: stats.network_rx_bytes,
                    network_tx_bytes: stats.network_tx_bytes,
                    fs_usage_bytes: stats.disk_usage_bytes,
                },
            );
        }
    }
}

// -- Pod listing ---------------------------------------------------------------

async fn list_pods(State(state): State<ServerState>) -> impl IntoResponse {
    let pods = state.pod_manager.list();
    let items: Vec<serde_json::Value> = pods
        .iter()
        .map(|p| {
            serde_json::json!({
                "metadata": {
                    "name": p.pod_ref.name,
                    "namespace": p.pod_ref.namespace,
                    "uid": p.uid.0,
                },
                "spec": {
                    "nodeName": p.node_name,
                    "containers": p.containers.iter().map(|c| serde_json::json!({
                        "name": c.name,
                        "image": c.image,
                    })).collect::<Vec<_>>(),
                }
            })
        })
        .collect();

    Json(serde_json::json!({
        "apiVersion": "v1",
        "kind": "PodList",
        "items": items,
    }))
}

// -- Stats ---------------------------------------------------------------------

async fn stats_summary(State(state): State<ServerState>) -> impl IntoResponse {
    let pods = state.pod_manager.list();
    let pod_stats: Vec<serde_json::Value> = pods
        .iter()
        .map(|p| {
            serde_json::json!({
                "podRef": {
                    "name": p.pod_ref.name,
                    "namespace": p.pod_ref.namespace,
                    "uid": p.uid.0
                },
                "containers": p.containers.iter().map(|c| serde_json::json!({
                    "name": c.name,
                    "cpu": { "usageNanoCores": 0, "usageCoreNanoSeconds": 0 },
                    "memory": { "usageBytes": 0, "workingSetBytes": 0 }
                })).collect::<Vec<_>>()
            })
        })
        .collect();

    Json(serde_json::json!({
        "node": {
            "nodeName": state.node_name,
            "systemContainers": [],
            "cpu": { "usageNanoCores": 0, "usageCoreNanoSeconds": 0 },
            "memory": { "usageBytes": 0, "workingSetBytes": 0 },
            "pods": pod_stats,
        }
    }))
}

#[derive(Deserialize)]
struct ContainerStatsPath {
    pod_name: String,
    container_name: String,
}

async fn container_stats_handler(
    axum::extract::Path(p): axum::extract::Path<ContainerStatsPath>,
    State(_state): State<ServerState>,
) -> impl IntoResponse {
    Json(serde_json::json!({
        "podRef": { "name": p.pod_name },
        "container": p.container_name,
        "cpu": { "usageNanoCores": 0 },
        "memory": { "usageBytes": 0 }
    }))
}

#[derive(Deserialize)]
struct ContainerStatsNsPath {
    ns: String,
    pod_name: String,
    uid: String,
    container_name: String,
}

async fn container_stats_handler_ns(
    axum::extract::Path(p): axum::extract::Path<ContainerStatsNsPath>,
    State(_state): State<ServerState>,
) -> impl IntoResponse {
    Json(serde_json::json!({
        "podRef": { "name": p.pod_name, "namespace": p.ns, "uid": p.uid },
        "container": p.container_name,
        "cpu": { "usageNanoCores": 0 },
        "memory": { "usageBytes": 0 }
    }))
}

// -- Node info -----------------------------------------------------------------

async fn node_spec_handler(State(state): State<ServerState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "nodeName": state.node_name,
        "machineInfo": {
            "numCores": num_cpus::get(),
            "memoryCapacity": 0
        }
    }))
}

async fn configz_handler(State(state): State<ServerState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "kubeletconfig": {
            "nodeName": state.node_name
        }
    }))
}

async fn configz_proxy_path_handler(
    axum::extract::Path(_node_name): axum::extract::Path<String>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    configz_handler(State(state)).await
}

// -- Streaming adapters --------------------------------------------------------
// These shim between the ServerState-based router and StreamState-based handlers.

async fn exec_ws_handler(
    ws: Result<
        axum::extract::ws::WebSocketUpgrade,
        axum::extract::ws::rejection::WebSocketUpgradeRejection,
    >,
    axum::extract::Path((ns, pod_name)): axum::extract::Path<(String, String)>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: axum::http::HeaderMap,
    State(state): State<ServerState>,
    req: Request,
) -> Response {
    let query = parse_exec_query(raw_query.as_deref().unwrap_or(""));
    let ws = match ws {
        Ok(ws) => ws,
        Err(rej) => {
            if is_spdy_upgrade(&headers) {
                return spdy_exec_handler_inner(
                    ns,
                    pod_name,
                    query,
                    headers,
                    state.to_stream_state(),
                    req,
                )
                .await;
            }
            return rej.into_response();
        }
    };

    let cert_cn = req
        .extensions()
        .get::<crate::tls_server::ClientCertCN>()
        .map(|c| c.0.clone());
    exec_handler_inner(
        ws,
        WsStreamParams {
            ns,
            pod_name,
            query,
            headers,
            cert_cn,
        },
        state.to_stream_state(),
    )
    .await
}

async fn attach_ws_handler(
    ws: Result<
        axum::extract::ws::WebSocketUpgrade,
        axum::extract::ws::rejection::WebSocketUpgradeRejection,
    >,
    axum::extract::Path((ns, pod_name)): axum::extract::Path<(String, String)>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: axum::http::HeaderMap,
    State(state): State<ServerState>,
    req: Request,
) -> Response {
    let query = parse_exec_query(raw_query.as_deref().unwrap_or(""));
    let ws = match ws {
        Ok(ws) => ws,
        Err(rej) => {
            if is_spdy_upgrade(&headers) {
                return spdy_attach_handler_inner(
                    ns,
                    pod_name,
                    query,
                    headers,
                    state.to_stream_state(),
                    req,
                )
                .await;
            }
            return rej.into_response();
        }
    };

    let cert_cn = req
        .extensions()
        .get::<crate::tls_server::ClientCertCN>()
        .map(|c| c.0.clone());
    attach_handler_inner(
        ws,
        WsStreamParams {
            ns,
            pod_name,
            query,
            headers,
            cert_cn,
        },
        state.to_stream_state(),
    )
    .await
}

async fn log_ws_handler(
    ws: Result<
        axum::extract::ws::WebSocketUpgrade,
        axum::extract::ws::rejection::WebSocketUpgradeRejection,
    >,
    axum::extract::Path((ns, pod_name)): axum::extract::Path<(String, String)>,
    query: Query<LogQuery>,
    headers: axum::http::HeaderMap,
    State(state): State<ServerState>,
    cert_cn_ext: Option<axum::Extension<crate::tls_server::ClientCertCN>>,
) -> Response {
    if let Ok(ws) = ws {
        return log_websocket_handler(
            ws,
            axum::extract::Path((ns, pod_name)),
            query,
            headers,
            State(state.to_stream_state()),
            cert_cn_ext,
        )
        .await;
    }

    log_handler(
        axum::extract::Path((ns, pod_name)),
        query,
        headers,
        State(state.to_stream_state()),
        cert_cn_ext,
    )
    .await
}

/// Handler for `/containerLogs/{ns}/{pod}/{container}` — used by the K8s API
/// server when proxying `kubectl logs` to the kubelet.
/// Handles both plain HTTP GET (normal log fetch) and WebSocket upgrades
/// (some API server implementations forward the upgrade directly to the kubelet).
async fn container_logs_ws_handler(
    ws: Result<
        axum::extract::ws::WebSocketUpgrade,
        axum::extract::ws::rejection::WebSocketUpgradeRejection,
    >,
    axum::extract::Path((ns, pod_name, container_name)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    Query(mut query): Query<LogQuery>,
    headers: axum::http::HeaderMap,
    State(state): State<ServerState>,
    cert_cn_ext: Option<axum::Extension<crate::tls_server::ClientCertCN>>,
) -> Response {
    if query.container.is_none() {
        query.container = Some(container_name.clone());
    }
    // Debug: log request headers to diagnose WebSocket log issues
    debug!(
        ns = %ns,
        pod = %pod_name,
        container = %container_name,
        ws_ok = ws.is_ok(),
        upgrade_hdr = %headers.get("upgrade").and_then(|v| v.to_str().ok()).unwrap_or(""),
        connection_hdr = %headers.get("connection").and_then(|v| v.to_str().ok()).unwrap_or(""),
        "container_logs_ws_handler"
    );
    if let Ok(ws) = ws {
        return log_websocket_handler(
            ws,
            axum::extract::Path((ns, pod_name)),
            Query(query),
            headers,
            State(state.to_stream_state()),
            cert_cn_ext,
        )
        .await;
    }
    log_handler(
        axum::extract::Path((ns, pod_name)),
        Query(query),
        headers,
        State(state.to_stream_state()),
        cert_cn_ext,
    )
    .await
}

/// Handler for `/containerLogs/{ns}/{pod}/{container}` — used by the K8s API
/// server when proxying `kubectl logs` to the kubelet.
#[allow(dead_code)]
async fn container_logs_handler(
    axum::extract::Path((ns, pod_name, container_name)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    Query(mut query): Query<LogQuery>,
    headers: axum::http::HeaderMap,
    State(state): State<ServerState>,
    cert_cn_ext: Option<axum::Extension<crate::tls_server::ClientCertCN>>,
) -> Response {
    // The container is in the path; the query param takes precedence if set.
    if query.container.is_none() {
        query.container = Some(container_name);
    }
    log_handler(
        axum::extract::Path((ns, pod_name)),
        Query(query),
        headers,
        State(state.to_stream_state()),
        cert_cn_ext,
    )
    .await
}

async fn pf_ws_handler(
    ws: Result<
        axum::extract::ws::WebSocketUpgrade,
        axum::extract::ws::rejection::WebSocketUpgradeRejection,
    >,
    axum::extract::Path((ns, pod_name)): axum::extract::Path<(String, String)>,
    query: Query<PortForwardQuery>,
    headers: axum::http::HeaderMap,
    State(state): State<ServerState>,
    req: Request,
) -> Response {
    let ws = match ws {
        Ok(ws) => ws,
        Err(rej) => {
            if is_spdy_upgrade(&headers) {
                return spdy_port_forward_handler(
                    axum::extract::Path((ns, pod_name)),
                    query,
                    headers,
                    State(state.to_stream_state()),
                    req,
                )
                .await;
            }
            return rej.into_response();
        }
    };

    port_forward_handler(
        ws,
        axum::extract::Path((ns, pod_name)),
        query,
        headers,
        State(state.to_stream_state()),
        {
            let cn = req
                .extensions()
                .get::<crate::tls_server::ClientCertCN>()
                .map(|c| c.0.clone());
            cn.map(|s| axum::Extension(crate::tls_server::ClientCertCN(s)))
        },
    )
    .await
}

/// Handler for `/exec/{ns}/{pod}/{container}` — the kubelet-native path used by
/// the K8s API server when proxying exec to the kubelet (from pod/strategy.go).
/// The container is in the URL path; inject it into the query before delegating.
async fn kubelet_exec_handler(
    ws: Result<
        axum::extract::ws::WebSocketUpgrade,
        axum::extract::ws::rejection::WebSocketUpgradeRejection,
    >,
    axum::extract::Path((ns, pod_name, container_name)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: axum::http::HeaderMap,
    State(state): State<ServerState>,
    req: Request,
) -> Response {
    let mut query = parse_exec_query(raw_query.as_deref().unwrap_or(""));
    if query.container.is_none() {
        query.container = Some(container_name);
    }
    let ws = match ws {
        Ok(ws) => ws,
        Err(rej) => {
            if is_spdy_upgrade(&headers) {
                return spdy_exec_handler_inner(
                    ns,
                    pod_name,
                    query,
                    headers,
                    state.to_stream_state(),
                    req,
                )
                .await;
            }
            return rej.into_response();
        }
    };
    let cert_cn = req
        .extensions()
        .get::<crate::tls_server::ClientCertCN>()
        .map(|c| c.0.clone());
    exec_handler_inner(
        ws,
        WsStreamParams {
            ns,
            pod_name,
            query,
            headers,
            cert_cn,
        },
        state.to_stream_state(),
    )
    .await
}

/// Handler for `/attach/{ns}/{pod}/{container}` — kubelet-native attach path.
async fn kubelet_attach_handler(
    ws: Result<
        axum::extract::ws::WebSocketUpgrade,
        axum::extract::ws::rejection::WebSocketUpgradeRejection,
    >,
    axum::extract::Path((ns, pod_name, container_name)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: axum::http::HeaderMap,
    State(state): State<ServerState>,
    req: Request,
) -> Response {
    let mut query = parse_exec_query(raw_query.as_deref().unwrap_or(""));
    if query.container.is_none() {
        query.container = Some(container_name);
    }
    let ws = match ws {
        Ok(ws) => ws,
        Err(rej) => {
            if is_spdy_upgrade(&headers) {
                return spdy_attach_handler_inner(
                    ns,
                    pod_name,
                    query,
                    headers,
                    state.to_stream_state(),
                    req,
                )
                .await;
            }
            return rej.into_response();
        }
    };
    let cert_cn = req
        .extensions()
        .get::<crate::tls_server::ClientCertCN>()
        .map(|c| c.0.clone());
    attach_handler_inner(
        ws,
        WsStreamParams {
            ns,
            pod_name,
            query,
            headers,
            cert_cn,
        },
        state.to_stream_state(),
    )
    .await
}

fn is_spdy_upgrade(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_ascii_lowercase().contains("spdy/3.1"))
        .unwrap_or(false)
}

// -- Tests ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_adapters::mock_runtime::MockRuntime;
    use kubelet_core::pod::manager::PodManager;
    use std::time::Duration;
    use tokio::sync::mpsc;

    async fn start_test_server() -> (u16, tokio::task::JoinHandle<()>) {
        let (tx, _rx) = mpsc::channel(10);
        let state = ServerState {
            pod_manager: Arc::new(PodManager::new(tx)),
            runtime: Arc::new(MockRuntime::new()),
            node_name: "test-node".to_string(),
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
        tokio::time::sleep(Duration::from_millis(30)).await;
        (port, handle)
    }

    #[tokio::test]
    async fn test_healthz_returns_ok() {
        let (port, handle) = start_test_server().await;
        let resp = reqwest::get(format!("http://127.0.0.1:{}/healthz", port))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        handle.abort();
    }

    #[tokio::test]
    async fn test_readyz_returns_ok() {
        let (port, handle) = start_test_server().await;
        let resp = reqwest::get(format!("http://127.0.0.1:{}/readyz", port))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        handle.abort();
    }

    #[tokio::test]
    async fn test_metrics_returns_200() {
        let (port, handle) = start_test_server().await;
        let resp = reqwest::get(format!("http://127.0.0.1:{}/metrics", port))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        handle.abort();
    }

    #[tokio::test]
    async fn test_cadvisor_metrics_returns_200() {
        let (port, handle) = start_test_server().await;
        let resp = reqwest::get(format!("http://127.0.0.1:{}/metrics/cadvisor", port))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body = resp.text().await.unwrap();
        assert!(body.contains("kubelet_running_pods"));
        handle.abort();
    }

    #[tokio::test]
    async fn test_list_pods_empty() {
        let (port, handle) = start_test_server().await;
        let resp = reqwest::get(format!("http://127.0.0.1:{}/pods", port))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let json: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(json["kind"], "PodList");
        assert_eq!(json["items"].as_array().unwrap().len(), 0);
        handle.abort();
    }

    #[tokio::test]
    async fn test_stats_summary() {
        let (port, handle) = start_test_server().await;
        let resp = reqwest::get(format!("http://127.0.0.1:{}/stats/summary", port))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let json: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(json["node"]["nodeName"], "test-node");
        handle.abort();
    }

    #[tokio::test]
    async fn test_exec_endpoint_exists() {
        let (port, handle) = start_test_server().await;
        // Exec endpoint returns 400/426 (not a WS) without WS upgrade -- that's correct.
        let resp = reqwest::get(format!(
            "http://127.0.0.1:{}/api/v1/namespaces/default/pods/mypod/exec",
            port
        ))
        .await
        .unwrap();
        // 400 = missing WS upgrade headers, which is the correct axum response.
        assert!(resp.status().as_u16() >= 400);
        handle.abort();
    }

    #[tokio::test]
    async fn test_log_endpoint_missing_pod_returns_404() {
        let (port, handle) = start_test_server().await;
        let resp = reqwest::get(format!(
            "http://127.0.0.1:{}/api/v1/namespaces/default/pods/mypod/log",
            port
        ))
        .await
        .unwrap();
        assert!(
            resp.status().as_u16() == 404
                || resp.status().as_u16() == 401
                || resp.status().as_u16() == 503
        );
        handle.abort();
    }
}
