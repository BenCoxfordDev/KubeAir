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

//! Probe runner -- executes liveness, readiness, and startup probes.
//!
//! Mirrors pkg/kubelet/prober/prober_manager.go.
//!
//! For each running container:
//!   - Startup probe: blocks readiness until success; kills container if it fails.
//!   - Liveness probe: kills container if it fails (triggers restart per policy).
//!   - Readiness probe: removes pod from endpoints if it fails.
//!
//! All probes run in background tasks spawned per-container.
//! Results are sent via a channel to the pod worker for action.

use kubelet_core::container::ContainerID;
use kubelet_core::error::{KubeletError, Result};
use kubelet_core::pod::{Probe, ProbeHandler};
use kubelet_core::types::PodUID;
use kubelet_ports::driven::container_runtime::ContainerRuntime;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

// -- Result types --------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum ProbeKind {
    Liveness,
    Readiness,
    Startup,
}

#[derive(Debug, Clone)]
pub struct ProbeOutcome {
    pub pod_uid: PodUID,
    pub container_name: String,
    pub kind: ProbeKind,
    pub success: bool,
    pub message: String,
}

// -- Probe runner --------------------------------------------------------------

/// Executes a single probe against a container.
pub struct ProbeRunner {
    runtime: Arc<dyn ContainerRuntime>,
    result_tx: mpsc::Sender<ProbeOutcome>,
}

impl ProbeRunner {
    pub fn new(runtime: Arc<dyn ContainerRuntime>, result_tx: mpsc::Sender<ProbeOutcome>) -> Self {
        Self { runtime, result_tx }
    }

    /// Spawn a background task running a probe repeatedly.
    pub fn spawn(
        self: Arc<Self>,
        pod_uid: PodUID,
        container_name: String,
        container_id: ContainerID,
        probe: Probe,
        kind: ProbeKind,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // Wait for initialDelaySeconds before first probe.
            sleep(Duration::from_secs(probe.initial_delay_seconds as u64)).await;

            let mut consecutive_failures: u32 = 0;
            let mut consecutive_successes: u32 = 0;

            loop {
                let result = self
                    .run_once(&probe.handler, &container_id, probe.timeout_seconds)
                    .await;

                match result {
                    Ok(()) => {
                        consecutive_failures = 0;
                        consecutive_successes += 1;
                        if consecutive_successes >= probe.success_threshold {
                            debug!(
                                pod = %pod_uid.0, container = %container_name, kind = ?kind,
                                "Probe success"
                            );
                            let _ = self
                                .result_tx
                                .send(ProbeOutcome {
                                    pod_uid: pod_uid.clone(),
                                    container_name: container_name.clone(),
                                    kind: kind.clone(),
                                    success: true,
                                    message: String::new(),
                                })
                                .await;
                        }
                    }
                    Err(e) => {
                        consecutive_successes = 0;
                        consecutive_failures += 1;
                        debug!(
                            pod = %pod_uid.0, container = %container_name,
                            kind = ?kind, error = %e, failures = consecutive_failures,
                            threshold = probe.failure_threshold,
                            "Probe failure"
                        );
                        if consecutive_failures >= probe.failure_threshold {
                            warn!(
                                pod = %pod_uid.0, container = %container_name,
                                kind = ?kind, "Probe failed threshold; triggering action"
                            );
                            let _ = self
                                .result_tx
                                .send(ProbeOutcome {
                                    pod_uid: pod_uid.clone(),
                                    container_name: container_name.clone(),
                                    kind: kind.clone(),
                                    success: false,
                                    message: e.to_string(),
                                })
                                .await;
                            // For liveness/startup: container will be killed by pod worker.
                            // Stop probing after reporting failure.
                            if kind != ProbeKind::Readiness {
                                return;
                            }
                            consecutive_failures = 0; // reset for readiness, keep probing
                        }
                    }
                }

                sleep(Duration::from_secs(probe.period_seconds as u64)).await;
            }
        })
    }

    /// Execute one probe attempt.
    async fn run_once(
        &self,
        handler: &ProbeHandler,
        container_id: &ContainerID,
        timeout_seconds: u32,
    ) -> Result<()> {
        let timeout = Duration::from_secs(timeout_seconds as u64);

        match handler {
            ProbeHandler::Exec { command } => {
                let result = tokio::time::timeout(
                    timeout + Duration::from_secs(1),
                    self.runtime
                        .exec_sync(container_id, command.clone(), timeout_seconds as u64),
                )
                .await
                .map_err(|_| KubeletError::Probe("exec probe timed out".to_string()))?
                .map_err(|e| KubeletError::Probe(format!("exec probe error: {}", e)))?;

                if result.exit_code != 0 {
                    let msg = String::from_utf8_lossy(&result.stderr).to_string();
                    return Err(KubeletError::Probe(format!(
                        "exec probe exited with {}: {}",
                        result.exit_code,
                        msg.trim()
                    )));
                }
                Ok(())
            }

            ProbeHandler::HttpGet {
                path,
                port,
                host,
                scheme,
            } => {
                let host_str = host.as_deref().unwrap_or("localhost");
                let url = format!("{}://{}:{}{}", scheme.to_lowercase(), host_str, port, path);

                let client = reqwest::Client::builder()
                    .timeout(timeout)
                    .build()
                    .map_err(|e| KubeletError::Probe(format!("HTTP client: {}", e)))?;

                let resp =
                    tokio::time::timeout(timeout + Duration::from_secs(1), client.get(&url).send())
                        .await
                        .map_err(|_| KubeletError::Probe("HTTP probe timed out".to_string()))?
                        .map_err(|e| KubeletError::Probe(format!("HTTP probe failed: {}", e)))?;

                let status = resp.status().as_u16();
                if (200..400).contains(&status) {
                    Ok(())
                } else {
                    Err(KubeletError::Probe(format!(
                        "HTTP probe: status {}",
                        status
                    )))
                }
            }

            ProbeHandler::TcpSocket { port, host } => {
                let host_str = host.as_deref().unwrap_or("localhost");
                tokio::time::timeout(
                    timeout,
                    tokio::net::TcpStream::connect(format!("{}:{}", host_str, port)),
                )
                .await
                .map_err(|_| KubeletError::Probe(format!("TCP probe timed out on port {}", port)))?
                .map(|_| ())
                .map_err(|e| KubeletError::Probe(format!("TCP probe failed: {}", e)))
            }

            ProbeHandler::Grpc { port, service } => {
                let endpoint = format!("http://localhost:{}", port);
                let channel = tokio::time::timeout(
                    timeout,
                    tonic::transport::Endpoint::from_shared(endpoint)
                        .map_err(|e| KubeletError::Probe(format!("gRPC endpoint: {}", e)))?
                        .connect_timeout(timeout)
                        .timeout(timeout)
                        .connect(),
                )
                .await
                .map_err(|_| KubeletError::Probe(format!("gRPC probe timed out on port {}", port)))?
                .map_err(|e| KubeletError::Probe(format!("gRPC connect failed: {}", e)))?;

                let mut client = tonic_health::pb::health_client::HealthClient::new(channel);
                let request = tonic_health::pb::HealthCheckRequest {
                    service: service.clone().unwrap_or_default(),
                };

                let response = tokio::time::timeout(timeout, client.check(request))
                    .await
                    .map_err(|_| KubeletError::Probe("gRPC health check timed out".to_string()))?
                    .map_err(|e| KubeletError::Probe(format!("gRPC health check failed: {}", e)))?
                    .into_inner();

                if response.status
                    == tonic_health::pb::health_check_response::ServingStatus::Serving as i32
                {
                    Ok(())
                } else {
                    Err(KubeletError::Probe(format!(
                        "gRPC health check returned non-serving status {}",
                        response.status
                    )))
                }
            }
        }
    }
}

// -- Probe manager -------------------------------------------------------------

/// Manages all probe tasks for all containers on this node.
pub struct ProbeManager {
    runtime: Arc<dyn ContainerRuntime>,
    pub result_tx: mpsc::Sender<ProbeOutcome>,
    pub result_rx: mpsc::Receiver<ProbeOutcome>,
    /// Active probe handles: (pod_uid, container_name, kind) -> JoinHandle
    handles: std::collections::HashMap<(String, String, String), tokio::task::JoinHandle<()>>,
}

impl ProbeManager {
    pub fn new(runtime: Arc<dyn ContainerRuntime>) -> Self {
        let (tx, rx) = mpsc::channel(256);
        Self {
            runtime,
            result_tx: tx,
            result_rx: rx,
            handles: Default::default(),
        }
    }

    /// Start probes for a container.
    pub fn start_probes(
        &mut self,
        pod_uid: &PodUID,
        container_name: &str,
        container_id: &ContainerID,
        liveness: Option<&Probe>,
        readiness: Option<&Probe>,
        startup: Option<&Probe>,
    ) {
        let runner = Arc::new(ProbeRunner::new(
            self.runtime.clone(),
            self.result_tx.clone(),
        ));

        // Startup probe blocks liveness/readiness (run first).
        if let Some(probe) = startup {
            let handle = runner.clone().spawn(
                pod_uid.clone(),
                container_name.to_string(),
                container_id.clone(),
                probe.clone(),
                ProbeKind::Startup,
            );
            self.handles.insert(
                (
                    pod_uid.0.clone(),
                    container_name.to_string(),
                    "startup".to_string(),
                ),
                handle,
            );
        }

        if let Some(probe) = liveness {
            let handle = runner.clone().spawn(
                pod_uid.clone(),
                container_name.to_string(),
                container_id.clone(),
                probe.clone(),
                ProbeKind::Liveness,
            );
            self.handles.insert(
                (
                    pod_uid.0.clone(),
                    container_name.to_string(),
                    "liveness".to_string(),
                ),
                handle,
            );
        }

        if let Some(probe) = readiness {
            let handle = runner.clone().spawn(
                pod_uid.clone(),
                container_name.to_string(),
                container_id.clone(),
                probe.clone(),
                ProbeKind::Readiness,
            );
            self.handles.insert(
                (
                    pod_uid.0.clone(),
                    container_name.to_string(),
                    "readiness".to_string(),
                ),
                handle,
            );
        }
    }

    /// Stop all probes for a container (on deletion/termination).
    pub fn stop_probes(&mut self, pod_uid: &PodUID, container_name: &str) {
        for kind in &["liveness", "readiness", "startup"] {
            let key = (
                pod_uid.0.clone(),
                container_name.to_string(),
                kind.to_string(),
            );
            if let Some(handle) = self.handles.remove(&key) {
                handle.abort();
            }
        }
    }

    /// Stop all probes for an entire pod.
    pub fn stop_all_pod_probes(&mut self, pod_uid: &PodUID) {
        let uid = &pod_uid.0;
        let to_remove: Vec<_> = self
            .handles
            .keys()
            .filter(|(u, _, _)| u == uid)
            .cloned()
            .collect();
        for key in to_remove {
            if let Some(h) = self.handles.remove(&key) {
                h.abort();
            }
        }
    }

    pub fn active_probe_count(&self) -> usize {
        self.handles.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_runtime::MockRuntime;
    use tonic_health::server::health_reporter;

    #[tokio::test]
    async fn test_exec_probe_success_with_mock() {
        let runtime = Arc::new(MockRuntime::new());
        let (tx, _rx) = mpsc::channel(10);
        let runner = Arc::new(ProbeRunner::new(runtime, tx));
        let handler = ProbeHandler::Exec {
            command: vec!["true".to_string()],
        };
        let cid = ContainerID("ctr-abc".to_string());
        let result = runner.run_once(&handler, &cid, 5).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_tcp_probe_fails_when_nothing_listening() {
        let runtime = Arc::new(MockRuntime::new());
        let (tx, _rx) = mpsc::channel(10);
        let runner = Arc::new(ProbeRunner::new(runtime, tx));
        let handler = ProbeHandler::TcpSocket {
            port: 19997,
            host: None,
        };
        let cid = ContainerID("ctr-abc".to_string());
        let result = runner.run_once(&handler, &cid, 1).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_probe_manager_starts_and_stops() {
        let runtime = Arc::new(MockRuntime::new());
        let mut mgr = ProbeManager::new(runtime);
        let uid = PodUID::new("uid-probe-1");
        let cid = ContainerID("ctr-123".to_string());
        let probe = Probe {
            handler: ProbeHandler::TcpSocket {
                port: 19996,
                host: None,
            },
            initial_delay_seconds: 3600, // far in future
            period_seconds: 10,
            timeout_seconds: 5,
            success_threshold: 1,
            failure_threshold: 3,
        };
        mgr.start_probes(&uid, "app", &cid, Some(&probe), None, None);
        assert_eq!(mgr.active_probe_count(), 1);
        mgr.stop_probes(&uid, "app");
        assert_eq!(mgr.active_probe_count(), 0);
    }

    #[tokio::test]
    async fn test_grpc_probe_success() {
        let runtime = Arc::new(MockRuntime::new());
        let (tx, _rx) = mpsc::channel(10);
        let runner = Arc::new(ProbeRunner::new(runtime, tx));

        let (mut reporter, service) = health_reporter();
        reporter
            .set_serving::<tonic_health::pb::health_server::HealthServer<
                tonic_health::server::HealthService,
            >>()
            .await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let handler = ProbeHandler::Grpc {
            port,
            service: None,
        };
        let cid = ContainerID("ctr-abc".to_string());
        let result = runner.run_once(&handler, &cid, 2).await;
        assert!(result.is_ok());
        server.abort();
    }

    #[tokio::test]
    async fn test_grpc_probe_connection_refused() {
        let runtime = Arc::new(MockRuntime::new());
        let (tx, _rx) = mpsc::channel(10);
        let runner = Arc::new(ProbeRunner::new(runtime, tx));

        let handler = ProbeHandler::Grpc {
            port: 19998,
            service: None,
        };
        let cid = ContainerID("ctr-abc".to_string());
        let result = runner.run_once(&handler, &cid, 1).await;
        assert!(result.is_err());
    }
}
