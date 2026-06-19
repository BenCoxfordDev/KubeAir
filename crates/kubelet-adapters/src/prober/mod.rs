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

//! Prober adapter - implements liveness, readiness, and startup probes.
//!
//! Mirrors pkg/kubelet/prober in the Go kubelet.

use kubelet_core::container::ContainerID;
use kubelet_core::error::{KubeletError, Result};
use kubelet_core::pod::{Probe, ProbeHandler};
use kubelet_ports::driven::container_runtime::ContainerRuntime;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::{debug, info, warn};

/// Shared reqwest HTTP client for all probe invocations.
///
/// Creating a new `reqwest::Client` on every probe invocation (potentially
/// hundreds of times per second across all containers) can exhaust file
/// descriptors and cause "builder error" failures.  A single shared client
/// is safe to use concurrently and reuses connection pools efficiently.
static PROBE_HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn probe_http_client() -> Option<&'static reqwest::Client> {
    Some(PROBE_HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .build()
            .expect("failed to build shared probe HTTP client")
    }))
}

/// Result of executing a probe.
#[derive(Debug, Clone, PartialEq)]
pub enum ProbeResult {
    Success,
    Failure(String),
    Unknown(String),
}

/// Probe type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProbeType {
    Liveness,
    Readiness,
    Startup,
}

impl std::fmt::Display for ProbeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Liveness => write!(f, "liveness"),
            Self::Readiness => write!(f, "readiness"),
            Self::Startup => write!(f, "startup"),
        }
    }
}

/// Probe state tracking for a single container.
#[derive(Debug, Clone, Default)]
pub struct ProbeState {
    pub consecutive_successes: u32,
    pub consecutive_failures: u32,
    pub last_result: Option<ProbeResult>,
}

/// Evaluates whether a probe has passed or failed based on thresholds.
pub fn evaluate_probe_result(
    result: &ProbeResult,
    state: &mut ProbeState,
    probe: &Probe,
    probe_type: ProbeType,
) -> ProbeDecision {
    match result {
        ProbeResult::Success => {
            state.consecutive_failures = 0;
            state.consecutive_successes += 1;
            if state.consecutive_successes >= probe.success_threshold {
                ProbeDecision::Pass
            } else {
                ProbeDecision::Pending
            }
        }
        ProbeResult::Failure(msg) | ProbeResult::Unknown(msg) => {
            state.consecutive_successes = 0;
            state.consecutive_failures += 1;
            if state.consecutive_failures >= probe.failure_threshold {
                warn!(
                    probe = %probe_type,
                    failures = state.consecutive_failures,
                    threshold = probe.failure_threshold,
                    reason = %msg,
                    "Probe failed - threshold exceeded"
                );
                ProbeDecision::Fail
            } else {
                info!(
                    probe = %probe_type,
                    failures = state.consecutive_failures,
                    threshold = probe.failure_threshold,
                    reason = %msg,
                    "Probe failed"
                );
                ProbeDecision::Pending
            }
        }
    }
}

/// Decision resulting from probe evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum ProbeDecision {
    /// Probe passed - container considered healthy.
    Pass,
    /// Probe failed and threshold exceeded.
    Fail,
    /// Still within grace/threshold period.
    Pending,
}

/// Executes an HTTP GET probe against a given host:port/path.
pub async fn run_http_probe(
    host: &str,
    port: u16,
    path: &str,
    scheme: &str,
    timeout: Duration,
) -> ProbeResult {
    let url = format!("{}://{}:{}{}", scheme.to_lowercase(), host, port, path);
    debug!("HTTP probe: GET {}", url);

    let client = match probe_http_client() {
        Some(c) => c,
        None => return ProbeResult::Unknown("HTTP probe client unavailable".to_string()),
    };

    match tokio::time::timeout(timeout, client.get(&url).send()).await {
        Ok(Ok(resp)) => {
            let status = resp.status().as_u16();
            if (200..=399).contains(&status) {
                ProbeResult::Success
            } else {
                let msg = format!("HTTP probe returned status {}", status);
                info!(url = %url, status = status, "HTTP probe failed");
                ProbeResult::Failure(msg)
            }
        }
        Ok(Err(e)) => {
            let msg = format!("HTTP probe error: {}", e);
            info!(url = %url, error = %e, "HTTP probe error");
            ProbeResult::Failure(msg)
        }
        Err(_) => {
            let msg = format!("HTTP probe timed out after {}s", timeout.as_secs());
            info!(url = %url, timeout_secs = timeout.as_secs(), "HTTP probe timed out");
            ProbeResult::Failure(msg)
        }
    }
}

/// Checks if a TCP port is accepting connections.
pub async fn run_tcp_probe(host: &str, port: u16, timeout: Duration) -> ProbeResult {
    debug!("TCP probe: {}:{}", host, port);
    match tokio::time::timeout(
        timeout,
        tokio::net::TcpStream::connect(format!("{}:{}", host, port)),
    )
    .await
    {
        Ok(Ok(_)) => ProbeResult::Success,
        Ok(Err(e)) => ProbeResult::Failure(format!("TCP connect failed: {}", e)),
        Err(_) => ProbeResult::Failure(format!("TCP probe timed out after {}s", timeout.as_secs())),
    }
}

/// Executes an exec probe by running a command inside the container.
pub async fn run_exec_probe(
    runtime: &dyn ContainerRuntime,
    cid: &ContainerID,
    command: Vec<String>,
    timeout: Duration,
) -> ProbeResult {
    debug!("Exec probe: {:?} in {}", command, cid);
    match tokio::time::timeout(
        timeout + Duration::from_secs(1),
        runtime.exec_sync(cid, command, timeout.as_secs()),
    )
    .await
    {
        Ok(Ok(r)) if r.exit_code == 0 => ProbeResult::Success,
        Ok(Ok(r)) => ProbeResult::Failure(format!("exit code {}", r.exit_code)),
        Ok(Err(e)) => ProbeResult::Failure(format!("exec error: {}", e)),
        Err(_) => {
            ProbeResult::Failure(format!("exec probe timed out after {}s", timeout.as_secs()))
        }
    }
}

/// Executes a gRPC health probe against the target endpoint.
pub async fn run_grpc_probe(
    host: &str,
    port: u16,
    service: Option<&str>,
    timeout: Duration,
) -> ProbeResult {
    debug!(host = %host, port = port, service = ?service, "gRPC probe");

    let endpoint = format!("http://{}:{}", host, port);
    let ep = match tonic::transport::Endpoint::from_shared(endpoint) {
        Ok(ep) => ep.connect_timeout(timeout).timeout(timeout),
        Err(e) => return ProbeResult::Failure(format!("gRPC endpoint: {}", e)),
    };
    let channel = match tokio::time::timeout(timeout, ep.connect()).await {
        Ok(Ok(channel)) => channel,
        Ok(Err(e)) => return ProbeResult::Failure(format!("gRPC connect failed: {}", e)),
        Err(_) => {
            return ProbeResult::Failure(format!(
                "gRPC probe timed out after {}s",
                timeout.as_secs()
            ))
        }
    };

    let mut client = tonic_health::pb::health_client::HealthClient::new(channel);
    let request = tonic_health::pb::HealthCheckRequest {
        service: service.unwrap_or_default().to_string(),
    };

    match tokio::time::timeout(timeout, client.check(request)).await {
        Ok(Ok(resp)) => {
            let status = resp.into_inner().status;
            if status == tonic_health::pb::health_check_response::ServingStatus::Serving as i32 {
                ProbeResult::Success
            } else {
                ProbeResult::Failure(format!(
                    "gRPC health returned non-serving status {}",
                    status
                ))
            }
        }
        Ok(Err(e)) => ProbeResult::Failure(format!("gRPC health check failed: {}", e)),
        Err(_) => ProbeResult::Failure("gRPC health check timed out".to_string()),
    }
}

/// Unified probe runner: dispatches to the appropriate handler.
///
/// `pod_ip` is used as the default host for HttpGet and TcpSocket probes
/// when no explicit host is set.
pub async fn run_probe(
    handler: &ProbeHandler,
    runtime: Arc<dyn ContainerRuntime>,
    cid: &ContainerID,
    pod_ip: &str,
    timeout: Duration,
) -> ProbeResult {
    match handler {
        ProbeHandler::Exec { command } => {
            run_exec_probe(runtime.as_ref(), cid, command.clone(), timeout).await
        }
        ProbeHandler::HttpGet {
            path,
            port,
            host,
            scheme,
        } => {
            let h = host.as_deref().unwrap_or(pod_ip);
            run_http_probe(h, *port, path, scheme, timeout).await
        }
        ProbeHandler::TcpSocket { port, host } => {
            let h = host.as_deref().unwrap_or(pod_ip);
            run_tcp_probe(h, *port, timeout).await
        }
        ProbeHandler::Grpc { port, service } => {
            run_grpc_probe(pod_ip, *port, service.as_deref(), timeout).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::pod::{Probe, ProbeHandler};
    use tonic_health::server::health_reporter;

    fn make_probe(success_threshold: u32, failure_threshold: u32) -> Probe {
        Probe {
            handler: ProbeHandler::TcpSocket {
                port: 8080,
                host: None,
            },
            initial_delay_seconds: 0,
            period_seconds: 10,
            timeout_seconds: 1,
            success_threshold,
            failure_threshold,
        }
    }

    #[test]
    fn test_single_success_with_threshold_1() {
        let probe = make_probe(1, 3);
        let mut state = ProbeState::default();
        let decision = evaluate_probe_result(
            &ProbeResult::Success,
            &mut state,
            &probe,
            ProbeType::Liveness,
        );
        assert_eq!(decision, ProbeDecision::Pass);
        assert_eq!(state.consecutive_successes, 1);
    }

    #[test]
    fn test_success_pending_until_threshold() {
        let probe = make_probe(3, 3);
        let mut state = ProbeState::default();

        let d1 = evaluate_probe_result(
            &ProbeResult::Success,
            &mut state,
            &probe,
            ProbeType::Readiness,
        );
        assert_eq!(d1, ProbeDecision::Pending);

        let d2 = evaluate_probe_result(
            &ProbeResult::Success,
            &mut state,
            &probe,
            ProbeType::Readiness,
        );
        assert_eq!(d2, ProbeDecision::Pending);

        let d3 = evaluate_probe_result(
            &ProbeResult::Success,
            &mut state,
            &probe,
            ProbeType::Readiness,
        );
        assert_eq!(d3, ProbeDecision::Pass);
    }

    #[test]
    fn test_failure_pending_until_threshold() {
        let probe = make_probe(1, 3);
        let mut state = ProbeState::default();

        let d1 = evaluate_probe_result(
            &ProbeResult::Failure("err".to_string()),
            &mut state,
            &probe,
            ProbeType::Liveness,
        );
        assert_eq!(d1, ProbeDecision::Pending);

        let d2 = evaluate_probe_result(
            &ProbeResult::Failure("err".to_string()),
            &mut state,
            &probe,
            ProbeType::Liveness,
        );
        assert_eq!(d2, ProbeDecision::Pending);

        let d3 = evaluate_probe_result(
            &ProbeResult::Failure("err".to_string()),
            &mut state,
            &probe,
            ProbeType::Liveness,
        );
        assert_eq!(d3, ProbeDecision::Fail);
    }

    #[test]
    fn test_failure_resets_success_count() {
        let probe = make_probe(2, 1);
        let mut state = ProbeState::default();
        evaluate_probe_result(
            &ProbeResult::Success,
            &mut state,
            &probe,
            ProbeType::Startup,
        );
        assert_eq!(state.consecutive_successes, 1);
        evaluate_probe_result(
            &ProbeResult::Failure("x".to_string()),
            &mut state,
            &probe,
            ProbeType::Startup,
        );
        assert_eq!(state.consecutive_successes, 0);
    }

    #[tokio::test]
    async fn test_tcp_probe_refusal() {
        // Port 1 is almost certainly not open
        let result = run_tcp_probe("127.0.0.1", 1, Duration::from_millis(200)).await;
        assert!(matches!(result, ProbeResult::Failure(_)));
    }

    #[tokio::test]
    async fn test_grpc_probe_success() {
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

        let result = run_grpc_probe("127.0.0.1", port, None, Duration::from_secs(1)).await;
        assert_eq!(result, ProbeResult::Success);
        server.abort();
    }

    #[tokio::test]
    async fn test_grpc_probe_connection_refused() {
        let result = run_grpc_probe("127.0.0.1", 19998, None, Duration::from_millis(300)).await;
        assert!(matches!(result, ProbeResult::Failure(_)));
    }
}
