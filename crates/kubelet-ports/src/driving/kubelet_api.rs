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

//! Kubelet API driving port.
//!
//! Exposes the kubelet's HTTP API surface (used by kubectl exec/logs/port-forward,
//! and also by the API server for health checks).

use async_trait::async_trait;
use kubelet_core::error::Result;

/// Request to exec into a running container.
#[derive(Debug, Clone)]
pub struct ExecRequest {
    pub namespace: String,
    pub pod_name: String,
    pub container_name: String,
    pub command: Vec<String>,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
}

/// Request to stream container logs.
#[derive(Debug, Clone)]
pub struct LogRequest {
    pub namespace: String,
    pub pod_name: String,
    pub container_name: String,
    pub follow: bool,
    pub tail_lines: Option<u64>,
    pub since_seconds: Option<u64>,
    pub timestamps: bool,
}

/// Request to port-forward into a pod.
#[derive(Debug, Clone)]
pub struct PortForwardRequest {
    pub namespace: String,
    pub pod_name: String,
    pub port: u16,
}

/// Kubelet API driving port - inbound interface for kubectl and API server.
#[async_trait]
pub trait KubeletApi: Send + Sync {
    /// Execute a command in a container (streaming websocket or SPDY).
    async fn exec(&self, request: ExecRequest) -> Result<Vec<u8>>;

    /// Stream or retrieve container logs.
    async fn logs(&self, request: LogRequest) -> Result<Vec<u8>>;

    /// Forward a port to a pod.
    async fn port_forward(&self, request: PortForwardRequest) -> Result<()>;

    /// Get pod stats (for metrics-server).
    async fn pod_stats(&self, namespace: &str, pod_name: &str) -> Result<serde_json::Value>;

    /// Healthz endpoint.
    async fn healthz(&self) -> Result<String>;

    /// Get node summary stats.
    async fn summary_stats(&self) -> Result<serde_json::Value>;
}
