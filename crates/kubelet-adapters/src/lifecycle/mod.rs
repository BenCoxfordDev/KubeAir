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

//! Pod lifecycle hook executor -- PostStart and PreStop.
//!
//! Mirrors pkg/kubelet/lifecycle/handlers.go.
//!
//! PostStart: executed immediately after a container starts.
//!   - If it fails, the container is killed and restarted.
//!   - kubelet does NOT wait for PostStart before starting next container.
//!   - But container stays in Waiting until PostStart completes.
//!
//! PreStop: executed before the container is sent SIGTERM.
//!   - Kubelet waits for PreStop to complete (up to terminationGracePeriodSeconds).
//!   - If PreStop fails, the container is still killed.

use kubelet_core::container::ContainerID;
use kubelet_core::error::{KubeletError, Result};
use kubelet_core::pod::LifecycleHandler;
use kubelet_ports::driven::container_runtime::{ContainerRuntime, ExecResult};
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Executes a lifecycle hook for a container.
pub struct LifecycleHookExecutor {
    timeout: Duration,
}

impl LifecycleHookExecutor {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    /// Execute a lifecycle handler, returning Ok(()) on success.
    pub async fn execute(
        &self,
        handler: &LifecycleHandler,
        container_id: &ContainerID,
        container_name: &str,
        runtime: &dyn ContainerRuntime,
    ) -> Result<()> {
        match handler {
            LifecycleHandler::Exec { command } => {
                self.exec_hook(command, container_id, container_name, runtime)
                    .await
            }
            LifecycleHandler::HttpGet {
                path,
                port,
                host,
                scheme,
            } => {
                self.http_hook(path, *port, host.as_deref(), scheme, container_name)
                    .await
            }
            LifecycleHandler::TcpSocket { port, host } => {
                self.tcp_hook(*port, host.as_deref(), container_name).await
            }
            LifecycleHandler::Sleep { seconds } => self.sleep_hook(*seconds, container_name).await,
        }
    }

    async fn exec_hook(
        &self,
        command: &[String],
        container_id: &ContainerID,
        container_name: &str,
        runtime: &dyn ContainerRuntime,
    ) -> Result<()> {
        debug!(container = %container_name, cmd = ?command, "Executing lifecycle exec hook");
        let result = runtime
            .exec_sync(container_id, command.to_vec(), self.timeout.as_secs())
            .await?;

        if result.exit_code != 0 {
            let stderr = String::from_utf8_lossy(&result.stderr);
            return Err(KubeletError::Lifecycle(format!(
                "lifecycle exec hook failed with exit code {}: {}",
                result.exit_code,
                stderr.trim()
            )));
        }

        debug!(container = %container_name, "Lifecycle exec hook succeeded");
        Ok(())
    }

    async fn http_hook(
        &self,
        path: &str,
        port: u16,
        host: Option<&str>,
        scheme: &str,
        container_name: &str,
    ) -> Result<()> {
        let host = host.unwrap_or("localhost");
        let url = format!("{}://{}:{}{}", scheme.to_lowercase(), host, port, path);
        debug!(container = %container_name, url = %url, "Executing lifecycle HTTP hook");

        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()
            .map_err(|e| KubeletError::Lifecycle(format!("HTTP client build: {}", e)))?;

        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(|e| KubeletError::Lifecycle(format!("HTTP hook GET failed: {}", e)))?;

        if !resp.status().is_success() && resp.status().as_u16() >= 400 {
            return Err(KubeletError::Lifecycle(format!(
                "HTTP hook returned error status: {}",
                resp.status()
            )));
        }

        debug!(container = %container_name, "Lifecycle HTTP hook succeeded");
        Ok(())
    }

    async fn tcp_hook(&self, port: u16, host: Option<&str>, container_name: &str) -> Result<()> {
        let host = host.unwrap_or("localhost");
        debug!(container = %container_name, host, port, "Executing lifecycle TCP hook");

        tokio::time::timeout(
            self.timeout,
            tokio::net::TcpStream::connect(format!("{}:{}", host, port)),
        )
        .await
        .map_err(|_| {
            KubeletError::Lifecycle(format!(
                "TCP hook timed out connecting to {}:{}",
                host, port
            ))
        })?
        .map_err(|e| KubeletError::Lifecycle(format!("TCP hook connect failed: {}", e)))?;

        debug!(container = %container_name, "Lifecycle TCP hook succeeded");
        Ok(())
    }

    async fn sleep_hook(&self, seconds: u64, container_name: &str) -> Result<()> {
        debug!(container = %container_name, seconds, "Executing lifecycle Sleep hook");
        tokio::time::timeout(
            self.timeout,
            tokio::time::sleep(Duration::from_secs(seconds)),
        )
        .await
        .map_err(|_| {
            KubeletError::Lifecycle(format!(
                "Sleep hook timed out after {}s (grace period exceeded)",
                self.timeout.as_secs()
            ))
        })?;
        debug!(container = %container_name, "Lifecycle Sleep hook completed");
        Ok(())
    }
}

/// Run the PostStart hook after a container starts.
/// Returns Err if the hook fails (caller should kill the container).
pub async fn run_post_start(
    handler: &LifecycleHandler,
    container_id: &ContainerID,
    container_name: &str,
    runtime: &dyn ContainerRuntime,
) -> Result<()> {
    info!(container = %container_name, "Running PostStart lifecycle hook");
    let executor = LifecycleHookExecutor::new(Duration::from_secs(30));
    executor
        .execute(handler, container_id, container_name, runtime)
        .await
}

/// Run the PreStop hook before sending SIGTERM.
/// Always returns Ok -- failures are logged but don't block termination.
pub async fn run_pre_stop(
    handler: &LifecycleHandler,
    container_id: &ContainerID,
    container_name: &str,
    runtime: &dyn ContainerRuntime,
    grace_period: Duration,
) {
    info!(container = %container_name, "Running PreStop lifecycle hook");
    let executor = LifecycleHookExecutor::new(grace_period);
    if let Err(e) = executor
        .execute(handler, container_id, container_name, runtime)
        .await
    {
        warn!(container = %container_name, error = %e, "PreStop hook failed (continuing with termination)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_runtime::MockRuntime;
    use kubelet_core::container::ContainerID;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_exec_hook_success() {
        let runtime = MockRuntime::new();
        let cid = ContainerID("test-container-123".to_string());
        let handler = LifecycleHandler::Exec {
            command: vec!["echo".to_string(), "hello".to_string()],
        };
        // MockRuntime exec_sync returns exit_code 0 by default.
        let executor = LifecycleHookExecutor::new(Duration::from_secs(5));
        let result = executor
            .execute(&handler, &cid, "my-container", &runtime)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_tcp_hook_failure_on_no_listener() {
        let handler = LifecycleHandler::TcpSocket {
            port: 19999,
            host: None,
        };
        let runtime = MockRuntime::new();
        let cid = ContainerID("fake".to_string());
        let executor = LifecycleHookExecutor::new(Duration::from_millis(100));
        // No server listening -- should fail quickly.
        let result = executor.execute(&handler, &cid, "app", &runtime).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pre_stop_never_panics_on_error() {
        let runtime = MockRuntime::new();
        let cid = ContainerID("fake".to_string());
        let handler = LifecycleHandler::TcpSocket {
            port: 19998,
            host: None,
        };
        // PreStop should always complete without panic, even on failure.
        run_pre_stop(&handler, &cid, "app", &runtime, Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_sleep_hook_completes_within_duration() {
        let runtime = MockRuntime::new();
        let cid = ContainerID("fake".to_string());
        let handler = LifecycleHandler::Sleep { seconds: 0 };
        let executor = LifecycleHookExecutor::new(Duration::from_secs(5));
        // Sleep for 0 seconds should succeed immediately.
        let result = executor.execute(&handler, &cid, "app", &runtime).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sleep_hook_times_out_when_exceeds_grace_period() {
        let runtime = MockRuntime::new();
        let cid = ContainerID("fake".to_string());
        // Request 60s sleep but only give 50ms grace period.
        let handler = LifecycleHandler::Sleep { seconds: 60 };
        let executor = LifecycleHookExecutor::new(Duration::from_millis(50));
        let result = executor.execute(&handler, &cid, "app", &runtime).await;
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("timed out") || msg.contains("grace period"),
            "unexpected error: {msg}"
        );
    }

    #[tokio::test]
    async fn test_sleep_hook_as_pre_stop_completes_gracefully() {
        let runtime = MockRuntime::new();
        let cid = ContainerID("fake".to_string());
        let handler = LifecycleHandler::Sleep { seconds: 0 };
        // PreStop with a 0-second sleep should succeed.
        run_pre_stop(&handler, &cid, "app", &runtime, Duration::from_secs(5)).await;
    }
}
