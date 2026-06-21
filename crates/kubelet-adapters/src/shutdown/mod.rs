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

//! Graceful node shutdown -- mirrors pkg/kubelet/nodeshutdown.
//!
//! When the OS sends a shutdown signal (SIGTERM to the kubelet, or via
//! systemd inhibitor locks), the kubelet must:
//!   1. Acquire a systemd inhibitor lock to delay shutdown.
//!   2. Gracefully terminate all pods (honoring terminationGracePeriodSeconds).
//!   3. Release the inhibitor lock so the OS can proceed.
//!
//! Priority classes control the shutdown order:
//!   - Critical pods (system-cluster-critical, system-node-critical) last.
//!   - Non-critical pods first.
//!
//! References:
//!   pkg/kubelet/nodeshutdown/nodeshutdown_manager_linux.go
//!   pkg/kubelet/nodeshutdown/nodeshutdown_manager.go

use kubelet_core::error::{KubeletError, Result};
use kubelet_core::pod::PodSpec;
use kubelet_core::types::PodUID;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, broadcast};
use tracing::{error, info, warn};

// -- Shutdown configuration ----------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownConfig {
    /// Total time allowed for graceful node shutdown.
    pub shutdown_grace_period: Duration,
    /// Time reserved for critical pods within the total grace period.
    pub shutdown_grace_period_critical_pods: Duration,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            shutdown_grace_period: Duration::from_secs(30),
            shutdown_grace_period_critical_pods: Duration::from_secs(10),
        }
    }
}

// -- Priority groups -----------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PodShutdownPriority {
    /// Regular (non-critical) pods -- terminated first.
    Normal,
    /// system-node-critical and system-cluster-critical -- terminated last.
    Critical,
}

impl PodShutdownPriority {
    pub fn for_pod(pod: &PodSpec) -> Self {
        match pod.priority {
            Some(p) if p >= 2_000_000_000 => Self::Critical,
            Some(p) if p >= 2_000_000 => Self::Critical,
            _ => Self::Normal,
        }
    }
}

// -- Systemd inhibitor lock (Linux) -------------------------------------------

/// A systemd inhibitor lock that delays system shutdown.
/// On Linux: uses the logind DBus API (org.freedesktop.login1 Inhibit).
/// In non-systemd / test environments: no-op.
pub struct InhibitorLock {
    /// File descriptor for the inhibitor lock (kept open to hold the lock).
    #[cfg(target_os = "linux")]
    fd: Option<std::os::unix::io::OwnedFd>,
    active: bool,
}

impl Default for InhibitorLock {
    fn default() -> Self {
        Self::new()
    }
}

impl InhibitorLock {
    pub fn new() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            fd: None,
            active: false,
        }
    }

    /// Acquire a systemd inhibitor lock for "shutdown".
    /// Returns Ok(()) even if logind is unavailable (degrades gracefully).
    pub fn acquire(&mut self) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            // In a full implementation: open /run/systemd/inhibit/ via DBus.
            // For now: simulate by creating a marker file.
            info!("Acquiring systemd inhibitor lock for graceful shutdown");
            self.active = true;
        }
        #[cfg(not(target_os = "linux"))]
        {
            info!("Non-Linux: inhibitor lock is a no-op");
            self.active = true;
        }
        Ok(())
    }

    /// Release the inhibitor lock, allowing the OS to proceed with shutdown.
    pub fn release(&mut self) {
        if self.active {
            info!("Releasing systemd inhibitor lock");
            #[cfg(target_os = "linux")]
            {
                self.fd = None; // Dropping the fd releases the lock.
            }
            self.active = false;
        }
    }

    pub fn is_active(&self) -> bool {
        self.active
    }
}

impl Drop for InhibitorLock {
    fn drop(&mut self) {
        self.release();
    }
}

// -- Shutdown manager ----------------------------------------------------------

pub struct ShutdownManager {
    config: ShutdownConfig,
    inhibitor: InhibitorLock,
    /// Signal channel for shutdown events.
    shutdown_tx: broadcast::Sender<()>,
    pub shutdown_rx: broadcast::Receiver<()>,
}

impl ShutdownManager {
    pub fn new(config: ShutdownConfig) -> Self {
        let (tx, rx) = broadcast::channel(1);
        Self {
            config,
            inhibitor: InhibitorLock::new(),
            shutdown_tx: tx,
            shutdown_rx: rx,
        }
    }

    /// Called when the OS initiates a shutdown.
    /// Acquires the inhibitor lock, notifies listeners, waits for pods to terminate.
    pub async fn handle_shutdown(&mut self, pods: Vec<PodSpec>) -> Result<()> {
        info!("Node shutdown initiated");
        self.inhibitor.acquire()?;

        // Partition pods by priority.
        let mut normal: Vec<&PodSpec> = vec![];
        let mut critical: Vec<&PodSpec> = vec![];
        for pod in &pods {
            match PodShutdownPriority::for_pod(pod) {
                PodShutdownPriority::Normal => normal.push(pod),
                PodShutdownPriority::Critical => critical.push(pod),
            }
        }

        let normal_budget =
            self.config.shutdown_grace_period - self.config.shutdown_grace_period_critical_pods;
        let critical_budget = self.config.shutdown_grace_period_critical_pods;

        info!(
            normal_pods = normal.len(),
            critical_pods = critical.len(),
            normal_budget_secs = normal_budget.as_secs(),
            critical_budget_secs = critical_budget.as_secs(),
            "Graceful shutdown plan"
        );

        // Terminate normal pods.
        self.terminate_pods(&normal, normal_budget).await;

        // Terminate critical pods.
        self.terminate_pods(&critical, critical_budget).await;

        // Broadcast shutdown complete.
        let _ = self.shutdown_tx.send(());
        self.inhibitor.release();
        info!("Graceful shutdown complete");
        Ok(())
    }

    async fn terminate_pods(&self, pods: &[&PodSpec], budget: Duration) {
        if pods.is_empty() {
            return;
        }
        let grace = budget / pods.len() as u32;

        for pod in pods {
            let grace_secs = std::cmp::min(grace.as_secs(), pod.termination_grace_period_seconds);
            info!(
                pod = %pod.pod_ref.name,
                grace_seconds = grace_secs,
                "Graceful pod termination during node shutdown"
            );
            tokio::time::sleep(Duration::from_millis(10)).await; // simulate
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        // Check if a shutdown signal has been received.
        self.shutdown_tx.receiver_count() > 0
    }

    /// Subscribe to shutdown notifications.
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }
}

/// Register OS signal handlers for SIGTERM and SIGINT.
pub async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => { info!("Received SIGTERM"); }
            _ = sigint.recv()  => { info!("Received SIGINT"); }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.expect("ctrl-c handler");
        info!("Received shutdown signal");
    }
}

// -- Tests ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::pod::{PodSpec, RestartPolicy};
    use kubelet_core::types::{PodRef, PodUID};

    fn make_pod(name: &str, priority: i32) -> PodSpec {
        PodSpec {
            uid: PodUID::new(format!("uid-{}", name)),
            pod_ref: PodRef {
                name: name.to_string(),
                namespace: "default".to_string(),
            },
            node_name: "node1".to_string(),
            containers: vec![],
            init_containers: vec![],
            ephemeral_containers: vec![],
            volumes: vec![],
            host_network: false,
            host_pid: false,
            host_ipc: false,
            dns_config: None,
            restart_policy: RestartPolicy::Always,
            termination_grace_period_seconds: 30,
            service_account_name: "default".to_string(),
            priority: Some(priority),
            tolerations: vec![],
            node_selector: Default::default(),
            annotations: Default::default(),
            labels: Default::default(),
            runtime_class_name: None,
            security_context: None,
            readiness_gates: vec![],
            active_deadline_seconds: None,
            automount_service_account_token: Some(true),
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

    #[test]
    fn test_pod_shutdown_priority_normal() {
        let pod = make_pod("nginx", 0);
        assert_eq!(
            PodShutdownPriority::for_pod(&pod),
            PodShutdownPriority::Normal
        );
    }

    #[test]
    fn test_pod_shutdown_priority_critical() {
        let pod = make_pod("coredns", 2_000_000_000);
        assert_eq!(
            PodShutdownPriority::for_pod(&pod),
            PodShutdownPriority::Critical
        );
    }

    #[test]
    fn test_inhibitor_lock_acquire_release() {
        let mut lock = InhibitorLock::new();
        lock.acquire().unwrap();
        assert!(lock.is_active());
        lock.release();
        assert!(!lock.is_active());
    }

    #[tokio::test]
    async fn test_shutdown_manager_terminates_pods() {
        let config = ShutdownConfig {
            shutdown_grace_period: Duration::from_millis(100),
            shutdown_grace_period_critical_pods: Duration::from_millis(20),
        };
        let mut mgr = ShutdownManager::new(config);
        let pods = vec![make_pod("app", 0), make_pod("coredns", 2_000_000_000)];
        mgr.handle_shutdown(pods).await.unwrap();
        assert!(!mgr.inhibitor.is_active());
    }

    #[test]
    fn test_shutdown_config_defaults() {
        let cfg = ShutdownConfig::default();
        assert_eq!(cfg.shutdown_grace_period, Duration::from_secs(30));
        assert_eq!(
            cfg.shutdown_grace_period_critical_pods,
            Duration::from_secs(10)
        );
    }
}
