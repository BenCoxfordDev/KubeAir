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

//! Node lease controller adapter.
//!
//! Periodically renews the node lease with the Kubernetes API server.
//! The renewal interval is `lease_duration_seconds / 4` (default 10s for 40s lease).
//!
//! On first startup, acquires a new lease. On renewal failure, retries with
//! exponential backoff up to `max_retries` times before marking the node
//! as potentially partitioned.

use kubelet_core::error::{KubeletError, Result};
use kubelet_core::lease::NodeLease;
use kubelet_ports::driven::node_reporter::NodeReporter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};
use tracing::{debug, error, info, warn};

/// State of the lease controller.
#[derive(Debug, Clone, PartialEq)]
pub enum LeaseState {
    /// Lease is valid and up-to-date.
    Active,
    /// Lease renewal is in progress.
    Renewing,
    /// Lease renewal failed; retrying.
    Degraded { consecutive_failures: u32 },
    /// Lease has been permanently lost (too many failures).
    Lost,
}

/// The lease controller manages acquiring and renewing the node lease.
pub struct LeaseController {
    node_name: String,
    duration_seconds: u32,
    max_retries: u32,
    reporter: Arc<dyn NodeReporter>,
    lease: Arc<Mutex<NodeLease>>,
    state_tx: watch::Sender<LeaseState>,
    pub state_rx: watch::Receiver<LeaseState>,
}

impl LeaseController {
    pub fn new(
        node_name: impl Into<String>,
        duration_seconds: u32,
        max_retries: u32,
        reporter: Arc<dyn NodeReporter>,
    ) -> Self {
        let node_name = node_name.into();
        let lease = NodeLease::new(&node_name, duration_seconds);
        let (state_tx, state_rx) = watch::channel(LeaseState::Active);
        Self {
            node_name,
            duration_seconds,
            max_retries,
            reporter,
            lease: Arc::new(Mutex::new(lease)),
            state_tx,
            state_rx,
        }
    }

    /// Run the lease renewal loop. Runs until the channel is dropped.
    pub async fn run(&self) -> Result<()> {
        info!(
            node = %self.node_name,
            duration_secs = self.duration_seconds,
            "Node lease controller started"
        );

        // Initial acquisition
        self.acquire_or_renew().await;

        loop {
            let wait = {
                let lease = self.lease.lock().await;
                let d = lease.time_until_renewal();
                std::cmp::max(
                    Duration::from_millis(100),
                    Duration::from_secs(d.num_seconds().max(0) as u64),
                )
            };

            tokio::time::sleep(wait).await;
            self.acquire_or_renew().await;
        }
    }

    pub async fn acquire_or_renew(&self) {
        let _ = self.state_tx.send(LeaseState::Renewing);
        debug!(node = %self.node_name, "Renewing node lease");

        match self
            .reporter
            .renew_node_lease(&self.node_name, self.duration_seconds)
            .await
        {
            Ok(()) => {
                let mut lease = self.lease.lock().await;
                lease.renew();
                let _ = self.state_tx.send(LeaseState::Active);
                debug!(node = %self.node_name, "Node lease renewed");
            }
            Err(e) => {
                let current_state = self.state_tx.borrow().clone();
                let failures = match &current_state {
                    LeaseState::Degraded {
                        consecutive_failures,
                    } => consecutive_failures + 1,
                    _ => 1,
                };

                warn!(
                    node = %self.node_name,
                    error = %e,
                    consecutive_failures = failures,
                    "Node lease renewal failed"
                );

                if failures >= self.max_retries {
                    error!(
                        node = %self.node_name,
                        "Node lease permanently lost after {} failures",
                        failures
                    );
                    let _ = self.state_tx.send(LeaseState::Lost);
                } else {
                    let _ = self.state_tx.send(LeaseState::Degraded {
                        consecutive_failures: failures,
                    });
                }
            }
        }
    }

    /// Get the current lease state.
    pub fn current_state(&self) -> LeaseState {
        self.state_tx.borrow().clone()
    }

    /// Get a snapshot of the current lease.
    pub async fn current_lease(&self) -> NodeLease {
        self.lease.lock().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube_client::InMemoryNodeReporter;
    use std::sync::Arc;
    use std::time::Duration;

    fn make_controller(reporter: Arc<InMemoryNodeReporter>, max_retries: u32) -> LeaseController {
        LeaseController::new("test-node", 40, max_retries, reporter)
    }

    #[tokio::test]
    async fn test_initial_state_is_active() {
        let reporter = Arc::new(InMemoryNodeReporter::new());
        let controller = make_controller(reporter, 3);
        assert_eq!(controller.current_state(), LeaseState::Active);
    }

    #[tokio::test]
    async fn test_successful_renewal_stays_active() {
        let reporter = Arc::new(InMemoryNodeReporter::new());
        let controller = make_controller(reporter.clone(), 3);
        controller.acquire_or_renew().await;
        assert_eq!(controller.current_state(), LeaseState::Active);
        assert_eq!(reporter.lease_renewal_count().await, 1);
    }

    #[tokio::test]
    async fn test_lease_renewed_updates_renew_time() {
        let reporter = Arc::new(InMemoryNodeReporter::new());
        let controller = make_controller(reporter, 40);
        let before = controller.current_lease().await.renew_time;
        tokio::time::sleep(Duration::from_millis(10)).await;
        controller.acquire_or_renew().await;
        let after = controller.current_lease().await.renew_time;
        assert!(after > before, "renew_time should advance after renewal");
    }

    #[tokio::test]
    async fn test_multiple_renewals_accumulate() {
        let reporter = Arc::new(InMemoryNodeReporter::new());
        let controller = make_controller(reporter.clone(), 5);
        for _ in 0..5 {
            controller.acquire_or_renew().await;
        }
        assert_eq!(reporter.lease_renewal_count().await, 5);
        assert_eq!(controller.current_state(), LeaseState::Active);
    }

    #[tokio::test]
    async fn test_lease_not_expired_after_renewal() {
        let reporter = Arc::new(InMemoryNodeReporter::new());
        let controller = make_controller(reporter, 40);
        controller.acquire_or_renew().await;
        let lease = controller.current_lease().await;
        assert!(!lease.is_expired());
    }
}
