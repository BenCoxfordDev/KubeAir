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

//! Kubernetes API client adapter.
//!
//! Implements NodeReporter port using HTTP to the Kubernetes API server.
//! In a full implementation this would use kube-rs; here we provide a
//! standalone mock-capable implementation suitable for testing.

use async_trait::async_trait;
use kubelet_core::error::{KubeletError, Result};
use kubelet_core::node::{NodeCondition, NodeStatus};
use kubelet_core::pod::lifecycle::PodLifecycleState;
use kubelet_core::types::{PodRef, PodUID};
use kubelet_ports::driven::node_reporter::NodeReporter;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

type PodStatusReport = (PodRef, PodUID, PodLifecycleState);
type LeaseRenewal = (String, u32);
type ConditionPatch = (String, Vec<NodeCondition>);

/// Tracks reported statuses in memory (for testing / dry-run mode).
#[derive(Default)]
pub struct InMemoryNodeReporter {
    pub node_status_reports: Arc<Mutex<Vec<NodeStatus>>>,
    pub pod_status_reports: Arc<Mutex<Vec<PodStatusReport>>>,
    pub lease_renewals: Arc<Mutex<Vec<LeaseRenewal>>>,
    pub condition_patches: Arc<Mutex<Vec<ConditionPatch>>>,
}

impl InMemoryNodeReporter {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn node_report_count(&self) -> usize {
        self.node_status_reports.lock().await.len()
    }

    pub async fn pod_report_count(&self) -> usize {
        self.pod_status_reports.lock().await.len()
    }

    pub async fn lease_renewal_count(&self) -> usize {
        self.lease_renewals.lock().await.len()
    }
}

#[async_trait]
impl NodeReporter for InMemoryNodeReporter {
    async fn report_node_status(&self, status: &NodeStatus) -> Result<()> {
        info!(node = %status.name, "Reporting node status");
        self.node_status_reports.lock().await.push(status.clone());
        Ok(())
    }

    async fn report_pod_status(
        &self,
        pod_ref: &PodRef,
        uid: &PodUID,
        state: &PodLifecycleState,
    ) -> Result<()> {
        debug!(pod = %pod_ref, uid = %uid, phase = %state.phase, "Reporting pod status");
        self.pod_status_reports
            .lock()
            .await
            .push((pod_ref.clone(), uid.clone(), state.clone()));
        Ok(())
    }

    async fn delete_pod(&self, pod_ref: &PodRef, _uid: &PodUID) -> Result<()> {
        debug!(pod = %pod_ref, "InMemoryNodeReporter: skip pod delete");
        Ok(())
    }

    async fn patch_node_conditions(
        &self,
        node_name: &str,
        conditions: &[NodeCondition],
    ) -> Result<()> {
        debug!(
            node_name,
            conditions = conditions.len(),
            "Patching node conditions"
        );
        self.condition_patches
            .lock()
            .await
            .push((node_name.to_string(), conditions.to_vec()));
        Ok(())
    }

    async fn renew_node_lease(&self, node_name: &str, duration_seconds: u32) -> Result<()> {
        debug!(node_name, duration_seconds, "Renewing node lease");
        self.lease_renewals
            .lock()
            .await
            .push((node_name.to_string(), duration_seconds));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::node::NodeStatus;
    use kubelet_core::pod::lifecycle::{PodLifecycleState, PodPhase};
    use kubelet_core::types::{PodRef, PodUID};

    #[tokio::test]
    async fn test_report_node_status() {
        let reporter = InMemoryNodeReporter::new();
        let status = NodeStatus::new("node1");
        reporter.report_node_status(&status).await.unwrap();
        assert_eq!(reporter.node_report_count().await, 1);
    }

    #[tokio::test]
    async fn test_report_pod_status() {
        let reporter = InMemoryNodeReporter::new();
        let pod_ref = PodRef::new("default", "my-pod");
        let uid = PodUID::new("uid-123");
        let state = PodLifecycleState::default();
        reporter
            .report_pod_status(&pod_ref, &uid, &state)
            .await
            .unwrap();
        assert_eq!(reporter.pod_report_count().await, 1);
    }

    #[tokio::test]
    async fn test_renew_node_lease() {
        let reporter = InMemoryNodeReporter::new();
        reporter.renew_node_lease("node1", 40).await.unwrap();
        reporter.renew_node_lease("node1", 40).await.unwrap();
        assert_eq!(reporter.lease_renewal_count().await, 2);
    }

    #[tokio::test]
    async fn test_patch_node_conditions() {
        let reporter = InMemoryNodeReporter::new();
        use chrono::Utc;
        use kubelet_core::node::{NodeCondition, NodeConditionStatus, NodeConditionType};
        let cond = NodeCondition {
            condition_type: NodeConditionType::Ready,
            status: NodeConditionStatus::True,
            last_heartbeat_time: Utc::now(),
            last_transition_time: Utc::now(),
            reason: "KubeletReady".to_string(),
            message: "ready".to_string(),
        };
        reporter
            .patch_node_conditions("node1", &[cond])
            .await
            .unwrap();
        let patches = reporter.condition_patches.lock().await;
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0].0, "node1");
    }
}
