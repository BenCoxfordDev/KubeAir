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

//! Node reporter port - interface for reporting node and pod status to the API server.

use async_trait::async_trait;
use kubelet_core::error::Result;
use kubelet_core::node::NodeStatus;
use kubelet_core::pod::lifecycle::PodLifecycleState;
use kubelet_core::types::{PodRef, PodUID};

/// Describes a Kubernetes container lifecycle event to be emitted to the API server.
///
/// Groups the three string fields that together describe an event, reducing the
/// argument count of [`NodeReporter::emit_container_event`].
pub struct ContainerEvent<'a> {
    /// `"Normal"` or `"Warning"`.
    pub event_type: &'a str,
    /// Short machine-readable reason, e.g. `"Started"`, `"Killing"`, `"Failed"`.
    pub reason: &'a str,
    /// Human-readable detail shown in `kubectl describe pod`.
    pub message: &'a str,
}

/// Port for reporting node and pod status to the Kubernetes API server.
#[async_trait]
pub trait NodeReporter: Send + Sync {
    /// Report updated node status to the API server.
    async fn report_node_status(&self, status: &NodeStatus) -> Result<()>;

    /// Report pod status for a single pod.
    async fn report_pod_status(
        &self,
        pod_ref: &PodRef,
        uid: &PodUID,
        state: &PodLifecycleState,
    ) -> Result<()>;

    /// Force-delete a pod from the API server after it has been terminated.
    /// This is called after PreStop hooks have run and containers have been stopped.
    async fn delete_pod(&self, pod_ref: &PodRef, uid: &PodUID) -> Result<()>;

    /// Patch node conditions only (lighter than full node status update).
    async fn patch_node_conditions(
        &self,
        node_name: &str,
        conditions: &[kubelet_core::node::NodeCondition],
    ) -> Result<()>;

    /// Renew the node lease (heartbeat to the API server).
    async fn renew_node_lease(&self, node_name: &str, duration_seconds: u32) -> Result<()>;

    /// Emit a Kubernetes Event for a container lifecycle transition.
    async fn emit_container_event(
        &self,
        pod_ref: &PodRef,
        uid: &PodUID,
        container_name: &str,
        event: ContainerEvent<'_>,
    ) -> Result<()> {
        // Default: no-op (standalone / logging modes)
        let _ = (pod_ref, uid, container_name, event);
        Ok(())
    }
}
