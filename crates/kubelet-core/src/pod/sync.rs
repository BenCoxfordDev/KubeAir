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

//! Pod sync loop - orchestrates desired vs actual state reconciliation.
//!
//! Mirrors the Go kubelet's `syncPod` / `syncPodFn` pattern.

use crate::error::Result;
use crate::pod::{PodOperation, PodSpec, PodUpdate};
use tracing::{debug, error, info, warn};

/// Outcome of a single sync iteration for a pod.
#[derive(Debug, Clone, PartialEq)]
pub enum SyncOutcome {
    /// Pod is fully reconciled - no action needed.
    InSync,
    /// Pod was started or containers were created.
    Started,
    /// Pod was updated (config change applied).
    Updated,
    /// Pod teardown was initiated.
    Terminating,
    /// Pod was fully removed.
    Removed,
    /// Sync encountered a transient error; will retry.
    Error(String),
}

/// Determines what sync action is needed for a pod update.
pub fn determine_sync_action(update: &PodUpdate) -> SyncAction {
    match update.op {
        PodOperation::Add => SyncAction::Create,
        PodOperation::Update => SyncAction::Update,
        PodOperation::Remove => SyncAction::Delete,
        PodOperation::Reconcile => SyncAction::Reconcile,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SyncAction {
    Create,
    Update,
    Delete,
    Reconcile,
}

/// Validates a pod spec for sanity before syncing.
pub fn validate_pod(pod: &PodSpec) -> Result<()> {
    if pod.pod_ref.name.is_empty() {
        return Err(crate::error::KubeletError::Config(
            "pod name cannot be empty".to_string(),
        ));
    }
    if pod.pod_ref.namespace.is_empty() {
        return Err(crate::error::KubeletError::Config(
            "pod namespace cannot be empty".to_string(),
        ));
    }
    if pod.uid.0.is_empty() {
        return Err(crate::error::KubeletError::Config(
            "pod UID cannot be empty".to_string(),
        ));
    }
    for c in &pod.containers {
        if c.name.is_empty() {
            return Err(crate::error::KubeletError::Config(
                "container name cannot be empty".to_string(),
            ));
        }
        if c.image.is_empty() {
            return Err(crate::error::KubeletError::Config(format!(
                "container '{}' has no image",
                c.name
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pod::{
        ContainerSpec, ImagePullPolicy, PodOperation, PodUpdate, ResourceRequirements,
        RestartPolicy,
    };
    use crate::types::{PodRef, PodUID};

    fn valid_pod() -> PodSpec {
        PodSpec {
            uid: PodUID::new("test-uid"),
            pod_ref: PodRef::new("default", "my-pod"),
            containers: vec![ContainerSpec {
                name: "main".to_string(),
                image: "nginx:1.25".to_string(),
                command: vec![],
                args: vec![],
                working_dir: None,
                ports: vec![],
                env: vec![],
                resources: ResourceRequirements::default(),
                volume_mounts: vec![],
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                image_pull_policy: ImagePullPolicy::IfNotPresent,
                security_context: None,
                termination_message_path: None,
                termination_message_policy: None,
                lifecycle: None,
                env_from: vec![],
                stdin: None,
                stdin_once: None,
                tty: None,
                restart_policy: None,
            }],
            init_containers: vec![],
            ephemeral_containers: vec![],
            volumes: vec![],
            node_name: "node1".to_string(),
            host_network: false,
            host_pid: false,
            host_ipc: false,
            dns_config: None,
            restart_policy: RestartPolicy::Always,
            termination_grace_period_seconds: 30,
            service_account_name: "default".to_string(),
            priority: None,
            tolerations: vec![],
            node_selector: Default::default(),
            annotations: Default::default(),
            labels: Default::default(),
            runtime_class_name: None,
            security_context: None,
            readiness_gates: vec![],
            active_deadline_seconds: None,
            automount_service_account_token: None,
            image_pull_secrets: vec![],
            enable_service_links: None,
            share_process_namespace: None,
            resource_claims: vec![],
            host_aliases: vec![],
            hostname: None,
            subdomain: None,
            observed_start_time: None,
            generation: None,
        }
    }

    #[test]
    fn test_validate_valid_pod() {
        assert!(validate_pod(&valid_pod()).is_ok());
    }

    #[test]
    fn test_validate_empty_name_fails() {
        let mut pod = valid_pod();
        pod.pod_ref.name = String::new();
        assert!(validate_pod(&pod).is_err());
    }

    #[test]
    fn test_validate_empty_namespace_fails() {
        let mut pod = valid_pod();
        pod.pod_ref.namespace = String::new();
        assert!(validate_pod(&pod).is_err());
    }

    #[test]
    fn test_validate_empty_uid_fails() {
        let mut pod = valid_pod();
        pod.uid = PodUID::new("");
        assert!(validate_pod(&pod).is_err());
    }

    #[test]
    fn test_validate_empty_container_image_fails() {
        let mut pod = valid_pod();
        pod.containers[0].image = String::new();
        assert!(validate_pod(&pod).is_err());
    }

    #[test]
    fn test_determine_sync_action_add() {
        let update = PodUpdate {
            pod: valid_pod(),
            op: PodOperation::Add,
        };
        assert_eq!(determine_sync_action(&update), SyncAction::Create);
    }

    #[test]
    fn test_determine_sync_action_remove() {
        let update = PodUpdate {
            pod: valid_pod(),
            op: PodOperation::Remove,
        };
        assert_eq!(determine_sync_action(&update), SyncAction::Delete);
    }

    #[test]
    fn test_determine_sync_action_reconcile() {
        let update = PodUpdate {
            pod: valid_pod(),
            op: PodOperation::Reconcile,
        };
        assert_eq!(determine_sync_action(&update), SyncAction::Reconcile);
    }
}
