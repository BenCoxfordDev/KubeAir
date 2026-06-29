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

//! Pod lifecycle state machine.
//!
//! Mirrors the kubelet's pod lifecycle management: pending -> running -> succeeded/failed.
//! Handles init containers, ephemeral containers, and restart policies.

use crate::pod::RestartPolicy;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Overall phase of a pod, as reported to the API server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PodPhase {
    #[default]
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

impl std::fmt::Display for PodPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "Pending"),
            Self::Running => write!(f, "Running"),
            Self::Succeeded => write!(f, "Succeeded"),
            Self::Failed => write!(f, "Failed"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

/// The state of a single container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContainerState {
    Waiting {
        reason: String,
        message: Option<String>,
    },
    Running {
        started_at: DateTime<Utc>,
    },
    Terminated {
        exit_code: i32,
        reason: String,
        message: Option<String>,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
    },
}

impl Default for ContainerState {
    fn default() -> Self {
        Self::Waiting {
            reason: "ContainerCreating".to_string(),
            message: None,
        }
    }
}

/// Status of a single container within a running pod.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerStatus {
    pub name: String,
    pub state: ContainerState,
    pub last_state: Option<ContainerState>,
    pub ready: bool,
    pub restart_count: u32,
    pub image: String,
    pub image_id: String,
    pub container_id: Option<String>,
    pub started: Option<bool>,
}

/// Full pod lifecycle status tracked internally by the kubelet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodLifecycleState {
    pub phase: PodPhase,
    /// Pod-level reason for the current phase (e.g. "DeadlineExceeded", "Evicted").
    /// Maps to `PodStatus.reason` in the Kubernetes API.
    pub reason: Option<String>,
    pub conditions: Vec<PodCondition>,
    pub container_statuses: Vec<ContainerStatus>,
    pub init_container_statuses: Vec<ContainerStatus>,
    pub ephemeral_container_statuses: Vec<ContainerStatus>,
    pub start_time: Option<DateTime<Utc>>,
    pub pod_ip: Option<String>,
    pub host_ip: Option<String>,
    pub nominated_node_name: Option<String>,
    /// The last generation of the pod spec that the kubelet has processed.
    /// Reported as `status.observedGeneration` to unblock controllers and
    /// conformance tests that wait for observedGeneration >= 1.
    pub observed_generation: Option<i64>,
}

impl Default for PodLifecycleState {
    fn default() -> Self {
        Self {
            phase: PodPhase::Pending,
            reason: None,
            conditions: Vec::new(),
            container_statuses: Vec::new(),
            init_container_statuses: Vec::new(),
            ephemeral_container_statuses: Vec::new(),
            start_time: None,
            pod_ip: None,
            host_ip: None,
            nominated_node_name: None,
            observed_generation: None,
        }
    }
}

/// Pod condition as defined by the Kubernetes API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodCondition {
    pub condition_type: PodConditionType,
    pub status: ConditionStatus,
    pub last_probe_time: Option<DateTime<Utc>>,
    pub last_transition_time: Option<DateTime<Utc>>,
    pub reason: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PodConditionType {
    PodScheduled,
    ContainersReady,
    Initialized,
    Ready,
    PodReadyToStartContainers,
    DisruptionTarget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConditionStatus {
    True,
    False,
    Unknown,
}

/// Determines the next phase based on container states and restart policy.
pub fn compute_pod_phase(
    init_statuses: &[ContainerStatus],
    statuses: &[ContainerStatus],
    restart_policy: &RestartPolicy,
) -> PodPhase {
    // Check regular containers first.  If any regular container is already
    // Running, the pod is Running — sidecar init containers (restartPolicy=Always)
    // are allowed to remain Running once regular containers have started, so we
    // must not let a Running sidecar init container keep the phase at Pending.
    let any_running = statuses
        .iter()
        .any(|s| matches!(&s.state, ContainerState::Running { .. }));
    if any_running {
        return PodPhase::Running;
    }

    // A container in CrashLoopBackOff is Waiting but has previously been running
    // and will be restarted — the pod phase must remain Running (not Pending).
    // Phase oscillating Running→Pending→Running during backoff confuses Endpoints
    // controllers and pods that gate on the pod being Running.
    let any_crash_loop = statuses.iter().any(|s| matches!(&s.state, ContainerState::Waiting { reason, .. } if reason == "CrashLoopBackOff"));
    if any_crash_loop {
        return PodPhase::Running;
    }

    // If no regular container is Running yet, check init containers.
    // A Running init container means either a non-sidecar init container is still
    // executing (real Pending) or a sidecar is starting up (also Pending until
    // regular containers begin).
    for status in init_statuses {
        match &status.state {
            ContainerState::Waiting { .. } => return PodPhase::Pending,
            ContainerState::Running { .. } => return PodPhase::Pending,
            ContainerState::Terminated { exit_code, .. } => {
                if *exit_code != 0 {
                    return match restart_policy {
                        RestartPolicy::Never => PodPhase::Failed,
                        _ => PodPhase::Pending, // will be restarted
                    };
                }
                // This init container succeeded; continue checking
            }
        }
    }

    let all_succeeded = statuses.iter().all(
        |s| matches!(&s.state, ContainerState::Terminated { exit_code, .. } if *exit_code == 0),
    );
    let any_failed = statuses.iter().any(
        |s| matches!(&s.state, ContainerState::Terminated { exit_code, .. } if *exit_code != 0),
    );

    if all_succeeded && !statuses.is_empty() {
        return match restart_policy {
            RestartPolicy::Never | RestartPolicy::OnFailure => PodPhase::Succeeded,
            RestartPolicy::Always => PodPhase::Running, // will be restarted
        };
    }

    if any_failed {
        return match restart_policy {
            RestartPolicy::Never => PodPhase::Failed,
            _ => PodPhase::Running, // will be restarted
        };
    }

    PodPhase::Pending
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running_status(name: &str) -> ContainerStatus {
        ContainerStatus {
            name: name.to_string(),
            state: ContainerState::Running {
                started_at: Utc::now(),
            },
            last_state: None,
            ready: true,
            restart_count: 0,
            image: "test:latest".to_string(),
            image_id: "sha256:abc".to_string(),
            container_id: Some("ctr://abc123".to_string()),
            started: Some(true),
        }
    }

    fn terminated_status(name: &str, exit_code: i32) -> ContainerStatus {
        let now = Utc::now();
        ContainerStatus {
            name: name.to_string(),
            state: ContainerState::Terminated {
                exit_code,
                reason: if exit_code == 0 {
                    "Completed".to_string()
                } else {
                    "Error".to_string()
                },
                message: None,
                started_at: now,
                finished_at: now,
            },
            last_state: None,
            ready: false,
            restart_count: 0,
            image: "test:latest".to_string(),
            image_id: "sha256:abc".to_string(),
            container_id: None,
            started: Some(false),
        }
    }

    #[test]
    fn test_running_containers_gives_running_phase() {
        let statuses = vec![running_status("app")];
        assert_eq!(
            compute_pod_phase(&[], &statuses, &RestartPolicy::Always),
            PodPhase::Running
        );
    }

    #[test]
    fn test_all_succeeded_never_policy_gives_succeeded() {
        let statuses = vec![terminated_status("app", 0)];
        assert_eq!(
            compute_pod_phase(&[], &statuses, &RestartPolicy::Never),
            PodPhase::Succeeded
        );
    }

    #[test]
    fn test_failed_container_never_policy_gives_failed() {
        let statuses = vec![terminated_status("app", 1)];
        assert_eq!(
            compute_pod_phase(&[], &statuses, &RestartPolicy::Never),
            PodPhase::Failed
        );
    }

    #[test]
    fn test_failed_container_always_policy_stays_running() {
        let statuses = vec![terminated_status("app", 1)];
        assert_eq!(
            compute_pod_phase(&[], &statuses, &RestartPolicy::Always),
            PodPhase::Running
        );
    }

    #[test]
    fn test_init_container_running_gives_pending() {
        let init_statuses = vec![running_status("init")];
        let statuses = vec![];
        assert_eq!(
            compute_pod_phase(&init_statuses, &statuses, &RestartPolicy::Always),
            PodPhase::Pending
        );
    }

    #[test]
    fn test_init_container_failed_never_gives_failed() {
        let init_statuses = vec![terminated_status("init", 1)];
        let statuses = vec![];
        assert_eq!(
            compute_pod_phase(&init_statuses, &statuses, &RestartPolicy::Never),
            PodPhase::Failed
        );
    }

    #[test]
    fn test_pod_phase_display() {
        assert_eq!(format!("{}", PodPhase::Running), "Running");
        assert_eq!(format!("{}", PodPhase::Failed), "Failed");
        assert_eq!(format!("{}", PodPhase::Succeeded), "Succeeded");
    }
}
