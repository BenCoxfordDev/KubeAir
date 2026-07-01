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

//! Pod status management - tracks and derives pod status from container states.

use crate::pod::lifecycle::{
    ConditionStatus, ContainerStatus, PodCondition, PodConditionType, PodLifecycleState, PodPhase,
};
use crate::pod::{PodSpec, RestartPolicy};
use crate::types::PodUID;
use chrono::Utc;
use dashmap::DashMap;
use std::sync::Arc;

/// Thread-safe store of pod lifecycle states.
pub struct PodStatusManager {
    states: Arc<DashMap<PodUID, PodLifecycleState>>,
}

impl PodStatusManager {
    pub fn new() -> Self {
        Self {
            states: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, uid: &PodUID) -> Option<PodLifecycleState> {
        self.states.get(uid).map(|s| s.clone())
    }

    pub fn set(&self, uid: PodUID, state: PodLifecycleState) {
        self.states.insert(uid, state);
    }

    pub fn remove(&self, uid: &PodUID) {
        self.states.remove(uid);
    }

    /// Seed the start_time for a pod from an external source (e.g. the API server).
    /// Only sets it if no start_time is currently stored, so it never overwrites
    /// a value that was set by initialize() or a prior sync cycle.
    pub fn seed_start_time(&self, uid: &PodUID, start_time: chrono::DateTime<Utc>) {
        if let Some(mut entry) = self.states.get_mut(uid)
            && entry.start_time.is_none()
        {
            entry.start_time = Some(start_time);
        }
    }

    pub fn all(&self) -> Vec<(PodUID, PodLifecycleState)> {
        self.states
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    pub fn initialize(&self, pod: &PodSpec) {
        let mut state = PodLifecycleState::default();
        // Preserve existing start_time across kubelet restarts — startTime in the
        // Kubernetes API represents when the pod was first scheduled/accepted, not
        // when the kubelet process started.  Always resetting to Utc::now() causes
        // the API server to show an incorrect (too-recent) startTime after restarts.
        // Priority order: 1) already-stored state, 2) value from API server status,
        // 3) current time (new pod seen for the first time).
        state.start_time = self
            .states
            .get(&pod.uid)
            .and_then(|s| s.start_time)
            .or(pod.observed_start_time)
            .or_else(|| Some(Utc::now()));

        // Initialize container statuses as Waiting/ContainerCreating
        state.container_statuses = pod
            .containers
            .iter()
            .map(|c| ContainerStatus {
                name: c.name.clone(),
                state: crate::pod::lifecycle::ContainerState::Waiting {
                    reason: "ContainerCreating".to_string(),
                    message: None,
                },
                last_state: None,
                ready: false,
                restart_count: 0,
                image: c.image.clone(),
                image_id: String::new(),
                container_id: None,
                started: Some(false),
                resources: Some(c.resources.clone()),
                allocated_resources: c.resources.requests.clone(),
            })
            .collect();

        state.init_container_statuses = pod
            .init_containers
            .iter()
            .map(|c| ContainerStatus {
                name: c.name.clone(),
                state: crate::pod::lifecycle::ContainerState::Waiting {
                    reason: "PodInitializing".to_string(),
                    message: None,
                },
                last_state: None,
                ready: false,
                restart_count: 0,
                image: c.image.clone(),
                image_id: String::new(),
                container_id: None,
                started: Some(false),
                resources: Some(c.resources.clone()),
                allocated_resources: c.resources.requests.clone(),
            })
            .collect();

        state.conditions = vec![
            PodCondition {
                condition_type: PodConditionType::PodScheduled,
                status: ConditionStatus::True,
                last_probe_time: None,
                last_transition_time: Some(Utc::now()),
                reason: None,
                message: None,
            },
            PodCondition {
                condition_type: PodConditionType::Initialized,
                status: if pod.init_containers.is_empty() {
                    ConditionStatus::True
                } else {
                    ConditionStatus::False
                },
                last_probe_time: None,
                last_transition_time: Some(Utc::now()),
                reason: None,
                message: None,
            },
        ];

        self.states.insert(pod.uid.clone(), state);
    }

    /// Derive overall pod readiness from container statuses.
    pub fn compute_readiness(&self, uid: &PodUID) -> bool {
        if let Some(state) = self.states.get(uid) {
            state.container_statuses.iter().all(|s| s.ready)
        } else {
            false
        }
    }
}

impl Default for PodStatusManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pod::{ContainerSpec, ImagePullPolicy, ResourceRequirements};
    use crate::types::{PodRef, PodUID};

    fn make_pod(uid: &str, containers: usize) -> PodSpec {
        PodSpec {
            uid: PodUID::new(uid),
            pod_ref: PodRef::new("default", "test-pod"),
            containers: (0..containers)
                .map(|i| ContainerSpec {
                    name: format!("container-{}", i),
                    image: "nginx:latest".to_string(),
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
                })
                .collect(),
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
    fn test_initialize_creates_waiting_statuses() {
        let manager = PodStatusManager::new();
        let pod = make_pod("uid-123", 2);
        manager.initialize(&pod);

        let state = manager.get(&PodUID::new("uid-123")).unwrap();
        assert_eq!(state.container_statuses.len(), 2);
        assert_eq!(state.phase, PodPhase::Pending);
        assert!(matches!(
            &state.container_statuses[0].state,
            crate::pod::lifecycle::ContainerState::Waiting { reason, .. } if reason == "ContainerCreating"
        ));
    }

    #[test]
    fn test_readiness_false_when_not_ready() {
        let manager = PodStatusManager::new();
        let pod = make_pod("uid-456", 1);
        manager.initialize(&pod);
        assert!(!manager.compute_readiness(&PodUID::new("uid-456")));
    }

    #[test]
    fn test_remove_clears_state() {
        let manager = PodStatusManager::new();
        let pod = make_pod("uid-789", 1);
        manager.initialize(&pod);
        manager.remove(&PodUID::new("uid-789"));
        assert!(manager.get(&PodUID::new("uid-789")).is_none());
    }

    #[test]
    fn test_all_returns_all_states() {
        let manager = PodStatusManager::new();
        for i in 0..3 {
            manager.initialize(&make_pod(&format!("uid-{}", i), 1));
        }
        assert_eq!(manager.all().len(), 3);
    }
}
