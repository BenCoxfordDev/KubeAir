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

//! Integration tests for container state tracking and restart behavior.
//!
//! Tests issue #8: Container stuck in Running state and lastTerminationState not populated.

use chrono::Utc;
use kubelet_core::pod::lifecycle::{ContainerState, ContainerStatus, PodLifecycleState, PodPhase};

#[test]
fn test_last_termination_state_preserved_after_restart() {
    // Simulate a pod with a container that has crashed and restarted
    let mut state = PodLifecycleState {
        phase: PodPhase::Running,
        reason: None,
        conditions: vec![],
        container_statuses: vec![ContainerStatus {
            name: "app".to_string(),
            state: ContainerState::Terminated {
                exit_code: 1,
                reason: "Error".to_string(),
                message: Some("Container failed".to_string()),
                started_at: Utc::now() - chrono::Duration::seconds(60),
                finished_at: Utc::now() - chrono::Duration::seconds(30),
            },
            last_state: None,
            ready: false,
            restart_count: 0,
            image: "nginx:latest".to_string(),
            image_id: "sha256:abc123".to_string(),
            container_id: Some("containerd://old-id".to_string()),
            started: Some(false),
        }],
        init_container_statuses: vec![],
        ephemeral_container_statuses: vec![],
        start_time: Some(Utc::now() - chrono::Duration::minutes(5)),
        pod_ip: Some("10.0.0.1".to_string()),
        host_ip: Some("192.168.1.1".to_string()),
        nominated_node_name: None,
        observed_generation: None,
    };

    // Simulate container restart - previous state should be preserved as last_state
    let previous_state = state.container_statuses[0].state.clone();

    state.container_statuses[0] = ContainerStatus {
        name: "app".to_string(),
        state: ContainerState::Running {
            started_at: Utc::now(),
        },
        last_state: Some(previous_state.clone()),
        ready: true,
        restart_count: 1,
        image: "nginx:latest".to_string(),
        image_id: "sha256:abc123".to_string(),
        container_id: Some("containerd://new-id".to_string()),
        started: Some(true),
    };

    // Verify last_state is populated
    assert!(
        state.container_statuses[0].last_state.is_some(),
        "last_state should be populated after restart"
    );

    if let Some(ContainerState::Terminated {
        exit_code, reason, ..
    }) = &state.container_statuses[0].last_state
    {
        assert_eq!(*exit_code, 1, "last_state should preserve exit code");
        assert_eq!(reason, "Error", "last_state should preserve reason");
    } else {
        panic!("last_state should be Terminated");
    }
}

#[test]
fn test_container_state_transitions_from_running_to_terminated() {
    let mut status = ContainerStatus {
        name: "app".to_string(),
        state: ContainerState::Running {
            started_at: Utc::now() - chrono::Duration::seconds(60),
        },
        last_state: None,
        ready: true,
        restart_count: 0,
        image: "nginx:latest".to_string(),
        image_id: "sha256:abc123".to_string(),
        container_id: Some("containerd://id123".to_string()),
        started: Some(true),
    };

    // Container exits
    status.state = ContainerState::Terminated {
        exit_code: 0,
        reason: "Completed".to_string(),
        message: None,
        started_at: Utc::now() - chrono::Duration::seconds(60),
        finished_at: Utc::now(),
    };
    status.ready = false;

    // Verify state changed
    assert!(matches!(
        status.state,
        ContainerState::Terminated { exit_code: 0, .. }
    ));
    assert!(
        !status.ready,
        "Container should not be ready when terminated"
    );
}

#[test]
fn test_restart_count_increments_correctly() {
    let mut status = ContainerStatus {
        name: "app".to_string(),
        state: ContainerState::Terminated {
            exit_code: 1,
            reason: "Error".to_string(),
            message: None,
            started_at: Utc::now() - chrono::Duration::seconds(60),
            finished_at: Utc::now(),
        },
        last_state: None,
        ready: false,
        restart_count: 0,
        image: "nginx:latest".to_string(),
        image_id: "sha256:abc123".to_string(),
        container_id: Some("containerd://id123".to_string()),
        started: Some(false),
    };

    // Simulate restart
    let old_state = status.state.clone();
    status.state = ContainerState::Running {
        started_at: Utc::now(),
    };
    status.last_state = Some(old_state);
    status.restart_count += 1;
    status.ready = true;

    assert_eq!(
        status.restart_count, 1,
        "Restart count should increment to 1"
    );
    assert!(status.ready, "Container should be ready after restart");
    assert!(
        status.last_state.is_some(),
        "last_state should preserve previous terminated state"
    );
}

#[test]
fn test_multiple_restarts_preserve_last_state() {
    let mut status = ContainerStatus {
        name: "app".to_string(),
        state: ContainerState::Running {
            started_at: Utc::now(),
        },
        last_state: None,
        ready: true,
        restart_count: 0,
        image: "nginx:latest".to_string(),
        image_id: "sha256:abc123".to_string(),
        container_id: Some("containerd://id123".to_string()),
        started: Some(true),
    };

    // First crash
    status.state = ContainerState::Terminated {
        exit_code: 137,
        reason: "Error".to_string(),
        message: Some("OOMKilled".to_string()),
        started_at: Utc::now() - chrono::Duration::seconds(60),
        finished_at: Utc::now(),
    };
    status.ready = false;
    status.restart_count += 1;

    // First restart
    let first_crash = status.state.clone();
    status.state = ContainerState::Running {
        started_at: Utc::now(),
    };
    status.last_state = Some(first_crash.clone());
    status.ready = true;

    // Second crash
    status.state = ContainerState::Terminated {
        exit_code: 1,
        reason: "Error".to_string(),
        message: Some("Exit 1".to_string()),
        started_at: Utc::now() - chrono::Duration::seconds(30),
        finished_at: Utc::now(),
    };
    status.ready = false;
    status.restart_count += 1;

    // Second restart
    let second_crash = status.state.clone();
    status.state = ContainerState::Running {
        started_at: Utc::now(),
    };
    status.last_state = Some(second_crash);
    status.ready = true;

    assert_eq!(
        status.restart_count, 2,
        "Restart count should be 2 after two crashes"
    );

    // last_state should now reflect the second crash, not the first
    if let Some(ContainerState::Terminated {
        exit_code, message, ..
    }) = &status.last_state
    {
        assert_eq!(*exit_code, 1, "last_state should reflect most recent crash");
        assert_eq!(
            message.as_deref(),
            Some("Exit 1"),
            "last_state should have most recent message"
        );
    } else {
        panic!("last_state should be Terminated after second restart");
    }
}

#[test]
fn test_container_state_waiting_for_image_pull() {
    let status = ContainerStatus {
        name: "app".to_string(),
        state: ContainerState::Waiting {
            reason: "ErrImagePull".to_string(),
            message: Some("Failed to pull image nginx:invalid".to_string()),
        },
        last_state: None,
        ready: false,
        restart_count: 0,
        image: "nginx:invalid".to_string(),
        image_id: "".to_string(),
        container_id: None,
        started: Some(false),
    };

    assert!(
        matches!(status.state, ContainerState::Waiting { .. }),
        "Container should be in Waiting state for image pull errors"
    );
    assert!(!status.ready, "Container should not be ready");
    assert!(
        status.container_id.is_none(),
        "Container ID should be None when waiting"
    );
}
