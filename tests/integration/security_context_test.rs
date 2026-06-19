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

//! Integration tests for container security context enforcement.
//!
//! Tests issue #7: Security context (runAsUser, privileged, capabilities) not enforced.

use kubelet_adapters::container_builder::build_container_config;
use kubelet_core::pod::{
    Capabilities, ContainerSpec, PodSecurityContext, PodSpec, RestartPolicy, SecurityContext,
};
use kubelet_core::types::{PodRef, PodUID};

#[test]
fn test_container_level_run_as_user_overrides_pod_level() {
    let pod = PodSpec {
        uid: PodUID("test-uid".to_string()),
        pod_ref: PodRef {
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
        },
        node_name: "test-node".to_string(),
        security_context: Some(PodSecurityContext {
            run_as_user: Some(1000),
            run_as_group: Some(2000),
            fs_group: Some(3000),
            supplemental_groups: vec![4000, 5000],
            ..Default::default()
        }),
        containers: vec![],
        init_containers: vec![],
        restart_policy: RestartPolicy::Always,
        ..Default::default()
    };

    let container = ContainerSpec {
        name: "test-container".to_string(),
        image: "nginx:latest".to_string(),
        security_context: Some(SecurityContext {
            run_as_user: Some(9999), // Container-level overrides pod-level
            run_as_group: Some(8888),
            privileged: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    };

    let config = build_container_config(
        &pod,
        &container,
        "sandbox-123",
        &Default::default(),
        "/var/log/pods",
    );

    assert_eq!(config.security.run_as_user, Some(9999));
    assert_eq!(config.security.run_as_group, Some(8888));
    assert!(!config.security.privileged);
}

#[test]
fn test_pod_level_security_context_applied_when_no_container_override() {
    let pod = PodSpec {
        uid: PodUID("test-uid".to_string()),
        pod_ref: PodRef {
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
        },
        node_name: "test-node".to_string(),
        security_context: Some(PodSecurityContext {
            run_as_user: Some(1000),
            run_as_group: Some(2000),
            fs_group: Some(3000),
            supplemental_groups: vec![4000, 5000],
            ..Default::default()
        }),
        containers: vec![],
        init_containers: vec![],
        restart_policy: RestartPolicy::Always,
        ..Default::default()
    };

    let container = ContainerSpec {
        name: "test-container".to_string(),
        image: "nginx:latest".to_string(),
        security_context: None, // No container-level override
        ..Default::default()
    };

    let config = build_container_config(
        &pod,
        &container,
        "sandbox-123",
        &Default::default(),
        "/var/log/pods",
    );

    assert_eq!(config.security.run_as_user, Some(1000));
    assert_eq!(config.security.run_as_group, Some(2000));
    assert_eq!(
        config.security.supplemental_groups,
        vec![4000, 5000],
        "Supplemental groups should come from pod security context"
    );
}

#[test]
fn test_privileged_flag_propagated() {
    let pod = PodSpec {
        uid: PodUID("test-uid".to_string()),
        pod_ref: PodRef {
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
        },
        node_name: "test-node".to_string(),
        containers: vec![],
        init_containers: vec![],
        restart_policy: RestartPolicy::Always,
        ..Default::default()
    };

    let container = ContainerSpec {
        name: "privileged-container".to_string(),
        image: "nginx:latest".to_string(),
        security_context: Some(SecurityContext {
            privileged: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };

    let config = build_container_config(
        &pod,
        &container,
        "sandbox-123",
        &Default::default(),
        "/var/log/pods",
    );

    assert!(
        config.security.privileged,
        "Privileged flag should be set to true"
    );
}

#[test]
fn test_capabilities_add_and_drop() {
    let pod = PodSpec {
        uid: PodUID("test-uid".to_string()),
        pod_ref: PodRef {
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
        },
        node_name: "test-node".to_string(),
        containers: vec![],
        init_containers: vec![],
        restart_policy: RestartPolicy::Always,
        ..Default::default()
    };

    let container = ContainerSpec {
        name: "test-container".to_string(),
        image: "nginx:latest".to_string(),
        security_context: Some(SecurityContext {
            capabilities: Some(Capabilities {
                add: vec!["NET_ADMIN".to_string(), "SYS_TIME".to_string()],
                drop: vec!["ALL".to_string()],
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    let config = build_container_config(
        &pod,
        &container,
        "sandbox-123",
        &Default::default(),
        "/var/log/pods",
    );

    assert_eq!(
        config.security.capabilities_add,
        vec!["NET_ADMIN".to_string(), "SYS_TIME".to_string()]
    );
    assert_eq!(config.security.capabilities_drop, vec!["ALL".to_string()]);
}

#[test]
fn test_read_only_root_filesystem() {
    let pod = PodSpec {
        uid: PodUID("test-uid".to_string()),
        pod_ref: PodRef {
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
        },
        node_name: "test-node".to_string(),
        containers: vec![],
        init_containers: vec![],
        restart_policy: RestartPolicy::Always,
        ..Default::default()
    };

    let container = ContainerSpec {
        name: "test-container".to_string(),
        image: "nginx:latest".to_string(),
        security_context: Some(SecurityContext {
            read_only_root_filesystem: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };

    let config = build_container_config(
        &pod,
        &container,
        "sandbox-123",
        &Default::default(),
        "/var/log/pods",
    );

    assert!(
        config.security.read_only_root_filesystem,
        "Read-only root filesystem should be enabled"
    );
}

#[test]
fn test_allow_privilege_escalation() {
    let pod = PodSpec {
        uid: PodUID("test-uid".to_string()),
        pod_ref: PodRef {
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
        },
        node_name: "test-node".to_string(),
        containers: vec![],
        init_containers: vec![],
        restart_policy: RestartPolicy::Always,
        ..Default::default()
    };

    let container = ContainerSpec {
        name: "test-container".to_string(),
        image: "nginx:latest".to_string(),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    };

    let config = build_container_config(
        &pod,
        &container,
        "sandbox-123",
        &Default::default(),
        "/var/log/pods",
    );

    assert_eq!(
        config.security.allow_privilege_escalation,
        Some(false),
        "Allow privilege escalation should be set to false"
    );
}

#[test]
fn test_seccomp_profile_runtime_default() {
    let pod = PodSpec {
        uid: PodUID("test-uid".to_string()),
        pod_ref: PodRef {
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
        },
        node_name: "test-node".to_string(),
        security_context: Some(PodSecurityContext {
            seccomp_profile: Some(kubelet_core::pod::SeccompSpec {
                type_: "RuntimeDefault".to_string(),
                localhost_profile: None,
            }),
            ..Default::default()
        }),
        containers: vec![],
        init_containers: vec![],
        restart_policy: RestartPolicy::Always,
        ..Default::default()
    };

    let container = ContainerSpec {
        name: "test-container".to_string(),
        image: "nginx:latest".to_string(),
        ..Default::default()
    };

    let config = build_container_config(
        &pod,
        &container,
        "sandbox-123",
        &Default::default(),
        "/var/log/pods",
    );

    assert_eq!(
        config.security.seccomp_profile_type,
        Some("RuntimeDefault".to_string())
    );
}

#[test]
fn test_seccomp_profile_localhost() {
    let pod = PodSpec {
        uid: PodUID("test-uid".to_string()),
        pod_ref: PodRef {
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
        },
        node_name: "test-node".to_string(),
        containers: vec![],
        init_containers: vec![],
        restart_policy: RestartPolicy::Always,
        ..Default::default()
    };

    let container = ContainerSpec {
        name: "test-container".to_string(),
        image: "nginx:latest".to_string(),
        security_context: Some(SecurityContext {
            seccomp_profile: Some(kubelet_core::pod::SeccompSpec {
                type_: "Localhost".to_string(),
                localhost_profile: Some("/path/to/profile.json".to_string()),
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    let config = build_container_config(
        &pod,
        &container,
        "sandbox-123",
        &Default::default(),
        "/var/log/pods",
    );

    assert_eq!(
        config.security.seccomp_profile_type,
        Some("Localhost".to_string())
    );
    assert_eq!(
        config.security.seccomp_localhost_path,
        Some("/path/to/profile.json".to_string())
    );
}
