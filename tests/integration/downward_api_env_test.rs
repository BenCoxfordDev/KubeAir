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

//! Integration tests for Downward API env-var resolution, ConfigMap/Secret env
//! injection, and multi-container pod behaviour.
//!
//! These mirror the Kubernetes conformance tests from:
//!   test/e2e_node/pods.go — "should expose pod name/namespace/uid/labels/annotations"
//!   test/e2e_node/pods.go — "should expose container resource requests/limits"
//!   test/e2e_node/configmap.go — "should be consumable via env"
//!   test/e2e_node/secret.go   — "should be consumable via env"
//!   test/e2e_node/init_container.go — "should run init containers in order"

use kubelet_adapters::container_builder::{
    resolve_configmap_env, resolve_downward_api_env, resolve_secret_env,
};
use kubelet_core::pod::{
    ContainerSpec, EnvFromRef, EnvFromSource, EnvVar, EnvVarSource, ImagePullPolicy, PodSpec,
    RestartPolicy, VolumeMount, VolumeSource, VolumeSpec,
};
use kubelet_core::types::{PodRef, PodUID, ResourceQuantity};
use std::collections::HashMap;

// ── helpers ───────────────────────────────────────────────────────────────────

fn pod(uid: &str, name: &str, namespace: &str) -> PodSpec {
    PodSpec {
        uid: PodUID::new(uid),
        pod_ref: PodRef::new(namespace, name),
        containers: vec![ContainerSpec {
            name: "main".to_string(),
            image: "nginx:1.25".to_string(),
            command: vec![],
            args: vec![],
            working_dir: None,
            ports: vec![],
            env: vec![],
            resources: Default::default(),
            volume_mounts: vec![],
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            image_pull_policy: ImagePullPolicy::IfNotPresent,
            security_context: None,
            termination_message_path: None,
            ..Default::default()
        }],
        init_containers: vec![],
        ephemeral_containers: vec![],
        volumes: vec![],
        node_name: "integ-node".to_string(),
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

// ── Downward API field-ref tests ──────────────────────────────────────────────

/// Integration: metadata.name and metadata.namespace resolve correctly together.
/// Mirrors pods.go "should expose pod name as environment variable" [NodeConformance]
#[test]
fn integ_downward_api_name_and_namespace_together() {
    let mut p = pod("uid-da-1", "my-app", "production");
    p.containers[0].env = vec![
        EnvVar {
            name: "POD_NAME".to_string(),
            value: None,
            value_from: Some(EnvVarSource::FieldRef {
                field_path: "metadata.name".to_string(),
            }),
        },
        EnvVar {
            name: "POD_NAMESPACE".to_string(),
            value: None,
            value_from: Some(EnvVarSource::FieldRef {
                field_path: "metadata.namespace".to_string(),
            }),
        },
    ];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert_eq!(resolved["POD_NAME"], "my-app");
    assert_eq!(resolved["POD_NAMESPACE"], "production");
}

/// Integration: metadata.uid resolves to the pod's UID.
#[test]
fn integ_downward_api_uid_resolution() {
    let mut p = pod("unique-id-abc123", "my-pod", "default");
    p.containers[0].env = vec![EnvVar {
        name: "POD_UID".to_string(),
        value: None,
        value_from: Some(EnvVarSource::FieldRef {
            field_path: "metadata.uid".to_string(),
        }),
    }];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert_eq!(resolved["POD_UID"], "unique-id-abc123");
}

/// Integration: spec.nodeName resolves to the node the pod is assigned to.
#[test]
fn integ_downward_api_node_name_resolution() {
    let mut p = pod("uid-node", "my-pod", "default");
    p.node_name = "worker-42".to_string();
    p.containers[0].env = vec![EnvVar {
        name: "MY_NODE".to_string(),
        value: None,
        value_from: Some(EnvVarSource::FieldRef {
            field_path: "spec.nodeName".to_string(),
        }),
    }];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert_eq!(resolved["MY_NODE"], "worker-42");
}

/// Integration: metadata.labels['key'] resolves to the label value.
/// Mirrors pods.go "should expose pod labels as environment variables [NodeConformance]"
#[test]
fn integ_downward_api_label_env_resolution() {
    let mut p = pod("uid-labels", "labeled-pod", "default");
    p.labels.insert("tier".to_string(), "backend".to_string());
    p.labels.insert("version".to_string(), "v2".to_string());
    p.containers[0].env = vec![
        EnvVar {
            name: "TIER".to_string(),
            value: None,
            value_from: Some(EnvVarSource::FieldRef {
                field_path: "metadata.labels['tier']".to_string(),
            }),
        },
        EnvVar {
            name: "VERSION".to_string(),
            value: None,
            value_from: Some(EnvVarSource::FieldRef {
                field_path: "metadata.labels['version']".to_string(),
            }),
        },
    ];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert_eq!(resolved["TIER"], "backend");
    assert_eq!(resolved["VERSION"], "v2");
}

/// Integration: missing label resolves to empty string (not an error).
#[test]
fn integ_downward_api_missing_label_is_empty_string() {
    let mut p = pod("uid-missing-label", "pod-a", "default");
    p.containers[0].env = vec![EnvVar {
        name: "MISSING_LABEL".to_string(),
        value: None,
        value_from: Some(EnvVarSource::FieldRef {
            field_path: "metadata.labels['nonexistent']".to_string(),
        }),
    }];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert_eq!(resolved.get("MISSING_LABEL").map(|s| s.as_str()), Some(""));
}

/// Integration: metadata.annotations['key'] resolves to annotation value.
#[test]
fn integ_downward_api_annotation_env_resolution() {
    let mut p = pod("uid-annot", "annot-pod", "default");
    p.annotations
        .insert("build-id".to_string(), "42".to_string());
    p.containers[0].env = vec![EnvVar {
        name: "BUILD_ID".to_string(),
        value: None,
        value_from: Some(EnvVarSource::FieldRef {
            field_path: "metadata.annotations['build-id']".to_string(),
        }),
    }];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert_eq!(resolved["BUILD_ID"], "42");
}

/// Integration: spec.serviceAccountName resolves correctly.
#[test]
fn integ_downward_api_service_account_name() {
    let mut p = pod("uid-sa", "sa-pod", "default");
    p.service_account_name = "my-service-account".to_string();
    p.containers[0].env = vec![EnvVar {
        name: "SA_NAME".to_string(),
        value: None,
        value_from: Some(EnvVarSource::FieldRef {
            field_path: "spec.serviceAccountName".to_string(),
        }),
    }];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert_eq!(resolved["SA_NAME"], "my-service-account");
}

// ── Downward API resource-ref tests ─────────────────────────────────────────

/// Integration: requests.cpu resolves to millicores value.
/// Mirrors pods.go "should expose container resource requests and limits" [NodeConformance]
#[test]
fn integ_downward_api_cpu_requests_resolution() {
    let mut p = pod("uid-res", "res-pod", "default");
    p.containers[0]
        .resources
        .requests
        .insert("cpu".to_string(), ResourceQuantity::cpu_millicores(750));
    p.containers[0].env = vec![EnvVar {
        name: "CPU_REQ".to_string(),
        value: None,
        value_from: Some(EnvVarSource::ResourceFieldRef {
            container_name: None,
            resource: "requests.cpu".to_string(),
        }),
    }];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert_eq!(resolved["CPU_REQ"], "750");
}

/// Integration: requests.memory resolves to byte value as string.
#[test]
fn integ_downward_api_memory_requests_resolution() {
    let mut p = pod("uid-mem", "mem-pod", "default");
    p.containers[0].resources.requests.insert(
        "memory".to_string(),
        ResourceQuantity::memory_bytes(512 * 1024 * 1024),
    );
    p.containers[0].env = vec![EnvVar {
        name: "MEM_REQ".to_string(),
        value: None,
        value_from: Some(EnvVarSource::ResourceFieldRef {
            container_name: None,
            resource: "requests.memory".to_string(),
        }),
    }];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert_eq!(resolved["MEM_REQ"], "536870912", "512 MiB in bytes");
}

/// Integration: no resource request → resolves to "0".
#[test]
fn integ_downward_api_missing_resource_resolves_to_zero() {
    let mut p = pod("uid-zero", "zero-pod", "default");
    p.containers[0].env = vec![EnvVar {
        name: "CPU_LIMIT".to_string(),
        value: None,
        value_from: Some(EnvVarSource::ResourceFieldRef {
            container_name: None,
            resource: "limits.cpu".to_string(),
        }),
    }];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert_eq!(resolved.get("CPU_LIMIT").map(|s| s.as_str()), Some("0"));
}

// ── ConfigMap env resolution integration tests ────────────────────────────────

/// Integration: ConfigMapKeyRef with multiple keys.
/// Mirrors configmap.go "should be consumable via env variable" [NodeConformance]
#[test]
fn integ_configmap_env_multiple_key_refs() {
    let p = pod("uid-cm", "cm-pod", "default");
    let mut container = p.containers[0].clone();
    container.env = vec![
        EnvVar {
            name: "LOG_LEVEL".to_string(),
            value: None,
            value_from: Some(EnvVarSource::ConfigMapKeyRef {
                name: "app-cfg".to_string(),
                key: "log.level".to_string(),
                optional: false,
            }),
        },
        EnvVar {
            name: "MAX_CONN".to_string(),
            value: None,
            value_from: Some(EnvVarSource::ConfigMapKeyRef {
                name: "app-cfg".to_string(),
                key: "max.connections".to_string(),
                optional: false,
            }),
        },
    ];
    let mut cm_data: HashMap<String, HashMap<String, String>> = HashMap::new();
    cm_data.insert(
        "app-cfg".to_string(),
        [
            ("log.level".to_string(), "debug".to_string()),
            ("max.connections".to_string(), "100".to_string()),
        ]
        .into_iter()
        .collect(),
    );
    let resolved: HashMap<_, _> = resolve_configmap_env(&container, &cm_data)
        .into_iter()
        .collect();
    assert_eq!(resolved["LOG_LEVEL"], "debug");
    assert_eq!(resolved["MAX_CONN"], "100");
}

/// Integration: envFrom ConfigMap without prefix populates all keys unprefixed.
#[test]
fn integ_configmap_env_from_no_prefix() {
    let p = pod("uid-cm-from", "cm-from-pod", "default");
    let mut container = p.containers[0].clone();
    container.env_from = vec![EnvFromSource {
        prefix: None,
        config_map_ref: Some(EnvFromRef {
            name: "settings".to_string(),
            optional: false,
        }),
        secret_ref: None,
    }];
    let mut cm_data: HashMap<String, HashMap<String, String>> = HashMap::new();
    cm_data.insert(
        "settings".to_string(),
        [
            ("FEATURE_FLAGS".to_string(), "alpha,beta".to_string()),
            ("REGION".to_string(), "eu-west-1".to_string()),
        ]
        .into_iter()
        .collect(),
    );
    let resolved: HashMap<_, _> = resolve_configmap_env(&container, &cm_data)
        .into_iter()
        .collect();
    assert_eq!(resolved["FEATURE_FLAGS"], "alpha,beta");
    assert_eq!(resolved["REGION"], "eu-west-1");
}

/// Integration: optional ConfigMapKeyRef for missing key produces no entry.
#[test]
fn integ_configmap_env_optional_missing_key_skipped() {
    let p = pod("uid-opt", "opt-pod", "default");
    let mut container = p.containers[0].clone();
    container.env = vec![EnvVar {
        name: "OPTIONAL_KEY".to_string(),
        value: None,
        value_from: Some(EnvVarSource::ConfigMapKeyRef {
            name: "cfg".to_string(),
            key: "missing-key".to_string(),
            optional: true,
        }),
    }];
    let cm_data: HashMap<String, HashMap<String, String>> = HashMap::new(); // no CMs
    let resolved: HashMap<_, _> = resolve_configmap_env(&container, &cm_data)
        .into_iter()
        .collect();
    // Optional missing key → not present in resolved env
    assert!(
        !resolved.contains_key("OPTIONAL_KEY"),
        "Optional missing key must not appear in resolved env"
    );
}

// ── Secret env resolution integration tests ──────────────────────────────────

/// Integration: SecretKeyRef with multiple secrets.
/// Mirrors secret.go "should be consumable via env variable" [NodeConformance]
#[test]
fn integ_secret_env_multiple_key_refs() {
    let p = pod("uid-sec", "sec-pod", "default");
    let mut container = p.containers[0].clone();
    container.env = vec![
        EnvVar {
            name: "DB_PASSWORD".to_string(),
            value: None,
            value_from: Some(EnvVarSource::SecretKeyRef {
                name: "db-creds".to_string(),
                key: "password".to_string(),
                optional: false,
            }),
        },
        EnvVar {
            name: "DB_USER".to_string(),
            value: None,
            value_from: Some(EnvVarSource::SecretKeyRef {
                name: "db-creds".to_string(),
                key: "username".to_string(),
                optional: false,
            }),
        },
    ];
    let mut secret_data: HashMap<String, HashMap<String, Vec<u8>>> = HashMap::new();
    secret_data.insert(
        "db-creds".to_string(),
        [
            ("password".to_string(), b"p@ss!word".to_vec()),
            ("username".to_string(), b"dbadmin".to_vec()),
        ]
        .into_iter()
        .collect(),
    );
    let resolved: HashMap<_, _> = resolve_secret_env(&container, &secret_data)
        .into_iter()
        .collect();
    assert_eq!(resolved["DB_PASSWORD"], "p@ss!word");
    assert_eq!(resolved["DB_USER"], "dbadmin");
}

/// Integration: envFrom Secret without prefix populates all keys.
#[test]
fn integ_secret_env_from_no_prefix() {
    let p = pod("uid-sec-from", "sec-from-pod", "default");
    let mut container = p.containers[0].clone();
    container.env_from = vec![EnvFromSource {
        prefix: None,
        config_map_ref: None,
        secret_ref: Some(EnvFromRef {
            name: "tls-secret".to_string(),
            optional: false,
        }),
    }];
    let mut secret_data: HashMap<String, HashMap<String, Vec<u8>>> = HashMap::new();
    secret_data.insert(
        "tls-secret".to_string(),
        [
            ("tls.crt".to_string(), b"cert-data".to_vec()),
            ("tls.key".to_string(), b"key-data".to_vec()),
        ]
        .into_iter()
        .collect(),
    );
    let resolved: HashMap<_, _> = resolve_secret_env(&container, &secret_data)
        .into_iter()
        .collect();
    assert_eq!(resolved["tls.crt"], "cert-data");
    assert_eq!(resolved["tls.key"], "key-data");
}

/// Integration: envFrom Secret with prefix prepends prefix to each key.
#[test]
fn integ_secret_env_from_with_prefix() {
    let p = pod("uid-sec-pfx", "sec-pfx-pod", "default");
    let mut container = p.containers[0].clone();
    container.env_from = vec![EnvFromSource {
        prefix: Some("APP_".to_string()),
        config_map_ref: None,
        secret_ref: Some(EnvFromRef {
            name: "app-secret".to_string(),
            optional: false,
        }),
    }];
    let mut secret_data: HashMap<String, HashMap<String, Vec<u8>>> = HashMap::new();
    secret_data.insert(
        "app-secret".to_string(),
        [
            ("TOKEN".to_string(), b"mytoken".to_vec()),
            ("KEY".to_string(), b"mykey".to_vec()),
        ]
        .into_iter()
        .collect(),
    );
    let resolved: HashMap<_, _> = resolve_secret_env(&container, &secret_data)
        .into_iter()
        .collect();
    assert_eq!(resolved["APP_TOKEN"], "mytoken");
    assert_eq!(resolved["APP_KEY"], "mykey");
}

// ── Init container ordering integration tests ────────────────────────────────

/// Integration: init containers run sequentially (modelled by order in vec).
/// Mirrors init_container.go "should run init containers in order" [NodeConformance]
#[test]
fn integ_init_containers_ordered_in_spec() {
    let mut p = pod("uid-init-order", "init-order-pod", "default");
    p.init_containers = vec![
        ContainerSpec {
            name: "init-db".to_string(),
            image: "postgres:15".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "migrate".to_string()],
            ..Default::default()
        },
        ContainerSpec {
            name: "init-cache".to_string(),
            image: "redis:7".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "warmup".to_string()],
            ..Default::default()
        },
    ];
    // Verify ordering is preserved in spec (the kubelet must run them in order).
    assert_eq!(p.init_containers[0].name, "init-db");
    assert_eq!(p.init_containers[1].name, "init-cache");
}

/// Integration: multi-container pod carries all container specs.
/// Mirrors pods.go "should support multi-container pods" [NodeConformance]
#[test]
fn integ_multi_container_pod_all_specs_present() {
    let mut p = pod("uid-multi", "multi-pod", "default");
    p.containers = vec![
        ContainerSpec {
            name: "app".to_string(),
            image: "nginx:1.25".to_string(),
            ..Default::default()
        },
        ContainerSpec {
            name: "sidecar".to_string(),
            image: "fluentd:v1.16".to_string(),
            ..Default::default()
        },
        ContainerSpec {
            name: "metrics".to_string(),
            image: "prom/node-exporter:v1.7".to_string(),
            ..Default::default()
        },
    ];
    assert_eq!(p.containers.len(), 3);
    assert_eq!(p.containers[0].name, "app");
    assert_eq!(p.containers[1].name, "sidecar");
    assert_eq!(p.containers[2].name, "metrics");
}

/// Integration: VolumeMount sub_path None is the default.
#[test]
fn integ_volume_mount_subpath_defaults_to_none() {
    let vm = VolumeMount {
        name: "data".to_string(),
        mount_path: "/var/data".to_string(),
        sub_path: None,
        sub_path_expr: None,
        read_only: false,
    };
    assert!(vm.sub_path.is_none());
    assert!(vm.sub_path_expr.is_none());
}

/// Integration: VolumeMount read_only flag is respected in spec.
#[test]
fn integ_volume_mount_read_only_flag() {
    let vm = VolumeMount {
        name: "config".to_string(),
        mount_path: "/etc/config".to_string(),
        sub_path: None,
        sub_path_expr: None,
        read_only: true,
    };
    assert!(vm.read_only);
}

/// Integration: emptydir_huge_pages_medium source is modelled with correct medium.
/// Mirrors emptydir.go "should support tmpfs and hugetlb emptydir volumes"
#[test]
fn integ_emptydir_huge_pages_medium() {
    let mut p = pod("uid-hp", "hp-pod", "default");
    p.volumes = vec![VolumeSpec {
        name: "hugepages".to_string(),
        source: VolumeSource::EmptyDir {
            medium: Some("HugePages-2Mi".to_string()),
            size_limit: None,
        },
    }];
    match &p.volumes[0].source {
        VolumeSource::EmptyDir { medium, .. } => {
            assert_eq!(medium.as_deref(), Some("HugePages-2Mi"));
        }
        _ => panic!("expected EmptyDir source"),
    }
}

// ── Downward API volume status-field env tests ────────────────────────────────
//
// Mirrors:
//   test/e2e/common/node/downwardapi.go — "should provide host IP as an env var"
//   test/e2e/common/node/downwardapi.go — "should provide pod UID as an env var"
//   test/e2e/common/node/downwardapi.go — "should provide pod name/namespace as env vars"
//
// status.hostIP and status.podIP are resolved in env-var paths (resolve_downward_api_env)
// and in volume-file paths (resolve_field_ref_with_pod_ip inside pod_worker).
// The env-var path is tested here; the volume-file path is tested in the unit
// tests embedded in pod_worker::mod.rs (test_resolve_downward_api_value_host_ip_*).

/// status.hostIP env var resolves to a non-empty address.
/// Mirrors [sig-node] Downward API "should provide host IP as an env var"
#[test]
fn integ_downward_api_host_ip_env_var_non_empty() {
    let mut p = pod("uid-hostip", "hostip-pod", "default");
    p.containers[0].env = vec![EnvVar {
        name: "HOST_IP".to_string(),
        value: None,
        value_from: Some(EnvVarSource::FieldRef {
            field_path: "status.hostIP".to_string(),
        }),
    }];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert!(
        resolved.contains_key("HOST_IP"),
        "HOST_IP must be present in env"
    );
    assert!(
        !resolved["HOST_IP"].is_empty(),
        "status.hostIP must resolve to a non-empty address, got {:?}",
        resolved["HOST_IP"]
    );
}

/// metadata.uid env var resolves to the pod UID string.
/// Mirrors [sig-node] Downward API "should provide pod UID as an env var"
#[test]
fn integ_downward_api_pod_uid_env_var() {
    let mut p = pod("pod-uid-999", "uid-pod", "default");
    p.containers[0].env = vec![EnvVar {
        name: "POD_UID".to_string(),
        value: None,
        value_from: Some(EnvVarSource::FieldRef {
            field_path: "metadata.uid".to_string(),
        }),
    }];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert_eq!(resolved["POD_UID"], "pod-uid-999");
}

/// metadata.name and metadata.namespace env vars for sig-node conformance.
/// Mirrors [sig-node] Downward API "should provide pod name/namespace as env vars"
#[test]
fn integ_downward_api_pod_name_and_namespace_env_vars() {
    let mut p = pod("uid-nn", "my-workload", "kube-system");
    p.containers[0].env = vec![
        EnvVar {
            name: "POD_NAME".to_string(),
            value: None,
            value_from: Some(EnvVarSource::FieldRef {
                field_path: "metadata.name".to_string(),
            }),
        },
        EnvVar {
            name: "POD_NAMESPACE".to_string(),
            value: None,
            value_from: Some(EnvVarSource::FieldRef {
                field_path: "metadata.namespace".to_string(),
            }),
        },
    ];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert_eq!(resolved["POD_NAME"], "my-workload");
    assert_eq!(resolved["POD_NAMESPACE"], "kube-system");
}

/// status.podIP for a host-network pod must resolve to a non-empty address
/// (the node's own IP) even without a sandbox IP.
/// Mirrors [sig-node] Downward API "should provide host IP and pod IP as an env var
/// if pod uses host network [LinuxOnly]"
#[test]
fn integ_downward_api_pod_ip_host_network_falls_back_to_host_ip() {
    let mut p = pod("uid-hn", "hn-pod", "default");
    p.host_network = true;
    p.containers[0].env = vec![
        EnvVar {
            name: "POD_IP".to_string(),
            value: None,
            value_from: Some(EnvVarSource::FieldRef {
                field_path: "status.podIP".to_string(),
            }),
        },
        EnvVar {
            name: "HOST_IP".to_string(),
            value: None,
            value_from: Some(EnvVarSource::FieldRef {
                field_path: "status.hostIP".to_string(),
            }),
        },
    ];
    let resolved: HashMap<_, _> = resolve_downward_api_env(&p, &p.containers[0])
        .into_iter()
        .collect();
    assert!(
        !resolved["POD_IP"].is_empty(),
        "status.podIP for host-network pod must not be empty, got {:?}",
        resolved["POD_IP"]
    );
    assert_eq!(
        resolved["POD_IP"], resolved["HOST_IP"],
        "For host-network pods, POD_IP and HOST_IP must be the same address"
    );
}
