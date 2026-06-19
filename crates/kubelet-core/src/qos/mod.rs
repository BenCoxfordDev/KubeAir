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

//! Pod Quality of Service (QoS) classification.
//!
//! Mirrors k8s.io/api/core/v1/helper.GetPodQOS.

use crate::pod::{ContainerSpec, PodSpec};
use serde::{Deserialize, Serialize};

/// Pod QoS class as defined by Kubernetes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QosClass {
    /// All containers have CPU and memory requests == limits (non-zero).
    Guaranteed,
    /// At least one container has a request or limit but not Guaranteed.
    Burstable,
    /// No container has requests or limits.
    BestEffort,
}

impl std::fmt::Display for QosClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Guaranteed => write!(f, "Guaranteed"),
            Self::Burstable => write!(f, "Burstable"),
            Self::BestEffort => write!(f, "BestEffort"),
        }
    }
}

/// Compute the QoS class for a pod.
pub fn compute_qos_class(pod: &PodSpec) -> QosClass {
    let all_containers: Vec<&ContainerSpec> = pod
        .containers
        .iter()
        .chain(pod.init_containers.iter())
        .collect();

    if all_containers.is_empty() {
        return QosClass::BestEffort;
    }

    let any_has_resources = all_containers
        .iter()
        .any(|c| !c.resources.requests.is_empty() || !c.resources.limits.is_empty());

    if !any_has_resources {
        return QosClass::BestEffort;
    }

    // Guaranteed: every container has CPU+memory requests AND limits, and requests == limits
    let all_guaranteed = all_containers.iter().all(|c| {
        let cpu_req = c.resources.requests.get("cpu");
        let cpu_lim = c.resources.limits.get("cpu");
        let mem_req = c.resources.requests.get("memory");
        let mem_lim = c.resources.limits.get("memory");

        match (cpu_req, cpu_lim, mem_req, mem_lim) {
            (Some(cr), Some(cl), Some(mr), Some(ml)) => cr == cl && mr == ml,
            _ => false,
        }
    });

    if all_guaranteed {
        QosClass::Guaranteed
    } else {
        QosClass::Burstable
    }
}

/// OOM score adjustment for a QoS class.
/// Lower OOM score = less likely to be OOM killed.
pub fn oom_score_adj(qos: &QosClass) -> i32 {
    match qos {
        QosClass::Guaranteed => -997,
        QosClass::Burstable => 0, // calculated per-container in practice
        QosClass::BestEffort => 1000,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pod::ResourceRequirements;
    use crate::pod::{ImagePullPolicy, RestartPolicy};
    use crate::types::{PodRef, PodUID, ResourceQuantity};
    use std::collections::HashMap;

    fn base_pod() -> PodSpec {
        PodSpec {
            uid: PodUID::new("qos-uid"),
            pod_ref: PodRef::new("default", "qos-pod"),
            containers: vec![],
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
        }
    }

    fn make_container(
        cpu_req: Option<i64>,
        cpu_lim: Option<i64>,
        mem_req: Option<i64>,
        mem_lim: Option<i64>,
    ) -> ContainerSpec {
        let mut requests = HashMap::new();
        let mut limits = HashMap::new();
        if let Some(v) = cpu_req {
            requests.insert("cpu".to_string(), ResourceQuantity::cpu_millicores(v));
        }
        if let Some(v) = cpu_lim {
            limits.insert("cpu".to_string(), ResourceQuantity::cpu_millicores(v));
        }
        if let Some(v) = mem_req {
            requests.insert("memory".to_string(), ResourceQuantity::memory_bytes(v));
        }
        if let Some(v) = mem_lim {
            limits.insert("memory".to_string(), ResourceQuantity::memory_bytes(v));
        }

        ContainerSpec {
            name: "c".to_string(),
            image: "nginx".to_string(),
            command: vec![],
            args: vec![],
            working_dir: None,
            ports: vec![],
            env: vec![],
            resources: ResourceRequirements { requests, limits },
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
        }
    }

    #[test]
    fn test_best_effort_with_no_resources() {
        let mut pod = base_pod();
        pod.containers = vec![make_container(None, None, None, None)];
        assert_eq!(compute_qos_class(&pod), QosClass::BestEffort);
    }

    #[test]
    fn test_guaranteed_with_equal_requests_and_limits() {
        let mut pod = base_pod();
        pod.containers = vec![make_container(
            Some(500),
            Some(500),
            Some(128_000_000),
            Some(128_000_000),
        )];
        assert_eq!(compute_qos_class(&pod), QosClass::Guaranteed);
    }

    #[test]
    fn test_burstable_with_different_request_and_limit() {
        let mut pod = base_pod();
        pod.containers = vec![make_container(
            Some(250),
            Some(500),
            Some(64_000_000),
            Some(128_000_000),
        )];
        assert_eq!(compute_qos_class(&pod), QosClass::Burstable);
    }

    #[test]
    fn test_best_effort_empty_pod() {
        let pod = base_pod();
        assert_eq!(compute_qos_class(&pod), QosClass::BestEffort);
    }

    #[test]
    fn test_oom_score_guaranteed() {
        assert_eq!(oom_score_adj(&QosClass::Guaranteed), -997);
    }

    #[test]
    fn test_oom_score_best_effort() {
        assert_eq!(oom_score_adj(&QosClass::BestEffort), 1000);
    }

    #[test]
    fn test_qos_class_display() {
        assert_eq!(format!("{}", QosClass::Guaranteed), "Guaranteed");
        assert_eq!(format!("{}", QosClass::BestEffort), "BestEffort");
    }
}
