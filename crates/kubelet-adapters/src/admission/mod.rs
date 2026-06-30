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

//! Pod admission controller -- node-local admission checks.
//!
//! Mirrors pkg/kubelet/lifecycle/predicate.go + pkg/kubelet/resource_analyzer.go.
//!
//! Checks performed before a pod is admitted:
//!   1. Node resources: CPU + memory requests <= remaining allocatable.
//!   2. Max pods: total pod count < maxPods.
//!   3. Port conflicts: no hostPort collisions with running pods.
//!   4. RuntimeClass: handler is known on this node.
//!   5. Node selector: pod selects this node (basic label match).
//!   6. Tolerations: node taints are tolerated.

use kubelet_core::error::{KubeletError, Result};
use kubelet_core::pod::{ContainerPort, ContainerSpec, PodSpec, Protocol};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};

// -- Resource accounting -------------------------------------------------------

/// Resources currently consumed by running pods.
#[derive(Debug, Clone, Default)]
pub struct ResourceUsage {
    pub cpu_millicores: u64,
    pub memory_bytes: u64,
    pub overhead_cpu_millicores: u64,
    pub overhead_memory_bytes: u64,
    pub pod_count: u32,
    /// hostPort -> pod_uid
    pub host_ports: HashMap<(u16, String), String>,
}

impl ResourceUsage {
    pub fn add_pod(&mut self, pod: &PodSpec) {
        self.add_pod_with_overhead(pod, &HashMap::new());
    }

    pub fn add_pod_with_overhead(
        &mut self,
        pod: &PodSpec,
        runtime_overheads: &HashMap<String, HashMap<String, String>>,
    ) {
        self.pod_count += 1;
        for ctr in pod.containers.iter().chain(pod.init_containers.iter()) {
            self.cpu_millicores +=
                parse_cpu_millis(ctr.resources.requests.get("cpu").map(|q| q.value));
            self.memory_bytes +=
                parse_memory_bytes(ctr.resources.requests.get("memory").map(|q| q.value));
            for port in &ctr.ports {
                if let Some(hp) = port.host_port {
                    let proto = format!("{:?}", port.protocol);
                    self.host_ports.insert((hp, proto), pod.uid.0.clone());
                }
            }
        }

        let overhead = pod_runtime_overhead(pod, runtime_overheads);
        self.overhead_cpu_millicores +=
            parse_cpu_overhead_millis(overhead.and_then(|o| o.get("cpu")));
        self.overhead_memory_bytes +=
            parse_memory_overhead_bytes(overhead.and_then(|o| o.get("memory")));
    }

    pub fn remove_pod(&mut self, pod: &PodSpec) {
        self.remove_pod_with_overhead(pod, &HashMap::new());
    }

    pub fn remove_pod_with_overhead(
        &mut self,
        pod: &PodSpec,
        runtime_overheads: &HashMap<String, HashMap<String, String>>,
    ) {
        self.pod_count = self.pod_count.saturating_sub(1);
        for ctr in pod.containers.iter().chain(pod.init_containers.iter()) {
            self.cpu_millicores = self.cpu_millicores.saturating_sub(parse_cpu_millis(
                ctr.resources.requests.get("cpu").map(|q| q.value),
            ));
            self.memory_bytes = self.memory_bytes.saturating_sub(parse_memory_bytes(
                ctr.resources.requests.get("memory").map(|q| q.value),
            ));
            for port in &ctr.ports {
                if let Some(hp) = port.host_port {
                    let proto = format!("{:?}", port.protocol);
                    self.host_ports.remove(&(hp, proto));
                }
            }
        }

        let overhead = pod_runtime_overhead(pod, runtime_overheads);
        self.overhead_cpu_millicores =
            self.overhead_cpu_millicores
                .saturating_sub(parse_cpu_overhead_millis(
                    overhead.and_then(|o| o.get("cpu")),
                ));
        self.overhead_memory_bytes =
            self.overhead_memory_bytes
                .saturating_sub(parse_memory_overhead_bytes(
                    overhead.and_then(|o| o.get("memory")),
                ));
    }
}

/// Node allocatable resources.
#[derive(Debug, Clone)]
pub struct NodeAllocatable {
    pub cpu_millicores: u64,
    pub memory_bytes: u64,
    pub max_pods: u32,
}

// -- Admission controller ------------------------------------------------------

pub struct AdmissionController {
    allocatable: NodeAllocatable,
    node_labels: HashMap<String, String>,
    node_taints: Vec<NodeTaint>,
}

#[derive(Debug, Clone)]
pub struct NodeTaint {
    pub key: String,
    pub value: Option<String>,
    pub effect: TaintEffect,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaintEffect {
    NoSchedule,
    PreferNoSchedule,
    NoExecute,
}

/// Result of an admission check.
#[derive(Debug)]
pub enum AdmissionResult {
    Admit,
    Reject(String),
}

impl AdmissionController {
    pub fn new(
        allocatable: NodeAllocatable,
        node_labels: HashMap<String, String>,
        node_taints: Vec<NodeTaint>,
    ) -> Self {
        Self {
            allocatable,
            node_labels,
            node_taints,
        }
    }

    /// Run all admission predicates for a pod.
    pub fn admit(
        &self,
        pod: &PodSpec,
        current_usage: &ResourceUsage,
        known_runtime_classes: &HashSet<String>,
    ) -> AdmissionResult {
        self.admit_with_runtime_overhead(pod, current_usage, known_runtime_classes, &HashMap::new())
    }

    pub fn admit_with_runtime_overhead(
        &self,
        pod: &PodSpec,
        current_usage: &ResourceUsage,
        known_runtime_classes: &HashSet<String>,
        runtime_overheads: &HashMap<String, HashMap<String, String>>,
    ) -> AdmissionResult {
        // 1. Max pods
        if current_usage.pod_count >= self.allocatable.max_pods {
            return AdmissionResult::Reject(format!(
                "max pods exceeded: {} / {}",
                current_usage.pod_count, self.allocatable.max_pods
            ));
        }

        // 2. CPU
        let requested_cpu = pod_cpu_millis(pod)
            + parse_cpu_overhead_millis(
                pod_runtime_overhead(pod, runtime_overheads).and_then(|o| o.get("cpu")),
            );
        let available_cpu = self
            .allocatable
            .cpu_millicores
            .saturating_sub(current_usage.cpu_millicores + current_usage.overhead_cpu_millicores);
        if requested_cpu > available_cpu {
            return AdmissionResult::Reject(format!(
                "insufficient CPU: requested {}m, available {}m",
                requested_cpu, available_cpu
            ));
        }

        // 3. Memory
        let requested_mem = pod_memory_bytes(pod)
            + parse_memory_overhead_bytes(
                pod_runtime_overhead(pod, runtime_overheads).and_then(|o| o.get("memory")),
            );
        let available_mem = self
            .allocatable
            .memory_bytes
            .saturating_sub(current_usage.memory_bytes + current_usage.overhead_memory_bytes);
        if requested_mem > available_mem {
            return AdmissionResult::Reject(format!(
                "insufficient memory: requested {} bytes, available {} bytes",
                requested_mem, available_mem
            ));
        }

        // 4. Port conflicts
        for ctr in pod.containers.iter().chain(pod.init_containers.iter()) {
            for port in &ctr.ports {
                if let Some(hp) = port.host_port {
                    let proto = format!("{:?}", port.protocol);
                    if let Some(existing_uid) = current_usage.host_ports.get(&(hp, proto.clone())) {
                        return AdmissionResult::Reject(format!(
                            "port conflict: hostPort {}/{} already in use by pod {}",
                            hp, proto, existing_uid
                        ));
                    }
                }
            }
        }

        // 5. RuntimeClass
        if let Some(rc) = &pod.runtime_class_name
            && !known_runtime_classes.contains(rc.as_str())
        {
            return AdmissionResult::Reject(format!(
                "RuntimeClass '{}' not available on this node",
                rc
            ));
        }

        // 6. Node selector
        if !pod.node_selector.is_empty() {
            for (k, v) in &pod.node_selector {
                if self.node_labels.get(k).map(|l| l.as_str()) != Some(v.as_str()) {
                    return AdmissionResult::Reject(format!(
                        "node selector mismatch: '{}={}' not on node",
                        k, v
                    ));
                }
            }
        }

        // 7. Taints + tolerations
        for taint in &self.node_taints {
            if (taint.effect == TaintEffect::NoSchedule || taint.effect == TaintEffect::NoExecute)
                && !pod_tolerates_taint(pod, taint)
            {
                return AdmissionResult::Reject(format!(
                    "pod does not tolerate taint {}={:?}:{:?}",
                    taint.key, taint.value, taint.effect
                ));
            }
        }

        // 8. Dynamic Resource Allocation claims must be allocated and prepared.
        for claim in &pod.resource_claims {
            if !claim.allocated {
                return AdmissionResult::Reject(format!(
                    "resource claim '{}' is not allocated",
                    claim.name
                ));
            }
            if !claim.prepared {
                return AdmissionResult::Reject(format!(
                    "resource claim '{}' is not prepared",
                    claim.name
                ));
            }
        }

        info!(pod = %pod.pod_ref.name, "Pod admitted");
        AdmissionResult::Admit
    }
}

fn pod_cpu_millis(pod: &PodSpec) -> u64 {
    pod.containers
        .iter()
        .chain(pod.init_containers.iter())
        .map(|c| parse_cpu_millis(c.resources.requests.get("cpu").map(|q| q.value)))
        .sum()
}

fn pod_memory_bytes(pod: &PodSpec) -> u64 {
    pod.containers
        .iter()
        .chain(pod.init_containers.iter())
        .map(|c| parse_memory_bytes(c.resources.requests.get("memory").map(|q| q.value)))
        .sum()
}

fn parse_cpu_millis(s: Option<i64>) -> u64 {
    s.map(|v| v.max(0) as u64).unwrap_or(0)
}

fn parse_memory_bytes(s: Option<i64>) -> u64 {
    s.map(|v| v.max(0) as u64).unwrap_or(0)
}

fn pod_runtime_overhead<'a>(
    pod: &PodSpec,
    runtime_overheads: &'a HashMap<String, HashMap<String, String>>,
) -> Option<&'a HashMap<String, String>> {
    pod.runtime_class_name
        .as_ref()
        .and_then(|rc| runtime_overheads.get(rc))
}

fn parse_cpu_overhead_millis(v: Option<&String>) -> u64 {
    let Some(v) = v else {
        return 0;
    };
    let s = v.trim();
    if let Some(m) = s.strip_suffix('m') {
        return m.parse::<u64>().unwrap_or(0);
    }
    s.parse::<u64>().unwrap_or(0).saturating_mul(1000)
}

fn parse_memory_overhead_bytes(v: Option<&String>) -> u64 {
    let Some(v) = v else {
        return 0;
    };
    let s = v.trim();
    if let Some(x) = s.strip_suffix("Ki") {
        return x.parse::<u64>().unwrap_or(0).saturating_mul(1024);
    }
    if let Some(x) = s.strip_suffix("Mi") {
        return x
            .parse::<u64>()
            .unwrap_or(0)
            .saturating_mul(1024)
            .saturating_mul(1024);
    }
    if let Some(x) = s.strip_suffix("Gi") {
        return x
            .parse::<u64>()
            .unwrap_or(0)
            .saturating_mul(1024)
            .saturating_mul(1024)
            .saturating_mul(1024);
    }
    s.parse::<u64>().unwrap_or(0)
}

fn pod_tolerates_taint(pod: &PodSpec, taint: &NodeTaint) -> bool {
    for toleration in &pod.tolerations {
        let key_match = toleration
            .key
            .as_deref()
            .map(|k| k == taint.key)
            .unwrap_or(true);
        if !key_match {
            continue;
        }
        let value_match = match &toleration.operator {
            kubelet_core::pod::TolerationOperator::Exists => true,
            kubelet_core::pod::TolerationOperator::Equal => toleration.value == taint.value,
        };
        if value_match {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::pod::{RestartPolicy, Toleration, TolerationOperator};
    use kubelet_core::types::{PodRef, PodUID, ResourceQuantity};
    use std::collections::HashSet;

    fn base_allocatable() -> NodeAllocatable {
        NodeAllocatable {
            cpu_millicores: 4000,
            memory_bytes: 8 * 1024 * 1024 * 1024,
            max_pods: 110,
        }
    }

    fn empty_pod(name: &str) -> PodSpec {
        PodSpec {
            uid: PodUID::new(format!("uid-{}", name)),
            pod_ref: PodRef {
                name: name.to_string(),
                namespace: "default".to_string(),
            },
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
            node_selector: HashMap::new(),
            annotations: HashMap::new(),
            labels: HashMap::new(),
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

    fn pod_with_resources(name: &str, cpu_millis: i64, mem_bytes: i64) -> PodSpec {
        let mut p = empty_pod(name);
        p.containers = vec![ContainerSpec {
            name: "app".to_string(),
            image: "nginx".to_string(),
            command: vec![],
            args: vec![],
            working_dir: None,
            ports: vec![],
            env: vec![],
            env_from: vec![],
            resources: kubelet_core::pod::ResourceRequirements {
                requests: [
                    (
                        "cpu".to_string(),
                        ResourceQuantity::cpu_millicores(cpu_millis),
                    ),
                    (
                        "memory".to_string(),
                        ResourceQuantity::memory_bytes(mem_bytes),
                    ),
                ]
                .into_iter()
                .collect(),
                limits: Default::default(),
            },
            volume_mounts: vec![],
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            lifecycle: None,
            image_pull_policy: kubelet_core::pod::ImagePullPolicy::IfNotPresent,
            security_context: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            restart_policy: Some(RestartPolicy::Always),
        }];
        p
    }

    #[test]
    fn test_admit_pod_within_resources() {
        let ctrl = AdmissionController::new(base_allocatable(), HashMap::new(), vec![]);
        let pod = pod_with_resources("app", 500, 256 * 1024 * 1024);
        let usage = ResourceUsage::default();
        let rcs = HashSet::new();
        assert!(matches!(
            ctrl.admit(&pod, &usage, &rcs),
            AdmissionResult::Admit
        ));
    }

    #[test]
    fn test_reject_pod_insufficient_cpu() {
        let ctrl = AdmissionController::new(base_allocatable(), HashMap::new(), vec![]);
        let pod = pod_with_resources("hungry", 8000, 256 * 1024 * 1024); // > 4 cores
        let usage = ResourceUsage::default();
        let rcs = HashSet::new();
        assert!(matches!(
            ctrl.admit(&pod, &usage, &rcs),
            AdmissionResult::Reject(_)
        ));
    }

    #[test]
    fn test_reject_pod_max_pods_reached() {
        let ctrl = AdmissionController::new(base_allocatable(), HashMap::new(), vec![]);
        let usage = ResourceUsage {
            pod_count: 110,
            ..Default::default()
        };
        let pod = empty_pod("overflow");
        let rcs = HashSet::new();
        assert!(matches!(
            ctrl.admit(&pod, &usage, &rcs),
            AdmissionResult::Reject(_)
        ));
    }

    #[test]
    fn test_reject_pod_port_conflict() {
        let ctrl = AdmissionController::new(base_allocatable(), HashMap::new(), vec![]);
        let mut usage = ResourceUsage::default();
        usage
            .host_ports
            .insert((8080, "TCP".to_string()), "uid-existing".to_string());

        let mut pod = empty_pod("conflict");
        pod.containers = vec![ContainerSpec {
            name: "app".to_string(),
            image: "nginx".to_string(),
            command: vec![],
            args: vec![],
            working_dir: None,
            ports: vec![ContainerPort {
                name: None,
                container_port: 80,
                host_port: Some(8080),
                protocol: Protocol::TCP,
                host_ip: None,
            }],
            env: vec![],
            env_from: vec![],
            resources: Default::default(),
            volume_mounts: vec![],
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            lifecycle: None,
            image_pull_policy: kubelet_core::pod::ImagePullPolicy::IfNotPresent,
            security_context: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            restart_policy: Some(RestartPolicy::Always),
        }];
        let rcs = HashSet::new();
        assert!(matches!(
            ctrl.admit(&pod, &usage, &rcs),
            AdmissionResult::Reject(_)
        ));
    }

    #[test]
    fn test_reject_unknown_runtime_class() {
        let ctrl = AdmissionController::new(base_allocatable(), HashMap::new(), vec![]);
        let mut pod = empty_pod("gvisor-pod");
        pod.runtime_class_name = Some("gvisor".to_string());
        let usage = ResourceUsage::default();
        let rcs = HashSet::new(); // gvisor not registered
        assert!(matches!(
            ctrl.admit(&pod, &usage, &rcs),
            AdmissionResult::Reject(_)
        ));
    }

    #[test]
    fn test_reject_node_selector_mismatch() {
        let mut labels = HashMap::new();
        labels.insert("zone".to_string(), "us-east-1a".to_string());
        let ctrl = AdmissionController::new(base_allocatable(), labels, vec![]);
        let mut pod = empty_pod("zonal");
        pod.node_selector
            .insert("zone".to_string(), "us-west-2a".to_string());
        let usage = ResourceUsage::default();
        let rcs = HashSet::new();
        assert!(matches!(
            ctrl.admit(&pod, &usage, &rcs),
            AdmissionResult::Reject(_)
        ));
    }

    #[test]
    fn test_resource_usage_add_remove() {
        let pod = pod_with_resources("app", 500, 256 * 1024 * 1024);
        let mut usage = ResourceUsage::default();
        usage.add_pod(&pod);
        assert_eq!(usage.cpu_millicores, 500);
        assert_eq!(usage.memory_bytes, 256 * 1024 * 1024);
        usage.remove_pod(&pod);
        assert_eq!(usage.cpu_millicores, 0);
    }

    #[test]
    fn test_parse_cpu_millis() {
        assert_eq!(parse_cpu_millis(Some(500)), 500);
        assert_eq!(parse_cpu_millis(Some(2000)), 2000);
        assert_eq!(parse_cpu_millis(None), 0);
    }

    #[test]
    fn test_parse_memory_bytes() {
        assert_eq!(
            parse_memory_bytes(Some(256 * 1024 * 1024)),
            256 * 1024 * 1024
        );
        assert_eq!(
            parse_memory_bytes(Some(1024 * 1024 * 1024)),
            1024 * 1024 * 1024
        );
        assert_eq!(parse_memory_bytes(Some(1024 * 1024)), 1024 * 1024);
    }

    #[test]
    fn test_reject_unprepared_dra_claim() {
        let ctrl = AdmissionController::new(base_allocatable(), HashMap::new(), vec![]);
        let mut pod = empty_pod("dra-unprepared");
        pod.resource_claims = vec![kubelet_core::pod::ResourceClaimRef {
            name: "gpu-claim".to_string(),
            resource_class_name: Some("nvidia.com/gpu".to_string()),
            allocated: true,
            prepared: false,
        }];

        let usage = ResourceUsage::default();
        let rcs = HashSet::new();
        assert!(matches!(
            ctrl.admit(&pod, &usage, &rcs),
            AdmissionResult::Reject(_)
        ));
    }

    #[test]
    fn test_admit_prepared_dra_claim() {
        let ctrl = AdmissionController::new(base_allocatable(), HashMap::new(), vec![]);
        let mut pod = empty_pod("dra-prepared");
        pod.resource_claims = vec![kubelet_core::pod::ResourceClaimRef {
            name: "gpu-claim".to_string(),
            resource_class_name: Some("nvidia.com/gpu".to_string()),
            allocated: true,
            prepared: true,
        }];

        let usage = ResourceUsage::default();
        let rcs = HashSet::new();
        assert!(matches!(
            ctrl.admit(&pod, &usage, &rcs),
            AdmissionResult::Admit
        ));
    }

    #[test]
    fn test_admission_rejects_when_runtime_overhead_exceeds_memory() {
        let ctrl = AdmissionController::new(base_allocatable(), HashMap::new(), vec![]);
        let mut pod = pod_with_resources("overhead-pod", 500, 256 * 1024 * 1024);
        pod.runtime_class_name = Some("gvisor".to_string());

        let mut rcs = HashSet::new();
        rcs.insert("gvisor".to_string());

        let usage = ResourceUsage {
            memory_bytes: 7 * 1024 * 1024 * 1024,
            ..Default::default()
        };

        let mut runtime_overheads = HashMap::new();
        runtime_overheads.insert(
            "gvisor".to_string(),
            [
                ("cpu".to_string(), "100m".to_string()),
                ("memory".to_string(), "1Gi".to_string()),
            ]
            .into_iter()
            .collect(),
        );

        assert!(matches!(
            ctrl.admit_with_runtime_overhead(&pod, &usage, &rcs, &runtime_overheads),
            AdmissionResult::Reject(_)
        ));
    }
}
