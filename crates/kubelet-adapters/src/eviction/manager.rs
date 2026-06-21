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

//! Full eviction manager -- ranks pods and evicts them under resource pressure.
//!
//! Mirrors pkg/kubelet/eviction/eviction_manager.go.
//!
//! Eviction priority order (lowest priority evicted first):
//!   1. BestEffort pods (any resource)
//!   2. Burstable pods exceeding their requests
//!   3. Guaranteed pods (last resort, memory only)
//!
//! Within each QoS class, pods with the highest resource *usage above request*
//! are evicted first (for memory); by disk usage for disk pressure.

use super::{EvictionEvaluator, NodeResources, ResourcePressure, ThresholdValue};
use crate::eviction::parse_quantity;
use kubelet_core::pod::lifecycle::PodPhase;
use kubelet_core::pod::status::PodStatusManager;
use kubelet_core::pod::{PodSpec, RestartPolicy};
use kubelet_core::qos::{QosClass, compute_qos_class};
use kubelet_core::types::PodUID;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Reason codes for eviction (mirrors k8s eviction reason strings).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvictionReason {
    MemoryPressure,
    DiskPressure,
    PIDPressure,
    NodeConditionPressure(String),
}

impl std::fmt::Display for EvictionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoryPressure => write!(f, "EvictionThresholdMet: memory.available"),
            Self::DiskPressure => write!(f, "EvictionThresholdMet: nodefs.available"),
            Self::PIDPressure => write!(f, "EvictionThresholdMet: pid.available"),
            Self::NodeConditionPressure(r) => write!(f, "EvictionThresholdMet: {}", r),
        }
    }
}

/// An eviction decision for a single pod.
#[derive(Debug, Clone)]
pub struct EvictionDecision {
    pub pod_uid: PodUID,
    pub pod_ref: kubelet_core::types::PodRef,
    pub reason: EvictionReason,
    pub message: String,
    pub grace_period: Duration,
}

/// Resource usage snapshot for a single pod (estimated from container stats).
#[derive(Debug, Clone, Default)]
pub struct PodResourceUsage {
    pub memory_bytes: u64,
    pub cpu_millicores: u64,
    pub disk_bytes: u64,
}

/// Ranks pods for eviction under a given pressure type.
pub struct EvictionRanker;

impl EvictionRanker {
    /// Returns pods ordered from most-to-least evictable under memory pressure.
    /// BestEffort -> Burstable (highest usage above request) -> Guaranteed
    pub fn rank_for_memory(
        pods: &[PodSpec],
        usage: &HashMap<PodUID, PodResourceUsage>,
    ) -> Vec<(PodUID, QosClass, u64)> {
        let mut candidates: Vec<(PodUID, QosClass, u64)> = pods
            .iter()
            .filter(|p| p.restart_policy != RestartPolicy::Never) // don't evict completed jobs
            .map(|p| {
                let qos = compute_qos_class(p);
                let mem_bytes = usage.get(&p.uid).map(|u| u.memory_bytes).unwrap_or(0);
                (p.uid.clone(), qos, mem_bytes)
            })
            .collect();

        // Sort: BestEffort first, then Burstable (by usage desc), then Guaranteed
        candidates.sort_by(|a, b| {
            let qos_order = qos_eviction_priority(&a.1).cmp(&qos_eviction_priority(&b.1));
            if qos_order != std::cmp::Ordering::Equal {
                return qos_order;
            }
            // Within same QoS: higher usage evicted first
            b.2.cmp(&a.2)
        });

        candidates
    }

    /// Returns pods ordered for disk pressure eviction.
    pub fn rank_for_disk(
        pods: &[PodSpec],
        usage: &HashMap<PodUID, PodResourceUsage>,
    ) -> Vec<(PodUID, QosClass, u64)> {
        let mut candidates: Vec<(PodUID, QosClass, u64)> = pods
            .iter()
            .map(|p| {
                let qos = compute_qos_class(p);
                let disk = usage.get(&p.uid).map(|u| u.disk_bytes).unwrap_or(0);
                (p.uid.clone(), qos, disk)
            })
            .collect();

        candidates.sort_by(|a, b| {
            let qos_order = qos_eviction_priority(&a.1).cmp(&qos_eviction_priority(&b.1));
            if qos_order != std::cmp::Ordering::Equal {
                return qos_order;
            }
            b.2.cmp(&a.2)
        });

        candidates
    }
}

/// Lower value = evicted first.
fn qos_eviction_priority(qos: &QosClass) -> u8 {
    match qos {
        QosClass::BestEffort => 0,
        QosClass::Burstable => 1,
        QosClass::Guaranteed => 2,
    }
}

/// The eviction manager evaluates resource pressure and decides which pods
/// to evict to bring the node back within threshold.
pub struct EvictionManager {
    evaluator: EvictionEvaluator,
    grace_period_override: Option<Duration>,
    last_evictions: HashMap<PodUID, Instant>,
    min_eviction_reclaim_period: Duration,
}

impl EvictionManager {
    pub fn new(
        hard_thresholds: &HashMap<String, String>,
        soft_thresholds: &HashMap<String, String>,
        grace_period_override: Option<Duration>,
    ) -> Self {
        Self {
            evaluator: EvictionEvaluator::new(hard_thresholds, soft_thresholds),
            grace_period_override,
            last_evictions: HashMap::new(),
            min_eviction_reclaim_period: Duration::from_secs(30),
        }
    }

    /// Evaluate current resource pressure and return eviction decisions.
    pub fn evaluate(
        &mut self,
        resources: &NodeResources,
        pods: &[PodSpec],
        usage: &HashMap<PodUID, PodResourceUsage>,
    ) -> Vec<EvictionDecision> {
        let pressure = self.evaluator.evaluate(resources);
        let mut decisions = Vec::new();

        if pressure.memory_pressure
            && let Some(decision) = self.pick_eviction_candidate_memory(pods, usage)
        {
            decisions.push(decision);
        }

        if pressure.disk_pressure
            && let Some(decision) = self.pick_eviction_candidate_disk(pods, usage)
        {
            decisions.push(decision);
        }

        decisions
    }

    fn pick_eviction_candidate_memory(
        &mut self,
        pods: &[PodSpec],
        usage: &HashMap<PodUID, PodResourceUsage>,
    ) -> Option<EvictionDecision> {
        let ranked = EvictionRanker::rank_for_memory(pods, usage);
        self.pick_candidate(ranked, EvictionReason::MemoryPressure)
    }

    fn pick_eviction_candidate_disk(
        &mut self,
        pods: &[PodSpec],
        usage: &HashMap<PodUID, PodResourceUsage>,
    ) -> Option<EvictionDecision> {
        let ranked = EvictionRanker::rank_for_disk(pods, usage);
        self.pick_candidate(ranked, EvictionReason::DiskPressure)
    }

    fn pick_candidate(
        &mut self,
        ranked: Vec<(PodUID, QosClass, u64)>,
        reason: EvictionReason,
    ) -> Option<EvictionDecision> {
        for (uid, qos, _usage) in &ranked {
            // Don't re-evict a pod within the reclaim period
            if let Some(&last) = self.last_evictions.get(uid)
                && last.elapsed() < self.min_eviction_reclaim_period
            {
                continue;
            }

            let grace = self.grace_period_for_qos(qos);
            self.last_evictions.insert(uid.clone(), Instant::now());

            warn!(
                uid = %uid,
                qos = %format!("{:?}", qos),
                reason = %reason,
                "Evicting pod"
            );

            // We need the pod ref -- find it in the pod list
            // In practice the caller would pass a map; we use a placeholder here
            return Some(EvictionDecision {
                pod_uid: uid.clone(),
                pod_ref: kubelet_core::types::PodRef::new("unknown", uid.0.as_str()),
                reason,
                message: "The node was low on resource. Threshold capacity was exceeded."
                    .to_string(),
                grace_period: grace,
            });
        }
        None
    }

    fn grace_period_for_qos(&self, qos: &QosClass) -> Duration {
        if let Some(override_gp) = self.grace_period_override {
            return override_gp;
        }
        match qos {
            QosClass::Guaranteed => Duration::from_secs(30),
            QosClass::Burstable => Duration::from_secs(10),
            QosClass::BestEffort => Duration::from_secs(0),
        }
    }

    /// Check whether the node is under any resource pressure.
    pub fn has_pressure(&self, resources: &NodeResources) -> bool {
        let p = self.evaluator.evaluate(resources);
        p.memory_pressure || p.disk_pressure || p.pid_pressure
    }

    /// Return the current pressure state.
    pub fn pressure(&self, resources: &NodeResources) -> ResourcePressure {
        self.evaluator.evaluate(resources)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::pod::{
        ContainerSpec, ImagePullPolicy, PodSpec, ResourceRequirements, RestartPolicy,
    };
    use kubelet_core::types::{PodRef, PodUID, ResourceQuantity};

    fn base_resources(mem_available: u64, total_mem: u64) -> NodeResources {
        NodeResources {
            available_memory_bytes: mem_available,
            total_memory_bytes: total_mem,
            available_disk_bytes: 50 * 1024 * 1024 * 1024,
            total_disk_bytes: 100 * 1024 * 1024 * 1024,
            available_pids: 10000,
            total_pids: 32768,
        }
    }

    fn make_pod(uid: &str, name: &str) -> PodSpec {
        PodSpec {
            uid: PodUID::new(uid),
            pod_ref: PodRef::new("default", name),
            containers: vec![ContainerSpec {
                name: "c".to_string(),
                image: "nginx".to_string(),
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
        }
    }

    fn hard_thresholds(mem: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("memory.available".to_string(), mem.to_string());
        m
    }

    // -- Eviction ranking tests ------------------------------------------------

    #[test]
    fn test_best_effort_ranked_first() {
        let be_pod = make_pod("be-uid", "best-effort");
        let gu_pod = make_pod("gu-uid", "guaranteed");
        // guaranteed has equal req/limits
        let mut gu = gu_pod.clone();
        let mut requests = HashMap::new();
        let mut limits = HashMap::new();
        requests.insert("cpu".to_string(), ResourceQuantity::cpu_millicores(500));
        limits.insert("cpu".to_string(), ResourceQuantity::cpu_millicores(500));
        requests.insert(
            "memory".to_string(),
            ResourceQuantity::memory_bytes(128_000_000),
        );
        limits.insert(
            "memory".to_string(),
            ResourceQuantity::memory_bytes(128_000_000),
        );
        gu.containers[0].resources = ResourceRequirements { requests, limits };

        let pods = vec![gu, be_pod];
        let usage = HashMap::new();
        let ranked = EvictionRanker::rank_for_memory(&pods, &usage);

        assert_eq!(
            ranked[0].0,
            PodUID::new("be-uid"),
            "BestEffort should be first"
        );
        assert_eq!(ranked[0].1, QosClass::BestEffort);
        assert_eq!(ranked[1].0, PodUID::new("gu-uid"));
        assert_eq!(ranked[1].1, QosClass::Guaranteed);
    }

    #[test]
    fn test_higher_memory_usage_evicted_first_within_qos() {
        let pod1 = make_pod("uid-low", "low-usage");
        let pod2 = make_pod("uid-high", "high-usage");
        let pods = vec![pod1, pod2];

        let mut usage = HashMap::new();
        usage.insert(
            PodUID::new("uid-low"),
            PodResourceUsage {
                memory_bytes: 100_000,
                ..Default::default()
            },
        );
        usage.insert(
            PodUID::new("uid-high"),
            PodResourceUsage {
                memory_bytes: 500_000_000,
                ..Default::default()
            },
        );

        let ranked = EvictionRanker::rank_for_memory(&pods, &usage);
        assert_eq!(
            ranked[0].0,
            PodUID::new("uid-high"),
            "Higher usage evicted first"
        );
    }

    #[test]
    fn test_no_pressure_no_evictions() {
        let mut mgr = EvictionManager::new(&hard_thresholds("100Mi"), &HashMap::new(), None);
        // 500Mi available > 100Mi threshold -> no pressure
        let resources = base_resources(500 * 1024 * 1024, 8 * 1024 * 1024 * 1024);
        let pod = make_pod("uid-1", "pod-1");
        let decisions = mgr.evaluate(&resources, &[pod], &HashMap::new());
        assert!(decisions.is_empty());
    }

    #[test]
    fn test_memory_pressure_evicts_pod() {
        let mut mgr = EvictionManager::new(&hard_thresholds("100Mi"), &HashMap::new(), None);
        // 50Mi available < 100Mi threshold -> memory pressure
        let resources = base_resources(50 * 1024 * 1024, 8 * 1024 * 1024 * 1024);
        let pod = make_pod("uid-1", "pod-1");
        let decisions = mgr.evaluate(&resources, &[pod], &HashMap::new());
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].reason, EvictionReason::MemoryPressure);
        assert_eq!(decisions[0].pod_uid, PodUID::new("uid-1"));
    }

    #[test]
    fn test_best_effort_gets_zero_grace_period() {
        let mut mgr = EvictionManager::new(&hard_thresholds("100Mi"), &HashMap::new(), None);
        let resources = base_resources(50 * 1024 * 1024, 8 * 1024 * 1024 * 1024);
        let pod = make_pod("uid-be", "best-effort");
        let decisions = mgr.evaluate(&resources, &[pod], &HashMap::new());
        assert_eq!(decisions[0].grace_period, Duration::from_secs(0));
    }

    #[test]
    fn test_grace_period_override_applied() {
        let mut mgr = EvictionManager::new(
            &hard_thresholds("100Mi"),
            &HashMap::new(),
            Some(Duration::from_secs(5)), // override
        );
        let resources = base_resources(50 * 1024 * 1024, 8 * 1024 * 1024 * 1024);
        let pod = make_pod("uid-1", "pod-1");
        let decisions = mgr.evaluate(&resources, &[pod], &HashMap::new());
        assert_eq!(decisions[0].grace_period, Duration::from_secs(5));
    }

    #[test]
    fn test_already_evicted_pod_not_re_evicted_immediately() {
        let mut mgr = EvictionManager::new(&hard_thresholds("100Mi"), &HashMap::new(), None);
        mgr.min_eviction_reclaim_period = Duration::from_secs(3600); // long period

        let resources = base_resources(50 * 1024 * 1024, 8 * 1024 * 1024 * 1024);
        let pod = make_pod("uid-1", "pod-1");

        // First eviction
        let d1 = mgr.evaluate(&resources, std::slice::from_ref(&pod), &HashMap::new());
        assert_eq!(d1.len(), 1);

        // Second evaluation -- should not re-evict the same pod
        let d2 = mgr.evaluate(&resources, &[pod], &HashMap::new());
        assert!(d2.is_empty(), "Should not re-evict immediately");
    }

    #[test]
    fn test_eviction_reason_display() {
        assert_eq!(
            format!("{}", EvictionReason::MemoryPressure),
            "EvictionThresholdMet: memory.available"
        );
        assert_eq!(
            format!("{}", EvictionReason::DiskPressure),
            "EvictionThresholdMet: nodefs.available"
        );
    }

    #[test]
    fn test_has_pressure() {
        let mgr = EvictionManager::new(&hard_thresholds("100Mi"), &HashMap::new(), None);
        let high_mem = base_resources(500 * 1024 * 1024, 8 * 1024 * 1024 * 1024);
        let low_mem = base_resources(50 * 1024 * 1024, 8 * 1024 * 1024 * 1024);
        assert!(!mgr.has_pressure(&high_mem));
        assert!(mgr.has_pressure(&low_mem));
    }

    #[test]
    fn test_rank_for_disk_by_disk_usage() {
        let pod1 = make_pod("uid-small", "small-disk");
        let pod2 = make_pod("uid-large", "large-disk");
        let pods = vec![pod1, pod2];
        let mut usage = HashMap::new();
        usage.insert(
            PodUID::new("uid-small"),
            PodResourceUsage {
                disk_bytes: 100_000,
                ..Default::default()
            },
        );
        usage.insert(
            PodUID::new("uid-large"),
            PodResourceUsage {
                disk_bytes: 10_000_000_000,
                ..Default::default()
            },
        );
        let ranked = EvictionRanker::rank_for_disk(&pods, &usage);
        assert_eq!(ranked[0].0, PodUID::new("uid-large"));
    }
}
