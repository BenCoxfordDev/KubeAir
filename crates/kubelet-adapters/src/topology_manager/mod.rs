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

//! Topology Manager -- mirrors pkg/kubelet/cm/topologymanager.
//!
//! Coordinates NUMA-aware resource allocation across the CPU Manager,
//! Memory Manager, and Device Plugin Manager.
//!
//! Policies:
//!   "none"             -- Default. No NUMA affinity enforcement.
//!   "best-effort"      -- Try to honor NUMA hints, but never reject.
//!   "restricted"       -- Reject pods that can't get NUMA-aligned resources.
//!   "single-numa-node" -- Require all resources on a single NUMA node.
//!
//! Scopes:
//!   "container" -- Each container gets its own hint set (default).
//!   "pod"       -- All containers in a pod share one hint set.
//!
//! References: pkg/kubelet/cm/topologymanager/policy.go

use kubelet_core::error::{KubeletError, Result};
use kubelet_core::qos::QosClass;
use kubelet_core::types::PodUID;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};

// -- Topology hint -------------------------------------------------------------

/// A hint from a resource provider (CPU, memory, device) about NUMA affinity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopologyHint {
    /// NUMA node IDs that satisfy this hint. None = no preference.
    pub numa_affinity: Option<HashSet<u32>>,
    /// If true, this hint is a hard requirement. If false, preferred.
    pub preferred: bool,
}

impl TopologyHint {
    pub fn no_preference() -> Self {
        Self {
            numa_affinity: None,
            preferred: true,
        }
    }

    pub fn with_numa_nodes(nodes: impl IntoIterator<Item = u32>, preferred: bool) -> Self {
        Self {
            numa_affinity: Some(nodes.into_iter().collect()),
            preferred,
        }
    }

    /// Compute the intersection of two hints.
    pub fn intersect(&self, other: &TopologyHint) -> TopologyHint {
        match (&self.numa_affinity, &other.numa_affinity) {
            (None, None) => TopologyHint::no_preference(),
            (Some(a), None) | (None, Some(a)) => {
                TopologyHint::with_numa_nodes(a.clone(), self.preferred && other.preferred)
            }
            (Some(a), Some(b)) => {
                let intersection: HashSet<u32> = a.intersection(b).cloned().collect();
                TopologyHint::with_numa_nodes(intersection, self.preferred && other.preferred)
            }
        }
    }

    pub fn is_empty_affinity(&self) -> bool {
        self.numa_affinity
            .as_ref()
            .map(|s| s.is_empty())
            .unwrap_or(false)
    }
}

// -- Policy --------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum TopologyPolicy {
    None,
    BestEffort,
    Restricted,
    SingleNumaNode,
}

impl TopologyPolicy {
    pub fn parse(s: &str) -> Self {
        match s {
            "best-effort" => Self::BestEffort,
            "restricted" => Self::Restricted,
            "single-numa-node" => Self::SingleNumaNode,
            _ => Self::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TopologyScope {
    Container,
    Pod,
}

impl TopologyScope {
    pub fn parse(s: &str) -> Self {
        match s {
            "pod" => Self::Pod,
            _ => Self::Container,
        }
    }
}

// -- Topology Manager ---------------------------------------------------------

pub struct TopologyManager {
    policy: TopologyPolicy,
    scope: TopologyScope,
    /// Registered hint providers (resource type -> provider function name).
    /// In a real impl these are trait objects that return TopologyHint.
    providers: Vec<String>,
    /// pod -> container -> accepted hint.
    accepted_hints: HashMap<(String, String), TopologyHint>,
}

impl TopologyManager {
    pub fn new(policy: &str, scope: &str) -> Self {
        info!(policy, scope, "TopologyManager initialized");
        Self {
            policy: TopologyPolicy::parse(policy),
            scope: TopologyScope::parse(scope),
            providers: vec!["cpu".to_string(), "memory".to_string()],
            accepted_hints: HashMap::new(),
        }
    }

    /// Admit a container, computing the merged topology hint.
    ///
    /// Returns Ok(hint) if the policy allows it, Err if the policy rejects.
    pub fn admit(
        &mut self,
        pod_uid: &PodUID,
        container_name: &str,
        hints: Vec<TopologyHint>,
    ) -> Result<TopologyHint> {
        let merged = self.merge_hints(hints);

        match &self.policy {
            TopologyPolicy::None => {
                // No enforcement.
                Ok(TopologyHint::no_preference())
            }
            TopologyPolicy::BestEffort => {
                // Always admit; record the hint even if empty.
                let hint = if merged.is_empty_affinity() {
                    warn!(
                        pod = %pod_uid.0, container = %container_name,
                        "BestEffort: no NUMA alignment possible, admitting anyway"
                    );
                    TopologyHint::no_preference()
                } else {
                    merged.clone()
                };
                self.record(pod_uid, container_name, hint.clone());
                Ok(hint)
            }
            TopologyPolicy::Restricted => {
                // Reject if no aligned hint.
                if merged.is_empty_affinity() {
                    return Err(KubeletError::Admission(format!(
                        "topology restricted: no aligned NUMA resources for {}/{}",
                        pod_uid.0, container_name
                    )));
                }
                self.record(pod_uid, container_name, merged.clone());
                Ok(merged)
            }
            TopologyPolicy::SingleNumaNode => {
                // Require exactly one NUMA node.
                let single = self.single_numa_hint(&merged);
                match single {
                    Some(h) => {
                        self.record(pod_uid, container_name, h.clone());
                        Ok(h)
                    }
                    None => Err(KubeletError::Admission(format!(
                        "topology single-numa-node: cannot satisfy on one NUMA node for {}/{}",
                        pod_uid.0, container_name
                    ))),
                }
            }
        }
    }

    fn merge_hints(&self, hints: Vec<TopologyHint>) -> TopologyHint {
        hints
            .into_iter()
            .reduce(|acc, h| acc.intersect(&h))
            .unwrap_or_else(TopologyHint::no_preference)
    }

    fn single_numa_hint(&self, merged: &TopologyHint) -> Option<TopologyHint> {
        let affinity = merged.numa_affinity.as_ref()?;
        // Must have at least one node in the affinity set.
        if affinity.is_empty() {
            return None;
        }
        // Pick the first (lowest-ID) NUMA node that satisfies.
        let node = affinity.iter().min().cloned()?;
        Some(TopologyHint::with_numa_nodes([node], true))
    }

    fn record(&mut self, pod_uid: &PodUID, container_name: &str, hint: TopologyHint) {
        self.accepted_hints
            .insert((pod_uid.0.clone(), container_name.to_string()), hint);
    }

    /// Remove hint records for a pod (on deletion).
    pub fn remove_pod(&mut self, pod_uid: &PodUID) {
        self.accepted_hints.retain(|(uid, _), _| uid != &pod_uid.0);
    }

    /// Get the accepted hint for a container.
    pub fn get_hint(&self, pod_uid: &PodUID, container_name: &str) -> Option<&TopologyHint> {
        self.accepted_hints
            .get(&(pod_uid.0.clone(), container_name.to_string()))
    }

    pub fn policy(&self) -> &TopologyPolicy {
        &self.policy
    }
    pub fn scope(&self) -> &TopologyScope {
        &self.scope
    }
    pub fn admitted_count(&self) -> usize {
        self.accepted_hints.len()
    }
}

// -- Tests ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::types::PodUID;

    fn hint_nodes(nodes: &[u32], preferred: bool) -> TopologyHint {
        TopologyHint::with_numa_nodes(nodes.iter().cloned(), preferred)
    }

    #[test]
    fn test_none_policy_always_admits() {
        let mut mgr = TopologyManager::new("none", "container");
        let hints = vec![hint_nodes(&[0], true), hint_nodes(&[1], true)];
        let result = mgr.admit(&PodUID::new("uid-1"), "app", hints);
        assert!(result.is_ok());
    }

    #[test]
    fn test_best_effort_admits_even_with_empty_intersection() {
        let mut mgr = TopologyManager::new("best-effort", "container");
        let hints = vec![hint_nodes(&[0], true), hint_nodes(&[1], true)];
        // Intersection of {0} and {1} is empty -> best-effort still admits.
        let result = mgr.admit(&PodUID::new("uid-2"), "app", hints);
        assert!(result.is_ok());
    }

    #[test]
    fn test_restricted_rejects_empty_intersection() {
        let mut mgr = TopologyManager::new("restricted", "container");
        let hints = vec![hint_nodes(&[0], true), hint_nodes(&[1], true)];
        let result = mgr.admit(&PodUID::new("uid-3"), "app", hints);
        assert!(result.is_err());
    }

    #[test]
    fn test_restricted_admits_common_node() {
        let mut mgr = TopologyManager::new("restricted", "container");
        let hints = vec![hint_nodes(&[0, 1], true), hint_nodes(&[0], true)];
        let result = mgr.admit(&PodUID::new("uid-4"), "app", hints);
        assert!(result.is_ok());
        let h = result.unwrap();
        assert_eq!(
            h.numa_affinity.unwrap(),
            std::collections::HashSet::from([0u32])
        );
    }

    #[test]
    fn test_single_numa_node_picks_one() {
        let mut mgr = TopologyManager::new("single-numa-node", "container");
        let hints = vec![hint_nodes(&[0, 1], true)];
        let result = mgr.admit(&PodUID::new("uid-5"), "app", hints);
        assert!(result.is_ok());
        let h = result.unwrap();
        assert_eq!(h.numa_affinity.unwrap().len(), 1);
    }

    #[test]
    fn test_hint_intersection() {
        let h1 = hint_nodes(&[0, 1, 2], true);
        let h2 = hint_nodes(&[1, 2, 3], true);
        let merged = h1.intersect(&h2);
        let affinity = merged.numa_affinity.unwrap();
        assert!(affinity.contains(&1));
        assert!(affinity.contains(&2));
        assert!(!affinity.contains(&0));
        assert!(!affinity.contains(&3));
    }

    #[test]
    fn test_hint_intersection_with_no_preference() {
        let h1 = TopologyHint::no_preference();
        let h2 = hint_nodes(&[0], true);
        let merged = h1.intersect(&h2);
        assert_eq!(
            merged.numa_affinity.unwrap(),
            std::collections::HashSet::from([0u32])
        );
    }

    #[test]
    fn test_remove_pod_clears_hints() {
        let mut mgr = TopologyManager::new("best-effort", "container");
        let uid = PodUID::new("uid-rm");
        mgr.admit(&uid, "app", vec![TopologyHint::no_preference()])
            .unwrap();
        assert_eq!(mgr.admitted_count(), 1);
        mgr.remove_pod(&uid);
        assert_eq!(mgr.admitted_count(), 0);
    }

    #[test]
    fn test_get_hint_returns_recorded_hint() {
        let mut mgr = TopologyManager::new("restricted", "container");
        let uid = PodUID::new("uid-get");
        let hints = vec![hint_nodes(&[0], true)];
        mgr.admit(&uid, "c1", hints).unwrap();
        let h = mgr.get_hint(&uid, "c1");
        assert!(h.is_some());
    }
}
