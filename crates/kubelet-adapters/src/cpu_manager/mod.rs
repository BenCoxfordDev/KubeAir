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

//! CPU Manager -- mirrors pkg/kubelet/cm/cpumanager.
//!
//! Two policies:
//!   "none"   -- Default. No explicit cpuset pinning; containers float across all CPUs.
//!   "static" -- Guaranteed QoS pods with integer CPU requests get exclusive CPUs.
//!
//! The static policy maintains a CPU pool:
//!   - Reserved CPUs (kube-reserved + system-reserved) are removed from the pool.
//!   - Guaranteed pods with integer requests are assigned dedicated CPUs.
//!   - All other containers (Burstable/BestEffort) share the remaining "default pool".
//!
//! After assignment, `cpuset` is written via CgroupManager (cpu.cpuset) and
//! updated via CRI UpdateContainerResources.
//!
//! References:
//!   pkg/kubelet/cm/cpumanager/policy_static.go
//!   pkg/kubelet/cm/cpumanager/cpu_assignment.go

use kubelet_core::error::{KubeletError, Result};
use kubelet_core::qos::QosClass;
use kubelet_core::types::PodUID;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use tracing::{debug, info, warn};

// -- CPU set -------------------------------------------------------------------

/// A set of logical CPU IDs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CpuSet(BTreeSet<u32>);

impl CpuSet {
    pub fn new(cpus: impl IntoIterator<Item = u32>) -> Self {
        Self(cpus.into_iter().collect())
    }

    pub fn from_range(start: u32, end: u32) -> Self {
        Self((start..=end).collect())
    }

    /// Parse a Linux cpuset string like "0-3,6,8-10".
    pub fn parse(s: &str) -> Result<Self> {
        let mut set = BTreeSet::new();
        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            if let Some((a, b)) = part.split_once('-') {
                let a: u32 = a.trim().parse().map_err(|_| {
                    KubeletError::Config(format!("invalid cpuset range start '{}'", a))
                })?;
                let b: u32 = b.trim().parse().map_err(|_| {
                    KubeletError::Config(format!("invalid cpuset range end '{}'", b))
                })?;
                for cpu in a..=b {
                    set.insert(cpu);
                }
            } else {
                let cpu: u32 = part
                    .parse()
                    .map_err(|_| KubeletError::Config(format!("invalid cpu id '{}'", part)))?;
                set.insert(cpu);
            }
        }
        Ok(Self(set))
    }

    /// Format as a Linux cpuset string, collapsing ranges.
    pub fn to_cpuset_string(&self) -> String {
        if self.0.is_empty() {
            return String::new();
        }
        let mut parts: Vec<String> = vec![];
        let mut iter = self.0.iter().peekable();
        let mut range_start = *iter.next().unwrap();
        let mut range_end = range_start;

        for &cpu in iter {
            if cpu == range_end + 1 {
                range_end = cpu;
            } else {
                if range_start == range_end {
                    parts.push(range_start.to_string());
                } else {
                    parts.push(format!("{}-{}", range_start, range_end));
                }
                range_start = cpu;
                range_end = cpu;
            }
        }
        if range_start == range_end {
            parts.push(range_start.to_string());
        } else {
            parts.push(format!("{}-{}", range_start, range_end));
        }
        parts.join(",")
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn contains(&self, cpu: u32) -> bool {
        self.0.contains(&cpu)
    }

    /// Remove `count` CPUs from this set and return them.
    pub fn take(&mut self, count: usize) -> Option<CpuSet> {
        if self.0.len() < count {
            return None;
        }
        let taken: BTreeSet<u32> = self.0.iter().take(count).cloned().collect();
        for cpu in &taken {
            self.0.remove(cpu);
        }
        Some(CpuSet(taken))
    }

    /// Union of two sets.
    pub fn union(&self, other: &CpuSet) -> CpuSet {
        CpuSet(self.0.union(&other.0).cloned().collect())
    }

    /// Difference: self - other.
    pub fn difference(&self, other: &CpuSet) -> CpuSet {
        CpuSet(self.0.difference(&other.0).cloned().collect())
    }
}

// -- Policy --------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum CpuManagerPolicy {
    None,
    Static,
}

impl CpuManagerPolicy {
    pub fn parse(s: &str) -> Self {
        match s {
            "static" => Self::Static,
            _ => Self::None,
        }
    }
}

// -- Container assignment ------------------------------------------------------

/// CPUs assigned to a specific container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuAssignment {
    pub pod_uid: PodUID,
    pub container_name: String,
    pub cpus: CpuSet,
    pub exclusive: bool,
}

// -- CPU Manager ---------------------------------------------------------------

/// Manages CPU assignments for containers on this node.
pub struct CpuManager {
    policy: CpuManagerPolicy,
    /// All CPUs on the node.
    total_cpus: CpuSet,
    /// CPUs available for Guaranteed exclusive allocation.
    available_cpus: CpuSet,
    /// CPUs reserved for kube/system daemons.
    reserved_cpus: CpuSet,
    /// Exclusive assignments: (pod_uid, container_name) -> CpuSet.
    assignments: HashMap<(String, String), CpuAssignment>,
}

impl CpuManager {
    /// Create a CpuManager.
    ///
    /// `total_cpus`    -- all logical CPUs (e.g., CpuSet::from_range(0, 15)).
    /// `reserved_cpus` -- CPUs to exclude from allocation (kube-reserved + system-reserved).
    /// `policy`        -- "none" or "static".
    pub fn new(policy: &str, total_cpus: CpuSet, reserved_cpus: CpuSet) -> Self {
        let available_cpus = total_cpus.difference(&reserved_cpus);
        info!(
            policy,
            total = total_cpus.len(),
            reserved = reserved_cpus.len(),
            available = available_cpus.len(),
            "CpuManager initialized"
        );
        Self {
            policy: CpuManagerPolicy::parse(policy),
            available_cpus,
            reserved_cpus,
            total_cpus,
            assignments: HashMap::new(),
        }
    }

    /// Detect the number of logical CPUs on this host.
    pub fn detect_host_cpus() -> CpuSet {
        let count = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1);
        CpuSet::from_range(0, count - 1)
    }

    /// Assign CPUs to a container.
    ///
    /// For the `static` policy:
    ///   - Guaranteed QoS + integer CPU request -> exclusive CPUs from the pool.
    ///   - All others -> shared pool (all non-exclusive CPUs).
    pub fn assign(
        &mut self,
        pod_uid: &PodUID,
        container_name: &str,
        qos: QosClass,
        cpu_request_millicores: u64,
    ) -> Result<CpuSet> {
        if self.policy == CpuManagerPolicy::None {
            return Ok(self.total_cpus.clone());
        }

        let key = (pod_uid.0.clone(), container_name.to_string());

        // Already assigned.
        if let Some(existing) = self.assignments.get(&key) {
            return Ok(existing.cpus.clone());
        }

        let cpus = if qos == QosClass::Guaranteed && cpu_request_millicores.is_multiple_of(1000) {
            // Exclusive allocation.
            let num_cpus = (cpu_request_millicores / 1000) as usize;
            let exclusive = self.available_cpus.take(num_cpus).ok_or_else(|| {
                KubeletError::Resource(format!(
                    "not enough CPUs: need {}, available {}",
                    num_cpus,
                    self.available_cpus.len()
                ))
            })?;
            info!(
                pod = %pod_uid.0, container = %container_name,
                cpus = %exclusive.to_cpuset_string(),
                "Static CPU allocation: exclusive"
            );
            self.assignments.insert(
                key,
                CpuAssignment {
                    pod_uid: pod_uid.clone(),
                    container_name: container_name.to_string(),
                    cpus: exclusive.clone(),
                    exclusive: true,
                },
            );
            exclusive
        } else {
            // Shared pool: all CPUs not exclusively allocated.
            let shared = self.shared_pool();
            debug!(
                pod = %pod_uid.0, container = %container_name,
                pool = %shared.to_cpuset_string(),
                "Static CPU allocation: shared pool"
            );
            shared
        };

        Ok(cpus)
    }

    /// Release exclusive CPUs when a container exits.
    pub fn release(&mut self, pod_uid: &PodUID, container_name: &str) {
        let key = (pod_uid.0.clone(), container_name.to_string());
        if let Some(assignment) = self.assignments.remove(&key) {
            if assignment.exclusive {
                info!(
                    pod = %pod_uid.0, container = %container_name,
                    cpus = %assignment.cpus.to_cpuset_string(),
                    "Releasing exclusive CPUs"
                );
                self.available_cpus = self.available_cpus.union(&assignment.cpus);
            }
        }
    }

    /// The shared CPU pool: total CPUs minus any exclusively assigned ones.
    pub fn shared_pool(&self) -> CpuSet {
        let exclusively_assigned: CpuSet = self
            .assignments
            .values()
            .filter(|a| a.exclusive)
            .fold(CpuSet::default(), |acc, a| acc.union(&a.cpus));
        self.total_cpus.difference(&exclusively_assigned)
    }

    pub fn policy(&self) -> &CpuManagerPolicy {
        &self.policy
    }
    pub fn available_count(&self) -> usize {
        self.available_cpus.len()
    }
    pub fn reserved_count(&self) -> usize {
        self.reserved_cpus.len()
    }
    pub fn assignment_count(&self) -> usize {
        self.assignments.len()
    }
    pub fn total_count(&self) -> usize {
        self.total_cpus.len()
    }

    /// Persist state to a checkpoint file (mirrors cpumanager_state_file.go).
    pub fn save_checkpoint(&self, path: &std::path::Path) -> Result<()> {
        let state = serde_json::json!({
            "policyName": format!("{:?}", self.policy).to_lowercase(),
            "cpuAssignments": self.assignments.values().collect::<Vec<_>>(),
            "defaultCpuSet": self.total_cpus.difference(&{
                self.assignments.values()
                    .filter(|a| a.exclusive)
                    .fold(CpuSet::default(), |acc, a| acc.union(&a.cpus))
            }).to_cpuset_string()
        });
        std::fs::write(path, serde_json::to_string_pretty(&state).unwrap())
            .map_err(|e| KubeletError::Config(format!("save cpumanager checkpoint: {}", e)))
    }

    /// Load state from a checkpoint file.
    pub fn load_checkpoint(path: &std::path::Path) -> Option<serde_json::Value> {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
    }
}

// -- Tests ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::types::PodUID;

    #[test]
    fn test_cpuset_parse_single() {
        let cs = CpuSet::parse("0").unwrap();
        assert_eq!(cs.len(), 1);
        assert!(cs.contains(0));
    }

    #[test]
    fn test_cpuset_parse_range() {
        let cs = CpuSet::parse("0-3").unwrap();
        assert_eq!(cs.len(), 4);
        assert!(cs.contains(2));
    }

    #[test]
    fn test_cpuset_parse_mixed() {
        let cs = CpuSet::parse("0-1,4,6-7").unwrap();
        assert_eq!(cs.len(), 5);
        assert!(cs.contains(4));
        assert!(!cs.contains(2));
    }

    #[test]
    fn test_cpuset_to_string_single() {
        let cs = CpuSet::new([5]);
        assert_eq!(cs.to_cpuset_string(), "5");
    }

    #[test]
    fn test_cpuset_to_string_range() {
        let cs = CpuSet::from_range(0, 3);
        assert_eq!(cs.to_cpuset_string(), "0-3");
    }

    #[test]
    fn test_cpuset_to_string_mixed() {
        let cs = CpuSet::new([0, 1, 4, 6, 7]);
        assert_eq!(cs.to_cpuset_string(), "0-1,4,6-7");
    }

    #[test]
    fn test_cpuset_take() {
        let mut pool = CpuSet::from_range(0, 7);
        let taken = pool.take(2).unwrap();
        assert_eq!(taken.len(), 2);
        assert_eq!(pool.len(), 6);
    }

    #[test]
    fn test_cpuset_take_too_many_returns_none() {
        let mut pool = CpuSet::from_range(0, 1);
        assert!(pool.take(5).is_none());
    }

    #[test]
    fn test_none_policy_returns_all_cpus() {
        let total = CpuSet::from_range(0, 7);
        let mut mgr = CpuManager::new("none", total.clone(), CpuSet::new([]));
        let assigned = mgr
            .assign(&PodUID::new("uid-1"), "nginx", QosClass::BestEffort, 250)
            .unwrap();
        assert_eq!(assigned.len(), 8);
    }

    #[test]
    fn test_static_policy_guaranteed_exclusive() {
        let total = CpuSet::from_range(0, 7);
        let reserved = CpuSet::new([0, 1]); // reserve 2 CPUs
        let mut mgr = CpuManager::new("static", total, reserved);
        // Guaranteed + 2000m (2 full CPUs) -> exclusive
        let assigned = mgr
            .assign(&PodUID::new("uid-g"), "app", QosClass::Guaranteed, 2000)
            .unwrap();
        assert_eq!(assigned.len(), 2);
        assert!(mgr.assignment_count() == 1);
        assert_eq!(mgr.available_count(), 4); // 8 - 2 reserved - 2 exclusive
    }

    #[test]
    fn test_static_policy_burstable_shares_pool() {
        let total = CpuSet::from_range(0, 7);
        let mut mgr = CpuManager::new("static", total.clone(), CpuSet::new([]));
        let assigned = mgr
            .assign(&PodUID::new("uid-b"), "app", QosClass::Burstable, 500)
            .unwrap();
        // Burstable gets the shared pool (all CPUs when nothing is exclusive)
        assert_eq!(assigned.len(), 8);
        assert_eq!(mgr.assignment_count(), 0); // not recorded
    }

    #[test]
    fn test_static_policy_release_returns_cpus() {
        let total = CpuSet::from_range(0, 7);
        let mut mgr = CpuManager::new("static", total, CpuSet::new([]));
        let uid = PodUID::new("uid-rel");
        mgr.assign(&uid, "app", QosClass::Guaranteed, 2000).unwrap();
        assert_eq!(mgr.available_count(), 6);
        mgr.release(&uid, "app");
        assert_eq!(mgr.available_count(), 8);
    }

    #[test]
    fn test_static_policy_insufficient_cpus_errors() {
        let total = CpuSet::from_range(0, 1); // only 2 CPUs
        let mut mgr = CpuManager::new("static", total, CpuSet::new([]));
        let result = mgr.assign(
            &PodUID::new("uid-big"),
            "app",
            QosClass::Guaranteed,
            4000, // wants 4 CPUs
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_cpuset_roundtrip_parse_format() {
        let original = "0-1,4,6-7";
        let cs = CpuSet::parse(original).unwrap();
        assert_eq!(cs.to_cpuset_string(), original);
    }

    #[test]
    fn test_save_checkpoint() {
        let dir = tempfile::TempDir::new().unwrap();
        let total = CpuSet::from_range(0, 3);
        let mgr = CpuManager::new("static", total, CpuSet::new([]));
        let path = dir.path().join("cpu_manager_state");
        mgr.save_checkpoint(&path).unwrap();
        assert!(path.exists());
        let loaded = CpuManager::load_checkpoint(&path);
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap()["policyName"], "static");
    }
}
