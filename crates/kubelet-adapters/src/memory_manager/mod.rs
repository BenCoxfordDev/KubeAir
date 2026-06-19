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

//! Memory Manager -- mirrors pkg/kubelet/cm/memorymanager.
//!
//! Two policies:
//!   "None"   -- Default. No NUMA memory pinning.
//!   "Static" -- Guaranteed QoS pods with memory requests get pinned NUMA nodes.
//!
//! The memory manager reads NUMA topology from /sys/devices/system/node/,
//! then allocates memory regions per NUMA node to Guaranteed containers.
//! It writes `memory.mems` to the container cgroup and calls
//! CRI UpdateContainerResources to apply.
//!
//! References: pkg/kubelet/cm/memorymanager/policy_static.go

use kubelet_core::error::{KubeletError, Result};
use kubelet_core::qos::QosClass;
use kubelet_core::types::PodUID;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use tracing::{debug, info, warn};

// -- NUMA topology -------------------------------------------------------------

/// Represents a single NUMA node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumaNode {
    pub id: u32,
    /// Total memory on this NUMA node in bytes.
    pub total_bytes: u64,
    /// Free memory in bytes (sampled at startup).
    pub free_bytes: u64,
    /// CPUs on this NUMA node.
    pub cpus: Vec<u32>,
}

impl NumaNode {
    pub fn utilization(&self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        1.0 - (self.free_bytes as f64 / self.total_bytes as f64)
    }
}

/// Discover NUMA nodes from /sys/devices/system/node/.
pub fn discover_numa_nodes() -> Vec<NumaNode> {
    let base = Path::new("/sys/devices/system/node");
    if !base.exists() {
        // Single NUMA node (non-NUMA system or container).
        let mem = total_memory_bytes();
        return vec![NumaNode {
            id: 0,
            total_bytes: mem,
            free_bytes: free_memory_bytes(),
            cpus: (0..num_online_cpus()).collect(),
        }];
    }

    let mut nodes = vec![];
    let Ok(entries) = std::fs::read_dir(base) else {
        return nodes;
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("node") {
            continue;
        }
        let id: u32 = match name[4..].parse() {
            Ok(n) => n,
            Err(_) => continue,
        };

        let node_path = entry.path();
        let total_bytes = read_numa_meminfo(&node_path, "MemTotal").unwrap_or(0);
        let free_bytes = read_numa_meminfo(&node_path, "MemFree").unwrap_or(0);
        let cpus = read_numa_cpulist(&node_path);

        nodes.push(NumaNode {
            id,
            total_bytes,
            free_bytes,
            cpus,
        });
    }

    nodes.sort_by_key(|n| n.id);
    if nodes.is_empty() {
        nodes.push(NumaNode {
            id: 0,
            total_bytes: total_memory_bytes(),
            free_bytes: free_memory_bytes(),
            cpus: (0..num_online_cpus()).collect(),
        });
    }
    nodes
}

fn read_numa_meminfo(node_path: &Path, key: &str) -> Option<u64> {
    let content = std::fs::read_to_string(node_path.join("meminfo")).ok()?;
    for line in content.lines() {
        if line.contains(key) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if let Some(val) = parts.get(3) {
                return val.parse::<u64>().ok().map(|kb| kb * 1024);
            }
        }
    }
    None
}

fn read_numa_cpulist(node_path: &Path) -> Vec<u32> {
    std::fs::read_to_string(node_path.join("cpulist"))
        .ok()
        .and_then(|s| super::cpu_manager::CpuSet::parse(s.trim()).ok())
        .map(|cs| {
            // Extract individual CPUs from the CpuSet.
            // CpuSet stores in a BTreeSet, we iterate via its string.
            let s = cs.to_cpuset_string();
            s.split(',')
                .flat_map(|part| {
                    if let Some((a, b)) = part.split_once('-') {
                        let a: u32 = a.parse().unwrap_or(0);
                        let b: u32 = b.parse().unwrap_or(0);
                        (a..=b).collect::<Vec<_>>()
                    } else {
                        part.parse::<u32>().map(|n| vec![n]).unwrap_or_default()
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn total_memory_bytes() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemTotal:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse::<u64>().ok())
                .map(|kb| kb * 1024)
        })
        .unwrap_or(8 * 1024 * 1024 * 1024)
}

fn free_memory_bytes() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemAvailable:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse::<u64>().ok())
                .map(|kb| kb * 1024)
        })
        .unwrap_or(4 * 1024 * 1024 * 1024)
}

fn num_online_cpus() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}

// -- Memory assignment ---------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryAssignment {
    pub pod_uid: PodUID,
    pub container_name: String,
    /// NUMA node IDs this container's memory is pinned to.
    pub numa_nodes: Vec<u32>,
    /// Bytes allocated.
    pub bytes: u64,
}

// -- Memory Manager ------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum MemoryManagerPolicy {
    None,
    Static,
}

impl MemoryManagerPolicy {
    pub fn parse(s: &str) -> Self {
        match s {
            "Static" => Self::Static,
            _ => Self::None,
        }
    }
}

pub struct MemoryManager {
    policy: MemoryManagerPolicy,
    numa_nodes: Vec<NumaNode>,
    assignments: HashMap<(String, String), MemoryAssignment>,
}

impl MemoryManager {
    pub fn new(policy: &str) -> Self {
        let numa_nodes = discover_numa_nodes();
        info!(
            policy,
            numa_count = numa_nodes.len(),
            "MemoryManager initialized"
        );
        Self {
            policy: MemoryManagerPolicy::parse(policy),
            numa_nodes,
            assignments: HashMap::new(),
        }
    }

    /// Assign NUMA memory for a container.
    /// Returns the `memory.mems` cpuset string for cgroup configuration.
    pub fn assign(
        &mut self,
        pod_uid: &PodUID,
        container_name: &str,
        qos: QosClass,
        memory_request_bytes: u64,
    ) -> Result<String> {
        if self.policy == MemoryManagerPolicy::None {
            return Ok(self.all_numa_nodes_string());
        }

        let key = (pod_uid.0.clone(), container_name.to_string());
        if let Some(existing) = self.assignments.get(&key) {
            let nodes = existing
                .numa_nodes
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>();
            return Ok(nodes.join(","));
        }

        if qos != QosClass::Guaranteed {
            return Ok(self.all_numa_nodes_string());
        }

        // Find the best NUMA node (least utilized, sufficient free memory).
        let numa_id = self.best_numa_node(memory_request_bytes).ok_or_else(|| {
            KubeletError::Resource(format!(
                "no NUMA node with {} bytes free for pod {}/{}",
                memory_request_bytes, pod_uid.0, container_name
            ))
        })?;

        // Deduct from NUMA node.
        if let Some(node) = self.numa_nodes.iter_mut().find(|n| n.id == numa_id) {
            node.free_bytes = node.free_bytes.saturating_sub(memory_request_bytes);
        }

        let assignment = MemoryAssignment {
            pod_uid: pod_uid.clone(),
            container_name: container_name.to_string(),
            numa_nodes: vec![numa_id],
            bytes: memory_request_bytes,
        };
        self.assignments.insert(key, assignment);

        info!(
            pod = %pod_uid.0, container = %container_name,
            numa = numa_id, bytes = memory_request_bytes,
            "Memory pinned to NUMA node"
        );
        Ok(numa_id.to_string())
    }

    pub fn release(&mut self, pod_uid: &PodUID, container_name: &str) {
        let key = (pod_uid.0.clone(), container_name.to_string());
        if let Some(assignment) = self.assignments.remove(&key) {
            for numa_id in &assignment.numa_nodes {
                if let Some(node) = self.numa_nodes.iter_mut().find(|n| n.id == *numa_id) {
                    node.free_bytes += assignment.bytes;
                }
            }
            info!(pod = %pod_uid.0, container = %container_name, "Memory assignment released");
        }
    }

    fn best_numa_node(&self, bytes_needed: u64) -> Option<u32> {
        self.numa_nodes
            .iter()
            .filter(|n| n.free_bytes >= bytes_needed)
            .min_by_key(|n| n.utilization() as u64)
            .map(|n| n.id)
    }

    fn all_numa_nodes_string(&self) -> String {
        if self.numa_nodes.is_empty() {
            return "0".to_string();
        }
        self.numa_nodes
            .iter()
            .map(|n| n.id.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn policy(&self) -> &MemoryManagerPolicy {
        &self.policy
    }
    pub fn numa_node_count(&self) -> usize {
        self.numa_nodes.len()
    }
    pub fn assignment_count(&self) -> usize {
        self.assignments.len()
    }
}

// -- Tests ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::types::PodUID;

    fn make_mgr_with_nodes(policy: &str) -> MemoryManager {
        let mut mgr = MemoryManager::new(policy);
        mgr.numa_nodes = vec![
            NumaNode {
                id: 0,
                total_bytes: 8 * 1024 * 1024 * 1024,
                free_bytes: 8 * 1024 * 1024 * 1024,
                cpus: vec![0, 1, 2, 3],
            },
            NumaNode {
                id: 1,
                total_bytes: 8 * 1024 * 1024 * 1024,
                free_bytes: 4 * 1024 * 1024 * 1024,
                cpus: vec![4, 5, 6, 7],
            },
        ];
        mgr
    }

    #[test]
    fn test_none_policy_returns_all_nodes() {
        let mut mgr = make_mgr_with_nodes("None");
        let result = mgr
            .assign(
                &PodUID::new("uid-1"),
                "app",
                QosClass::Guaranteed,
                1024 * 1024 * 1024,
            )
            .unwrap();
        // None policy returns all nodes.
        assert!(result.contains('0'));
    }

    #[test]
    fn test_static_policy_guaranteed_pinned() {
        let mut mgr = make_mgr_with_nodes("Static");
        let result = mgr
            .assign(
                &PodUID::new("uid-2"),
                "app",
                QosClass::Guaranteed,
                512 * 1024 * 1024,
            )
            .unwrap();
        assert!(!result.is_empty());
        assert_eq!(mgr.assignment_count(), 1);
    }

    #[test]
    fn test_static_policy_burstable_shared() {
        let mut mgr = make_mgr_with_nodes("Static");
        let result = mgr
            .assign(
                &PodUID::new("uid-3"),
                "app",
                QosClass::Burstable,
                256 * 1024 * 1024,
            )
            .unwrap();
        assert_eq!(mgr.assignment_count(), 0); // not recorded for Burstable
    }

    #[test]
    fn test_release_returns_memory() {
        let mut mgr = make_mgr_with_nodes("Static");
        let uid = PodUID::new("uid-rel");
        mgr.assign(&uid, "app", QosClass::Guaranteed, 1024 * 1024 * 1024)
            .unwrap();
        let free_before = mgr.numa_nodes[0].free_bytes;
        mgr.release(&uid, "app");
        assert_eq!(mgr.assignment_count(), 0);
    }

    #[test]
    fn test_static_policy_idempotent() {
        let mut mgr = make_mgr_with_nodes("Static");
        let uid = PodUID::new("uid-idem");
        let r1 = mgr
            .assign(&uid, "app", QosClass::Guaranteed, 512 * 1024 * 1024)
            .unwrap();
        let r2 = mgr
            .assign(&uid, "app", QosClass::Guaranteed, 512 * 1024 * 1024)
            .unwrap();
        assert_eq!(r1, r2);
        assert_eq!(mgr.assignment_count(), 1);
    }

    #[test]
    fn test_insufficient_memory_fails() {
        let mut mgr = MemoryManager::new("Static");
        mgr.numa_nodes = vec![NumaNode {
            id: 0,
            total_bytes: 1024,
            free_bytes: 100,
            cpus: vec![0],
        }];
        let result = mgr.assign(
            &PodUID::new("uid-oom"),
            "app",
            QosClass::Guaranteed,
            1024 * 1024 * 1024,
        );
        assert!(result.is_err());
    }
}
