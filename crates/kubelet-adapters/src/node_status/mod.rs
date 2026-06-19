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

//! Node status controller.
//!
//! Collects node status from the OS and runtime, and reports it to the
//! API server. Mirrors pkg/kubelet/nodestatus/ and pkg/kubelet/node_manager.go.

use chrono::Utc;
use kubelet_core::error::Result;
use kubelet_core::node::{
    NodeAddress, NodeAddressType, NodeAllocatable, NodeCapacity, NodeCondition,
    NodeConditionStatus, NodeConditionType, NodeStatus, NodeSystemInfo,
};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info};

/// Collects and builds node status from system information.
pub struct NodeStatusCollector {
    node_name: String,
    max_pods: u32,
    cpu_cores: f64,
    memory_bytes: u64,
    ephemeral_storage_bytes: u64,
}

impl NodeStatusCollector {
    pub fn new(
        node_name: impl Into<String>,
        max_pods: u32,
        cpu_cores: f64,
        memory_bytes: u64,
        ephemeral_storage_bytes: u64,
    ) -> Self {
        Self {
            node_name: node_name.into(),
            max_pods,
            cpu_cores,
            memory_bytes,
            ephemeral_storage_bytes,
        }
    }

    /// Build a full NodeStatus from current system state.
    pub fn collect(&self, addresses: Vec<NodeAddress>) -> NodeStatus {
        let mut status = NodeStatus::new(&self.node_name);

        status.capacity = NodeCapacity {
            cpu_cores: self.cpu_cores,
            memory_bytes: self.memory_bytes,
            pods: self.max_pods,
            ephemeral_storage_bytes: self.ephemeral_storage_bytes,
            hugepages: HashMap::new(),
            extended_resources: HashMap::new(),
        };

        // Allocatable = capacity minus kube/system reserved (simplified: 10% for system)
        status.allocatable = NodeAllocatable {
            cpu_millicores: (self.cpu_cores * 900.0) as u64, // 90% of capacity
            memory_bytes: (self.memory_bytes as f64 * 0.9) as u64,
            pods: self.max_pods,
            ephemeral_storage_bytes: (self.ephemeral_storage_bytes as f64 * 0.9) as u64,
        };

        status.addresses = addresses;
        status.system_info = self.collect_system_info();
        status.last_updated = Utc::now();
        status
    }

    /// Build system info from the running environment.
    pub fn collect_system_info(&self) -> NodeSystemInfo {
        NodeSystemInfo {
            kubelet_version: format!("v{}", env!("CARGO_PKG_VERSION")),
            operating_system: std::env::consts::OS.to_string(),
            architecture: std::env::consts::ARCH.to_string(),
            kernel_version: Self::read_kernel_version(),
            os_image: Self::read_os_image(),
            machine_id: Self::read_machine_id(),
            system_uuid: String::new(),
            boot_id: String::new(),
            container_runtime_version: "containerd://unknown".to_string(),
            kube_proxy_version: String::new(),
        }
    }

    fn read_kernel_version() -> String {
        std::fs::read_to_string("/proc/version")
            .ok()
            .and_then(|s| s.split_whitespace().nth(2).map(String::from))
            .unwrap_or_else(|| "unknown".to_string())
    }

    fn read_os_image() -> String {
        // Try /etc/os-release
        if let Ok(content) = std::fs::read_to_string("/etc/os-release") {
            for line in content.lines() {
                if line.starts_with("PRETTY_NAME=") {
                    return line
                        .trim_start_matches("PRETTY_NAME=")
                        .trim_matches('"')
                        .to_string();
                }
            }
        }
        std::env::consts::OS.to_string()
    }

    fn read_machine_id() -> String {
        std::fs::read_to_string("/etc/machine-id")
            .ok()
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| "unknown".to_string())
    }
}

/// Derives node conditions from observed resource pressure and runtime state.
pub struct NodeConditionDeriver {
    memory_pressure: bool,
    disk_pressure: bool,
    pid_pressure: bool,
    network_unavailable: bool,
    pleg_healthy: bool,
}

impl NodeConditionDeriver {
    pub fn new(
        memory_pressure: bool,
        disk_pressure: bool,
        pid_pressure: bool,
        network_unavailable: bool,
    ) -> Self {
        Self {
            memory_pressure,
            disk_pressure,
            pid_pressure,
            network_unavailable,
            pleg_healthy: true,
        }
    }

    pub fn with_pleg_health(mut self, healthy: bool) -> Self {
        self.pleg_healthy = healthy;
        self
    }

    /// Build the standard set of node conditions.
    pub fn build_conditions(&self) -> Vec<NodeCondition> {
        let now = Utc::now();
        vec![
            NodeCondition {
                condition_type: NodeConditionType::Ready,
                status: if !self.memory_pressure
                    && !self.disk_pressure
                    && !self.pid_pressure
                    && !self.network_unavailable
                    && self.pleg_healthy
                {
                    NodeConditionStatus::True
                } else {
                    NodeConditionStatus::False
                },
                last_heartbeat_time: now,
                last_transition_time: now,
                reason: "KubeletReady".to_string(),
                message: if self.pleg_healthy {
                    "kubelet is posting ready status".to_string()
                } else {
                    "PLEG is stale".to_string()
                },
            },
            NodeCondition {
                condition_type: NodeConditionType::MemoryPressure,
                status: if self.memory_pressure {
                    NodeConditionStatus::True
                } else {
                    NodeConditionStatus::False
                },
                last_heartbeat_time: now,
                last_transition_time: now,
                reason: if self.memory_pressure {
                    "KubeletHasInsufficientMemory".to_string()
                } else {
                    "KubeletHasSufficientMemory".to_string()
                },
                message: if self.memory_pressure {
                    "kubelet has insufficient memory available".to_string()
                } else {
                    "kubelet has sufficient memory available".to_string()
                },
            },
            NodeCondition {
                condition_type: NodeConditionType::DiskPressure,
                status: if self.disk_pressure {
                    NodeConditionStatus::True
                } else {
                    NodeConditionStatus::False
                },
                last_heartbeat_time: now,
                last_transition_time: now,
                reason: if self.disk_pressure {
                    "KubeletHasDiskPressure".to_string()
                } else {
                    "KubeletHasNoDiskPressure".to_string()
                },
                message: if self.disk_pressure {
                    "kubelet has disk pressure".to_string()
                } else {
                    "kubelet has no disk pressure".to_string()
                },
            },
            NodeCondition {
                condition_type: NodeConditionType::PIDPressure,
                status: if self.pid_pressure {
                    NodeConditionStatus::True
                } else {
                    NodeConditionStatus::False
                },
                last_heartbeat_time: now,
                last_transition_time: now,
                reason: if self.pid_pressure {
                    "KubeletHasInsufficientPID".to_string()
                } else {
                    "KubeletHasSufficientPID".to_string()
                },
                message: "kubelet has sufficient PID available".to_string(),
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::node::{NodeAddressType, NodeConditionStatus, NodeConditionType};

    fn make_collector() -> NodeStatusCollector {
        NodeStatusCollector::new(
            "test-node",
            110,
            4.0,
            8 * 1024 * 1024 * 1024,
            100 * 1024 * 1024 * 1024,
        )
    }

    fn make_addresses() -> Vec<NodeAddress> {
        vec![
            NodeAddress {
                address_type: NodeAddressType::InternalIP,
                address: "10.0.0.1".to_string(),
            },
            NodeAddress {
                address_type: NodeAddressType::Hostname,
                address: "test-node".to_string(),
            },
        ]
    }

    #[test]
    fn test_collect_sets_capacity() {
        let c = make_collector();
        let status = c.collect(make_addresses());
        assert_eq!(status.capacity.cpu_cores, 4.0);
        assert_eq!(status.capacity.memory_bytes, 8 * 1024 * 1024 * 1024);
        assert_eq!(status.capacity.pods, 110);
    }

    #[test]
    fn test_collect_sets_allocatable_less_than_capacity() {
        let c = make_collector();
        let status = c.collect(make_addresses());
        assert!(status.allocatable.memory_bytes < status.capacity.memory_bytes);
        assert!(status.allocatable.cpu_millicores < (status.capacity.cpu_cores * 1000.0) as u64);
    }

    #[test]
    fn test_collect_populates_addresses() {
        let c = make_collector();
        let status = c.collect(make_addresses());
        assert_eq!(status.addresses.len(), 2);
    }

    #[test]
    fn test_collect_sets_system_info() {
        let c = make_collector();
        let status = c.collect(make_addresses());
        assert!(!status.system_info.kubelet_version.is_empty());
        assert!(!status.system_info.operating_system.is_empty());
    }

    #[test]
    fn test_condition_deriver_no_pressure_ready() {
        let deriver = NodeConditionDeriver::new(false, false, false, false);
        let conditions = deriver.build_conditions();
        let ready = conditions
            .iter()
            .find(|c| c.condition_type == NodeConditionType::Ready)
            .unwrap();
        assert_eq!(ready.status, NodeConditionStatus::True);
    }

    #[test]
    fn test_condition_deriver_memory_pressure_not_ready() {
        let deriver = NodeConditionDeriver::new(true, false, false, false);
        let conditions = deriver.build_conditions();
        let ready = conditions
            .iter()
            .find(|c| c.condition_type == NodeConditionType::Ready)
            .unwrap();
        assert_eq!(ready.status, NodeConditionStatus::False);
        let mem = conditions
            .iter()
            .find(|c| c.condition_type == NodeConditionType::MemoryPressure)
            .unwrap();
        assert_eq!(mem.status, NodeConditionStatus::True);
    }

    #[test]
    fn test_condition_deriver_pleg_stale_not_ready() {
        let deriver = NodeConditionDeriver::new(false, false, false, false).with_pleg_health(false);
        let conditions = deriver.build_conditions();
        let ready = conditions
            .iter()
            .find(|c| c.condition_type == NodeConditionType::Ready)
            .unwrap();
        assert_eq!(ready.status, NodeConditionStatus::False);
    }

    #[test]
    fn test_condition_deriver_disk_pressure() {
        let deriver = NodeConditionDeriver::new(false, true, false, false);
        let conditions = deriver.build_conditions();
        let disk = conditions
            .iter()
            .find(|c| c.condition_type == NodeConditionType::DiskPressure)
            .unwrap();
        assert_eq!(disk.status, NodeConditionStatus::True);
    }

    #[test]
    fn test_condition_deriver_all_good_no_pressure() {
        let deriver = NodeConditionDeriver::new(false, false, false, false);
        let conditions = deriver.build_conditions();
        for cond in &conditions {
            match cond.condition_type {
                NodeConditionType::Ready => assert_eq!(cond.status, NodeConditionStatus::True),
                _ => assert_eq!(cond.status, NodeConditionStatus::False),
            }
        }
    }

    #[test]
    fn test_node_status_has_last_updated() {
        let c = make_collector();
        let before = Utc::now();
        let status = c.collect(make_addresses());
        assert!(status.last_updated >= before);
    }
}
