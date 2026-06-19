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

//! Node domain model - represents the node this kubelet manages.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Node resource capacity.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeCapacity {
    pub cpu_cores: f64,
    pub memory_bytes: u64,
    pub pods: u32,
    pub ephemeral_storage_bytes: u64,
    pub hugepages: HashMap<String, u64>,
    pub extended_resources: HashMap<String, u64>,
}

/// Node resource allocatable (capacity minus reserved).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeAllocatable {
    pub cpu_millicores: u64,
    pub memory_bytes: u64,
    pub pods: u32,
    pub ephemeral_storage_bytes: u64,
}

/// Current node conditions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCondition {
    pub condition_type: NodeConditionType,
    pub status: NodeConditionStatus,
    pub last_heartbeat_time: DateTime<Utc>,
    pub last_transition_time: DateTime<Utc>,
    pub reason: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeConditionType {
    Ready,
    MemoryPressure,
    DiskPressure,
    PIDPressure,
    NetworkUnavailable,
}

impl std::fmt::Display for NodeConditionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ready => write!(f, "Ready"),
            Self::MemoryPressure => write!(f, "MemoryPressure"),
            Self::DiskPressure => write!(f, "DiskPressure"),
            Self::PIDPressure => write!(f, "PIDPressure"),
            Self::NetworkUnavailable => write!(f, "NetworkUnavailable"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeConditionStatus {
    True,
    False,
    Unknown,
}

/// Node address (InternalIP, ExternalIP, Hostname, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAddress {
    pub address_type: NodeAddressType,
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeAddressType {
    ExternalIP,
    InternalIP,
    ExternalDNS,
    InternalDNS,
    Hostname,
}

/// Node system information.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeSystemInfo {
    pub machine_id: String,
    pub system_uuid: String,
    pub boot_id: String,
    pub kernel_version: String,
    pub os_image: String,
    pub container_runtime_version: String,
    pub kubelet_version: String,
    pub kube_proxy_version: String,
    pub operating_system: String,
    pub architecture: String,
}

/// Full node status as tracked by the kubelet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub name: String,
    pub capacity: NodeCapacity,
    pub allocatable: NodeAllocatable,
    pub conditions: Vec<NodeCondition>,
    pub addresses: Vec<NodeAddress>,
    pub system_info: NodeSystemInfo,
    pub images: Vec<NodeImage>,
    pub volumes_attached: Vec<AttachedVolume>,
    pub volumes_in_use: Vec<String>,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeImage {
    pub names: Vec<String>,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachedVolume {
    pub name: String,
    pub device_path: String,
}

impl NodeStatus {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            capacity: NodeCapacity::default(),
            allocatable: NodeAllocatable::default(),
            conditions: vec![],
            addresses: vec![],
            system_info: NodeSystemInfo::default(),
            images: vec![],
            volumes_attached: vec![],
            volumes_in_use: vec![],
            last_updated: Utc::now(),
        }
    }

    /// Returns true if the node is Ready.
    pub fn is_ready(&self) -> bool {
        self.conditions.iter().any(|c| {
            c.condition_type == NodeConditionType::Ready && c.status == NodeConditionStatus::True
        })
    }

    /// Returns true if any pressure condition is True.
    pub fn has_pressure(&self) -> bool {
        self.conditions.iter().any(|c| {
            matches!(
                c.condition_type,
                NodeConditionType::MemoryPressure
                    | NodeConditionType::DiskPressure
                    | NodeConditionType::PIDPressure
            ) && c.status == NodeConditionStatus::True
        })
    }

    /// Update or insert a condition.
    pub fn set_condition(&mut self, condition: NodeCondition) {
        if let Some(existing) = self
            .conditions
            .iter_mut()
            .find(|c| c.condition_type == condition.condition_type)
        {
            *existing = condition;
        } else {
            self.conditions.push(condition);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_condition(status: NodeConditionStatus) -> NodeCondition {
        NodeCondition {
            condition_type: NodeConditionType::Ready,
            status,
            last_heartbeat_time: Utc::now(),
            last_transition_time: Utc::now(),
            reason: "KubeletReady".to_string(),
            message: "kubelet is ready".to_string(),
        }
    }

    #[test]
    fn test_node_is_ready_when_ready_condition_true() {
        let mut node = NodeStatus::new("node1");
        node.conditions
            .push(ready_condition(NodeConditionStatus::True));
        assert!(node.is_ready());
    }

    #[test]
    fn test_node_not_ready_when_condition_false() {
        let mut node = NodeStatus::new("node1");
        node.conditions
            .push(ready_condition(NodeConditionStatus::False));
        assert!(!node.is_ready());
    }

    #[test]
    fn test_has_pressure_with_memory_pressure() {
        let mut node = NodeStatus::new("node1");
        node.conditions.push(NodeCondition {
            condition_type: NodeConditionType::MemoryPressure,
            status: NodeConditionStatus::True,
            last_heartbeat_time: Utc::now(),
            last_transition_time: Utc::now(),
            reason: "EvictionThresholdMet".to_string(),
            message: "memory threshold exceeded".to_string(),
        });
        assert!(node.has_pressure());
    }

    #[test]
    fn test_set_condition_updates_existing() {
        let mut node = NodeStatus::new("node1");
        node.set_condition(ready_condition(NodeConditionStatus::False));
        node.set_condition(ready_condition(NodeConditionStatus::True));

        let ready = node
            .conditions
            .iter()
            .find(|c| c.condition_type == NodeConditionType::Ready)
            .unwrap();
        assert_eq!(ready.status, NodeConditionStatus::True);
        assert_eq!(node.conditions.len(), 1);
    }

    #[test]
    fn test_node_condition_type_display() {
        assert_eq!(format!("{}", NodeConditionType::Ready), "Ready");
        assert_eq!(
            format!("{}", NodeConditionType::MemoryPressure),
            "MemoryPressure"
        );
    }
}
