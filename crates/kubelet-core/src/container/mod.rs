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

//! Container domain model.
//!
//! Represents the actual running state of containers as reported by the CRI.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Unique ID for a container assigned by the runtime.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContainerID(pub String);

impl ContainerID {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl std::fmt::Display for ContainerID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The runtime state of a single container as reported by CRI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeContainer {
    pub id: ContainerID,
    pub pod_uid: String,
    pub name: String,
    pub pid: Option<u32>,
    pub image: String,
    pub image_ref: String,
    pub state: RuntimeContainerState,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub exit_code: Option<i32>,
    pub exit_reason: Option<String>,
    pub labels: std::collections::HashMap<String, String>,
    /// CRI metadata attempt number — 0 on first start, incremented each restart.
    pub attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeContainerState {
    Created,
    Running,
    Exited,
    Unknown,
}

impl std::fmt::Display for RuntimeContainerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "created"),
            Self::Running => write!(f, "running"),
            Self::Exited => write!(f, "exited"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Container resource usage statistics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContainerStats {
    pub cpu_usage_nano_cores: u64,
    pub memory_usage_bytes: u64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
    pub disk_usage_bytes: u64,
    pub timestamp: Option<DateTime<Utc>>,
}

/// Image metadata returned by CRI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageInfo {
    pub id: String,
    pub repo_tags: Vec<String>,
    pub repo_digests: Vec<String>,
    pub size_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_container_id_display() {
        let id = ContainerID::new("abc123");
        assert_eq!(format!("{}", id), "abc123");
    }

    #[test]
    fn test_container_state_display() {
        assert_eq!(format!("{}", RuntimeContainerState::Running), "running");
        assert_eq!(format!("{}", RuntimeContainerState::Exited), "exited");
    }
}
