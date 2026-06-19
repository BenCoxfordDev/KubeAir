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

//! RuntimeClass handler -- mirrors pkg/kubelet/runtimeclass.
//!
//! RuntimeClass allows pods to select a container runtime handler
//! (e.g. gVisor/runsc, Kata Containers/kata-qemu).
//!
//! The kubelet:
//!   1. Watches RuntimeClass objects via the API server.
//!   2. When a pod has spec.runtimeClassName set, looks it up.
//!   3. Passes the handler name to CRI RunPodSandbox.
//!   4. Applies scheduling overhead from RuntimeClass.overhead.
//!
//! References: pkg/kubelet/runtimeclass/runtimeclass_manager.go

use kubelet_core::error::{KubeletError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};

// -- RuntimeClass types --------------------------------------------------------

/// A registered RuntimeClass.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeClass {
    pub name: String,
    /// The CRI handler string (e.g. "runsc", "kata-qemu", "runc").
    pub handler: String,
    /// Resource overhead for pods using this RuntimeClass.
    pub overhead: Option<RuntimeClassOverhead>,
    /// Scheduling constraints (node selector, tolerations).
    pub scheduling: Option<RuntimeClassScheduling>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeClassOverhead {
    /// Additional resource overhead (added to pod requests).
    pub pod_fixed: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeClassScheduling {
    pub node_selector: HashMap<String, String>,
    pub tolerations: Vec<RuntimeClassToleration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeClassToleration {
    pub key: String,
    pub operator: String,
    pub value: Option<String>,
    pub effect: Option<String>,
}

// -- RuntimeClass manager ------------------------------------------------------

pub struct RuntimeClassManager {
    /// name -> RuntimeClass
    classes: HashMap<String, RuntimeClass>,
}

impl RuntimeClassManager {
    pub fn new() -> Self {
        let mut mgr = Self {
            classes: HashMap::new(),
        };
        // Register the default runc handler.
        mgr.register(RuntimeClass {
            name: "runc".to_string(),
            handler: "runc".to_string(),
            overhead: None,
            scheduling: None,
        });
        mgr
    }

    /// Register or update a RuntimeClass (from API server watch events).
    pub fn register(&mut self, rc: RuntimeClass) {
        info!(name = %rc.name, handler = %rc.handler, "RuntimeClass registered");
        self.classes.insert(rc.name.clone(), rc);
    }

    /// Remove a RuntimeClass (deleted from API server).
    pub fn remove(&mut self, name: &str) {
        if self.classes.remove(name).is_some() {
            info!(name = %name, "RuntimeClass removed");
        }
    }

    /// Look up the CRI handler for a RuntimeClass name.
    /// Returns "runc" (default) when class_name is None.
    pub fn handler_for(&self, class_name: Option<&str>) -> Result<String> {
        match class_name {
            None => Ok("runc".to_string()),
            Some(name) => self
                .classes
                .get(name)
                .map(|rc| rc.handler.clone())
                .ok_or_else(|| {
                    KubeletError::Config(format!(
                        "unknown RuntimeClass '{}': not registered on this node",
                        name
                    ))
                }),
        }
    }

    /// Compute the total resource overhead for a pod using this RuntimeClass.
    pub fn overhead_for(&self, class_name: Option<&str>) -> HashMap<String, String> {
        class_name
            .and_then(|n| self.classes.get(n))
            .and_then(|rc| rc.overhead.as_ref())
            .map(|o| o.pod_fixed.clone())
            .unwrap_or_default()
    }

    pub fn class_count(&self) -> usize {
        self.classes.len()
    }

    /// Get all registered runtime class names.
    pub fn list_names(&self) -> Vec<&str> {
        self.classes.keys().map(|s| s.as_str()).collect()
    }

    /// Check if this node supports a given RuntimeClass (has the handler).
    pub fn supports(&self, class_name: &str) -> bool {
        self.classes.contains_key(class_name)
    }
}

impl Default for RuntimeClassManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gvisor_class() -> RuntimeClass {
        RuntimeClass {
            name: "gvisor".to_string(),
            handler: "runsc".to_string(),
            overhead: Some(RuntimeClassOverhead {
                pod_fixed: [
                    ("cpu".to_string(), "100m".to_string()),
                    ("memory".to_string(), "60Mi".to_string()),
                ]
                .into_iter()
                .collect(),
            }),
            scheduling: None,
        }
    }

    #[test]
    fn test_default_handler_is_runc() {
        let mgr = RuntimeClassManager::new();
        assert_eq!(mgr.handler_for(None).unwrap(), "runc");
    }

    #[test]
    fn test_known_class_returns_handler() {
        let mut mgr = RuntimeClassManager::new();
        mgr.register(gvisor_class());
        assert_eq!(mgr.handler_for(Some("gvisor")).unwrap(), "runsc");
    }

    #[test]
    fn test_unknown_class_returns_error() {
        let mgr = RuntimeClassManager::new();
        assert!(mgr.handler_for(Some("nonexistent")).is_err());
    }

    #[test]
    fn test_overhead_for_class_with_overhead() {
        let mut mgr = RuntimeClassManager::new();
        mgr.register(gvisor_class());
        let overhead = mgr.overhead_for(Some("gvisor"));
        assert_eq!(overhead["cpu"], "100m");
        assert_eq!(overhead["memory"], "60Mi");
    }

    #[test]
    fn test_overhead_for_none_class_is_empty() {
        let mgr = RuntimeClassManager::new();
        assert!(mgr.overhead_for(None).is_empty());
    }

    #[test]
    fn test_register_and_remove() {
        let mut mgr = RuntimeClassManager::new();
        mgr.register(gvisor_class());
        assert!(mgr.supports("gvisor"));
        mgr.remove("gvisor");
        assert!(!mgr.supports("gvisor"));
    }

    #[test]
    fn test_class_count() {
        let mut mgr = RuntimeClassManager::new();
        assert_eq!(mgr.class_count(), 1); // runc default
        mgr.register(gvisor_class());
        assert_eq!(mgr.class_count(), 2);
    }

    #[test]
    fn test_kata_containers() {
        let mut mgr = RuntimeClassManager::new();
        mgr.register(RuntimeClass {
            name: "kata".to_string(),
            handler: "kata-qemu".to_string(),
            overhead: None,
            scheduling: Some(RuntimeClassScheduling {
                node_selector: [("kata-containers".to_string(), "true".to_string())]
                    .into_iter()
                    .collect(),
                tolerations: vec![],
            }),
        });
        assert_eq!(mgr.handler_for(Some("kata")).unwrap(), "kata-qemu");
    }
}
