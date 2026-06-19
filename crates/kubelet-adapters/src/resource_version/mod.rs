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

//! Resource version tracker -- mirrors API server list-watch bookmarks.
//!
//! The kubelet tracks the resource version of pod list results so it can
//! efficiently resume Watch streams from the last seen version after reconnects.
//!
//! Also tracks which pods have been synced so we don't re-sync unchanged pods.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tracing::debug;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceVersionState {
    /// Last known resource version from the pod list/watch stream.
    pub last_resource_version: String,
    /// pod_uid -> last seen resource version (for per-pod change detection).
    pub pod_versions: HashMap<String, String>,
}

impl ResourceVersionState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update the resource version after receiving a watch event.
    pub fn set_resource_version(&mut self, rv: impl Into<String>) {
        self.last_resource_version = rv.into();
    }

    /// Record the resource version for a specific pod.
    pub fn set_pod_version(&mut self, pod_uid: &str, rv: impl Into<String>) {
        self.pod_versions.insert(pod_uid.to_string(), rv.into());
    }

    /// Check if a pod has changed (different resource version).
    pub fn pod_changed(&self, pod_uid: &str, rv: &str) -> bool {
        self.pod_versions.get(pod_uid).map(|s| s.as_str()) != Some(rv)
    }

    /// Remove tracking for a deleted pod.
    pub fn remove_pod(&mut self, pod_uid: &str) {
        self.pod_versions.remove(pod_uid);
    }

    /// Persist to disk.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)
    }

    /// Load from disk.
    pub fn load(path: &Path) -> Self {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_pod_changed_detection() {
        let mut state = ResourceVersionState::new();
        state.set_pod_version("uid-1", "100");
        assert!(!state.pod_changed("uid-1", "100")); // same version
        assert!(state.pod_changed("uid-1", "101")); // changed
        assert!(state.pod_changed("uid-unknown", "1")); // not seen
    }

    #[test]
    fn test_save_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rv_state.json");

        let mut state = ResourceVersionState::new();
        state.set_resource_version("12345");
        state.set_pod_version("uid-1", "200");
        state.save(&path).unwrap();

        let loaded = ResourceVersionState::load(&path);
        assert_eq!(loaded.last_resource_version, "12345");
        assert_eq!(loaded.pod_versions["uid-1"], "200");
    }

    #[test]
    fn test_remove_pod() {
        let mut state = ResourceVersionState::new();
        state.set_pod_version("uid-del", "50");
        state.remove_pod("uid-del");
        assert!(state.pod_changed("uid-del", "50")); // no longer tracked
    }
}
