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

//! Dynamic Resource Allocation manager.
//!
//! Tracks claim preparation state and offers a thin abstraction point for
//! PrepareResources / UnprepareResources calls.

use kubelet_core::error::{KubeletError, Result};
use kubelet_core::pod::ResourceClaim;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct DraManager {
    plugin_root: PathBuf,
    prepared_claims: HashSet<String>,
    claims: HashMap<String, ResourceClaim>,
}

impl DraManager {
    pub fn new(plugin_root: impl Into<PathBuf>) -> Self {
        Self {
            plugin_root: plugin_root.into(),
            prepared_claims: HashSet::new(),
            claims: HashMap::new(),
        }
    }

    pub fn register_claim(&mut self, claim: ResourceClaim) {
        self.claims
            .insert(format!("{}/{}", claim.namespace, claim.name), claim);
    }

    pub fn prepare_resources(&mut self, namespace: &str, claim_name: &str) -> Result<()> {
        let key = format!("{}/{}", namespace, claim_name);
        let Some(claim) = self.claims.get_mut(&key) else {
            return Err(KubeletError::Runtime(format!(
                "resource claim '{}' not found",
                key
            )));
        };

        if claim.allocated {
            claim.prepared = true;
            self.prepared_claims.insert(key);
            return Ok(());
        }

        Err(KubeletError::Runtime(format!(
            "resource claim '{}/{}' is not allocated",
            namespace, claim_name
        )))
    }

    pub fn unprepare_resources(&mut self, namespace: &str, claim_name: &str) {
        let key = format!("{}/{}", namespace, claim_name);
        if let Some(claim) = self.claims.get_mut(&key) {
            claim.prepared = false;
        }
        self.prepared_claims.remove(&key);
    }

    pub fn is_prepared(&self, namespace: &str, claim_name: &str) -> bool {
        self.prepared_claims
            .contains(&format!("{}/{}", namespace, claim_name))
    }

    pub fn plugin_root(&self) -> &std::path::Path {
        &self.plugin_root
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepare_and_unprepare_claim() {
        let mut manager = DraManager::new("/var/lib/kubelet/plugins");
        manager.register_claim(ResourceClaim {
            namespace: "default".to_string(),
            name: "gpu-a".to_string(),
            class_name: "nvidia.com/gpu".to_string(),
            allocated: true,
            prepared: false,
        });

        manager.prepare_resources("default", "gpu-a").unwrap();
        assert!(manager.is_prepared("default", "gpu-a"));

        manager.unprepare_resources("default", "gpu-a");
        assert!(!manager.is_prepared("default", "gpu-a"));
    }
}
