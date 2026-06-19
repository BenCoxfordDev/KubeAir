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

//! Network adapter - no-op CNI plugin for testing/standalone operation.

use async_trait::async_trait;
use kubelet_core::error::Result;
use kubelet_ports::driven::network::{NetworkAttachment, NetworkPlugin};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;

/// No-op network plugin - returns localhost-like attachments.
/// Used for testing or when running without a CNI plugin.
pub struct NoopNetworkPlugin;

#[async_trait]
impl NetworkPlugin for NoopNetworkPlugin {
    async fn setup_pod(
        &self,
        pod_uid: &str,
        pod_namespace: &str,
        pod_name: &str,
        sandbox_id: &str,
        _annotations: &HashMap<String, String>,
    ) -> Result<NetworkAttachment> {
        debug!(
            pod_uid,
            pod_namespace, pod_name, sandbox_id, "NoopNetworkPlugin: setup_pod"
        );
        Ok(NetworkAttachment {
            sandbox_id: sandbox_id.to_string(),
            interface_name: "eth0".to_string(),
            ip_addresses: vec!["127.0.0.1".to_string()],
            mac_address: "00:00:00:00:00:00".to_string(),
            gateway: Some("127.0.0.1".to_string()),
            dns: vec!["8.8.8.8".to_string()],
        })
    }

    async fn teardown_pod(
        &self,
        pod_uid: &str,
        _pod_namespace: &str,
        _pod_name: &str,
        _sandbox_id: &str,
    ) -> Result<()> {
        debug!(pod_uid, "NoopNetworkPlugin: teardown_pod");
        Ok(())
    }

    async fn pod_network_status(
        &self,
        _pod_uid: &str,
        _pod_namespace: &str,
        _pod_name: &str,
        sandbox_id: &str,
    ) -> Result<Option<NetworkAttachment>> {
        Ok(Some(NetworkAttachment {
            sandbox_id: sandbox_id.to_string(),
            interface_name: "eth0".to_string(),
            ip_addresses: vec!["127.0.0.1".to_string()],
            mac_address: "00:00:00:00:00:00".to_string(),
            gateway: Some("127.0.0.1".to_string()),
            dns: vec!["8.8.8.8".to_string()],
        }))
    }

    fn name(&self) -> &str {
        "noop"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_setup_pod_returns_attachment() {
        let plugin = NoopNetworkPlugin;
        let result = plugin
            .setup_pod("uid-1", "default", "my-pod", "sandbox-1", &HashMap::new())
            .await
            .unwrap();
        assert_eq!(result.interface_name, "eth0");
        assert!(!result.ip_addresses.is_empty());
    }

    #[tokio::test]
    async fn test_teardown_pod_succeeds() {
        let plugin = NoopNetworkPlugin;
        plugin
            .teardown_pod("uid-1", "default", "my-pod", "sandbox-1")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_pod_network_status() {
        let plugin = NoopNetworkPlugin;
        let status = plugin
            .pod_network_status("uid-1", "default", "my-pod", "sandbox-1")
            .await
            .unwrap();
        assert!(status.is_some());
    }

    #[test]
    fn test_plugin_name() {
        let plugin = NoopNetworkPlugin;
        assert_eq!(plugin.name(), "noop");
    }
}
