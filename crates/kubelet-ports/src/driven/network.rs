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

//! Network port - CNI plugin interface.

use async_trait::async_trait;
use kubelet_core::error::Result;
use std::collections::HashMap;

/// Network attachment result from CNI.
#[derive(Debug, Clone)]
pub struct NetworkAttachment {
    pub sandbox_id: String,
    pub interface_name: String,
    pub ip_addresses: Vec<String>,
    pub mac_address: String,
    pub gateway: Option<String>,
    pub dns: Vec<String>,
}

/// Port to interact with the container network plugin (CNI).
#[async_trait]
pub trait NetworkPlugin: Send + Sync {
    /// Set up networking for a new pod sandbox.
    async fn setup_pod(
        &self,
        pod_uid: &str,
        pod_namespace: &str,
        pod_name: &str,
        sandbox_id: &str,
        annotations: &HashMap<String, String>,
    ) -> Result<NetworkAttachment>;

    /// Tear down networking for a removed pod sandbox.
    async fn teardown_pod(
        &self,
        pod_uid: &str,
        pod_namespace: &str,
        pod_name: &str,
        sandbox_id: &str,
    ) -> Result<()>;

    /// Get the network status of an existing pod.
    async fn pod_network_status(
        &self,
        pod_uid: &str,
        pod_namespace: &str,
        pod_name: &str,
        sandbox_id: &str,
    ) -> Result<Option<NetworkAttachment>>;

    /// Plugin name for logging purposes.
    fn name(&self) -> &str;
}
