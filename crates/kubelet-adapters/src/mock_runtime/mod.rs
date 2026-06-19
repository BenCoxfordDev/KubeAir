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

//! Mock container runtime adapter - used for testing without a real CRI.

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use kubelet_core::container::{
    ContainerID, ContainerStats, ImageInfo, RuntimeContainer, RuntimeContainerState,
};
use kubelet_core::error::{KubeletError, Result};
use kubelet_core::pod::ContainerSpec;
use kubelet_ports::driven::container_runtime::{
    ContainerRuntime, CreateContainerConfig, CreateSandboxConfig, ExecResult, ImageManager,
    ImagePullSecret, SandboxNetworkStatus, SandboxState, SandboxStatus,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

/// In-memory mock container runtime for testing.
#[derive(Clone, Default)]
pub struct MockRuntime {
    sandboxes: Arc<DashMap<String, SandboxStatus>>,
    containers: Arc<DashMap<String, RuntimeContainer>>,
    images: Arc<DashMap<String, ImageInfo>>,
    /// If set, return this error on container start.
    pub fail_on_start: Arc<Mutex<bool>>,
    /// Track call counts.
    pub start_calls: Arc<Mutex<u32>>,
    pub stop_calls: Arc<Mutex<u32>>,
    pub create_sandbox_calls: Arc<Mutex<u32>>,
    pub fail_on_pull_image: Arc<Mutex<bool>>,
    /// If Some, containers transition to Exited with this exit code immediately on start.
    pub exit_on_start: Arc<Mutex<Option<i32>>>,
}

impl MockRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a MockRuntime that always fails pull_image.
    pub fn new_failing() -> Self {
        let rt = Self::default();
        if let Ok(mut fail) = rt.fail_on_pull_image.try_lock() {
            *fail = true;
        }
        rt
    }

    pub async fn set_fail_on_start(&self, fail: bool) {
        *self.fail_on_start.lock().await = fail;
    }

    /// Make containers immediately exit with the given code when started.
    /// Pass `None` to disable.
    pub async fn set_exit_on_start(&self, code: Option<i32>) {
        *self.exit_on_start.lock().await = code;
    }

    pub fn container_count(&self) -> usize {
        self.containers.len()
    }

    pub fn sandbox_count(&self) -> usize {
        self.sandboxes.len()
    }
}

#[async_trait]
impl ContainerRuntime for MockRuntime {
    async fn run_pod_sandbox(&self, config: CreateSandboxConfig) -> Result<String> {
        let id = Uuid::new_v4().to_string();
        *self.create_sandbox_calls.lock().await += 1;
        self.sandboxes.insert(
            id.clone(),
            SandboxStatus {
                id: id.clone(),
                pod_uid: config.pod_uid,
                pod_name: config.pod_name,
                pod_namespace: config.pod_namespace,
                state: SandboxState::Ready,
                created_at: Utc::now(),
                network: Some(SandboxNetworkStatus {
                    ip: "10.0.0.1".to_string(),
                    additional_ips: vec![],
                }),
                labels: config.labels,
            },
        );
        Ok(id)
    }

    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<()> {
        if let Some(mut entry) = self.sandboxes.get_mut(sandbox_id) {
            entry.state = SandboxState::NotReady;
        }
        Ok(())
    }

    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<()> {
        self.sandboxes.remove(sandbox_id);
        Ok(())
    }

    async fn pod_sandbox_status(&self, sandbox_id: &str) -> Result<Option<SandboxStatus>> {
        Ok(self.sandboxes.get(sandbox_id).map(|s| s.clone()))
    }

    async fn list_pod_sandboxes(&self) -> Result<Vec<SandboxStatus>> {
        Ok(self.sandboxes.iter().map(|e| e.value().clone()).collect())
    }

    async fn create_container(&self, config: CreateContainerConfig) -> Result<ContainerID> {
        let id = ContainerID::new(Uuid::new_v4().to_string());
        self.containers.insert(
            id.0.clone(),
            RuntimeContainer {
                id: id.clone(),
                pod_uid: config.pod_uid,
                name: config.container.name,
                attempt: config.attempt,
                pid: None,
                image: config.container.image,
                image_ref: "sha256:mock".to_string(),
                state: RuntimeContainerState::Created,
                created_at: Utc::now(),
                started_at: None,
                finished_at: None,
                exit_code: None,
                exit_reason: None,
                labels: HashMap::new(),
            },
        );
        Ok(id)
    }

    async fn start_container(&self, container_id: &ContainerID) -> Result<()> {
        if *self.fail_on_start.lock().await {
            return Err(KubeletError::Runtime("mock start failure".to_string()));
        }
        *self.start_calls.lock().await += 1;
        if let Some(mut c) = self.containers.get_mut(&container_id.0) {
            if let Some(exit_code) = *self.exit_on_start.lock().await {
                c.state = RuntimeContainerState::Exited;
                c.exit_code = Some(exit_code);
                c.started_at = Some(Utc::now());
                c.finished_at = Some(Utc::now());
            } else {
                c.state = RuntimeContainerState::Running;
                c.started_at = Some(Utc::now());
                c.pid = Some(std::process::id());
            }
        }
        Ok(())
    }

    async fn stop_container(
        &self,
        container_id: &ContainerID,
        _timeout_seconds: u64,
    ) -> Result<()> {
        *self.stop_calls.lock().await += 1;
        if let Some(mut c) = self.containers.get_mut(&container_id.0) {
            c.state = RuntimeContainerState::Exited;
            c.exit_code = Some(0);
            c.finished_at = Some(Utc::now());
        }
        Ok(())
    }

    async fn remove_container(&self, container_id: &ContainerID) -> Result<()> {
        self.containers.remove(&container_id.0);
        Ok(())
    }

    async fn list_containers(&self) -> Result<Vec<RuntimeContainer>> {
        Ok(self.containers.iter().map(|e| e.value().clone()).collect())
    }

    async fn container_status(
        &self,
        container_id: &ContainerID,
    ) -> Result<Option<RuntimeContainer>> {
        Ok(self.containers.get(&container_id.0).map(|c| c.clone()))
    }

    async fn container_stats(&self, _container_id: &ContainerID) -> Result<Option<ContainerStats>> {
        Ok(Some(ContainerStats {
            cpu_usage_nano_cores: 1_000_000,
            memory_usage_bytes: 10_485_760, // 10 MiB
            network_rx_bytes: 1024,
            network_tx_bytes: 512,
            disk_usage_bytes: 1_048_576,
            timestamp: Some(Utc::now()),
        }))
    }

    async fn exec_sync(
        &self,
        _container_id: &ContainerID,
        command: Vec<String>,
        _timeout_seconds: u64,
    ) -> Result<ExecResult> {
        Ok(ExecResult {
            stdout: format!("mock exec: {:?}", command).into_bytes(),
            stderr: vec![],
            exit_code: 0,
        })
    }

    async fn attach_sync(
        &self,
        container_id: &ContainerID,
        _timeout_seconds: u64,
    ) -> Result<ExecResult> {
        let Some(container) = self.containers.get(&container_id.0) else {
            return Err(KubeletError::Runtime("container not found".to_string()));
        };
        if container.state != RuntimeContainerState::Running {
            return Err(KubeletError::Runtime(
                "container is not running; cannot attach".to_string(),
            ));
        }
        Ok(ExecResult {
            stdout: format!("mock attach: {}", container_id.0).into_bytes(),
            stderr: vec![],
            exit_code: 0,
        })
    }
}

#[async_trait]
impl ImageManager for MockRuntime {
    async fn pull_image(&self, image: &str, _pull_secrets: Vec<ImagePullSecret>) -> Result<String> {
        if *self.fail_on_pull_image.lock().await {
            return Err(KubeletError::Runtime(format!(
                "mock pull failure for image '{}'",
                image
            )));
        }
        let id = format!("sha256:{}", uuid::Uuid::new_v4().simple());
        self.images.insert(
            id.clone(),
            ImageInfo {
                id: id.clone(),
                repo_tags: vec![image.to_string()],
                repo_digests: vec![],
                size_bytes: 50_000_000,
            },
        );
        Ok(id)
    }

    async fn list_images(&self) -> Result<Vec<ImageInfo>> {
        Ok(self.images.iter().map(|e| e.value().clone()).collect())
    }

    async fn remove_image(&self, image_id: &str) -> Result<()> {
        self.images.remove(image_id);
        Ok(())
    }

    async fn image_status(&self, image: &str) -> Result<Option<ImageInfo>> {
        Ok(self
            .images
            .iter()
            .find(|e| e.value().repo_tags.iter().any(|t| t == image))
            .map(|e| e.value().clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::pod::{ContainerSpec, ImagePullPolicy, ResourceRequirements};
    use kubelet_ports::driven::container_runtime::{
        ContainerRuntime, CreateContainerConfig, CreateSandboxConfig,
    };

    fn dummy_container_spec(name: &str) -> ContainerSpec {
        ContainerSpec {
            name: name.to_string(),
            image: "nginx:latest".to_string(),
            command: vec![],
            args: vec![],
            working_dir: None,
            ports: vec![],
            env: vec![],
            resources: ResourceRequirements::default(),
            volume_mounts: vec![],
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            image_pull_policy: ImagePullPolicy::IfNotPresent,
            security_context: None,
            termination_message_path: None,
            termination_message_policy: None,
            lifecycle: None,
            env_from: vec![],
            stdin: None,
            stdin_once: None,
            tty: None,
            restart_policy: None,
        }
    }

    fn sandbox_config(pod_uid: &str) -> CreateSandboxConfig {
        CreateSandboxConfig {
            pod_uid: pod_uid.to_string(),
            pod_name: "test".to_string(),
            pod_namespace: "default".to_string(),
            hostname: "test".to_string(),
            log_directory: "/tmp".to_string(),
            dns_config: None,
            port_mappings: vec![],
            labels: HashMap::new(),
            annotations: HashMap::new(),
            linux_cgroup_parent: "/kubepods/besteffort/podtest".to_string(),
            sysctls: HashMap::new(),
            host_network: false,
            host_pid: false,
            host_ipc: false,
            runtime_handler: "runc".to_string(),
            sandbox_image: "registry.k8s.io/pause:3.9".to_string(),
            supplemental_groups: vec![],
            privileged: false,
            share_process_namespace: false,
        }
    }

    fn container_config(pod_uid: &str, sandbox_id: &str, name: &str) -> CreateContainerConfig {
        CreateContainerConfig {
            pod_uid: pod_uid.to_string(),
            pod_name: "test".to_string(),
            pod_namespace: "default".to_string(),
            attempt: 0,
            container: dummy_container_spec(name),
            sandbox_id: sandbox_id.to_string(),
            image_id: "sha256:abc".to_string(),
            log_directory: "/tmp".to_string(),
            env_overrides: HashMap::new(),
            extra_env: vec![],
            security: Default::default(),
            linux_cgroup_parent: String::new(),
            extra_devices: vec![],
            extra_mounts: vec![],
            extra_device_envs: vec![],
            share_process_namespace: false,
            pod_hostname: "test".to_string(),
        }
    }

    #[tokio::test]
    async fn test_run_and_list_sandbox() {
        let rt = MockRuntime::new();
        let id = rt.run_pod_sandbox(sandbox_config("uid-1")).await.unwrap();
        let sandboxes = rt.list_pod_sandboxes().await.unwrap();
        assert_eq!(sandboxes.len(), 1);
        assert_eq!(sandboxes[0].id, id);
        assert_eq!(rt.sandbox_count(), 1);
    }

    #[tokio::test]
    async fn test_create_and_start_container() {
        let rt = MockRuntime::new();
        let sandbox_id = rt.run_pod_sandbox(sandbox_config("uid-2")).await.unwrap();
        let cid = rt
            .create_container(container_config("uid-2", &sandbox_id, "app"))
            .await
            .unwrap();
        rt.start_container(&cid).await.unwrap();

        let status = rt.container_status(&cid).await.unwrap().unwrap();
        assert_eq!(status.state, RuntimeContainerState::Running);
        assert_eq!(*rt.start_calls.lock().await, 1);
    }

    #[tokio::test]
    async fn test_stop_container() {
        let rt = MockRuntime::new();
        let sandbox_id = rt.run_pod_sandbox(sandbox_config("uid-3")).await.unwrap();
        let cid = rt
            .create_container(container_config("uid-3", &sandbox_id, "app"))
            .await
            .unwrap();
        rt.start_container(&cid).await.unwrap();
        rt.stop_container(&cid, 30).await.unwrap();

        let status = rt.container_status(&cid).await.unwrap().unwrap();
        assert_eq!(status.state, RuntimeContainerState::Exited);
        assert_eq!(status.exit_code, Some(0));
    }

    #[tokio::test]
    async fn test_fail_on_start() {
        let rt = MockRuntime::new();
        rt.set_fail_on_start(true).await;
        let sandbox_id = rt.run_pod_sandbox(sandbox_config("uid-4")).await.unwrap();
        let cid = rt
            .create_container(container_config("uid-4", &sandbox_id, "app"))
            .await
            .unwrap();
        let result = rt.start_container(&cid).await;
        assert!(result.is_err());
        assert_eq!(*rt.start_calls.lock().await, 0);
    }

    #[tokio::test]
    async fn test_remove_container() {
        let rt = MockRuntime::new();
        let sandbox_id = rt.run_pod_sandbox(sandbox_config("uid-5")).await.unwrap();
        let cid = rt
            .create_container(container_config("uid-5", &sandbox_id, "app"))
            .await
            .unwrap();
        rt.remove_container(&cid).await.unwrap();
        assert_eq!(rt.container_count(), 0);
    }

    #[tokio::test]
    async fn test_exec_sync() {
        let rt = MockRuntime::new();
        let sandbox_id = rt.run_pod_sandbox(sandbox_config("uid-6")).await.unwrap();
        let cid = rt
            .create_container(container_config("uid-6", &sandbox_id, "app"))
            .await
            .unwrap();
        let result = rt
            .exec_sync(&cid, vec!["echo".to_string(), "hello".to_string()], 5)
            .await
            .unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(!result.stdout.is_empty());
    }

    #[tokio::test]
    async fn test_pull_and_list_images() {
        let rt = MockRuntime::new();
        rt.pull_image("nginx:1.25", vec![]).await.unwrap();
        rt.pull_image("alpine:3.18", vec![]).await.unwrap();
        let images = rt.list_images().await.unwrap();
        assert_eq!(images.len(), 2);
    }

    #[tokio::test]
    async fn test_image_status_found() {
        let rt = MockRuntime::new();
        rt.pull_image("nginx:latest", vec![]).await.unwrap();
        let info = rt.image_status("nginx:latest").await.unwrap();
        assert!(info.is_some());
    }

    #[tokio::test]
    async fn test_image_status_not_found() {
        let rt = MockRuntime::new();
        let info = rt.image_status("nginx:latest").await.unwrap();
        assert!(info.is_none());
    }
}
