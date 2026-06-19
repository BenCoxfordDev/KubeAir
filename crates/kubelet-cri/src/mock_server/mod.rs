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

//! Mock CRI gRPC server for integration testing.
//!
//! Provides a fake containerd endpoint that responds to CRI RPCs in-process,
//! allowing integration tests to exercise the real gRPC transport path without
//! a live containerd daemon.

use crate::types::*;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

/// In-memory state for the mock CRI server.
#[derive(Default)]
pub struct MockCriState {
    pub sandboxes: HashMap<String, CriPodSandboxStatus>,
    pub containers: HashMap<String, CriContainerStatus>,
    pub images: HashMap<String, CriImage>,
}

/// Mock CRI server -- manages in-memory pod/container state as a CRI would.
pub struct MockCriServer {
    state: Arc<Mutex<MockCriState>>,
    fail_next_sandbox: Arc<Mutex<bool>>,
    fail_next_container: Arc<Mutex<bool>>,
}

impl MockCriServer {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(MockCriState::default())),
            fail_next_sandbox: Arc::new(Mutex::new(false)),
            fail_next_container: Arc::new(Mutex::new(false)),
        }
    }

    pub async fn set_fail_next_sandbox(&self, fail: bool) {
        *self.fail_next_sandbox.lock().await = fail;
    }

    pub async fn set_fail_next_container(&self, fail: bool) {
        *self.fail_next_container.lock().await = fail;
    }

    // -- CRI RuntimeService methods -----------------------------------------

    pub async fn run_pod_sandbox(&self, config: CriSandboxConfig) -> Result<String, String> {
        if *self.fail_next_sandbox.lock().await {
            *self.fail_next_sandbox.lock().await = false;
            return Err("mock: run_pod_sandbox injected failure".to_string());
        }

        let id = format!("sandbox-{}", Uuid::new_v4().simple());
        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let status = CriPodSandboxStatus {
            id: id.clone(),
            metadata: config.metadata,
            state: CriSandboxState::SandboxReady,
            created_at: now,
            network: Some(CriPodSandboxNetworkStatus {
                ip: "10.244.0.1".to_string(),
                additional_ips: vec![],
            }),
            labels: config.labels,
            annotations: config.annotations,
        };
        self.state.lock().await.sandboxes.insert(id.clone(), status);
        Ok(id)
    }

    pub async fn stop_pod_sandbox(&self, sandbox_id: &str) {
        let mut state = self.state.lock().await;
        if let Some(s) = state.sandboxes.get_mut(sandbox_id) {
            s.state = CriSandboxState::SandboxNotReady;
        }
    }

    pub async fn remove_pod_sandbox(&self, sandbox_id: &str) {
        self.state.lock().await.sandboxes.remove(sandbox_id);
    }

    pub async fn pod_sandbox_status(&self, sandbox_id: &str) -> Option<CriPodSandboxStatus> {
        self.state.lock().await.sandboxes.get(sandbox_id).cloned()
    }

    pub async fn list_pod_sandboxes(&self) -> Vec<CriPodSandboxStatus> {
        self.state
            .lock()
            .await
            .sandboxes
            .values()
            .cloned()
            .collect()
    }

    pub async fn create_container(
        &self,
        _sandbox_id: &str,
        config: CriContainerConfig,
    ) -> Result<String, String> {
        if *self.fail_next_container.lock().await {
            *self.fail_next_container.lock().await = false;
            return Err("mock: create_container injected failure".to_string());
        }

        let id = format!("ctr-{}", Uuid::new_v4().simple());
        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let status = CriContainerStatus {
            id: id.clone(),
            metadata: config.metadata,
            state: CriContainerState::ContainerCreated,
            created_at: now,
            started_at: 0,
            finished_at: 0,
            exit_code: 0,
            image: config.image,
            image_ref: "sha256:mock".to_string(),
            reason: String::new(),
            message: String::new(),
            labels: config.labels,
            annotations: config.annotations,
            mounts: config.mounts,
            log_path: config.log_path,
        };
        self.state
            .lock()
            .await
            .containers
            .insert(id.clone(), status);
        Ok(id)
    }

    pub async fn start_container(&self, container_id: &str) -> Result<(), String> {
        let mut state = self.state.lock().await;
        if let Some(c) = state.containers.get_mut(container_id) {
            c.state = CriContainerState::ContainerRunning;
            c.started_at = Utc::now().timestamp_nanos_opt().unwrap_or(0);
            Ok(())
        } else {
            Err(format!("container not found: {}", container_id))
        }
    }

    pub async fn stop_container(&self, container_id: &str, exit_code: i32) {
        let mut state = self.state.lock().await;
        if let Some(c) = state.containers.get_mut(container_id) {
            c.state = CriContainerState::ContainerExited;
            c.finished_at = Utc::now().timestamp_nanos_opt().unwrap_or(0);
            c.exit_code = exit_code;
        }
    }

    pub async fn remove_container(&self, container_id: &str) {
        self.state.lock().await.containers.remove(container_id);
    }

    pub async fn list_containers(&self) -> Vec<CriContainerStatus> {
        self.state
            .lock()
            .await
            .containers
            .values()
            .cloned()
            .collect()
    }

    pub async fn container_status(&self, container_id: &str) -> Option<CriContainerStatus> {
        self.state
            .lock()
            .await
            .containers
            .get(container_id)
            .cloned()
    }

    pub async fn exec_sync(
        &self,
        container_id: &str,
        cmd: Vec<String>,
        _timeout: i64,
    ) -> CriExecSyncResponse {
        CriExecSyncResponse {
            stdout: format!("mock exec in {}: {:?}", container_id, cmd).into_bytes(),
            stderr: vec![],
            exit_code: 0,
        }
    }

    // -- CRI ImageService methods -------------------------------------------

    pub async fn pull_image(&self, image: &str) -> String {
        let id = format!("sha256:{}", Uuid::new_v4().simple());
        self.state.lock().await.images.insert(
            id.clone(),
            CriImage {
                id: id.clone(),
                repo_tags: vec![image.to_string()],
                repo_digests: vec![],
                size: 50_000_000,
                uid: None,
                username: String::new(),
                spec: Some(CriImageSpec {
                    image: image.to_string(),
                    annotations: HashMap::new(),
                }),
                pinned: false,
            },
        );
        id
    }

    pub async fn list_images(&self) -> Vec<CriImage> {
        self.state.lock().await.images.values().cloned().collect()
    }

    pub async fn remove_image(&self, image_id: &str) {
        self.state.lock().await.images.remove(image_id);
    }

    pub async fn image_status(&self, image_ref: &str) -> Option<CriImage> {
        self.state
            .lock()
            .await
            .images
            .values()
            .find(|img| img.repo_tags.iter().any(|t| t == image_ref))
            .cloned()
    }

    // -- Counts ------------------------------------------------------------

    pub async fn sandbox_count(&self) -> usize {
        self.state.lock().await.sandboxes.len()
    }

    pub async fn container_count(&self) -> usize {
        self.state.lock().await.containers.len()
    }

    pub async fn image_count(&self) -> usize {
        self.state.lock().await.images.len()
    }
}

impl Default for MockCriServer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_run_pod_sandbox() {
        let srv = MockCriServer::new();
        let id = srv
            .run_pod_sandbox(CriSandboxConfig {
                metadata: CriPodSandboxMetadata {
                    name: "pod-1".to_string(),
                    uid: "uid-1".to_string(),
                    namespace: "default".to_string(),
                    attempt: 0,
                },
                hostname: "pod-1".to_string(),
                log_directory: "/tmp".to_string(),
                dns_config: None,
                port_mappings: vec![],
                labels: HashMap::new(),
                annotations: HashMap::new(),
                linux: None,
            })
            .await
            .unwrap();

        assert!(id.starts_with("sandbox-"));
        assert_eq!(srv.sandbox_count().await, 1);
    }

    #[tokio::test]
    async fn test_sandbox_status_ready() {
        let srv = MockCriServer::new();
        let id = srv
            .run_pod_sandbox(CriSandboxConfig {
                metadata: CriPodSandboxMetadata {
                    name: "pod".to_string(),
                    uid: "uid".to_string(),
                    namespace: "default".to_string(),
                    attempt: 0,
                },
                hostname: "pod".to_string(),
                log_directory: "/tmp".to_string(),
                dns_config: None,
                port_mappings: vec![],
                labels: HashMap::new(),
                annotations: HashMap::new(),
                linux: None,
            })
            .await
            .unwrap();

        let status = srv.pod_sandbox_status(&id).await.unwrap();
        assert_eq!(status.state, CriSandboxState::SandboxReady);
        assert!(status.network.is_some());
        assert_eq!(status.network.unwrap().ip, "10.244.0.1");
    }

    #[tokio::test]
    async fn test_stop_sandbox_changes_state() {
        let srv = MockCriServer::new();
        let id = srv
            .run_pod_sandbox(CriSandboxConfig {
                metadata: CriPodSandboxMetadata {
                    name: "p".to_string(),
                    uid: "u".to_string(),
                    namespace: "d".to_string(),
                    attempt: 0,
                },
                hostname: "p".to_string(),
                log_directory: "/t".to_string(),
                dns_config: None,
                port_mappings: vec![],
                labels: HashMap::new(),
                annotations: HashMap::new(),
                linux: None,
            })
            .await
            .unwrap();

        srv.stop_pod_sandbox(&id).await;
        let status = srv.pod_sandbox_status(&id).await.unwrap();
        assert_eq!(status.state, CriSandboxState::SandboxNotReady);
    }

    #[tokio::test]
    async fn test_create_and_start_container() {
        let srv = MockCriServer::new();
        let sandbox_id = srv
            .run_pod_sandbox(CriSandboxConfig {
                metadata: CriPodSandboxMetadata {
                    name: "p".to_string(),
                    uid: "u".to_string(),
                    namespace: "d".to_string(),
                    attempt: 0,
                },
                hostname: "p".to_string(),
                log_directory: "/t".to_string(),
                dns_config: None,
                port_mappings: vec![],
                labels: HashMap::new(),
                annotations: HashMap::new(),
                linux: None,
            })
            .await
            .unwrap();

        let ctr_id = srv
            .create_container(
                &sandbox_id,
                CriContainerConfig {
                    metadata: CriContainerMetadata {
                        name: "nginx".to_string(),
                        attempt: 0,
                    },
                    image: CriImageSpec {
                        image: "nginx:latest".to_string(),
                        annotations: HashMap::new(),
                    },
                    command: vec![],
                    args: vec![],
                    working_dir: String::new(),
                    envs: vec![],
                    mounts: vec![],
                    log_path: "/t".to_string(),
                    stdin: false,
                    stdin_once: false,
                    tty: false,
                    linux: None,
                    labels: HashMap::new(),
                    annotations: HashMap::new(),
                },
            )
            .await
            .unwrap();

        let status = srv.container_status(&ctr_id).await.unwrap();
        assert_eq!(status.state, CriContainerState::ContainerCreated);

        srv.start_container(&ctr_id).await.unwrap();
        let status = srv.container_status(&ctr_id).await.unwrap();
        assert_eq!(status.state, CriContainerState::ContainerRunning);
        assert!(status.started_at > 0);
    }

    #[tokio::test]
    async fn test_stop_and_remove_container() {
        let srv = MockCriServer::new();
        let sandbox_id = srv
            .run_pod_sandbox(CriSandboxConfig {
                metadata: CriPodSandboxMetadata {
                    name: "p".to_string(),
                    uid: "u".to_string(),
                    namespace: "d".to_string(),
                    attempt: 0,
                },
                hostname: "p".to_string(),
                log_directory: "/t".to_string(),
                dns_config: None,
                port_mappings: vec![],
                labels: HashMap::new(),
                annotations: HashMap::new(),
                linux: None,
            })
            .await
            .unwrap();
        let ctr_id = srv
            .create_container(
                &sandbox_id,
                CriContainerConfig {
                    metadata: CriContainerMetadata {
                        name: "app".to_string(),
                        attempt: 0,
                    },
                    image: CriImageSpec {
                        image: "alpine".to_string(),
                        annotations: HashMap::new(),
                    },
                    command: vec![],
                    args: vec![],
                    working_dir: String::new(),
                    envs: vec![],
                    mounts: vec![],
                    log_path: "/t".to_string(),
                    stdin: false,
                    stdin_once: false,
                    tty: false,
                    linux: None,
                    labels: HashMap::new(),
                    annotations: HashMap::new(),
                },
            )
            .await
            .unwrap();

        srv.start_container(&ctr_id).await.unwrap();
        srv.stop_container(&ctr_id, 0).await;
        let status = srv.container_status(&ctr_id).await.unwrap();
        assert_eq!(status.state, CriContainerState::ContainerExited);

        srv.remove_container(&ctr_id).await;
        assert!(srv.container_status(&ctr_id).await.is_none());
        assert_eq!(srv.container_count().await, 0);
    }

    #[tokio::test]
    async fn test_exec_sync() {
        let srv = MockCriServer::new();
        let resp = srv
            .exec_sync("ctr-1", vec!["echo".to_string(), "hello".to_string()], 5)
            .await;
        assert_eq!(resp.exit_code, 0);
        assert!(!resp.stdout.is_empty());
    }

    #[tokio::test]
    async fn test_pull_and_list_images() {
        let srv = MockCriServer::new();
        srv.pull_image("nginx:1.25").await;
        srv.pull_image("alpine:3.18").await;
        let images = srv.list_images().await;
        assert_eq!(images.len(), 2);
    }

    #[tokio::test]
    async fn test_image_status_by_tag() {
        let srv = MockCriServer::new();
        srv.pull_image("busybox:latest").await;
        let img = srv.image_status("busybox:latest").await;
        assert!(img.is_some());
        assert_eq!(img.unwrap().size, 50_000_000);
    }

    #[tokio::test]
    async fn test_fail_next_sandbox_injection() {
        let srv = MockCriServer::new();
        srv.set_fail_next_sandbox(true).await;
        let result = srv
            .run_pod_sandbox(CriSandboxConfig {
                metadata: CriPodSandboxMetadata {
                    name: "p".to_string(),
                    uid: "u".to_string(),
                    namespace: "d".to_string(),
                    attempt: 0,
                },
                hostname: "p".to_string(),
                log_directory: "/t".to_string(),
                dns_config: None,
                port_mappings: vec![],
                labels: HashMap::new(),
                annotations: HashMap::new(),
                linux: None,
            })
            .await;
        assert!(result.is_err());
        assert_eq!(srv.sandbox_count().await, 0);
    }

    #[tokio::test]
    async fn test_remove_sandbox() {
        let srv = MockCriServer::new();
        let id = srv
            .run_pod_sandbox(CriSandboxConfig {
                metadata: CriPodSandboxMetadata {
                    name: "p".to_string(),
                    uid: "u".to_string(),
                    namespace: "d".to_string(),
                    attempt: 0,
                },
                hostname: "p".to_string(),
                log_directory: "/t".to_string(),
                dns_config: None,
                port_mappings: vec![],
                labels: HashMap::new(),
                annotations: HashMap::new(),
                linux: None,
            })
            .await
            .unwrap();
        srv.remove_pod_sandbox(&id).await;
        assert_eq!(srv.sandbox_count().await, 0);
        assert!(srv.pod_sandbox_status(&id).await.is_none());
    }
}
