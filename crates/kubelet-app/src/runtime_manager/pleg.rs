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

use kubelet_core::container::{RuntimeContainer, RuntimeContainerState};
use kubelet_core::error::Result;
use kubelet_ports::driven::container_runtime::ContainerRuntime;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlegEventType {
    ContainerStarted,
    ContainerDied,
    ContainerRemoved,
    ContainerChanged,
}

#[derive(Debug, Clone)]
pub struct PlegEvent {
    pub pod_uid: String,
    pub container_id: String,
    pub container_name: String,
    pub event_type: PlegEventType,
}

pub struct GenericPleg {
    runtime: Arc<dyn ContainerRuntime>,
    relist_period: Duration,
    last_relist: Option<Instant>,
    cache: HashMap<String, RuntimeContainer>,
}

impl GenericPleg {
    pub fn new(runtime: Arc<dyn ContainerRuntime>, relist_period: Duration) -> Self {
        Self {
            runtime,
            relist_period,
            last_relist: None,
            cache: HashMap::new(),
        }
    }

    pub fn relist_period(&self) -> Duration {
        self.relist_period
    }

    pub fn is_healthy(&self, max_staleness: Duration) -> bool {
        self.last_relist
            .map(|t| t.elapsed() <= max_staleness)
            .unwrap_or(false)
    }

    pub async fn relist(&mut self) -> Result<Vec<PlegEvent>> {
        let live = self.runtime.list_containers().await?;
        let mut next = HashMap::new();
        let mut events = Vec::new();

        for container in live {
            let key = container.id.0.clone();
            if let Some(old) = self.cache.get(&key) {
                if old.state != container.state {
                    let event_type = match (&old.state, &container.state) {
                        (RuntimeContainerState::Running, RuntimeContainerState::Exited) => {
                            PlegEventType::ContainerDied
                        }
                        (_, RuntimeContainerState::Running) => PlegEventType::ContainerStarted,
                        _ => PlegEventType::ContainerChanged,
                    };

                    events.push(PlegEvent {
                        pod_uid: container.pod_uid.clone(),
                        container_id: container.id.0.clone(),
                        container_name: container.name.clone(),
                        event_type,
                    });
                }
            } else {
                events.push(PlegEvent {
                    pod_uid: container.pod_uid.clone(),
                    container_id: container.id.0.clone(),
                    container_name: container.name.clone(),
                    event_type: if container.state == RuntimeContainerState::Running {
                        PlegEventType::ContainerStarted
                    } else {
                        PlegEventType::ContainerChanged
                    },
                });
            }

            next.insert(key, container);
        }

        for (id, old) in &self.cache {
            if !next.contains_key(id) {
                events.push(PlegEvent {
                    pod_uid: old.pod_uid.clone(),
                    container_id: old.id.0.clone(),
                    container_name: old.name.clone(),
                    event_type: PlegEventType::ContainerRemoved,
                });
            }
        }

        self.cache = next;
        self.last_relist = Some(Instant::now());
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_adapters::mock_runtime::MockRuntime;
    use kubelet_core::pod::ContainerSpec;
    use kubelet_ports::driven::container_runtime::{
        ContainerRuntime, CreateContainerConfig, CreateSandboxConfig,
    };
    use std::collections::HashMap;

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

    fn container_config(pod_uid: &str, sandbox_id: &str) -> CreateContainerConfig {
        CreateContainerConfig {
            pod_uid: pod_uid.to_string(),
            pod_name: "test".to_string(),
            pod_namespace: "default".to_string(),
            attempt: 0,
            container: ContainerSpec {
                name: "app".to_string(),
                image: "nginx".to_string(),
                ..Default::default()
            },
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
    async fn test_relist_detects_started_and_removed() {
        let runtime = Arc::new(MockRuntime::new());
        let sandbox_id = runtime
            .run_pod_sandbox(sandbox_config("pod-a"))
            .await
            .unwrap();
        let cid = runtime
            .create_container(container_config("pod-a", &sandbox_id))
            .await
            .unwrap();

        let mut pleg = GenericPleg::new(runtime.clone(), Duration::from_secs(1));
        let initial = pleg.relist().await.unwrap();
        assert_eq!(initial.len(), 1);

        runtime.start_container(&cid).await.unwrap();
        let started = pleg.relist().await.unwrap();
        assert!(started
            .iter()
            .any(|e| e.event_type == PlegEventType::ContainerStarted));

        runtime.remove_container(&cid).await.unwrap();
        let removed = pleg.relist().await.unwrap();
        assert!(removed
            .iter()
            .any(|e| e.event_type == PlegEventType::ContainerRemoved));
        assert!(pleg.is_healthy(Duration::from_secs(5)));
    }
}
