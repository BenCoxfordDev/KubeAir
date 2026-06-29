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

//! Resource manager -- coordinates CPU, Memory, Topology, and Device allocation.
//!
//! This is the integration layer called during pod admission and container creation.
//! It wires together:
//!   - CpuManager: cpuset assignment for Guaranteed pods
//!   - MemoryManager: NUMA memory pinning
//!   - TopologyManager: hint-based NUMA admission
//!   - DeviceManager: GPU/FPGA/SR-IOV allocation
//!
//! Called by PodWorker at two points:
//!   1. Admit(pod): before creating the sandbox. Returns reject or admit+hints.
//!   2. Allocate(container): before CRI CreateContainer. Returns resource assignments.
//!
//! Mirrors pkg/kubelet/cm/container_manager.go.

use crate::cpu_manager::{CpuManager, CpuSet};
use crate::device_manager::DeviceManager;
use crate::memory_manager::MemoryManager;
use crate::topology_manager::{TopologyHint, TopologyManager};
use kubelet_core::error::{KubeletError, Result};
use kubelet_core::pod::{ContainerSpec, PodSpec};
use kubelet_core::qos::{QosClass, compute_qos_class};
use kubelet_core::types::PodUID;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

// -- Resource allocation result ------------------------------------------------

/// What gets injected into a container's CRI config after allocation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContainerResources {
    /// cpuset string for cpu.cpus cgroup (e.g. "0-3").
    pub cpuset_cpus: String,
    /// memory nodes for memory.mems cgroup (e.g. "0").
    pub cpuset_mems: String,
    /// Extra env vars from device plugins.
    pub device_env: HashMap<String, String>,
    /// Device node paths to expose.
    pub devices: Vec<crate::device_manager::DeviceSpec>,
    /// Extra mounts from device plugins.
    pub device_mounts: Vec<crate::device_manager::Mount>,
}

// -- Resource manager ----------------------------------------------------------

pub struct ResourceManager {
    cpu_manager: Arc<Mutex<CpuManager>>,
    memory_manager: Arc<Mutex<MemoryManager>>,
    topology_manager: Arc<Mutex<TopologyManager>>,
    device_manager: Arc<DeviceManager>,
}

impl ResourceManager {
    pub fn new(
        cpu_manager: CpuManager,
        memory_manager: MemoryManager,
        topology_manager: TopologyManager,
        device_manager: Arc<DeviceManager>,
    ) -> Self {
        Self {
            cpu_manager: Arc::new(Mutex::new(cpu_manager)),
            memory_manager: Arc::new(Mutex::new(memory_manager)),
            topology_manager: Arc::new(Mutex::new(topology_manager)),
            device_manager,
        }
    }

    /// Admit a container: run topology checks and pre-allocate hints.
    pub async fn admit_container(
        &self,
        pod_uid: &PodUID,
        container: &ContainerSpec,
        qos: QosClass,
    ) -> Result<()> {
        let cpu_request =
            parse_cpu_millis(container.resources.requests.get("cpu").map(|q| q.value));
        let mem_request =
            parse_memory_bytes(container.resources.requests.get("memory").map(|q| q.value));

        // Gather topology hints from each provider.
        let mut hints = vec![];

        // CPU hint: which NUMA nodes have enough CPU.
        hints.push(TopologyHint::no_preference()); // CPU manager provides NUMAless hints currently.

        // Memory hint: from memory manager.
        {
            let mem_mgr = self.memory_manager.lock().await;
            // All NUMA nodes for now -- MemoryManager static policy refines during allocate.
            let node_count = mem_mgr.numa_node_count();
            hints.push(TopologyHint::with_numa_nodes(
                (0..node_count as u32).collect::<Vec<_>>(),
                true,
            ));
        }

        // Topology manager admit.
        let mut topo_mgr = self.topology_manager.lock().await;
        topo_mgr.admit(pod_uid, &container.name, hints)?;

        debug!(pod = %pod_uid.0, container = %container.name, "Container admitted by resource manager");
        Ok(())
    }

    /// Allocate resources for a container (called just before CRI CreateContainer).
    pub async fn allocate(
        &self,
        pod: &PodSpec,
        container: &ContainerSpec,
    ) -> Result<ContainerResources> {
        let qos = compute_qos_class(pod);
        let cpu_request =
            parse_cpu_millis(container.resources.requests.get("cpu").map(|q| q.value));
        let mem_request =
            parse_memory_bytes(container.resources.requests.get("memory").map(|q| q.value));

        // 1. CPU Manager assignment.
        let cpuset = {
            let mut cpu_mgr = self.cpu_manager.lock().await;
            cpu_mgr.assign(&pod.uid, &container.name, qos.clone(), cpu_request)?
        };

        // 2. Memory Manager assignment.
        let cpuset_mems = {
            let mut mem_mgr = self.memory_manager.lock().await;
            mem_mgr.assign(&pod.uid, &container.name, qos.clone(), mem_request)?
        };

        // 3. Device allocations for extended resources.
        let mut device_env = HashMap::new();
        let mut devices = vec![];
        let mut device_mounts = vec![];

        for (resource, quantity) in &container.resources.limits {
            // Skip standard resources.
            if resource == "cpu" || resource == "memory" || resource == "ephemeral-storage" {
                continue;
            }
            let count = quantity.value.to_string().parse::<usize>().unwrap_or(1);
            match self
                .device_manager
                .allocate(&pod.uid, &container.name, resource, count)
                .await
            {
                Ok(response) => {
                    device_env.extend(response.envs);
                    devices.extend(response.devices);
                    device_mounts.extend(response.mounts);
                }
                Err(e) => {
                    warn!(resource, error = %e, "Device allocation failed");
                    return Err(e);
                }
            }
        }

        info!(
            pod = %pod.uid.0, container = %container.name,
            cpuset = %cpuset.to_cpuset_string(),
            mems = %cpuset_mems,
            devices = device_env.len(),
            "Resources allocated"
        );

        Ok(ContainerResources {
            cpuset_cpus: cpuset.to_cpuset_string(),
            cpuset_mems,
            device_env,
            devices,
            device_mounts,
        })
    }

    /// Release all resources allocated to a pod (on termination/deletion).
    pub async fn release_pod(&self, pod_uid: &PodUID, containers: &[String]) {
        let mut cpu_mgr = self.cpu_manager.lock().await;
        let mut mem_mgr = self.memory_manager.lock().await;
        let mut topo_mgr = self.topology_manager.lock().await;

        for container_name in containers {
            cpu_mgr.release(pod_uid, container_name);
            mem_mgr.release(pod_uid, container_name);
        }

        topo_mgr.remove_pod(pod_uid);
        self.device_manager.deallocate_pod(pod_uid).await;

        info!(pod = %pod_uid.0, "Pod resources released");
    }

    /// Return extended resource capacities from device plugins (for NodeStatus).
    pub async fn extended_resource_capacity(&self) -> HashMap<String, u64> {
        self.device_manager.extended_resources().await
    }
}

fn parse_cpu_millis(s: Option<i64>) -> u64 {
    s.map(|v| v.max(0) as u64).unwrap_or(0)
}

fn parse_memory_bytes(s: Option<i64>) -> u64 {
    s.map(|v| v.max(0) as u64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cpu_manager::CpuSet;
    use crate::device_manager::DeviceManager;
    use kubelet_core::pod::{ContainerSpec, ImagePullPolicy, ResourceRequirements, RestartPolicy};
    use kubelet_core::types::{PodRef, PodUID, ResourceQuantity};
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn make_resource_manager() -> ResourceManager {
        let total = CpuSet::from_range(0, 7);
        let cpu_mgr = CpuManager::new("static", total, CpuSet::new([]));
        let mem_mgr = MemoryManager::new("None");
        let topo_mgr = TopologyManager::new("none", "container");
        let dir = TempDir::new().unwrap();
        let device_mgr = Arc::new(DeviceManager::new(dir.keep()));
        ResourceManager::new(cpu_mgr, mem_mgr, topo_mgr, device_mgr)
    }

    fn make_pod_uid() -> PodUID {
        PodUID::new("uid-res-1")
    }

    fn make_container(cpu: &str, mem: &str) -> ContainerSpec {
        ContainerSpec {
            name: "app".to_string(),
            image: "nginx".to_string(),
            command: vec![],
            args: vec![],
            working_dir: None,
            ports: vec![],
            env: vec![],
            env_from: vec![],
            resources: ResourceRequirements {
                requests: [
                    (
                        "cpu".to_string(),
                        ResourceQuantity::cpu_millicores(cpu.parse().unwrap_or(100)),
                    ),
                    (
                        "memory".to_string(),
                        ResourceQuantity::memory_bytes(mem.parse().unwrap_or(128 * 1024 * 1024)),
                    ),
                ]
                .into_iter()
                .collect(),
                limits: Default::default(),
            },
            volume_mounts: vec![],
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            lifecycle: None,
            image_pull_policy: ImagePullPolicy::IfNotPresent,
            security_context: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            restart_policy: None,
        }
    }

    fn make_pod(uid: &str) -> PodSpec {
        PodSpec {
            uid: PodUID::new(uid),
            pod_ref: PodRef {
                name: "app".to_string(),
                namespace: "default".to_string(),
            },
            containers: vec![],
            init_containers: vec![],
            ephemeral_containers: vec![],
            volumes: vec![],
            node_name: "node1".to_string(),
            host_network: false,
            host_pid: false,
            host_ipc: false,
            dns_config: None,
            restart_policy: RestartPolicy::Always,
            termination_grace_period_seconds: 30,
            service_account_name: "default".to_string(),
            priority: None,
            tolerations: vec![],
            node_selector: Default::default(),
            annotations: Default::default(),
            labels: Default::default(),
            runtime_class_name: None,
            security_context: None,
            readiness_gates: vec![],
            active_deadline_seconds: None,
            automount_service_account_token: None,
            image_pull_secrets: vec![],
            enable_service_links: None,
            share_process_namespace: None,
            resource_claims: vec![],
            host_aliases: vec![],
            hostname: None,
            subdomain: None,
            observed_start_time: None,
            generation: None,
        }
    }

    #[tokio::test]
    async fn test_admit_and_allocate() {
        let mgr = make_resource_manager();
        let uid = make_pod_uid();
        let container = make_container("500m", "256Mi");
        mgr.admit_container(&uid, &container, QosClass::Burstable)
            .await
            .unwrap();
        let pod = make_pod("uid-res-1");
        let result = mgr.allocate(&pod, &container).await.unwrap();
        // None policy returns all CPUs.
        assert!(!result.cpuset_cpus.is_empty());
    }

    #[tokio::test]
    async fn test_release_pod() {
        let mgr = make_resource_manager();
        let uid = make_pod_uid();
        let pod = make_pod("uid-res-1");
        let container = make_container("2000m", "512Mi"); // 2 full CPUs -> exclusive
        mgr.allocate(&pod, &container).await.unwrap();
        mgr.release_pod(&uid, &["app".to_string()]).await;
        // After release, CPU pool should be restored.
        let cpu = mgr.cpu_manager.lock().await.available_count();
        assert_eq!(cpu, 8); // All 8 CPUs back in pool
    }

    #[test]
    fn test_parse_cpu_millis() {
        assert_eq!(parse_cpu_millis(Some(500)), 500);
        assert_eq!(parse_cpu_millis(Some(2000)), 2000);
        assert_eq!(parse_cpu_millis(None), 0);
    }

    #[test]
    fn test_parse_memory_bytes() {
        assert_eq!(
            parse_memory_bytes(Some(256 * 1024 * 1024)),
            256 * 1024 * 1024
        );
        assert_eq!(
            parse_memory_bytes(Some(4 * 1024 * 1024 * 1024)),
            4 * 1024 * 1024 * 1024
        );
    }
}
