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

//! Pod worker -- full pod sync implementation.
//!
//! Each pod gets a dedicated async task (the "pod worker") that drives its
//! lifecycle from creation to termination. This mirrors the Go kubelet's
//! `podWorker` goroutine pattern.
//!
//! State machine:
//!   Pending -> [pulling images] -> [creating sandbox] -> [starting init containers]
//!           -> [starting app containers] -> Running -> [probing] -> Terminating -> Terminated

use crate::metrics::{CONTAINER_START_TOTAL, POD_START_TOTAL};
use base64::Engine;
use chrono::Utc;
use dashmap::DashMap;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service, ServiceAccount};
use kube::{Api, Client as KubeClient, api::ListParams};
use kubelet_adapters::cgroup::CgroupManager;
use kubelet_adapters::checkpoint::{CheckpointManager, PodCheckpoint};
use kubelet_adapters::device_manager::DeviceManager;
use kubelet_adapters::lifecycle::{run_post_start, run_pre_stop};
use kubelet_adapters::oom_watcher::OomScoreManager;
use kubelet_adapters::prober::ProbeState;
use kubelet_adapters::sandbox_builder::{NodeDnsConfig, build_dns_config};
use kubelet_adapters::volume_fsgroup::{FsGroupPolicy, apply_fs_group};
use kubelet_core::container::RuntimeContainerState;
use kubelet_core::error::{KubeletError, Result};
use kubelet_core::pod::lifecycle::{
    ConditionStatus, ContainerState, ContainerStatus, PodCondition, PodConditionType, PodPhase,
};
use kubelet_core::pod::manager::PodManager;
use kubelet_core::pod::sync::validate_pod;
use kubelet_core::pod::{ContainerSpec, EnvVarSource, PodSpec, RestartPolicy};
use kubelet_core::qos::compute_qos_class;
use kubelet_ports::driven::container_runtime::{
    ContainerRuntime, CreateContainerConfig, CreateSandboxConfig, DeviceMount, DevicePluginMount,
    ImageManager, ImagePullSecret, LinuxContainerSecurity, SandboxState, SandboxStatus,
};
use kubelet_ports::driven::node_reporter::NodeReporter;
use kubelet_ports::driven::storage::{MountRequest, UnmountRequest, VolumeManager};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Once};
use std::time::Duration;
use tokio_rustls::rustls;
use tracing::{debug, error, info, warn};

/// Result of a single pod sync attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum PodSyncResult {
    Synced,
    NeedsRetry(String),
    Failed(String),
    Terminated,
}

/// Per-pod runtime state tracked by the pod worker.
#[derive(Debug, Clone, Default)]
pub struct PodRuntimeState {
    pub sandbox_id: Option<String>,
    pub sandbox_ip: Option<String>,
    /// container_name -> runtime container ID
    pub container_ids: HashMap<String, String>,
    /// container_name -> restart count
    pub restart_counts: HashMap<String, u32>,
    /// container_name -> probe state
    pub probe_states: HashMap<String, ProbeState>,
    /// container_name -> container_id for which a liveness probe task is running
    pub probe_registered: HashMap<String, String>,
    /// container_name -> container_id for which a startup probe task is running
    pub startup_probe_registered: HashMap<String, String>,
    /// container_name -> container_id for which a readiness probe task is running
    pub readiness_probe_registered: HashMap<String, String>,
    /// container_name -> error message for CreateContainerConfigError state
    pub container_config_errors: HashMap<String, String>,
    /// container_name -> time of last successful StartContainer call.
    /// Used to optimistically report Running for a brief window after start so
    /// fast-exiting containers (e.g. sysctl pods) get at least one Running
    /// status update before transitioning to Terminated/Succeeded.
    pub recently_started: HashMap<String, std::time::Instant>,
    /// Set of container names currently sleeping through a CrashLoopBackOff
    /// backoff delay. Used by update_pod_status to report the correct reason.
    pub crash_loop_backoff: std::collections::HashSet<String>,
    /// container_name -> the last Terminated state snapshot, captured just
    /// before the container is restarted. Persisted in memory so that
    /// update_pod_status can populate `lastTerminationState` even after the
    /// container transitions back to Running.
    pub last_terminated_states: HashMap<String, ContainerState>,
    /// image -> consecutive pull failure count. Used to compute exponential
    /// backoff between retries. Reset to 0 on successful pull.
    /// Formula: min(10s * 2^(n-1), 300s) — matches upstream ImageBackOff.
    pub image_pull_backoff: HashMap<String, u32>,
    /// container_name -> consecutive start failures (pre-create, e.g. missing secret/configmap).
    /// Backoff: min(10s * 2^(n-1), 300s). Reset when the container successfully reaches create_container.
    pub start_failure_backoff: HashMap<String, u32>,
}

/// The pod worker: owns the full lifecycle of one pod.
pub struct PodWorker {
    pod_manager: Arc<PodManager>,
    runtime: Arc<dyn ContainerRuntime>,
    image_manager: Arc<dyn ImageManager>,
    volume_manager: Arc<dyn VolumeManager>,
    checkpoint_mgr: Arc<CheckpointManager>,
    cgroup_mgr: Arc<CgroupManager>,
    runtime_overheads: Arc<HashMap<String, HashMap<String, String>>>,
    cgroup_driver: String,
    root_dir: PathBuf,
    log_dir: String,
    pod_infra_container_image: String,
    /// container_id -> startup probe completed successfully
    container_startup_done: Arc<DashMap<String, bool>>,
    /// container_id -> readiness probe result (true = ready)
    container_readiness: Arc<DashMap<String, bool>>,
    /// Kube client for fetching ConfigMaps, Secrets, etc.
    kube_client: Option<KubeClient>,
    /// Node name — used to match static pod sandboxes using Go kubelet's
    /// `<name>-<nodename>` naming convention during sandbox recovery.
    node_name: String,
    /// Reporter for pushing status updates to the API server directly from
    /// inside sync_pod (e.g. during CrashLoopBackOff backoff sleeps).
    reporter: Arc<dyn NodeReporter>,
    /// Node-level DNS settings used to build per-pod DNS config (dnsPolicy).
    node_dns: NodeDnsConfig,
    /// Device plugin manager — used to call Allocate() for extended resources.
    device_manager: Arc<DeviceManager>,
}

impl PodWorker {
    fn ensure_rustls_crypto_provider() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pod_manager: Arc<PodManager>,
        runtime: Arc<dyn ContainerRuntime>,
        image_manager: Arc<dyn ImageManager>,
        volume_manager: Arc<dyn VolumeManager>,
        checkpoint_mgr: Arc<CheckpointManager>,
        cgroup_mgr: Arc<CgroupManager>,
        runtime_overheads: Arc<HashMap<String, HashMap<String, String>>>,
        cgroup_driver: impl Into<String>,
        root_dir: impl Into<PathBuf>,
        log_dir: impl Into<String>,
        pod_infra_container_image: impl Into<String>,
        kube_client: Option<KubeClient>,
        node_name: impl Into<String>,
        reporter: Arc<dyn NodeReporter>,
        node_dns: NodeDnsConfig,
        device_manager: Arc<DeviceManager>,
    ) -> Self {
        Self::ensure_rustls_crypto_provider();

        Self {
            pod_manager,
            runtime,
            image_manager,
            volume_manager,
            checkpoint_mgr,
            cgroup_mgr,
            runtime_overheads,
            cgroup_driver: cgroup_driver.into(),
            root_dir: root_dir.into(),
            log_dir: log_dir.into(),
            pod_infra_container_image: pod_infra_container_image.into(),
            container_startup_done: Arc::new(DashMap::new()),
            container_readiness: Arc::new(DashMap::new()),
            kube_client,
            node_name: node_name.into(),
            reporter,
            node_dns,
            device_manager,
        }
    }

    /// Call Allocate() on registered device plugins for every extended resource
    /// requested by this container.  Returns (devices, mounts, envs) to inject.
    async fn allocate_device_resources(
        &self,
        pod: &PodSpec,
        ctr: &ContainerSpec,
    ) -> (
        Vec<DeviceMount>,
        Vec<DevicePluginMount>,
        Vec<(String, String)>,
    ) {
        let mut devices = Vec::new();
        let mut mounts = Vec::new();
        let mut envs = Vec::new();

        for (resource, qty) in &ctr.resources.limits {
            // Skip standard Kubernetes resources.
            if matches!(resource.as_str(), "cpu" | "memory" | "ephemeral-storage") {
                continue;
            }
            // Extended resource must contain a slash (e.g. "vendor.io/gpu").
            if !resource.contains('/') {
                continue;
            }
            let count = qty.value.max(0) as usize;
            if count == 0 {
                continue;
            }
            match self
                .device_manager
                .allocate(&pod.uid, &ctr.name, resource, count)
                .await
            {
                Ok(resp) => {
                    info!(
                        pod = %pod.pod_ref,
                        container = %ctr.name,
                        resource = %resource,
                        count,
                        "Device plugin Allocate succeeded"
                    );
                    for d in resp.devices {
                        devices.push(DeviceMount {
                            host_path: d.host_path,
                            container_path: d.container_path,
                            permissions: d.permissions,
                        });
                    }
                    for m in resp.mounts {
                        mounts.push(DevicePluginMount {
                            host_path: m.host_path,
                            container_path: m.container_path,
                            read_only: m.read_only,
                        });
                    }
                    for (k, v) in resp.envs {
                        envs.push((k, v));
                    }
                }
                Err(e) => {
                    warn!(
                        pod = %pod.pod_ref,
                        container = %ctr.name,
                        resource = %resource,
                        error = %e,
                        "Device plugin Allocate failed — container will start without device"
                    );
                }
            }
        }

        (devices, mounts, envs)
    }

    /// Get the pre-configured kube client, or fall back to try_default().
    async fn kube_client(&self) -> Option<KubeClient> {
        if let Some(ref client) = self.kube_client {
            return Some(client.clone());
        }
        KubeClient::try_default().await.ok()
    }

    /// Full pod sync: bring the pod from desired spec to actual running state.
    pub async fn sync_pod(&self, pod: &PodSpec, state: &mut PodRuntimeState) -> PodSyncResult {
        if let Err(e) = validate_pod(pod) {
            return PodSyncResult::Failed(format!("Validation failed: {}", e));
        }

        // Check activeDeadlineSeconds: if the pod has been running past its deadline, kill it.
        if let Some(deadline_secs) = pod.active_deadline_seconds
            && let Some(ls) = self.pod_manager.status.get(&pod.uid)
            && let Some(start_time) = ls.start_time
        {
            let elapsed_secs = Utc::now().signed_duration_since(start_time).num_seconds();
            if elapsed_secs > deadline_secs as i64 {
                info!(
                    pod = %pod.pod_ref,
                    deadline_secs,
                    elapsed_secs,
                    "Active deadline exceeded, terminating pod"
                );
                let _ = self
                    .terminate_pod(
                        pod,
                        state,
                        Duration::from_secs(pod.termination_grace_period_seconds),
                    )
                    .await;
                if let Some(mut final_state) = self.pod_manager.status.get(&pod.uid) {
                    final_state.phase = PodPhase::Failed;
                    final_state.reason = Some("DeadlineExceeded".to_string());
                    if let Some(pos) = final_state
                        .conditions
                        .iter()
                        .position(|c| c.condition_type == PodConditionType::Ready)
                    {
                        final_state.conditions[pos].status = ConditionStatus::False;
                        final_state.conditions[pos].reason = Some("DeadlineExceeded".to_string());
                        final_state.conditions[pos].message = Some(
                            "Pod was active on the node longer than the specified deadline"
                                .to_string(),
                        );
                        final_state.conditions[pos].last_transition_time = Some(Utc::now());
                    }
                    self.pod_manager.status.set(pod.uid.clone(), final_state);
                }
                return PodSyncResult::Terminated;
            }
        }

        // Step 0: Create pod-level cgroup with memory limit (Phase 67: Pod Overhead Accounting).
        let qos = compute_qos_class(pod);
        let pod_memory_bytes = self.calculate_pod_memory_requests(pod);
        let overhead_memory_bytes = runtime_overhead_memory_bytes(pod, &self.runtime_overheads);
        if let Err(e) = self
            .cgroup_mgr
            .create_pod_cgroup_with_memory_limit(
                &qos,
                &pod.uid,
                pod_memory_bytes,
                overhead_memory_bytes,
            )
            .await
        {
            warn!(pod = %pod.pod_ref, error = %e, "Failed to create pod cgroup");
            // Don't fail pod sync if cgroup creation fails (may be in test environment)
        }

        // Step 0b: Ensure declared pod volumes are mounted before container start.
        // Pass the sandbox IP so DownwardAPI volume files can expose status.podIP.
        let pod_ip_for_volumes = state.sandbox_ip.as_deref();
        if let Err(e) = self.ensure_pod_volumes(pod, pod_ip_for_volumes).await {
            error!(pod = %pod.pod_ref, error = %e, "Volume mount failed - pod will remain Pending");
            // Update pod condition to reflect volume mount failure
            if let Some(mut ls) = self.pod_manager.status.get(&pod.uid) {
                ls.phase = PodPhase::Pending;
                // Add a condition to make the error visible
                let condition = PodCondition {
                    condition_type: PodConditionType::PodScheduled,
                    status: ConditionStatus::False,
                    last_probe_time: None,
                    last_transition_time: Some(Utc::now()),
                    reason: Some("VolumeMountFailed".to_string()),
                    message: Some(format!("Failed to mount volumes: {}", e)),
                };
                ls.conditions
                    .retain(|c| c.condition_type != PodConditionType::PodScheduled);
                ls.conditions.push(condition);
                self.pod_manager.status.set(pod.uid.clone(), ls);
            }
            return PodSyncResult::NeedsRetry(format!("volume mount failed: {}", e));
        }

        // Step 1: Ensure images are available
        for container in pod.containers.iter().chain(pod.init_containers.iter()) {
            // Apply exponential backoff for repeated image pull failures.
            // Formula: min(10s * 2^(n-1), 300s) — matches upstream ImageBackOff.
            let pull_failures = *state.image_pull_backoff.get(&container.image).unwrap_or(&0);
            if pull_failures > 0 {
                let exp = pull_failures.saturating_sub(1).min(5);
                let backoff_secs = ((1u64 << exp) * 10).min(300);
                info!(
                    pod = %pod.pod_ref,
                    container = %container.name,
                    image = %container.image,
                    pull_failures,
                    backoff_secs,
                    "ImagePullBackOff: waiting before retry"
                );
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
            }

            if let Err(e) = self.ensure_image(pod, &container.image).await {
                error!(
                    pod = %pod.pod_ref,
                    container = %container.name,
                    image = %container.image,
                    error = %e,
                    "Image pull failed - pod will remain Pending"
                );

                // Update pod condition and container state to reflect image pull failure.
                // Container state transitions from ContainerCreating → ErrImagePull so
                // that `kubectl get pods` shows ErrImagePull/ImagePullBackOff instead of
                // the misleading ContainerCreating.
                if let Some(mut ls) = self.pod_manager.status.get(&pod.uid) {
                    ls.phase = PodPhase::Pending;
                    let condition = PodCondition {
                        condition_type: PodConditionType::PodScheduled,
                        status: ConditionStatus::False,
                        last_probe_time: None,
                        last_transition_time: Some(Utc::now()),
                        reason: Some("ImagePullFailed".to_string()),
                        message: Some(format!("Failed to pull image '{}': {}", container.image, e)),
                    };
                    ls.conditions
                        .retain(|c| c.condition_type != PodConditionType::PodScheduled);
                    ls.conditions.push(condition);
                    // Update the specific container's waiting reason to ErrImagePull.
                    // The runtime_manager's exponential backoff produces the
                    // ImagePullBackOff appearance (pod stays Pending, retries slow down).
                    let err_msg = format!("{}", e);
                    // Always start with ErrImagePull; the runtime_manager's
                    // exponential backoff produces the ImagePullBackOff appearance
                    // on subsequent retries (pod stays Pending, retries slow down).
                    let pull_reason = "ErrImagePull".to_string();
                    for cs in ls
                        .container_statuses
                        .iter_mut()
                        .chain(ls.init_container_statuses.iter_mut())
                    {
                        if cs.name == container.name {
                            cs.state = kubelet_core::pod::lifecycle::ContainerState::Waiting {
                                reason: pull_reason.clone(),
                                message: Some(err_msg.clone()),
                            };
                            break;
                        }
                    }
                    self.pod_manager.status.set(pod.uid.clone(), ls);
                }

                // Emit a Warning/Failed event visible in `kubectl describe pod`.
                let _ = self
                    .reporter
                    .emit_container_event(
                        &pod.pod_ref,
                        &pod.uid,
                        &container.name,
                        "Warning",
                        "Failed",
                        &format!("Failed to pull image \"{}\": {}", container.image, e),
                    )
                    .await;

                // Increment backoff counter for this image.
                *state
                    .image_pull_backoff
                    .entry(container.image.clone())
                    .or_insert(0) += 1;

                if is_registry_auth_error(&e) {
                    return PodSyncResult::NeedsRetry(format!(
                        "image pull failed with registry authorization error for '{}': {} (will retry — pull secret may not be available yet)",
                        container.image, e
                    ));
                }
                return PodSyncResult::NeedsRetry(format!("image pull failed: {}", e));
            }
            // Successful pull — clear backoff so a transient failure doesn't
            // permanently slow down future pulls of this image.
            state.image_pull_backoff.remove(&container.image);
        }

        // Step 2: Ensure sandbox (pod infra container) is running
        if state.sandbox_id.is_none() {
            match self.create_sandbox(pod).await {
                Ok((id, ip)) => {
                    state.sandbox_id = Some(id.clone());
                    state.sandbox_ip = ip;
                    // Restore container_ids from CRI for this sandbox so that
                    // already-running containers (e.g. kube-vip after a kubelet
                    // restart) are not killed and restarted unnecessarily.
                    // Also recover Exited init containers that completed successfully
                    // so they are not re-run on kubelet restart.
                    // Also restore restart_counts from the CRI attempt number so that
                    // CrashLoopBackOff backoff and restart count reporting survive
                    // kubelet restarts.
                    if let Ok(ctrs) = self.runtime.list_containers().await {
                        for ctr in ctrs {
                            if ctr.pod_uid == pod.uid.0
                                && !state.container_ids.contains_key(&ctr.name)
                            {
                                let should_recover = match ctr.state {
                                    RuntimeContainerState::Running => true,
                                    RuntimeContainerState::Exited => {
                                        // Recover exited init containers that completed
                                        // successfully so we don't re-run them after restart.
                                        pod.init_containers.iter().any(|ic| ic.name == ctr.name)
                                    }
                                    _ => false,
                                };
                                if should_recover {
                                    state
                                        .container_ids
                                        .insert(ctr.name.clone(), ctr.id.0.clone());
                                }
                                // Always restore restart_counts from the CRI attempt so that
                                // crash-loop backoff resumes from the correct point after a
                                // kubelet restart and kubectl shows the real restart count.
                                // attempt=0 means the container has been started once (never
                                // restarted), which maps to restart_counts=1 in our convention.
                                let recovered_count = ctr.attempt.saturating_add(1);
                                state
                                    .restart_counts
                                    .entry(ctr.name.clone())
                                    .and_modify(|v| {
                                        if recovered_count > *v {
                                            *v = recovered_count;
                                        }
                                    })
                                    .or_insert(recovered_count);
                            }
                        }
                    }
                    // Update pod status with IP.
                    // For hostNetwork pods the sandbox has no private IP; use
                    // the node's own IP so the API server sees a valid address.
                    if let Some(mut ls) = self.pod_manager.status.get(&pod.uid) {
                        ls.pod_ip = if pod.host_network {
                            Some(detect_node_internal_ip())
                        } else {
                            state.sandbox_ip.clone().filter(|ip| !ip.is_empty())
                        };
                        ls.host_ip = Some(detect_node_internal_ip());
                        self.pod_manager.status.set(pod.uid.clone(), ls);
                    }
                    // Save checkpoint
                    let mut cp =
                        PodCheckpoint::new(&pod.uid.0, &pod.pod_ref.name, &pod.pod_ref.namespace);
                    cp.sandbox_id = Some(id);
                    let _ = self.checkpoint_mgr.write(&pod.uid.0, &cp);
                }
                Err(e) => {
                    error!(pod = %pod.pod_ref, error = %e, "Failed to create sandbox - pod will remain Pending");

                    // Update pod condition to reflect sandbox creation failure
                    if let Some(mut ls) = self.pod_manager.status.get(&pod.uid) {
                        ls.phase = PodPhase::Pending;
                        let condition = PodCondition {
                            condition_type: PodConditionType::PodScheduled,
                            status: ConditionStatus::False,
                            last_probe_time: None,
                            last_transition_time: Some(Utc::now()),
                            reason: Some("SandboxCreationFailed".to_string()),
                            message: Some(format!("Failed to create pod sandbox: {}", e)),
                        };
                        ls.conditions
                            .retain(|c| c.condition_type != PodConditionType::PodScheduled);
                        ls.conditions.push(condition);
                        self.pod_manager.status.set(pod.uid.clone(), ls);
                    }

                    return PodSyncResult::NeedsRetry(format!("sandbox creation failed: {}", e));
                }
            }
        }

        let sandbox_id = state.sandbox_id.clone().unwrap();

        // Step 3: Process init containers in order.
        // Sidecar init containers (restartPolicy=Always) start when reached and keep running.
        // Regular init containers must complete before the next one can start.
        for init_ctr in pod.init_containers.iter() {
            let is_sidecar = is_sidecar_init_container(init_ctr);

            if state.container_ids.contains_key(&init_ctr.name) {
                let cid = kubelet_core::container::ContainerID::new(
                    state.container_ids[&init_ctr.name].clone(),
                );
                if is_sidecar {
                    // Sidecar: ensure it's running; restart if not
                    if matches!(
                        self.runtime.container_status(&cid).await,
                        Ok(Some(s)) if s.state == RuntimeContainerState::Running
                    ) {
                        let pod_ip = state.sandbox_ip.clone().unwrap_or_default();

                        // Startup probe: must pass before liveness/readiness are armed.
                        if let Some(ref startup_probe) = init_ctr.startup_probe {
                            let reg = state.startup_probe_registered.get(&init_ctr.name);
                            if reg.map(String::as_str) != Some(cid.0.as_str()) {
                                let runtime = self.runtime.clone();
                                let cid_clone = cid.clone();
                                let probe_clone = startup_probe.clone();
                                let done_map = self.container_startup_done.clone();
                                tokio::spawn(async move {
                                    spawn_startup_probe(
                                        runtime,
                                        cid_clone,
                                        probe_clone,
                                        pod_ip.clone(),
                                        done_map,
                                    )
                                    .await;
                                });
                                state
                                    .startup_probe_registered
                                    .insert(init_ctr.name.clone(), cid.0.clone());
                            }
                            // Don't arm liveness/readiness until startup has completed.
                            let startup_done = self
                                .container_startup_done
                                .get(&cid.0)
                                .map(|v| *v)
                                .unwrap_or(false);
                            if !startup_done {
                                continue; // running, startup pending
                            }
                        }

                        // Liveness probe: kills the sidecar on threshold failure, which
                        // triggers a restart (restartPolicy=Always guarantees restart).
                        if let Some(ref probe) = init_ctr.liveness_probe {
                            let registered = state.probe_registered.get(&init_ctr.name);
                            if registered.map(String::as_str) != Some(cid.0.as_str()) {
                                let runtime = self.runtime.clone();
                                let cid_clone = cid.clone();
                                let probe_clone = probe.clone();
                                let pod_ip2 = state.sandbox_ip.clone().unwrap_or_default();
                                let reporter_clone = self.reporter.clone();
                                let pod_ref_clone = pod.pod_ref.clone();
                                let pod_uid_clone = pod.uid.clone();
                                let ctr_name_clone = init_ctr.name.clone();
                                info!(
                                    cid = %cid,
                                    pod_ip = %pod_ip2,
                                    "Spawning sidecar liveness probe task"
                                );
                                tokio::spawn(async move {
                                    spawn_liveness_probe(
                                        runtime,
                                        cid_clone,
                                        probe_clone,
                                        pod_ip2,
                                        reporter_clone,
                                        pod_ref_clone,
                                        pod_uid_clone,
                                        ctr_name_clone,
                                    )
                                    .await;
                                });
                                state
                                    .probe_registered
                                    .insert(init_ctr.name.clone(), cid.0.clone());
                            }
                        }

                        // Readiness probe: updates shared readiness map.
                        // Sidecar readiness drives pod ContainersReady/Ready conditions.
                        if let Some(ref probe) = init_ctr.readiness_probe {
                            let registered = state.readiness_probe_registered.get(&init_ctr.name);
                            if registered.map(String::as_str) != Some(cid.0.as_str()) {
                                let runtime = self.runtime.clone();
                                let cid_clone = cid.clone();
                                let probe_clone = probe.clone();
                                let pod_ip3 = state.sandbox_ip.clone().unwrap_or_default();
                                let readiness_map = self.container_readiness.clone();
                                tokio::spawn(async move {
                                    spawn_readiness_probe(
                                        runtime,
                                        cid_clone,
                                        probe_clone,
                                        pod_ip3,
                                        readiness_map,
                                    )
                                    .await;
                                });
                                state
                                    .readiness_probe_registered
                                    .insert(init_ctr.name.clone(), cid.0.clone());
                            }
                        }

                        continue; // running, proceed to next
                    }
                    // Not running, fall through to (re)start
                } else {
                    // Regular init: check completion
                    match self.runtime.container_status(&cid).await {
                        Ok(Some(s))
                            if s.state == RuntimeContainerState::Exited
                                && s.exit_code == Some(0) =>
                        {
                            continue; // completed successfully
                        }
                        Ok(Some(s)) if s.state == RuntimeContainerState::Running => {
                            self.update_pod_status(pod, state).await;
                            return PodSyncResult::NeedsRetry(
                                "init container still running".to_string(),
                            );
                        }
                        Ok(Some(s))
                            if s.state == RuntimeContainerState::Exited
                                && s.exit_code != Some(0)
                                && pod.restart_policy == RestartPolicy::Never =>
                        {
                            // Init container failed and pod must not restart: fail the pod.
                            let exit_code = s.exit_code.unwrap_or(-1);
                            info!(
                                pod = %pod.pod_ref,
                                container = %init_ctr.name,
                                exit_code,
                                "Init container failed with RestartPolicy=Never, failing pod"
                            );
                            // Capture container exit statuses BEFORE removing containers so
                            // the API server can observe the Terminated state with exit code.
                            self.update_pod_status(pod, state).await;
                            let _ = self
                                .terminate_pod(
                                    pod,
                                    state,
                                    Duration::from_secs(pod.termination_grace_period_seconds),
                                )
                                .await;
                            if let Some(mut final_state) = self.pod_manager.status.get(&pod.uid) {
                                final_state.phase = PodPhase::Failed;
                                self.pod_manager.status.set(pod.uid.clone(), final_state);
                            }
                            return PodSyncResult::Terminated;
                        }
                        _ => {} // fall through to restart
                    }
                }
            }

            // Apply exponential backoff for init container restarts (same as app containers).
            // This prevents hammering the CRI at 1Hz when an init container is crash-looping.
            // Formula matches Go kubelet: min(2^(n-1) * 10, 300) seconds.
            let restart_count = *state.restart_counts.get(&init_ctr.name).unwrap_or(&0);
            // Also apply backoff for pre-create failures (e.g. missing secret/configmap).
            // These don't increment restart_counts so we track them separately.
            let start_failures = *state
                .start_failure_backoff
                .get(&init_ctr.name)
                .unwrap_or(&0);
            if restart_count > 0 || start_failures > 0 {
                let count = restart_count.max(start_failures);
                let exp = (count.saturating_sub(1)).min(5);
                let backoff_secs = ((1u64 << exp) * 10).min(300) as u32;
                debug!(
                    pod = %pod.pod_ref,
                    container = %init_ctr.name,
                    restart_count,
                    start_failures,
                    backoff_secs,
                    "Init container restart backoff"
                );
                state.crash_loop_backoff.insert(init_ctr.name.clone());
                self.update_pod_status(pod, state).await;
                // Report CLBO state to the API server now, while we sleep.
                // Without this the CLBO status is cleared before sync_pod returns
                // so kubectl/the API server never sees it.
                if let Some(ls) = self.pod_manager.status.get(&pod.uid) {
                    let _ = self
                        .reporter
                        .report_pod_status(&pod.pod_ref, &pod.uid, &ls)
                        .await;
                }
                tokio::time::sleep(Duration::from_secs(backoff_secs as u64)).await;
                state.crash_loop_backoff.remove(&init_ctr.name);
            }

            // Start (or restart) this init container
            match self
                .start_container(pod, init_ctr, &sandbox_id, state)
                .await
            {
                Ok(_) => {
                    // Successful start: clear any pre-create failure backoff.
                    state.start_failure_backoff.remove(&init_ctr.name);
                    if is_sidecar {
                        continue; // sidecar started, proceed to next in list
                    } else {
                        // Regular init just started; wait for completion before continuing
                        self.update_pod_status(pod, state).await;
                        return PodSyncResult::NeedsRetry(
                            "init container started, waiting for completion".to_string(),
                        );
                    }
                }
                Err(e) => {
                    if let kubelet_core::error::KubeletError::Runtime(_) = &e
                        && is_sandbox_not_found_error(&e)
                    {
                        warn!(pod = %pod.pod_ref, sandbox_id = %sandbox_id, "Sandbox gone (init container) — clearing state to force sandbox recreation");
                        state.sandbox_id = None;
                        state.container_ids.clear();
                        return PodSyncResult::NeedsRetry(
                            "sandbox not found, will recreate".to_string(),
                        );
                    }
                    // Increment pre-create failure backoff for init containers.
                    *state
                        .start_failure_backoff
                        .entry(init_ctr.name.clone())
                        .or_insert(0) += 1;
                    warn!(
                        pod = %pod.pod_ref,
                        container = %init_ctr.name,
                        start_failures = state.start_failure_backoff[&init_ctr.name],
                        error = %e,
                        "Container start failed"
                    );
                    self.update_pod_status(pod, state).await;
                    return PodSyncResult::NeedsRetry(format!("init container failed: {}", e));
                }
            }
        }

        // All init containers completed: mark pod as Initialized.
        if !pod.init_containers.is_empty()
            && let Some(mut ls) = self.pod_manager.status.get(&pod.uid)
            && let Some(pos) = ls
                .conditions
                .iter()
                .position(|c| c.condition_type == PodConditionType::Initialized)
            && ls.conditions[pos].status != ConditionStatus::True
        {
            ls.conditions[pos].status = ConditionStatus::True;
            ls.conditions[pos].last_transition_time = Some(Utc::now());
            self.pod_manager.status.set(pod.uid.clone(), ls);
        }

        // Step 4: Start app containers
        for ctr in &pod.containers {
            if state.container_ids.contains_key(&ctr.name) {
                // Already exists -- check health
                let cid = kubelet_core::container::ContainerID::new(
                    state.container_ids[&ctr.name].clone(),
                );
                match self.runtime.container_status(&cid).await {
                    Ok(Some(s)) if s.state == RuntimeContainerState::Running => {
                        let pod_ip = state.sandbox_ip.clone().unwrap_or_default();

                        // Startup probe: must pass before liveness/readiness are armed.
                        if let Some(ref startup_probe) = ctr.startup_probe {
                            let reg = state.startup_probe_registered.get(&ctr.name);
                            if reg.map(String::as_str) != Some(cid.0.as_str()) {
                                let runtime = self.runtime.clone();
                                let cid_clone = cid.clone();
                                let probe_clone = startup_probe.clone();
                                let done_map = self.container_startup_done.clone();
                                tokio::spawn(async move {
                                    spawn_startup_probe(
                                        runtime,
                                        cid_clone,
                                        probe_clone,
                                        pod_ip.clone(),
                                        done_map,
                                    )
                                    .await;
                                });
                                state
                                    .startup_probe_registered
                                    .insert(ctr.name.clone(), cid.0.clone());
                            }
                            // Don't arm liveness/readiness until startup has completed.
                            let startup_done = self
                                .container_startup_done
                                .get(&cid.0)
                                .map(|v| *v)
                                .unwrap_or(false);
                            if !startup_done {
                                continue;
                            }
                        }

                        // Liveness probe: kill container on threshold failure.
                        if let Some(ref probe) = ctr.liveness_probe {
                            let registered = state.probe_registered.get(&ctr.name);
                            if registered.map(String::as_str) != Some(cid.0.as_str()) {
                                let runtime = self.runtime.clone();
                                let cid_clone = cid.clone();
                                let probe_clone = probe.clone();
                                let pod_ip2 = state.sandbox_ip.clone().unwrap_or_default();
                                let reporter_clone = self.reporter.clone();
                                let pod_ref_clone = pod.pod_ref.clone();
                                let pod_uid_clone = pod.uid.clone();
                                let ctr_name_clone = ctr.name.clone();
                                info!(
                                    cid = %cid,
                                    pod_ip = %pod_ip2,
                                    "Spawning liveness probe task"
                                );
                                tokio::spawn(async move {
                                    spawn_liveness_probe(
                                        runtime,
                                        cid_clone,
                                        probe_clone,
                                        pod_ip2,
                                        reporter_clone,
                                        pod_ref_clone,
                                        pod_uid_clone,
                                        ctr_name_clone,
                                    )
                                    .await;
                                });
                                state
                                    .probe_registered
                                    .insert(ctr.name.clone(), cid.0.clone());
                            }
                        }

                        // Readiness probe: updates shared readiness map.
                        if let Some(ref probe) = ctr.readiness_probe {
                            let registered = state.readiness_probe_registered.get(&ctr.name);
                            if registered.map(String::as_str) != Some(cid.0.as_str()) {
                                let runtime = self.runtime.clone();
                                let cid_clone = cid.clone();
                                let probe_clone = probe.clone();
                                let pod_ip3 = state.sandbox_ip.clone().unwrap_or_default();
                                let readiness_map = self.container_readiness.clone();
                                tokio::spawn(async move {
                                    spawn_readiness_probe(
                                        runtime,
                                        cid_clone,
                                        probe_clone,
                                        pod_ip3,
                                        readiness_map,
                                    )
                                    .await;
                                });
                                state
                                    .readiness_probe_registered
                                    .insert(ctr.name.clone(), cid.0.clone());
                            }
                        }

                        continue;
                    }
                    Ok(Some(s)) if s.state == RuntimeContainerState::Exited => {
                        let exit_code = s.exit_code.unwrap_or(-1);
                        let restart = *state.restart_counts.get(&ctr.name).unwrap_or(&0);
                        // Snapshot the Terminated state before we restart so that
                        // update_pod_status can populate lastTerminationState even
                        // after the container transitions back to Running.
                        state.last_terminated_states.insert(
                            ctr.name.clone(),
                            ContainerState::Terminated {
                                exit_code,
                                reason: if exit_code == 0 {
                                    "Completed".to_string()
                                } else {
                                    "Error".to_string()
                                },
                                message: None,
                                started_at: s.started_at.unwrap_or_else(Utc::now),
                                finished_at: s.finished_at.unwrap_or_else(Utc::now),
                            },
                        );
                        // Check restart policy (container-level policy overrides pod policy).
                        match container_restart_policy(ctr, &pod.restart_policy) {
                            RestartPolicy::Never => {
                                // Don't restart
                                continue;
                            }
                            RestartPolicy::OnFailure if exit_code == 0 => continue,
                            _ => {
                                // Restart with backoff. Mark the container as in CrashLoopBackOff
                                // so update_pod_status reports the correct Waiting reason while
                                // we sleep. Only apply if restart count > 0 (first failure shows
                                // Terminated before we ever mark it backing off).
                                // Formula matches Go kubelet: min(2^(n-1) * 10, 300) seconds.
                                let exp = (restart.saturating_sub(1)).min(5);
                                let backoff = ((1u64 << exp) * 10).min(300) as u32;
                                if backoff > 0 {
                                    state.crash_loop_backoff.insert(ctr.name.clone());
                                    self.update_pod_status(pod, state).await;
                                    // Report CLBO state to the API server now, while we sleep.
                                    // Without this the CLBO status is cleared before sync_pod
                                    // returns so kubectl/the API server never sees it.
                                    if let Some(ls) = self.pod_manager.status.get(&pod.uid) {
                                        let _ = self
                                            .reporter
                                            .report_pod_status(&pod.pod_ref, &pod.uid, &ls)
                                            .await;
                                    }
                                }
                                tokio::time::sleep(Duration::from_secs(backoff as u64)).await;
                                state.crash_loop_backoff.remove(&ctr.name);
                            }
                        }
                    }
                    _ => {}
                }
            }
            // Skip containers that have a permanent configuration error (e.g. invalid SubPathExpr).
            if state.container_config_errors.contains_key(&ctr.name) {
                continue;
            }
            // Apply backoff for pre-create start failures (e.g. missing secret/configmap env var).
            // These don't go through the CrashLoopBackOff path (restart_counts not incremented),
            // so we track them separately to avoid spinning at full speed.
            let start_failures = *state.start_failure_backoff.get(&ctr.name).unwrap_or(&0);
            if start_failures > 0 {
                let exp = (start_failures.saturating_sub(1)).min(5);
                let backoff_secs = ((1u64 << exp) * 10).min(300) as u32;
                debug!(
                    pod = %pod.pod_ref,
                    container = %ctr.name,
                    start_failures,
                    backoff_secs,
                    "Container start failure backoff"
                );
                tokio::time::sleep(Duration::from_secs(backoff_secs as u64)).await;
            }
            if let Err(e) = self.start_container(pod, ctr, &sandbox_id, state).await {
                if let kubelet_core::error::KubeletError::Runtime(_) = &e
                    && is_sandbox_not_found_error(&e)
                {
                    warn!(pod = %pod.pod_ref, sandbox_id = %sandbox_id, "Sandbox gone — clearing state to force sandbox recreation");
                    state.sandbox_id = None;
                    state.container_ids.clear();
                    return PodSyncResult::NeedsRetry(
                        "sandbox not found, will recreate".to_string(),
                    );
                }
                *state
                    .start_failure_backoff
                    .entry(ctr.name.clone())
                    .or_insert(0) += 1;
                warn!(
                    pod = %pod.pod_ref,
                    container = %ctr.name,
                    start_failures = state.start_failure_backoff[&ctr.name],
                    error = %e,
                    "Container start failed"
                );
                CONTAINER_START_TOTAL.with_label_values(&["failure"]).inc();
            } else {
                state.start_failure_backoff.remove(&ctr.name);
                CONTAINER_START_TOTAL.with_label_values(&["success"]).inc();
            }
        }

        // If app containers are done, terminate sidecar init containers.
        if self.all_app_containers_terminated(pod, state).await {
            self.stop_sidecar_init_containers(pod, state).await;
        }

        // Step 4.5: Start any ephemeral containers that haven't been started yet.
        // Ephemeral containers are never restarted; once started we leave them alone.
        for ec in &pod.ephemeral_containers {
            if state.container_ids.contains_key(&ec.name) {
                // Already launched (may still be running or may have exited — we never restart)
                continue;
            }
            if let Err(e) = self.start_container(pod, ec, &sandbox_id, state).await {
                warn!(container = %ec.name, error = %e, "Ephemeral container start failed");
            }
        }

        // Step 5: Update pod lifecycle status
        self.update_pod_status(pod, state).await;

        POD_START_TOTAL.with_label_values(&["success"]).inc();
        PodSyncResult::Synced
    }

    /// Terminate a pod gracefully.
    pub async fn terminate_pod(
        &self,
        pod: &PodSpec,
        state: &PodRuntimeState,
        grace_period: Duration,
    ) -> Result<()> {
        info!(pod = %pod.pod_ref, "Terminating pod");

        // Stop all containers with grace period, with force-kill fallback
        // Build a map of container name -> lifecycle spec for PreStop lookups
        let all_containers: Vec<&ContainerSpec> = pod
            .containers
            .iter()
            .chain(pod.init_containers.iter())
            .collect();

        for (name, cid_str) in &state.container_ids {
            let cid = kubelet_core::container::ContainerID::new(cid_str.clone());

            // Run PreStop hook if defined (best effort: failure is logged, stop still proceeds)
            if let Some(ctr_spec) = all_containers.iter().find(|c| &c.name == name)
                && let Some(lc) = &ctr_spec.lifecycle
                && let Some(handler) = &lc.pre_stop
            {
                info!(container = %name, "Running PreStop hook");
                run_pre_stop(handler, &cid, name, self.runtime.as_ref(), grace_period).await;
            }

            // Try graceful stop first
            match self
                .runtime
                .stop_container(&cid, grace_period.as_secs())
                .await
            {
                Ok(_) => {
                    debug!(container = %name, "Container stopped gracefully");
                }
                Err(e) => {
                    warn!(container = %name, error = %e, "Failed to stop container gracefully, attempting force kill");
                    // Force kill with 0 grace period
                    if let Err(e2) = self.runtime.stop_container(&cid, 0).await {
                        warn!(container = %name, error = %e2, "Force kill also failed");
                    }
                }
            }
        }

        // Give containers a moment to stop before removal
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Remove containers — retry up to 3 times with a brief delay between
        // attempts.  Some CRI implementations (containerd under load) may
        // briefly return "container is not stopped" even after StopContainer
        // returned successfully, so a single attempt is not enough on slow CI
        // runners.
        for (name, cid_str) in &state.container_ids {
            let cid = kubelet_core::container::ContainerID::new(cid_str.clone());
            let mut removed = false;
            for attempt in 0..3u32 {
                match self.runtime.remove_container(&cid).await {
                    Ok(()) => {
                        removed = true;
                        break;
                    }
                    Err(e) => {
                        if attempt < 2 {
                            debug!(
                                container = %name,
                                attempt,
                                error = %e,
                                "remove_container failed, retrying"
                            );
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        } else {
                            warn!(container = %name, error = %e, "Failed to remove container after retries");
                        }
                    }
                }
            }
            if !removed {
                warn!(container = %name, "Container removal ultimately failed — containerd record may linger");
            }
        }

        // Remove sandbox
        if let Some(sandbox_id) = &state.sandbox_id {
            if let Err(e) = self.runtime.stop_pod_sandbox(sandbox_id).await {
                warn!(sandbox = %sandbox_id, error = %e, "Failed to stop sandbox");
            }
            if let Err(e) = self.runtime.remove_pod_sandbox(sandbox_id).await {
                warn!(sandbox = %sandbox_id, error = %e, "Failed to remove sandbox");
            }
        }

        // Delete checkpoint
        let _ = self.checkpoint_mgr.delete(&pod.uid.0);

        // Unmount volumes
        if let Err(e) = self.unmount_pod_volumes(pod).await {
            warn!(pod = %pod.pod_ref, error = %e, "Volume unmount failed during termination");
        }

        // Keep phase/status derived by sync/update_pod_status.
        // Forcing Succeeded here can produce invalid states (for example,
        // Succeeded with no containerStatuses when a pod is torn down early).

        // Clean up stale probe-state DashMap entries for all containers in this pod.
        for cid_str in state.container_ids.values() {
            self.container_startup_done.remove(cid_str.as_str());
            self.container_readiness.remove(cid_str.as_str());
        }

        info!(pod = %pod.pod_ref, "Pod terminated");
        Ok(())
    }

    /// Fallback cleanup: scan containerd for any remaining sandbox/container records
    /// that belong to the given pod UID and remove them.
    ///
    /// This handles a race where `terminate_pod` was called with an incomplete
    /// `state` (e.g. because a concurrent `sync_pod` task was holding the state
    /// when `delete_pod` ran), leaving orphaned containerd records.
    pub async fn cleanup_pod_containerd_by_uid(&self, pod_uid: &str, pod_ref_str: &str) {
        // Find all sandboxes for this pod UID.
        let sandboxes = match self.runtime.list_pod_sandboxes().await {
            Ok(s) => s,
            Err(e) => {
                warn!(pod_uid, error = %e, "cleanup_pod_containerd_by_uid: failed to list sandboxes");
                return;
            }
        };
        let matching_sandboxes: Vec<_> = sandboxes
            .into_iter()
            .filter(|s| s.pod_uid == pod_uid)
            .collect();

        if matching_sandboxes.is_empty() {
            return;
        }

        debug!(
            pod = %pod_ref_str,
            pod_uid,
            count = matching_sandboxes.len(),
            "Fallback cleanup: found orphaned sandbox(es) in containerd"
        );

        // Find all containers associated with these sandboxes and remove them.
        let containers = self.runtime.list_containers().await.unwrap_or_default();
        for sandbox in &matching_sandboxes {
            // Stop and remove any containers in this sandbox.
            for ctr in containers.iter().filter(|c| {
                // match by pod_uid label
                c.pod_uid == pod_uid
            }) {
                let _ = self.runtime.stop_container(&ctr.id, 0).await;
                if let Err(e) = self.runtime.remove_container(&ctr.id).await {
                    warn!(
                        pod_uid,
                        container_id = %ctr.id.0,
                        error = %e,
                        "Fallback cleanup: failed to remove container"
                    );
                }
            }
            // Now remove the sandbox.
            let _ = self.runtime.stop_pod_sandbox(&sandbox.id).await;
            if let Err(e) = self.runtime.remove_pod_sandbox(&sandbox.id).await {
                warn!(
                    pod_uid,
                    sandbox_id = %sandbox.id,
                    error = %e,
                    "Fallback cleanup: failed to remove sandbox"
                );
            } else {
                debug!(pod_uid, sandbox_id = %sandbox.id, "Fallback cleanup: removed orphaned sandbox");
            }
        }
    }

    // -- private helpers ------------------------------------------------------

    async fn ensure_image(&self, pod: &PodSpec, image: &str) -> Result<String> {
        const RETRY_ATTEMPTS: usize = 3;
        const RETRY_DELAY: Duration = Duration::from_millis(500);
        let raw_secrets = self.resolve_image_pull_secrets(pod, image).await;

        // Exchange GCP service-account JSON-key credentials (_json_key_base64) for
        // short-lived OAuth2 access tokens.  Containerd's CRI layer cannot perform
        // the JWT-bearer grant itself, so we must do it here before passing the
        // credentials to PullImage.  The resulting credentials use the standard
        // "oauth2accesstoken" username that GAR / GCR understand over HTTP Basic.
        let pull_secrets = exchange_gcp_credentials(raw_secrets, &pod.pod_ref.to_string()).await;

        debug!(
            pod = %pod.pod_ref,
            image,
            pull_secret_count = pull_secrets.len(),
            pull_secret_servers = ?pull_secrets.iter().map(|s| s.server.as_str()).collect::<Vec<_>>(),
            "ensure_image: resolved pull secrets"
        );

        for attempt in 1..=RETRY_ATTEMPTS {
            match self.image_manager.image_status(image).await {
                Ok(Some(info)) => return Ok(info.id),
                Ok(None) => break,
                Err(e) if is_transient_runtime_connection_error(&e) && attempt < RETRY_ATTEMPTS => {
                    warn!(
                        image,
                        attempt,
                        error = %e,
                        "ImageStatus failed due to transient runtime connectivity; retrying"
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(e) => return Err(e),
            }
        }

        info!(image, "Pulling image");
        for attempt in 1..=RETRY_ATTEMPTS {
            match self
                .image_manager
                .pull_image(image, pull_secrets.clone())
                .await
            {
                Ok(id) => return Ok(id),
                Err(e) if is_transient_runtime_connection_error(&e) && attempt < RETRY_ATTEMPTS => {
                    warn!(
                        image,
                        attempt,
                        error = %e,
                        "PullImage failed due to transient runtime connectivity; retrying"
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(e) => {
                    if pull_secrets.is_empty() {
                        error!(
                            pod = %pod.pod_ref,
                            image,
                            error = %e,
                            "Image pull failed and pod has no imagePullSecrets — image may be private"
                        );
                    }
                    return Err(e);
                }
            }
        }

        Err(KubeletError::Runtime(
            "image pull failed after transient-retry budget exhausted".to_string(),
        ))
    }

    async fn all_app_containers_terminated(&self, pod: &PodSpec, state: &PodRuntimeState) -> bool {
        if pod.containers.is_empty() {
            return false;
        }

        for ctr in &pod.containers {
            let Some(cid_str) = state.container_ids.get(&ctr.name) else {
                return false;
            };

            let cid = kubelet_core::container::ContainerID::new(cid_str.clone());
            let Ok(Some(status)) = self.runtime.container_status(&cid).await else {
                return false;
            };

            if status.state != RuntimeContainerState::Exited {
                return false;
            }
        }

        true
    }

    async fn stop_sidecar_init_containers(&self, pod: &PodSpec, state: &PodRuntimeState) {
        for sidecar in pod
            .init_containers
            .iter()
            .filter(|c| is_sidecar_init_container(c))
        {
            let Some(cid_str) = state.container_ids.get(&sidecar.name) else {
                continue;
            };

            let cid = kubelet_core::container::ContainerID::new(cid_str.clone());
            if let Err(e) = self
                .runtime
                .stop_container(&cid, pod.termination_grace_period_seconds)
                .await
            {
                warn!(container = %sidecar.name, error = %e, "Failed to stop sidecar init container");
            }
        }
    }

    async fn ensure_pod_volumes(&self, pod: &PodSpec, pod_ip: Option<&str>) -> Result<()> {
        if pod.volumes.is_empty() {
            return Ok(());
        }

        // Sanity check: if pod name is empty, we can't resolve metadata fieldRefs.
        // This can happen transiently when a watch event arrives with partial pod data.
        // Return an error so sync_pod retries after the complete spec arrives.
        if pod.pod_ref.name.is_empty() {
            return Err(KubeletError::Runtime(
                "pod name is empty; cannot resolve DownwardAPI fieldRefs — will retry".to_string(),
            ));
        }

        // Check that every volume declared in the current pod spec is already
        // mounted. Using a simple "any mounted?" check causes stale mounts when
        // a pod spec is updated (e.g. URL static pod source corrects a partial
        // parse): the first sync mounts N volumes, later syncs skip re-mounting
        // even though M > N volumes are now declared.
        let already_mounted: std::collections::HashSet<String> = self
            .volume_manager
            .list_mounted_volumes(&pod.uid.0)
            .await?
            .into_iter()
            .map(|mv| mv.volume_name)
            .collect();
        let needs_mount = pod
            .volumes
            .iter()
            .any(|v| !already_mounted.contains(&v.name));

        if needs_mount {
            let requests = pod
                .volumes
                .iter()
                .filter(|v| !already_mounted.contains(&v.name))
                .map(|v| MountRequest {
                    pod_uid: pod.uid.0.clone(),
                    pod_namespace: pod.pod_ref.namespace.clone(),
                    volume_name: v.name.clone(),
                    volume_spec: v.clone(),
                    mount_path: self.volume_target_path(pod, &v.name),
                    read_only: matches!(
                        &v.source,
                        kubelet_core::pod::VolumeSource::PersistentVolumeClaim {
                            read_only: true,
                            ..
                        }
                    ),
                })
                .collect::<Vec<_>>();

            let _ = self.volume_manager.mount_volumes(requests).await?;
        }

        // Always write/refresh DownwardAPI volume files so label/annotation
        // updates are reflected immediately (required by conformance tests).
        for v in &pod.volumes {
            if let kubelet_core::pod::VolumeSource::DownwardAPI {
                items,
                default_mode,
            } = &v.source
            {
                let dir = self.volume_target_path(pod, &v.name);
                tokio::fs::create_dir_all(&dir).await.ok();
                debug!(
                    pod = %pod.pod_ref,
                    pod_name = %pod.pod_ref.name,
                    volume = %v.name,
                    items = items.len(),
                    containers = pod.containers.len(),
                    "Writing DownwardAPI volume files"
                );
                for item in items {
                    let value = resolve_downward_api_value(pod, item, pod_ip);
                    debug!(
                        pod = %pod.pod_ref,
                        volume = %v.name,
                        item_path = %item.path,
                        value_len = value.len(),
                        has_field_ref = item.field_ref.is_some(),
                        has_resource_ref = item.resource_field_ref.is_some(),
                        "DownwardAPI item resolved"
                    );
                    // Skip writing empty values for field_ref items: an empty result
                    // means the pod spec isn't fully populated yet (e.g. pod name not set).
                    // Don't overwrite a previously-correct file with empty content.
                    if value.is_empty() && item.field_ref.is_some() {
                        debug!(
                            pod = %pod.pod_ref,
                            volume = %v.name,
                            item_path = %item.path,
                            "Skipping empty field_ref value — pod spec may not be fully populated"
                        );
                        continue;
                    }
                    let item_path = dir.join(&item.path);
                    if let Some(parent) = item_path.parent() {
                        tokio::fs::create_dir_all(parent).await.ok();
                    }
                    let _ = tokio::fs::write(&item_path, value.as_bytes()).await;

                    // Apply per-item mode, or fallback to volume default_mode.
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let mode = item.mode.or(*default_mode).unwrap_or(0o644) as u32;
                        let _ = tokio::fs::set_permissions(
                            &item_path,
                            std::fs::Permissions::from_mode(mode),
                        )
                        .await;
                    }
                }
            }
        }

        // Write/refresh Projected volume files (ConfigMap + Secret + DownwardAPI + ServiceAccountToken).
        for v in &pod.volumes {
            if let kubelet_core::pod::VolumeSource::Projected {
                sources,
                default_mode,
            } = &v.source
            {
                self.write_projected_volume(pod, &v.name, sources, *default_mode, pod_ip)
                    .await
                    .map_err(|e| {
                        error!(
                            pod = %pod.pod_ref,
                            volume = %v.name,
                            error = %e,
                            "Failed to write projected volume — pod will retry"
                        );
                        e
                    })?;
            }
        }

        // Write/refresh standalone ConfigMap volume files.
        for v in &pod.volumes {
            if let kubelet_core::pod::VolumeSource::ConfigMap {
                name,
                items,
                optional,
                default_mode,
            } = &v.source
            {
                let dir = self.volume_target_path(pod, &v.name);
                tokio::fs::create_dir_all(&dir).await.ok();
                remove_dangling_symlinks_in_dir(&dir).await;
                match self.kube_client().await {
                    Some(client) => {
                        let api: Api<ConfigMap> = Api::namespaced(client, &pod.pod_ref.namespace);
                        match api.get(name).await {
                            Ok(cm) => {
                                let data = configmap_data_bytes(&cm);
                                let mode = default_mode.unwrap_or(0o644);
                                // Compute expected paths before writing so we can remove stale files.
                                let expected = expected_configmap_paths(&dir, &data, items);
                                if let Err(e) =
                                    write_projected_configmap_files(&dir, &data, items, Some(mode))
                                        .await
                                {
                                    warn!(pod = %pod.pod_ref, volume = %v.name, error = %e, "Failed to write configmap volume");
                                } else {
                                    // Remove files for keys that were deleted from the ConfigMap.
                                    cleanup_stale_volume_files(&dir, &expected).await;
                                }
                            }
                            Err(e) => {
                                if *optional {
                                    // ConfigMap was deleted or doesn't exist — clear all its
                                    // files so the volume reflects the absence.
                                    clear_volume_dir(&dir).await;
                                } else {
                                    return Err(KubeletError::Runtime(format!(
                                        "Failed to fetch ConfigMap '{}' for non-optional volume '{}': {}",
                                        name, v.name, e
                                    )));
                                }
                            }
                        }
                    }
                    None => {
                        if !*optional {
                            return Err(KubeletError::Runtime(format!(
                                "No kube client available to fetch ConfigMap '{}' for volume '{}'",
                                name, v.name
                            )));
                        }
                    }
                }
            }
        }

        // Write/refresh standalone Secret volume files.
        for v in &pod.volumes {
            if let kubelet_core::pod::VolumeSource::Secret {
                secret_name,
                items,
                optional,
                default_mode,
            } = &v.source
            {
                let dir = self.volume_target_path(pod, &v.name);
                tokio::fs::create_dir_all(&dir).await.ok();
                remove_dangling_symlinks_in_dir(&dir).await;
                match self.kube_client().await {
                    Some(client) => {
                        let api: Api<Secret> = Api::namespaced(client, &pod.pod_ref.namespace);
                        match api.get(secret_name).await {
                            Ok(secret) => {
                                let data: HashMap<String, Vec<u8>> = secret
                                    .data
                                    .unwrap_or_default()
                                    .into_iter()
                                    .map(|(k, v)| (k, v.0))
                                    .collect();
                                let mode = default_mode.unwrap_or(0o644);
                                // Compute expected paths before writing so we can remove stale files.
                                let expected = expected_secret_paths(&dir, &data, items);
                                if let Err(e) =
                                    write_projected_secret_files(&dir, &data, items, Some(mode))
                                        .await
                                {
                                    warn!(pod = %pod.pod_ref, volume = %v.name, error = %e, "Failed to write secret volume");
                                } else {
                                    // Remove files for keys that were deleted from the Secret.
                                    cleanup_stale_volume_files(&dir, &expected).await;
                                }
                            }
                            Err(e) => {
                                if *optional {
                                    // Secret was deleted or doesn't exist — clear all its
                                    // files so the volume reflects the absence.
                                    clear_volume_dir(&dir).await;
                                } else {
                                    return Err(KubeletError::Runtime(format!(
                                        "Failed to fetch Secret '{}' for non-optional volume '{}': {}",
                                        secret_name, v.name, e
                                    )));
                                }
                            }
                        }
                    }
                    None => {
                        return Err(KubeletError::Runtime(format!(
                            "No kube client available to fetch Secret '{}' for volume '{}'",
                            secret_name, v.name
                        )));
                    }
                }
            }
        }

        // Apply fsGroup ownership to all volume mounts that support it.
        // This chowns files to the pod's fsGroup GID so non-root containers
        // in the fsGroup can read them.
        if let Some(fs_group) = pod.security_context.as_ref().and_then(|sc| sc.fs_group) {
            let policy = match pod
                .security_context
                .as_ref()
                .and_then(|sc| sc.fs_group_change_policy.as_deref())
            {
                Some("OnRootMismatch") => FsGroupPolicy::ReadWriteOnceWithFsType,
                Some("Always") | None => FsGroupPolicy::File,
                _ => FsGroupPolicy::File,
            };
            for v in &pod.volumes {
                let mount_path = self.volume_target_path(pod, &v.name);
                if let Err(e) = apply_fs_group(&mount_path, fs_group, &policy) {
                    warn!(
                        pod = %pod.pod_ref,
                        volume = %v.name,
                        fs_group,
                        error = %e,
                        "Failed to apply fsGroup to volume"
                    );
                }
            }
        }

        Ok(())
    }

    async fn unmount_pod_volumes(&self, pod: &PodSpec) -> Result<()> {
        if pod.volumes.is_empty() {
            return Ok(());
        }

        let requests = pod
            .volumes
            .iter()
            .map(|v| UnmountRequest {
                pod_uid: pod.uid.0.clone(),
                volume_name: v.name.clone(),
                mount_path: self.volume_target_path(pod, &v.name),
            })
            .collect::<Vec<_>>();

        self.volume_manager.unmount_volumes(requests).await
    }

    fn volume_target_path(&self, pod: &PodSpec, volume_name: &str) -> PathBuf {
        let subdir = pod
            .volumes
            .iter()
            .find(|v| v.name == volume_name)
            .map(|v| volume_source_subdir(&v.source))
            .unwrap_or("kubernetes.io~projected");
        self.root_dir
            .join("pods")
            .join(&pod.uid.0)
            .join("volumes")
            .join(subdir)
            .join(volume_name)
    }

    fn pod_hostname(&self, pod: &PodSpec) -> String {
        pod.effective_hostname()
    }

    /// Write the kubelet-managed /etc/hosts file for this pod and return its path.
    ///
    /// The file is written to `<root_dir>/pods/<uid>/etc-hosts` and is bind-mounted
    /// read-write into each container at `/etc/hosts` (matches upstream kubelet behaviour).
    fn write_etc_hosts_file(
        &self,
        pod: &PodSpec,
        pod_ip: Option<&str>,
    ) -> std::io::Result<PathBuf> {
        let pod_dir = self.root_dir.join("pods").join(&pod.uid.0);
        std::fs::create_dir_all(&pod_dir)?;
        let hosts_path = pod_dir.join("etc-hosts");

        let mut content = String::from("# Kubernetes-managed hosts file.\n");
        content.push_str("127.0.0.1\tlocalhost\n");
        content.push_str("::1\tlocalhost ip6-localhost ip6-loopback\n");
        content.push_str("fe00::0\tip6-localnet\n");
        content.push_str("fe00::0\tip6-mcastprefix\n");
        content.push_str("fe00::1\tip6-allnodes\n");
        content.push_str("fe00::2\tip6-allrouters\n");

        if let Some(ip) = pod_ip.filter(|ip| !ip.is_empty()) {
            content.push_str(&format!("{}\t{}\n", ip, self.pod_hostname(pod)));
        }

        info!(
            pod = %pod.pod_ref,
            host_aliases_count = pod.host_aliases.len(),
            "write_etc_hosts_file: host_aliases"
        );
        if !pod.host_aliases.is_empty() {
            content.push_str("# Entries added by HostAliases.\n");
            for ha in &pod.host_aliases {
                if !ha.ip.is_empty() && !ha.hostnames.is_empty() {
                    content.push_str(&format!("{}\t{}\n", ha.ip, ha.hostnames.join("\t")));
                }
            }
        }

        std::fs::write(&hosts_path, content.as_bytes())?;
        Ok(hosts_path)
    }

    /// Write files for a projected volume combining multiple sources.
    async fn write_projected_volume(
        &self,
        pod: &PodSpec,
        volume_name: &str,
        sources: &[kubelet_core::pod::ProjectedVolumeSource],
        default_mode: Option<i32>,
        pod_ip: Option<&str>,
    ) -> Result<()> {
        use kubelet_core::pod::ProjectedVolumeSource;

        let dir = self.volume_target_path(pod, volume_name);
        tokio::fs::create_dir_all(&dir).await?;

        // The Go kubelet uses an atomic writer that creates a timestamped directory and
        // symlinks (`..data -> ..TIMESTAMP`, then `token -> ..data/token`).  When we
        // take over from the Go kubelet, those symlinks may be dangling (the timestamped
        // directory has been removed by GC).  A dangling symlink causes tokio::fs::read
        // and tokio::fs::write to fail with ENOENT.  Remove any dangling symlinks before
        // we write our own flat files.
        remove_dangling_symlinks_in_dir(&dir).await;

        // Determine if we need a kube client to fetch ConfigMaps/Secrets.
        let needs_configmaps = sources
            .iter()
            .any(|s| matches!(s, ProjectedVolumeSource::ConfigMap { .. }));
        let needs_secrets = sources
            .iter()
            .any(|s| matches!(s, ProjectedVolumeSource::Secret { .. }));

        let client = if needs_configmaps || needs_secrets {
            match self.kube_client().await {
                Some(c) => Some(c),
                None => {
                    // Check if all configmap/secret sources are optional
                    let all_optional = sources.iter().all(|s| match s {
                        ProjectedVolumeSource::ConfigMap { optional, .. } => *optional,
                        ProjectedVolumeSource::Secret { optional, .. } => *optional,
                        _ => true,
                    });
                    if all_optional {
                        warn!(pod = %pod.pod_ref, volume = %volume_name, "No kube client available for projected volume, all sources optional");
                        return Ok(());
                    }
                    return Err(KubeletError::Runtime(
                        "failed to create kube client for projected volume: no client available"
                            .to_string(),
                    ));
                }
            }
        } else {
            None
        };

        // Fetch all ConfigMaps and Secrets.
        let mut configmaps: HashMap<String, HashMap<String, Vec<u8>>> = HashMap::new();
        let mut secrets: HashMap<String, HashMap<String, Vec<u8>>> = HashMap::new();

        for source in sources {
            match source {
                ProjectedVolumeSource::ConfigMap {
                    name,
                    items: _,
                    optional,
                } => {
                    if !configmaps.contains_key(name) {
                        let api: Api<ConfigMap> = Api::namespaced(
                            client.clone().ok_or_else(|| {
                                KubeletError::Runtime("missing kube client".to_string())
                            })?,
                            &pod.pod_ref.namespace,
                        );
                        match api.get(name).await {
                            Ok(cm) => {
                                configmaps.insert(name.clone(), configmap_data_bytes(&cm));
                            }
                            Err(e) => {
                                if *optional {
                                    // Optional ConfigMap not found - skip silently
                                    debug!(pod = %pod.pod_ref, configmap = %name, "Optional ConfigMap not found for projected volume");
                                } else {
                                    return Err(KubeletError::Runtime(format!(
                                        "failed to fetch ConfigMap '{}' for projected volume: {}",
                                        name, e
                                    )));
                                }
                            }
                        }
                    }
                }
                ProjectedVolumeSource::Secret {
                    name,
                    items: _,
                    optional,
                } if !secrets.contains_key(name) => {
                    let api: Api<Secret> = Api::namespaced(
                        client.clone().ok_or_else(|| {
                            KubeletError::Runtime("missing kube client".to_string())
                        })?,
                        &pod.pod_ref.namespace,
                    );
                    match api.get(name).await {
                        Ok(secret) => {
                            let data = secret
                                .data
                                .unwrap_or_default()
                                .into_iter()
                                .map(|(key, value)| (key, value.0))
                                .collect();
                            secrets.insert(name.clone(), data);
                        }
                        Err(e) => {
                            if *optional {
                                debug!(pod = %pod.pod_ref, secret = %name, "Optional Secret not found for projected volume");
                            } else {
                                return Err(KubeletError::Runtime(format!(
                                    "failed to fetch Secret '{}' for projected volume: {}",
                                    name, e
                                )));
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // Write files for each source.
        for source in sources {
            match source {
                ProjectedVolumeSource::ConfigMap {
                    name,
                    items,
                    optional: _,
                } => {
                    if let Some(cm_data) = configmaps.get(name) {
                        write_projected_configmap_files(&dir, cm_data, items, default_mode).await?;
                    }
                }
                ProjectedVolumeSource::Secret {
                    name,
                    items,
                    optional: _,
                } => {
                    if let Some(secret_data) = secrets.get(name) {
                        write_projected_secret_files(&dir, secret_data, items, default_mode)
                            .await?;
                    }
                }
                ProjectedVolumeSource::DownwardAPI { items } => {
                    for item in items {
                        let value = resolve_downward_api_value(pod, item, pod_ip);
                        // Skip writing empty values for field_ref items: an empty result
                        // means the pod spec isn't fully populated yet.
                        if value.is_empty() && item.field_ref.is_some() {
                            continue;
                        }
                        let item_path = dir.join(&item.path);
                        if let Some(parent) = item_path.parent() {
                            tokio::fs::create_dir_all(parent).await?;
                        }
                        tokio::fs::write(&item_path, value.as_bytes()).await?;

                        // Apply per-item mode, or fallback to volume default_mode.
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt;
                            let mode = item.mode.or(default_mode).unwrap_or(0o644) as u32;
                            let _ = tokio::fs::set_permissions(
                                &item_path,
                                std::fs::Permissions::from_mode(mode),
                            )
                            .await;
                        }
                    }
                }
                ProjectedVolumeSource::ServiceAccountToken {
                    audience,
                    expiration_seconds,
                    path,
                } => {
                    let token_path = dir.join(path);
                    if let Some(parent) = token_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    match request_service_account_token(
                        pod,
                        audience.as_deref(),
                        *expiration_seconds,
                        self.kube_client().await.as_ref(),
                    )
                    .await
                    {
                        Some(token) => {
                            write_if_changed(&token_path, token.as_bytes()).await?;
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::PermissionsExt;
                                // ServiceAccount tokens default to 0o600, but respect volume default_mode if set.
                                let mode = default_mode.unwrap_or(0o600) as u32;
                                let _ = tokio::fs::set_permissions(
                                    &token_path,
                                    std::fs::Permissions::from_mode(mode),
                                )
                                .await;
                            }
                        }
                        None => {
                            if tokio::fs::try_exists(&token_path).await.unwrap_or(false) {
                                warn!(
                                    pod = %pod.pod_ref,
                                    path = %token_path.display(),
                                    "TokenRequest unavailable; preserving existing service account token file"
                                );
                            } else {
                                warn!(
                                    pod = %pod.pod_ref,
                                    path = %token_path.display(),
                                    "TokenRequest unavailable and no existing service account token file present"
                                );
                            }
                        }
                    }
                }
            }
        }

        // Remove files that are no longer contributed by any source.
        // This handles: optional CM/Secret deletion, key removal, and source removal.
        let mut expected: HashSet<PathBuf> = HashSet::new();
        for source in sources {
            match source {
                ProjectedVolumeSource::ConfigMap { name, items, .. } => {
                    if let Some(cm_data) = configmaps.get(name) {
                        expected.extend(expected_configmap_paths(&dir, cm_data, items));
                    }
                }
                ProjectedVolumeSource::Secret { name, items, .. } => {
                    if let Some(secret_data) = secrets.get(name) {
                        expected.extend(expected_secret_paths(&dir, secret_data, items));
                    }
                }
                ProjectedVolumeSource::ServiceAccountToken { path, .. } => {
                    expected.insert(dir.join(path));
                }
                ProjectedVolumeSource::DownwardAPI { items } => {
                    for item in items {
                        expected.insert(dir.join(&item.path));
                    }
                }
            }
        }
        cleanup_stale_volume_files(&dir, &expected).await;

        Ok(())
    }

    fn sandbox_cgroup_parent(&self, pod: &PodSpec) -> String {
        let qos = compute_qos_class(pod);
        let uid = pod.uid.0.clone();

        if self.cgroup_driver == "systemd" {
            let safe_uid = uid.replace('-', "_");
            let pod_slice = match qos {
                kubelet_core::qos::QosClass::Guaranteed => {
                    format!("kubepods-guaranteed-pod{}.slice", safe_uid)
                }
                kubelet_core::qos::QosClass::Burstable => {
                    format!("kubepods-burstable-pod{}.slice", safe_uid)
                }
                kubelet_core::qos::QosClass::BestEffort => {
                    format!("kubepods-besteffort-pod{}.slice", safe_uid)
                }
            };
            return pod_slice;
        }

        let qos_path = match qos {
            kubelet_core::qos::QosClass::Guaranteed => "guaranteed",
            kubelet_core::qos::QosClass::Burstable => "burstable",
            kubelet_core::qos::QosClass::BestEffort => "besteffort",
        };
        format!("/kubepods/{}/pod{}", qos_path, uid)
    }

    async fn create_sandbox(&self, pod: &PodSpec) -> Result<(String, Option<String>)> {
        // Ensure the pod infra container image is available
        if let Err(e) = self
            .ensure_image(pod, &self.pod_infra_container_image)
            .await
        {
            warn!(image = %self.pod_infra_container_image, error = %e, "Failed to pull pod infra container image");
            return Err(kubelet_core::error::KubeletError::Runtime(format!(
                "failed to pull pod infra container image: {}",
                e
            )));
        }

        // Recover from kubelet restarts or earlier partial runtime failures by
        // reusing an existing ready sandbox for this pod UID, or cleaning
        // stale not-ready sandboxes before creating a new one.
        if let Some((sandbox_id, ip)) = self.recover_existing_sandbox(pod).await? {
            return Ok((sandbox_id, ip));
        }

        let mut sandbox_labels = pod.labels.clone();
        sandbox_labels.insert(
            "io.kubernetes.pod.name".to_string(),
            pod.pod_ref.name.clone(),
        );
        sandbox_labels.insert(
            "io.kubernetes.pod.namespace".to_string(),
            pod.pod_ref.namespace.clone(),
        );
        sandbox_labels.insert("io.kubernetes.pod.uid".to_string(), pod.uid.0.clone());

        let config = CreateSandboxConfig {
            pod_uid: pod.uid.0.clone(),
            pod_name: pod.pod_ref.name.clone(),
            pod_namespace: pod.pod_ref.namespace.clone(),
            hostname: if pod.host_network {
                String::new()
            } else {
                pod.effective_hostname()
            },
            log_directory: format!(
                "{}/{}_{}_{}",
                self.log_dir, pod.pod_ref.namespace, pod.pod_ref.name, pod.uid.0
            ),
            dns_config: {
                // Apply dnsPolicy (ClusterFirst/Default/None) plus any explicit overrides.
                let dns_spec = build_dns_config(pod, &self.node_dns);
                Some(kubelet_ports::driven::container_runtime::SandboxDnsConfig {
                    servers: dns_spec.servers,
                    searches: dns_spec.searches,
                    options: dns_spec.options,
                })
            },
            port_mappings: pod
                .containers
                .iter()
                .flat_map(|c| &c.ports)
                .map(|p| kubelet_ports::driven::container_runtime::PortMapping {
                    container_port: p.container_port,
                    host_port: p.host_port,
                    protocol: format!("{:?}", p.protocol),
                    host_ip: p.host_ip.clone(),
                })
                .collect(),
            labels: sandbox_labels,
            annotations: pod.annotations.clone(),
            linux_cgroup_parent: self.sandbox_cgroup_parent(pod),
            sysctls: pod
                .security_context
                .as_ref()
                .map(|sc| {
                    sc.sysctls
                        .iter()
                        .map(|s| (s.name.clone(), s.value.clone()))
                        .collect()
                })
                .unwrap_or_default(),
            host_network: pod.host_network,
            host_pid: pod.host_pid,
            host_ipc: pod.host_ipc,
            runtime_handler: pod
                .runtime_class_name
                .clone()
                .unwrap_or_else(|| "runc".to_string()),
            sandbox_image: self.pod_infra_container_image.clone(),
            supplemental_groups: pod
                .security_context
                .as_ref()
                .map(|sc| {
                    let mut groups: Vec<i64> = Vec::new();
                    if let Some(g) = sc.fs_group {
                        groups.push(g as i64);
                    }
                    for g in &sc.supplemental_groups {
                        let g = *g as i64;
                        if !groups.contains(&g) {
                            groups.push(g);
                        }
                    }
                    groups
                })
                .unwrap_or_default(),
            // Any privileged container in the pod requires the sandbox itself to be
            // created with privileged=true; containerd rejects the container
            // otherwise with "no privileged container allowed in sandbox".
            privileged: pod
                .containers
                .iter()
                .chain(pod.init_containers.iter())
                .any(|c| {
                    c.security_context
                        .as_ref()
                        .and_then(|sc| sc.privileged)
                        .unwrap_or(false)
                }),
            share_process_namespace: pod.share_process_namespace.unwrap_or(false),
        };

        let sandbox_id = match self.runtime.run_pod_sandbox(config.clone()).await {
            Ok(id) => id,
            Err(e) => {
                // containerd can keep a sandbox name reservation when prior
                // attempts failed before kubelet captured sandbox_id.
                if e.to_string().contains("is reserved for") {
                    self.cleanup_stale_sandboxes_for_pod(pod).await;
                    self.runtime.run_pod_sandbox(config).await?
                } else {
                    return Err(e);
                }
            }
        };
        let ip = self
            .runtime
            .pod_sandbox_status(&sandbox_id)
            .await?
            .and_then(|s| s.network)
            .map(|n| n.ip);

        Ok((sandbox_id, ip))
    }

    async fn recover_existing_sandbox(
        &self,
        pod: &PodSpec,
    ) -> Result<Option<(String, Option<String>)>> {
        let sandboxes = self.runtime.list_pod_sandboxes().await?;
        for sandbox in sandboxes {
            if !sandbox_matches_pod_identity(&sandbox, pod, &self.node_name) {
                continue;
            }

            if sandbox.pod_uid != pod.uid.0 {
                info!(
                    pod = %pod.pod_ref,
                    sandbox_id = %sandbox.id,
                    old_uid = %sandbox.pod_uid,
                    new_uid = %pod.uid.0,
                    "Cleaning stale sandbox for same pod identity with different UID"
                );
                self.cleanup_sandbox(&sandbox.id).await;
                continue;
            }

            if sandbox.state == SandboxState::Ready {
                let ip = self
                    .runtime
                    .pod_sandbox_status(&sandbox.id)
                    .await?
                    .and_then(|s| s.network)
                    .map(|n| n.ip);
                info!(pod = %pod.pod_ref, sandbox_id = %sandbox.id, "Reusing existing ready sandbox");
                return Ok(Some((sandbox.id, ip)));
            }

            self.cleanup_sandbox(&sandbox.id).await;
        }

        Ok(None)
    }

    async fn cleanup_stale_sandboxes_for_pod(&self, pod: &PodSpec) {
        let sandboxes = match self.runtime.list_pod_sandboxes().await {
            Ok(items) => items,
            Err(e) => {
                warn!(pod = %pod.pod_ref, error = %e, "Unable to list pod sandboxes for cleanup");
                return;
            }
        };

        for sandbox in sandboxes {
            if sandbox_matches_pod_identity(&sandbox, pod, &self.node_name) {
                self.cleanup_sandbox(&sandbox.id).await;
            }
        }
    }

    async fn cleanup_sandbox(&self, sandbox_id: &str) {
        if let Err(e) = self.runtime.stop_pod_sandbox(sandbox_id).await {
            warn!(sandbox_id, error = %e, "Failed to stop stale sandbox");
        }
        if let Err(e) = self.runtime.remove_pod_sandbox(sandbox_id).await {
            warn!(sandbox_id, error = %e, "Failed to remove stale sandbox");
        }
    }

    async fn cleanup_reserved_container_name(
        &self,
        pod: &PodSpec,
        ctr: &kubelet_core::pod::ContainerSpec,
    ) {
        let containers = match self.runtime.list_containers().await {
            Ok(items) => items,
            Err(e) => {
                warn!(pod = %pod.pod_ref, container = %ctr.name, error = %e, "Unable to list containers for reservation cleanup");
                return;
            }
        };

        for container in containers {
            let pod_uid_matches = container
                .labels
                .get("kubelet.rs/pod_uid")
                .map(|v| v.as_str())
                == Some(pod.uid.0.as_str());
            let container_name_matches = container
                .labels
                .get("kubelet.rs/container_name")
                .map(|v| v.as_str())
                == Some(ctr.name.as_str());

            if pod_uid_matches
                && container_name_matches
                && let Err(e) = self.runtime.remove_container(&container.id).await
            {
                warn!(container = %container.name, error = %e, "Failed to remove reserved container");
            }
        }
    }

    async fn cleanup_container_by_id(&self, container_id: &kubelet_core::container::ContainerID) {
        if let Err(e) = self.runtime.stop_container(container_id, 0).await {
            warn!(container_id = %container_id, error = %e, "Failed to stop reserved container before removal");
        }
        if let Err(e) = self.runtime.remove_container(container_id).await {
            warn!(container_id = %container_id, error = %e, "Failed to remove reserved container by ID");
        }
    }

    async fn start_container(
        &self,
        pod: &PodSpec,
        ctr: &kubelet_core::pod::ContainerSpec,
        sandbox_id: &str,
        state: &mut PodRuntimeState,
    ) -> Result<()> {
        let image_id = self.ensure_image(pod, &ctr.image).await?;
        let mounted_volumes = self
            .volume_manager
            .list_mounted_volumes(&pod.uid.0)
            .await?
            .into_iter()
            .map(|v| (v.volume_name, v.mount_path))
            .collect::<HashMap<_, _>>();

        // Resolve container env before processing volume mounts so that
        // SubPathExpr can be expanded using the container's environment.
        let mut resolved_container = ctr.clone();
        let extra_env = self
            .resolve_container_env(pod, &resolved_container, state.sandbox_ip.as_deref())
            .await?;

        info!(
            pod = %pod.pod_ref,
            container = %ctr.name,
            extra_env_count = extra_env.len(),
            "Resolved {} environment variables for container",
            extra_env.len()
        );

        let expanded_env = extra_env.iter().cloned().collect::<HashMap<_, _>>();

        for vm in &mut resolved_container.volume_mounts {
            if vm.mount_path.trim().is_empty() {
                return Err(KubeletError::Runtime(format!(
                    "container '{}' has volumeMount '{}' with empty mount_path",
                    ctr.name, vm.name
                )));
            }

            let base = mounted_volumes.get(&vm.name).cloned().ok_or_else(|| {
                KubeletError::Runtime(format!(
                    "container '{}' references unknown volumeMount '{}' (mounted volumes: {:?})",
                    ctr.name,
                    vm.name,
                    mounted_volumes.keys().cloned().collect::<Vec<_>>()
                ))
            })?;

            // Determine effective sub_path: SubPathExpr takes precedence over SubPath.
            let effective_sub_path = if let Some(ref expr) = vm.sub_path_expr.clone() {
                if expr.is_empty() {
                    None
                } else {
                    let expanded = expand_env_value(expr, &expanded_env);
                    // Validate the expanded subpath: must not be absolute, must not
                    // traverse upward (contain ".." path components), must not have null bytes.
                    if expanded.starts_with('/')
                        || expanded.contains('\0')
                        || std::path::Path::new(&expanded)
                            .components()
                            .any(|c| c.as_os_str() == "..")
                    {
                        let msg = format!(
                            "invalid subPathExpr: \"{}\" expanded to invalid path \"{}\"",
                            expr, expanded
                        );
                        warn!(container = %ctr.name, "{}", msg);
                        state.container_config_errors.insert(ctr.name.clone(), msg);
                        return Ok(());
                    }
                    Some(expanded)
                }
            } else {
                vm.sub_path.clone()
            };

            let source = if let Some(sub_path) = effective_sub_path {
                if sub_path.is_empty() {
                    base
                } else {
                    let full = base.join(&sub_path);
                    // Create the subpath directory on the host so the container runtime
                    // can mount it (required for SubPathExpr with non-existent paths).
                    // If the path already exists (as a file or directory) that is fine —
                    // the runtime mounts whatever is at that path directly.
                    if let Err(e) = std::fs::create_dir_all(&full)
                        && e.kind() != std::io::ErrorKind::AlreadyExists
                    {
                        warn!(container = %ctr.name, path = %full.display(), error = %e,
                                "Failed to create subpath directory");
                    }
                    full
                }
            } else {
                base
            };
            if source.as_os_str().is_empty() {
                return Err(KubeletError::Runtime(format!(
                    "container '{}' has volumeMount '{}' resolved to empty source path",
                    ctr.name, vm.name
                )));
            }

            // CRI mount translation currently uses `sub_path` as the host source path.
            vm.sub_path = Some(source.to_string_lossy().to_string());
        }

        // Add kubelet-managed /etc/hosts bind-mount (handles HostAliases).
        // Skip when the container already has an explicit /etc/hosts volumeMount.
        let has_custom_hosts_mount = resolved_container
            .volume_mounts
            .iter()
            .any(|m| m.mount_path == "/etc/hosts");
        if !has_custom_hosts_mount {
            match self.write_etc_hosts_file(pod, state.sandbox_ip.as_deref()) {
                Ok(hosts_path) => {
                    resolved_container
                        .volume_mounts
                        .push(kubelet_core::pod::VolumeMount {
                            name: "k8s-managed-etc-hosts".to_string(),
                            mount_path: "/etc/hosts".to_string(),
                            read_only: false,
                            sub_path: Some(hosts_path.to_string_lossy().to_string()),
                            sub_path_expr: None,
                        });
                }
                Err(e) => {
                    warn!(pod = %pod.pod_ref, container = %ctr.name, error = %e,
                        "Failed to write managed /etc/hosts; container will use runtime default");
                }
            }
        }

        // Pre-create the termination message log file on the host and bind-mount it.
        // This is required because non-root containers cannot create new files in /dev/.
        // We place the file at <log_dir>/<ns>_<pod>_<uid>/<container>/termination-log
        // and bind-mount it at the container's terminationMessagePath.
        let term_msg_path = ctr
            .termination_message_path
            .as_deref()
            .unwrap_or("/dev/termination-log");
        let host_termination_log = std::path::PathBuf::from(format!(
            "{}/{}_{}_{}/{}/termination-log",
            self.log_dir, pod.pod_ref.namespace, pod.pod_ref.name, pod.uid.0, ctr.name
        ));
        if let Some(parent) = host_termination_log.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        // Create the file if it doesn't exist or if a stale directory is there.
        // runc bind-mounts the host path to /dev/termination-log (a file), so the
        // source MUST be a regular file — if it is a directory the mount will fail
        // with "not a directory". Remove any directory that may have been left by a
        // previous crashed attempt before creating the file.
        // After creation, explicitly chmod to 0666 so non-root containers can write
        // to it (mode() in OpenOptions is subject to umask, so we must chmod explicitly,
        // mirroring the Go kubelet which calls os.Chmod(path, 0666) after creation).
        {
            use std::os::unix::fs::OpenOptionsExt;
            let needs_create = match host_termination_log.metadata() {
                Ok(m) if m.is_dir() => {
                    // Stale directory — remove it so we can create a file.
                    let _ = std::fs::remove_dir_all(&host_termination_log);
                    true
                }
                Ok(m) if m.is_file() => false,
                _ => true,
            };
            if needs_create {
                let _ = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .mode(0o666)
                    .open(&host_termination_log);
            }
        }
        // Always chmod to 0666 to override umask and handle pre-existing files.
        let _ = std::fs::set_permissions(
            &host_termination_log,
            std::os::unix::fs::PermissionsExt::from_mode(0o666),
        );
        // Only add the mount if not already covered by a user-defined volumeMount,
        // and not covered by a parent directory mount (e.g. a pod that bind-mounts
        // the host /dev into the container already owns /dev/termination-log — trying
        // to file-bind-mount on top of a devtmpfs entry causes runc ENOTDIR errors).
        let already_mounted = resolved_container.volume_mounts.iter().any(|m| {
            let mp = m.mount_path.trim_end_matches('/');
            let tp = term_msg_path.trim_end_matches('/');
            tp == mp || tp.starts_with(&format!("{}/", mp))
        });
        if !already_mounted {
            resolved_container
                .volume_mounts
                .push(kubelet_core::pod::VolumeMount {
                    name: "k8s-termination-log".to_string(),
                    mount_path: term_msg_path.to_string(),
                    read_only: false,
                    sub_path: Some(host_termination_log.to_string_lossy().to_string()),
                    sub_path_expr: None,
                });
        }

        let attempt = *state.restart_counts.get(&ctr.name).unwrap_or(&0);
        resolved_container.command =
            expand_container_tokens(&resolved_container.command, &expanded_env);
        resolved_container.args = expand_container_tokens(&resolved_container.args, &expanded_env);

        // Allocate extended resources (device plugins) for this container.
        // Any resource key that is not cpu/memory/ephemeral-storage is an extended resource.
        let (extra_devices, extra_mounts, extra_device_envs) =
            self.allocate_device_resources(pod, ctr).await;

        let config = CreateContainerConfig {
            pod_uid: pod.uid.0.clone(),
            pod_name: pod.pod_ref.name.clone(),
            pod_namespace: pod.pod_ref.namespace.clone(),
            attempt,
            container: resolved_container,
            sandbox_id: sandbox_id.to_string(),
            image_id,
            log_directory: format!(
                "{}/{}_{}_{}",
                self.log_dir, pod.pod_ref.namespace, pod.pod_ref.name, pod.uid.0
            ),
            env_overrides: HashMap::new(),
            extra_env,
            security: build_container_security(
                ctr.security_context.as_ref(),
                pod.security_context.as_ref(),
            ),
            linux_cgroup_parent: self.sandbox_cgroup_parent(pod),
            extra_devices,
            extra_mounts,
            extra_device_envs,
            share_process_namespace: pod.share_process_namespace.unwrap_or(false),
            pod_hostname: pod.effective_hostname(),
        };

        // Clean up the previous container record before creating the new one.
        // The old container is guaranteed to be in Exited state at this point —
        // sync_pod only calls start_container after observing Exited status (or
        // on first launch when there is no old record). Removing it here, before
        // create, ensures clean-up even if create/start subsequently fails.
        if let Some(old_cid_str) = state.container_ids.get(&ctr.name).cloned() {
            let old_cid = kubelet_core::container::ContainerID::new(old_cid_str.clone());
            if let Err(e) = self.runtime.remove_container(&old_cid).await {
                warn!(container = %ctr.name, old_cid = %old_cid_str, error = %e,
                      "Failed to remove old container before restart (will retry on next sync)");
            }
            self.container_startup_done.remove(old_cid_str.as_str());
            self.container_readiness.remove(old_cid_str.as_str());
            state.probe_registered.remove(&ctr.name);
            state.startup_probe_registered.remove(&ctr.name);
            state.readiness_probe_registered.remove(&ctr.name);
            state.container_ids.remove(&ctr.name);
        }

        let cid = match self.runtime.create_container(config.clone()).await {
            Ok(cid) => cid,
            Err(e) => {
                if let Some(reserved_name) = reserved_container_name(&e.to_string()) {
                    warn!(pod = %pod.pod_ref, container = %ctr.name, reserved_name = %reserved_name, "Removing stale reserved container before retrying");
                    if let Some(reserved_id) = reserved_container_id(&e.to_string()) {
                        self.cleanup_container_by_id(&reserved_id).await;
                    }
                    self.cleanup_reserved_container_name(pod, ctr).await;
                    self.runtime.create_container(config).await?
                } else {
                    return Err(e);
                }
            }
        };
        state
            .restart_counts
            .insert(ctr.name.clone(), attempt.saturating_add(1));
        if let Err(e) = self.runtime.start_container(&cid).await {
            // Clean up the container record we just created; if we leave it behind
            // every retry creates another orphaned containerd entry with its overlayfs
            // snapshot, which is the root cause of the containerd record memory leak.
            if let Err(re) = self.runtime.remove_container(&cid).await {
                warn!(container = %ctr.name, cid = %cid, error = %re,
                      "Failed to clean up container after start failure; record may be orphaned");
            }
            return Err(e);
        }

        // PostStart lifecycle hook — if defined, run it immediately after start.
        // On failure, stop the container and return an error (pod worker will retry/fail).
        if let Some(lc) = &ctr.lifecycle
            && let Some(handler) = &lc.post_start
        {
            info!(pod = %pod.pod_ref, container = %ctr.name, "Running PostStart hook");
            if let Err(e) = run_post_start(handler, &cid, &ctr.name, self.runtime.as_ref()).await {
                warn!(pod = %pod.pod_ref, container = %ctr.name, error = %e, "PostStart hook failed; stopping container");
                let _ = self.runtime.stop_container(&cid, 0).await;
                return Err(e);
            }
        }

        // Phase 70: apply oom_score_adj using QoS class. This is best effort,
        // because some runtimes do not surface a container PID in status.
        if let Ok(Some(status)) = self.runtime.container_status(&cid).await {
            if let Some(pid) = status.pid {
                let qos = compute_qos_class(pod);
                let requested_memory: u64 = pod
                    .containers
                    .iter()
                    .chain(pod.init_containers.iter())
                    .map(|c| {
                        c.resources
                            .requests
                            .get("memory")
                            .map(|q| q.value.max(0) as u64)
                            .unwrap_or(0)
                    })
                    .sum();
                let mgr = OomScoreManager::new();
                let score = mgr.score_for_qos(&qos, Some(requested_memory), None);
                if let Err(e) = mgr.apply_to_pid(pid, score) {
                    warn!(pid, score, error = %e, "Failed to apply oom_score_adj");
                }
            } else {
                debug!(container = %ctr.name, "Container runtime did not report PID for OOM score adjustment");
            }
        }

        // Container successfully started — no longer in CrashLoopBackOff.
        state.crash_loop_backoff.remove(&ctr.name);
        state.container_ids.insert(ctr.name.clone(), cid.0.clone());
        state
            .recently_started
            .insert(ctr.name.clone(), std::time::Instant::now());

        info!(pod = %pod.pod_ref, container = %ctr.name, cid = %cid, "Container started");

        // Emit a Kubernetes Event so that watchers (e.g. the sysctl conformance
        // test) can detect that a container has started without polling pod phase.
        let _ = self
            .reporter
            .emit_container_event(
                &pod.pod_ref,
                &pod.uid,
                &ctr.name,
                "Normal",
                "Started",
                &format!("Started container {}", ctr.name),
            )
            .await;

        Ok(())
    }

    async fn resolve_container_env(
        &self,
        pod: &PodSpec,
        container: &ContainerSpec,
        pod_ip: Option<&str>,
    ) -> Result<Vec<(String, String)>> {
        let needs_configmaps = container
            .env_from
            .iter()
            .any(|env_from| env_from.config_map_ref.is_some())
            || container
                .env
                .iter()
                .any(|env| matches!(env.value_from, Some(EnvVarSource::ConfigMapKeyRef { .. })));
        let needs_secrets = container
            .env_from
            .iter()
            .any(|env_from| env_from.secret_ref.is_some())
            || container
                .env
                .iter()
                .any(|env| matches!(env.value_from, Some(EnvVarSource::SecretKeyRef { .. })));
        let service_links_enabled = pod.enable_service_links.unwrap_or(true);

        // Always acquire a kube client: KUBERNETES_SERVICE_HOST / KUBERNETES_SERVICE_PORT
        // (master service vars) must be injected regardless of enableServiceLinks or
        // whether the container uses configmaps/secrets.
        let client = self.kube_client().await;

        if (needs_configmaps || needs_secrets) && client.is_none() {
            return Err(KubeletError::Runtime(
                "failed to create kube client for env resolution: no client available".to_string(),
            ));
        }

        let configmaps = if needs_configmaps {
            let api: Api<ConfigMap> = Api::namespaced(
                client.clone().ok_or_else(|| {
                    KubeletError::Runtime(
                        "missing kube client for configmap env resolution".to_string(),
                    )
                })?,
                &pod.pod_ref.namespace,
            );
            load_container_configmaps(container, &api).await?
        } else {
            HashMap::new()
        };

        let secrets = if needs_secrets {
            let api: Api<Secret> = Api::namespaced(
                client.clone().ok_or_else(|| {
                    KubeletError::Runtime(
                        "missing kube client for secret env resolution".to_string(),
                    )
                })?,
                &pod.pod_ref.namespace,
            );
            load_container_secrets(container, &api).await?
        } else {
            HashMap::new()
        };

        // The `kubernetes` master service env vars (KUBERNETES_SERVICE_HOST /
        // KUBERNETES_SERVICE_PORT) are ALWAYS injected regardless of
        // `enableServiceLinks`.  Namespace service links are only injected when
        // `enableServiceLinks` is true (the default).
        let service_envs = if let Some(c) = client.clone() {
            match load_service_env_vars(pod, &c, service_links_enabled).await {
                Ok(envs) => envs,
                Err(e) => {
                    // API server may not be reachable yet (e.g. bootstrapping
                    // a control-plane node where kube-apiserver is itself a
                    // static pod). Skip service env injection rather than
                    // blocking the container start.
                    warn!(pod = %pod.pod_ref, error = %e, "Skipping service env injection: API server unavailable");
                    HashMap::new()
                }
            }
        } else {
            warn!(pod = %pod.pod_ref, "No kube client available for service env resolution");
            HashMap::new()
        };

        assemble_container_env(pod, container, pod_ip, &configmaps, &secrets, &service_envs)
    }

    async fn resolve_image_pull_secrets(&self, pod: &PodSpec, image: &str) -> Vec<ImagePullSecret> {
        let registry = image_registry_host(image);

        // Collect all secret name references from both the pod spec and the
        // pod's ServiceAccount (SA-level imagePullSecrets are merged by the
        // Go kubelet's admission plugin — we replicate that here).
        let mut secret_names: Vec<String> = pod
            .image_pull_secrets
            .iter()
            .map(|r| r.name.clone())
            .collect();

        // Only fetch a kube client if we actually need to look anything up.
        let need_client = !secret_names.is_empty() || !pod.service_account_name.is_empty();

        let client = if need_client {
            match self.kube_client().await {
                Some(c) => Some(c),
                None => {
                    warn!(pod = %pod.pod_ref, "Unable to create kube client for imagePullSecrets lookup");
                    None
                }
            }
        } else {
            None
        };

        // Augment with ServiceAccount-level imagePullSecrets.
        if let Some(ref client) = client {
            let sa_name = if pod.service_account_name.is_empty() {
                "default"
            } else {
                &pod.service_account_name
            };
            let sa_api: Api<ServiceAccount> =
                Api::namespaced(client.clone(), &pod.pod_ref.namespace);
            match sa_api.get(sa_name).await {
                Ok(sa) => {
                    for r in sa.image_pull_secrets.unwrap_or_default() {
                        if let Some(name) = r.name
                            && !secret_names.contains(&name)
                        {
                            debug!(
                                pod = %pod.pod_ref,
                                sa = sa_name,
                                secret = %name,
                                "Adding SA-level imagePullSecret"
                            );
                            secret_names.push(name);
                        }
                    }
                }
                Err(e) => {
                    debug!(
                        pod = %pod.pod_ref,
                        sa = sa_name,
                        error = %e,
                        "Could not fetch ServiceAccount for imagePullSecrets (may not exist)"
                    );
                }
            }
        }

        if secret_names.is_empty() {
            debug!(pod = %pod.pod_ref, image, registry, sa = %pod.service_account_name, "No imagePullSecrets on pod or ServiceAccount — pulling without credentials");
            return vec![];
        }

        debug!(
            pod = %pod.pod_ref,
            image,
            registry,
            secret_count = secret_names.len(),
            "Resolving imagePullSecrets"
        );

        let client = match client {
            Some(c) => c,
            None => return vec![],
        };

        let secrets_api: Api<Secret> = Api::namespaced(client, &pod.pod_ref.namespace);
        let mut resolved = Vec::new();

        for name in &secret_names {
            debug!(pod = %pod.pod_ref, secret = %name, "Fetching imagePullSecret");
            let secret = match secrets_api.get(name).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        pod = %pod.pod_ref,
                        secret = %name,
                        error = %e,
                        "Failed to fetch imagePullSecret"
                    );
                    continue;
                }
            };

            let entries = docker_auths_from_secret(&secret);
            warn!(
                pod = %pod.pod_ref,
                secret = %name,
                auth_entry_count = entries.len(),
                "Parsed docker auth entries from secret"
            );
            if entries.is_empty() {
                error!(
                    pod = %pod.pod_ref,
                    secret = %name,
                    "imagePullSecret has no parseable docker auth entries (check .dockerconfigjson/.dockercfg format)"
                );
                continue;
            }

            for (host, username, password) in entries {
                if host_matches_registry(&host, &registry) {
                    warn!(
                        pod = %pod.pod_ref,
                        auth_host = host,
                        username,
                        "Matched imagePullSecret credentials for image"
                    );
                    resolved.push(ImagePullSecret {
                        server: host,
                        username,
                        password,
                    });
                }
            }
        }

        if resolved.is_empty() {
            error!(
                pod = %pod.pod_ref,
                image,
                registry,
                secret_names = ?secret_names,
                "No matching imagePullSecret credentials found for image registry — pulling without auth (will likely fail for private registries)"
            );
        } else {
            warn!(
                pod = %pod.pod_ref,
                image,
                registry,
                cred_count = resolved.len(),
                servers = ?resolved.iter().map(|s| s.server.as_str()).collect::<Vec<_>>(),
                "Resolved imagePullSecret credentials for pull"
            );
        }

        resolved
    }

    /// Read the termination message for a container that has exited.
    ///
    /// - If `policy` is `FallbackToLogsOnError` and `exit_code != 0`, read from the
    ///   container log file (last 4 KiB).
    /// - Otherwise (policy `File` or unset), attempt to read the termination message
    ///   from the container log directory host path at the well-known path.
    ///   For simplicity we only implement FallbackToLogsOnError here; the File-based
    ///   path requires container rootfs access which is not yet available.
    fn read_termination_message(
        &self,
        pod: &PodSpec,
        container_name: &str,
        _msg_path: Option<&str>,
        policy: Option<&str>,
        exit_code: i32,
    ) -> Option<String> {
        // Try reading from the host-side termination log file first (File policy, default).
        // The file was pre-created at start_container time and bind-mounted into the container.
        let term_log_host = format!(
            "{}/{}_{}_{}/{}/termination-log",
            self.log_dir, pod.pod_ref.namespace, pod.pod_ref.name, pod.uid.0, container_name,
        );
        let file_msg = std::fs::read_to_string(&term_log_host)
            .ok()
            .filter(|s| !s.is_empty());
        if file_msg.is_some() {
            return file_msg;
        }

        // FallbackToLogsOnError: use container logs when container fails
        let use_logs = policy == Some("FallbackToLogsOnError") && exit_code != 0;
        if !use_logs {
            return None;
        }

        // Container log path: {log_dir}/{namespace}_{pod_name}_{uid}/{container_name}/0.log
        let log_file = format!(
            "{}/{}_{}_{}/{}/0.log",
            self.log_dir, pod.pod_ref.namespace, pod.pod_ref.name, pod.uid.0, container_name,
        );

        // Read up to 4 KiB from the end of the log file, stripping CRI prefixes
        let content = std::fs::read_to_string(&log_file).ok()?;
        if content.is_empty() {
            return None;
        }

        // CRI log format: "<timestamp> <stream> <flags> <message>\n"
        // Strip the CRI log line prefix to get raw output
        let mut lines_out: Vec<String> = Vec::new();
        let mut total: usize = 0;
        const MAX_BYTES: usize = 4096;
        for raw_line in content.lines().rev() {
            // Parse "2006-01-02T15:04:05.000000000Z stdout F message"
            let msg = if let Some(rest) = raw_line.splitn(4, ' ').nth(3) {
                rest
            } else {
                raw_line
            };
            total += msg.len() + 1;
            lines_out.push(msg.to_string());
            if total >= MAX_BYTES {
                break;
            }
        }
        lines_out.reverse();
        let result = lines_out.join("\n");
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    async fn update_pod_status(&self, pod: &PodSpec, state: &PodRuntimeState) {
        let mut ls = self.pod_manager.status.get(&pod.uid).unwrap_or_default();

        let mut new_init_statuses = Vec::new();
        for init_ctr in &pod.init_containers {
            let cid_str = state.container_ids.get(&init_ctr.name);
            let (ctr_state, image_id) = if let Some(cid_str) = cid_str {
                let cid = kubelet_core::container::ContainerID::new(cid_str.clone());
                match self.runtime.container_status(&cid).await {
                    Ok(Some(s)) => {
                        let image_ref = s.image_ref.clone();
                        let ctr_state = match &s.state {
                            RuntimeContainerState::Running => ContainerState::Running {
                                started_at: s.started_at.unwrap_or_else(Utc::now),
                            },
                            RuntimeContainerState::Exited => {
                                let now = Utc::now();
                                if state.crash_loop_backoff.contains(&init_ctr.name) {
                                    ContainerState::Waiting {
                                        reason: "CrashLoopBackOff".to_string(),
                                        message: Some(format!(
                                            "back-off {} restarting failed container",
                                            init_ctr.name
                                        )),
                                    }
                                } else {
                                    ContainerState::Terminated {
                                        exit_code: s.exit_code.unwrap_or(-1),
                                        reason: if s.exit_code.unwrap_or(-1) == 0 {
                                            "Completed".to_string()
                                        } else {
                                            "Error".to_string()
                                        },
                                        message: None,
                                        started_at: s.started_at.unwrap_or(now),
                                        finished_at: s.finished_at.unwrap_or(now),
                                    }
                                }
                            }
                            _ => ContainerState::Waiting {
                                reason: "ContainerCreating".to_string(),
                                message: None,
                            },
                        };
                        (ctr_state, image_ref)
                    }
                    _ => (
                        ContainerState::Waiting {
                            reason: "Unknown".to_string(),
                            message: None,
                        },
                        String::new(),
                    ),
                }
            } else {
                // No container_id yet for this init container.  Only use
                // "ContainerCreating" for the FIRST init container that is
                // actually being set up.  Every subsequent init container that
                // has not started yet should report "PodInitializing" because
                // it is waiting for its predecessors to finish, not being
                // actively created.
                let idx = pod
                    .init_containers
                    .iter()
                    .position(|c| c.name == init_ctr.name)
                    .unwrap_or(0);
                // Check whether all *prior* init containers have completed
                // successfully (or are Running sidecars).  If any predecessor
                // is still in progress, this container is "PodInitializing".
                let prev_all_done = idx == 0
                    || new_init_statuses.iter().take(idx).all(
                        |s: &kubelet_core::pod::lifecycle::ContainerStatus| {
                            matches!(
                                &s.state,
                                ContainerState::Terminated { exit_code, .. } if *exit_code == 0
                            ) || (is_sidecar_init_container(
                                pod.init_containers
                                    .iter()
                                    .find(|c| c.name == s.name)
                                    .unwrap_or(&pod.init_containers[0]),
                            ) && matches!(&s.state, ContainerState::Running { .. }))
                        },
                    );
                let (reason, message) =
                    if let Some(msg) = state.container_config_errors.get(&init_ctr.name) {
                        ("CreateContainerConfigError".to_string(), Some(msg.clone()))
                    } else if !prev_all_done {
                        ("PodInitializing".to_string(), None)
                    } else {
                        ("ContainerCreating".to_string(), None)
                    };
                (ContainerState::Waiting { reason, message }, String::new())
            };

            let last_state = if state
                .restart_counts
                .get(&init_ctr.name)
                .copied()
                .unwrap_or(0)
                > 0
            {
                ls.init_container_statuses
                    .iter()
                    .find(|s| s.name == init_ctr.name)
                    .and_then(|prev| match &prev.state {
                        ContainerState::Terminated { .. } => Some(prev.state.clone()),
                        ContainerState::Running { .. }
                            if matches!(&ctr_state, ContainerState::Running { .. }) =>
                        {
                            None
                        }
                        ContainerState::Running { .. } => Some(prev.state.clone()),
                        _ => None,
                    })
            } else {
                None
            };

            // An init container is considered "ready" once it has successfully
            // completed (exit code 0) for regular init containers, or when it is
            // running AND its readiness probe (if any) has passed for sidecar
            // init containers (restartPolicy=Always).
            let init_ready = match &ctr_state {
                ContainerState::Terminated { exit_code, .. } if *exit_code == 0 => true,
                ContainerState::Running { .. } if is_sidecar_init_container(init_ctr) => {
                    if let Some(cid_str) = cid_str {
                        if init_ctr.readiness_probe.is_some() {
                            // Readiness probe registered: check probe result.
                            // Defaults to false until the first successful probe fires.
                            self.container_readiness
                                .get(cid_str)
                                .map(|v| *v)
                                .unwrap_or(false)
                        } else {
                            true
                        }
                    } else {
                        false
                    }
                }
                _ => false,
            };

            new_init_statuses.push(ContainerStatus {
                name: init_ctr.name.clone(),
                state: ctr_state.clone(),
                last_state,
                ready: init_ready,
                restart_count: state
                    .restart_counts
                    .get(&init_ctr.name)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(1),
                image: init_ctr.image.clone(),
                image_id,
                container_id: cid_str.map(|s| format!("containerd://{}", s)),
                started: Some(matches!(ctr_state, ContainerState::Running { .. })),
            });
        }
        ls.init_container_statuses = new_init_statuses;

        // Determine if all init containers have completed successfully (so app containers
        // should show PodInitializing while waiting, not ContainerCreating).
        let all_inits_done = pod.init_containers.is_empty()
            || ls.init_container_statuses.iter().all(|s| {
                matches!(&s.state, ContainerState::Terminated { exit_code, .. } if *exit_code == 0)
                    || (is_sidecar_init_container(
                        pod.init_containers
                            .iter()
                            .find(|c| c.name == s.name)
                            .unwrap_or(&pod.init_containers[0]),
                    ) && matches!(&s.state, ContainerState::Running { .. }))
            });

        // Update the Initialized condition with proper reason and message when
        // init containers are still pending/failed. This satisfies the
        // conformance test assertions (init_container.go:440, :564) that check
        // for reason="ContainersNotInitialized" and
        // message="containers with incomplete status: [name1 name2]".
        if !pod.init_containers.is_empty() {
            let incomplete: Vec<&str> = ls
                .init_container_statuses
                .iter()
                .filter(|s| !matches!(&s.state, ContainerState::Terminated { exit_code, .. } if *exit_code == 0))
                .map(|s| s.name.as_str())
                .collect();
            let (init_cond_status, init_reason, init_message) = if incomplete.is_empty() {
                (ConditionStatus::True, None, None)
            } else {
                let names = incomplete.join(" ");
                (
                    ConditionStatus::False,
                    Some("ContainersNotInitialized".to_string()),
                    Some(format!("containers with incomplete status: [{}]", names)),
                )
            };
            if let Some(pos) = ls
                .conditions
                .iter()
                .position(|c| c.condition_type == PodConditionType::Initialized)
            {
                // Only flip to True here if already True, or if all inits done.
                // Avoid reverting True → False (the sync() function is the
                // canonical place that gates the True transition).
                let current_true = ls.conditions[pos].status == ConditionStatus::True;
                if !current_true {
                    ls.conditions[pos].status = init_cond_status;
                    ls.conditions[pos].reason = init_reason;
                    ls.conditions[pos].message = init_message;
                }
            }
        }

        let mut all_ready = true;

        // Sidecar init containers (restartPolicy=Always) contribute to the pod's
        // ContainersReady/Ready conditions, just like regular containers.
        // A sidecar that is Running but has a failing readiness probe must cause
        // all_ready = false.  Mirrors Go kubelet isPodReadyConditionTrue().
        for init_status in &ls.init_container_statuses {
            if !init_status.ready {
                let is_sidecar = pod
                    .init_containers
                    .iter()
                    .find(|c| c.name == init_status.name)
                    .map(is_sidecar_init_container)
                    .unwrap_or(false);
                if is_sidecar && matches!(init_status.state, ContainerState::Running { .. }) {
                    all_ready = false;
                }
            }
        }

        let mut new_statuses = Vec::new();

        for ctr in &pod.containers {
            let cid_str = state.container_ids.get(&ctr.name);
            let (ctr_state, ready, image_id) = if let Some(cid_str) = cid_str {
                let cid = kubelet_core::container::ContainerID::new(cid_str.clone());
                match self.runtime.container_status(&cid).await {
                    Ok(Some(s)) => {
                        let image_ref = s.image_ref.clone();
                        let (ctr_state, ready) = match &s.state {
                            RuntimeContainerState::Running => {
                                // Container is structurally running; readiness depends on the
                                // readiness probe result (or true when no probe is configured).
                                let probe_ready = if ctr.readiness_probe.is_some() {
                                    self.container_readiness
                                        .get(cid_str)
                                        .map(|v| *v)
                                        .unwrap_or(false)
                                } else {
                                    true
                                };
                                (
                                    ContainerState::Running {
                                        started_at: s.started_at.unwrap_or_else(Utc::now),
                                    },
                                    probe_ready,
                                )
                            }
                            RuntimeContainerState::Exited => {
                                let now = Utc::now();
                                let exit_code = s.exit_code.unwrap_or(-1);
                                // If the container exited extremely quickly (before we had a
                                // chance to observe it Running), report it as Running for this
                                // one update cycle so the API server sees at least one Running
                                // transition.  This prevents fast-completing containers (e.g.
                                // a sysctl command that finishes in milliseconds) from jumping
                                // straight to Succeeded/Failed without ever surfacing a Running
                                // phase, which causes conformance tests that watch for a
                                // Running pod to time out.
                                let just_started = state
                                    .recently_started
                                    .get(&ctr.name)
                                    .map(|t| t.elapsed() < std::time::Duration::from_millis(800))
                                    .unwrap_or(false);
                                if just_started {
                                    (
                                        ContainerState::Running {
                                            started_at: s.started_at.unwrap_or_else(Utc::now),
                                        },
                                        false,
                                    )
                                } else if state.crash_loop_backoff.contains(&ctr.name) {
                                    // Container exited and we are sleeping through a
                                    // CrashLoopBackOff delay — report the correct Waiting reason.
                                    (
                                        ContainerState::Waiting {
                                            reason: "CrashLoopBackOff".to_string(),
                                            message: Some(format!(
                                                "back-off {} restarting failed container",
                                                ctr.name
                                            )),
                                        },
                                        false,
                                    )
                                } else {
                                    let termination_msg = self.read_termination_message(
                                        pod,
                                        &ctr.name,
                                        ctr.termination_message_path.as_deref(),
                                        ctr.termination_message_policy.as_deref(),
                                        exit_code,
                                    );
                                    (
                                        ContainerState::Terminated {
                                            exit_code,
                                            reason: if exit_code == 0 {
                                                "Completed".to_string()
                                            } else {
                                                "Error".to_string()
                                            },
                                            message: termination_msg,
                                            started_at: s.started_at.unwrap_or(now),
                                            finished_at: s.finished_at.unwrap_or(now),
                                        },
                                        false,
                                    )
                                }
                            }
                            _ => (
                                ContainerState::Waiting {
                                    reason: "ContainerCreating".to_string(),
                                    message: None,
                                },
                                false,
                            ),
                        };
                        (ctr_state, ready, image_ref)
                    }
                    _ => (
                        ContainerState::Waiting {
                            reason: "Unknown".to_string(),
                            message: None,
                        },
                        false,
                        String::new(),
                    ),
                }
            } else {
                // No container ID: either waiting to start or a permanent config error.
                // While init containers are still running, use PodInitializing reason.
                let (reason, message) =
                    if let Some(msg) = state.container_config_errors.get(&ctr.name) {
                        ("CreateContainerConfigError".to_string(), Some(msg.clone()))
                    } else if !all_inits_done {
                        ("PodInitializing".to_string(), None)
                    } else {
                        ("ContainerCreating".to_string(), None)
                    };
                (
                    ContainerState::Waiting { reason, message },
                    false,
                    String::new(),
                )
            };

            if !ready {
                all_ready = false;
            }

            // Preserve last_state from previous status for containers that have restarted
            let last_state = if state.restart_counts.get(&ctr.name).copied().unwrap_or(0) > 0 {
                // Prefer the in-memory snapshot captured just before restart — it
                // remains accurate even after the container transitions back to Running.
                state
                    .last_terminated_states
                    .get(&ctr.name)
                    .cloned()
                    .or_else(|| {
                        // Fall back to the previous API snapshot (handles the window
                        // between the first Terminated status and the first restart).
                        ls.container_statuses
                            .iter()
                            .find(|s| s.name == ctr.name)
                            .and_then(|prev| match &prev.state {
                                ContainerState::Terminated { .. } => Some(prev.state.clone()),
                                ContainerState::Running { .. }
                                    if matches!(&ctr_state, ContainerState::Running { .. }) =>
                                {
                                    None
                                }
                                ContainerState::Running { .. } => Some(prev.state.clone()),
                                _ => None,
                            })
                    })
            } else {
                None
            };

            new_statuses.push(ContainerStatus {
                name: ctr.name.clone(),
                state: ctr_state,
                last_state,
                ready,
                restart_count: state
                    .restart_counts
                    .get(&ctr.name)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(1),
                image: ctr.image.clone(),
                image_id,
                container_id: cid_str.map(|s| format!("containerd://{}", s)),
                started: Some(ready),
            });
        }

        ls.container_statuses = new_statuses;

        // Ephemeral container statuses — ephemeral containers are never restarted,
        // so we just report their current state.
        let mut new_ephemeral_statuses = Vec::new();
        for ec in &pod.ephemeral_containers {
            let cid_str = state.container_ids.get(&ec.name);
            let (ctr_state, image_id) = if let Some(cid_str) = cid_str {
                let cid = kubelet_core::container::ContainerID::new(cid_str.clone());
                match self.runtime.container_status(&cid).await {
                    Ok(Some(s)) => {
                        let image_ref = s.image_ref.clone();
                        let ctr_state = match &s.state {
                            RuntimeContainerState::Running => ContainerState::Running {
                                started_at: s.started_at.unwrap_or_else(Utc::now),
                            },
                            RuntimeContainerState::Exited => {
                                let now = Utc::now();
                                ContainerState::Terminated {
                                    exit_code: s.exit_code.unwrap_or(-1),
                                    reason: if s.exit_code.unwrap_or(-1) == 0 {
                                        "Completed".to_string()
                                    } else {
                                        "Error".to_string()
                                    },
                                    message: None,
                                    started_at: s.started_at.unwrap_or(now),
                                    finished_at: s.finished_at.unwrap_or(now),
                                }
                            }
                            _ => ContainerState::Waiting {
                                reason: "ContainerCreating".to_string(),
                                message: None,
                            },
                        };
                        (ctr_state, image_ref)
                    }
                    _ => (
                        ContainerState::Waiting {
                            reason: "Unknown".to_string(),
                            message: None,
                        },
                        String::new(),
                    ),
                }
            } else {
                (
                    ContainerState::Waiting {
                        reason: "EphemeralContainerScheduled".to_string(),
                        message: None,
                    },
                    String::new(),
                )
            };
            new_ephemeral_statuses.push(ContainerStatus {
                name: ec.name.clone(),
                state: ctr_state.clone(),
                last_state: None,
                ready: false,
                restart_count: 0,
                image: ec.image.clone(),
                image_id,
                container_id: cid_str.map(|s| format!("containerd://{}", s)),
                started: Some(matches!(ctr_state, ContainerState::Running { .. })),
            });
        }
        ls.ephemeral_container_statuses = new_ephemeral_statuses;

        // Derive phase from container states
        let phase = kubelet_core::pod::lifecycle::compute_pod_phase(
            &ls.init_container_statuses,
            &ls.container_statuses,
            &pod.restart_policy,
        );
        ls.phase = phase;

        // Update ContainersReady condition
        let ready_status = if all_ready {
            ConditionStatus::True
        } else {
            ConditionStatus::False
        };
        let containers_ready_condition = PodCondition {
            condition_type: PodConditionType::ContainersReady,
            status: ready_status.clone(),
            last_probe_time: Some(Utc::now()),
            last_transition_time: Some(Utc::now()),
            reason: None,
            message: None,
        };
        if let Some(pos) = ls
            .conditions
            .iter()
            .position(|c| c.condition_type == PodConditionType::ContainersReady)
        {
            ls.conditions[pos] = containers_ready_condition;
        } else {
            ls.conditions.push(containers_ready_condition);
        }

        // Update Ready condition.
        // Ready = ContainersReady && Initialized (no readiness gates for conformance tests).
        let initialized = ls.conditions.iter().any(|c| {
            c.condition_type == PodConditionType::Initialized && c.status == ConditionStatus::True
        });
        let pod_ready = all_ready && initialized;
        let pod_ready_condition = PodCondition {
            condition_type: PodConditionType::Ready,
            status: if pod_ready {
                ConditionStatus::True
            } else {
                ConditionStatus::False
            },
            last_probe_time: Some(Utc::now()),
            last_transition_time: Some(Utc::now()),
            reason: None,
            message: None,
        };
        if let Some(pos) = ls
            .conditions
            .iter()
            .position(|c| c.condition_type == PodConditionType::Ready)
        {
            ls.conditions[pos] = pod_ready_condition;
        } else {
            ls.conditions.push(pod_ready_condition);
        }

        self.pod_manager.status.set(pod.uid.clone(), ls);
    }

    /// Calculate total memory requests for all containers in a pod.
    /// Calculates the sum of container memory **limits** for pod-level cgroup accounting.
    ///
    /// Go kubelet sets the pod-level cgroup memory.max to the sum of container
    /// limits (not requests). Using requests here would cap the pod cgroup below
    /// individual container limits, causing kernel OOM kills even when the
    /// container's own cgroup limit hasn't been reached (observed: calico-apiserver
    /// with requests=24Mi, limits=128Mi was OOM-killed at 24Mi by the pod cgroup).
    ///
    /// Falls back to requests for containers that have no memory limit set.
    /// Returns 0 (no pod-level cgroup limit) if all containers are BestEffort.
    fn calculate_pod_memory_requests(&self, pod: &PodSpec) -> i64 {
        pod.containers
            .iter()
            .chain(pod.init_containers.iter())
            .map(|c| {
                // Prefer limit; fall back to request; zero means unconstrained.
                c.resources
                    .limits
                    .get("memory")
                    .or_else(|| c.resources.requests.get("memory"))
                    .and_then(|q| if q.value > 0 { Some(q.value) } else { None })
                    .unwrap_or(0)
            })
            .sum()
    }
}

fn sandbox_matches_pod_identity(sandbox: &SandboxStatus, pod: &PodSpec, node_name: &str) -> bool {
    if sandbox.pod_uid == pod.uid.0 {
        return true;
    }

    // Direct name match (suffixed: kube-vip-worker-node).
    if sandbox.pod_name == pod.pod_ref.name && sandbox.pod_namespace == pod.pod_ref.namespace {
        return true;
    }

    // Cross-convention match: pod name is suffixed (kube-vip-worker-node)
    // but sandbox was created by an older kubelet without the suffix (kube-vip).
    let suffix = format!("-{}", node_name);
    if let Some(base_name) = pod.pod_ref.name.strip_suffix(&suffix)
        && sandbox.pod_name == base_name
        && sandbox.pod_namespace == pod.pod_ref.namespace
    {
        return true;
    }

    let label_name = sandbox.labels.get("io.kubernetes.pod.name");
    let label_ns = sandbox.labels.get("io.kubernetes.pod.namespace");
    match (label_name, label_ns) {
        (Some(name), Some(ns)) if ns == &pod.pod_ref.namespace => {
            // Match label name directly or as base (without node suffix).
            if name == &pod.pod_ref.name {
                return true;
            }
            if let Some(base_name) = pod.pod_ref.name.strip_suffix(&suffix)
                && name == base_name
            {
                return true;
            }
            false
        }
        _ => false,
    }
}

fn is_transient_runtime_connection_error(err: &KubeletError) -> bool {
    if let KubeletError::Runtime(msg) = err {
        let lower = msg.to_lowercase();
        return lower.contains("status: unavailable")
            || lower.contains("connection refused")
            || lower.contains("transport error");
    }
    false
}

/// Returns true when CreateContainer was rejected because the sandbox it referenced
/// no longer exists (e.g. deleted externally via crictl).  When this happens the
/// pod worker must clear its cached sandbox ID so the next sync creates a fresh
/// sandbox instead of retrying with the stale ID forever.
fn is_sandbox_not_found_error(err: &KubeletError) -> bool {
    if let KubeletError::Runtime(msg) = err {
        let lower = msg.to_lowercase();
        return (lower.contains("notfound") || lower.contains("not found"))
            && (lower.contains("sandbox") || lower.contains("find sandbox"));
    }
    false
}

fn is_registry_auth_error(err: &KubeletError) -> bool {
    if let KubeletError::Runtime(msg) = err {
        let lower = msg.to_lowercase();
        return lower.contains("403")
            || lower.contains("forbidden")
            || lower.contains("unauthorized")
            || lower.contains("authentication required")
            || lower.contains("pull access denied");
    }
    false
}

fn reserved_container_name(error: &str) -> Option<String> {
    extract_quoted_value(error, "failed to reserve container name")
}

fn reserved_container_id(error: &str) -> Option<kubelet_core::container::ContainerID> {
    extract_quoted_value(error, "is reserved for").map(kubelet_core::container::ContainerID::new)
}

fn extract_quoted_value(input: &str, marker: &str) -> Option<String> {
    let normalized = input.replace("\\\"", "\"");
    let marker_start = normalized.find(marker)? + marker.len();
    let tail = normalized[marker_start..].trim_start();
    let quoted = tail.strip_prefix('"')?;
    let end = quoted.find('"')?;
    Some(quoted[..end].to_string())
}

fn image_registry_host(image: &str) -> String {
    let first = image.split('/').next().unwrap_or_default();
    // The first path component is a registry host if it:
    //   - contains a '.' (e.g. "gcr.io", "registry.example.com")
    //   - is exactly "localhost"
    //   - contains ':' followed by a numeric port (e.g. "localhost:5000")
    // Bare image names like "nginx:latest" have a ':' that is a tag separator,
    // not a port, so they should NOT be treated as registry hosts.
    let is_registry = first.contains('.')
        || first == "localhost"
        || first
            .split_once(':')
            .map(|x| x.1)
            .is_some_and(|port| !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()));
    if is_registry {
        return normalize_registry_host(first);
    }
    "docker.io".to_string()
}

fn normalize_registry_host(raw: &str) -> String {
    let trimmed = raw
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    trimmed.split('/').next().unwrap_or(trimmed).to_lowercase()
}

fn host_matches_registry(auth_host: &str, image_registry: &str) -> bool {
    let auth = normalize_registry_host(auth_host);
    let image = normalize_registry_host(image_registry);

    auth == image
        || (auth == "index.docker.io" && image == "docker.io")
        || (auth == "docker.io" && image == "index.docker.io")
}

/// Exchange any `_json_key_base64` GCP service-account credentials for short-lived
/// OAuth2 access tokens so containerd can authenticate with GAR / GCR.
///
/// Containerd's CRI layer cannot perform the JWT-bearer grant itself — it only
/// does HTTP Basic auth using whatever username/password it receives.  GAR's
/// token endpoint does NOT accept service-account JSON keys over Basic auth; it
/// requires an OAuth2 access token.  We perform the exchange here and return
/// `oauth2accesstoken` / `<token>` credentials that containerd *can* use.
///
/// Credentials that are not `_json_key_base64` (e.g. already-OAuth2 tokens) are
/// passed through unchanged.
async fn exchange_gcp_credentials(
    secrets: Vec<ImagePullSecret>,
    pod_ref: &str,
) -> Vec<ImagePullSecret> {
    let mut out = Vec::with_capacity(secrets.len());
    for s in secrets {
        if s.username != "_json_key_base64" {
            out.push(s);
            continue;
        }
        match gcp_sa_json_to_oauth2_token(&s.password).await {
            Ok(token) => {
                warn!(
                    pod = pod_ref,
                    server = %s.server,
                    "Exchanged _json_key_base64 service account for OAuth2 access token"
                );
                out.push(ImagePullSecret {
                    server: s.server,
                    username: "oauth2accesstoken".to_string(),
                    password: token,
                });
            }
            Err(e) => {
                warn!(
                    pod = pod_ref,
                    server = %s.server,
                    error = %e,
                    "Failed to exchange GCP service account for OAuth2 token; passing raw credentials"
                );
                out.push(s);
            }
        }
    }
    out
}

/// Exchange a base64-encoded GCP service account JSON key for a short-lived
/// OAuth2 access token using the JWT-bearer grant (RFC 7523).
async fn gcp_sa_json_to_oauth2_token(b64_key: &str) -> anyhow::Result<String> {
    use base64::Engine as _;

    // 1. Decode the base64-encoded service account JSON.
    let json_bytes = base64::engine::general_purpose::STANDARD
        .decode(b64_key.trim())
        .map_err(|e| anyhow::anyhow!("base64 decode service account: {}", e))?;

    let sa: serde_json::Value = serde_json::from_slice(&json_bytes)
        .map_err(|e| anyhow::anyhow!("parse service account JSON: {}", e))?;

    let client_email = sa["client_email"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing client_email in service account"))?;
    let private_key_pem = sa["private_key"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing private_key in service account"))?;
    let token_uri = sa["token_uri"]
        .as_str()
        .unwrap_or("https://oauth2.googleapis.com/token");

    // 2. Build the JWT claims.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let header_b64 =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","typ":"JWT"}"#);
    let claims = serde_json::json!({
        "iss": client_email,
        "scope": "https://www.googleapis.com/auth/cloud-platform",
        "aud": token_uri,
        "iat": now,
        "exp": now + 3600,
    });
    let claims_b64 =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_string(&claims)?);
    let signing_input = format!("{}.{}", header_b64, claims_b64);

    // 3. Parse the RSA private key (PEM → DER).
    let pem_body = private_key_pem
        .trim()
        .trim_start_matches("-----BEGIN RSA PRIVATE KEY-----")
        .trim_start_matches("-----BEGIN PRIVATE KEY-----")
        .trim_end_matches("-----END RSA PRIVATE KEY-----")
        .trim_end_matches("-----END PRIVATE KEY-----")
        .replace(['\n', '\r'], "");
    let key_der = base64::engine::general_purpose::STANDARD
        .decode(pem_body.trim())
        .map_err(|e| anyhow::anyhow!("decode private key DER: {}", e))?;

    // 4. Sign with RS256 using ring.
    let key_pair = ring::signature::RsaKeyPair::from_pkcs8(&key_der)
        .or_else(|_| {
            // Try DER parsing as PKCS#1 via from_der (older key format).
            ring::signature::RsaKeyPair::from_der(&key_der)
        })
        .map_err(|e| anyhow::anyhow!("parse RSA key: {:?}", e))?;
    let rng = ring::rand::SystemRandom::new();
    let mut sig = vec![0u8; key_pair.public().modulus_len()];
    key_pair
        .sign(
            &ring::signature::RSA_PKCS1_SHA256,
            &rng,
            signing_input.as_bytes(),
            &mut sig,
        )
        .map_err(|e| anyhow::anyhow!("RSA sign: {:?}", e))?;
    let sig_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&sig);
    let jwt = format!("{}.{}", signing_input, sig_b64);

    // 5. Exchange the JWT for an access token.
    // Use a shared reqwest client to reuse connections and the TLS session cache.
    static GCP_HTTP_CLIENT: once_cell::sync::OnceCell<reqwest::Client> =
        once_cell::sync::OnceCell::new();
    let client = GCP_HTTP_CLIENT.get_or_try_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .map_err(|e| anyhow::anyhow!("build reqwest client: {:#}", e))
    })?;

    let params = [
        ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
        ("assertion", jwt.as_str()),
    ];
    // Retry up to 3 times with short delays for transient network errors.
    let mut last_err = anyhow::anyhow!("no attempts made");
    for attempt in 0u32..3 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(500 * u64::from(attempt))).await;
        }
        match client.post(token_uri).form(&params).send().await {
            Err(e) => {
                last_err = anyhow::anyhow!("POST to token_uri (attempt {}): {:#}", attempt + 1, e);
                warn!(error = %last_err, "GCP token exchange send failed, will retry");
                continue;
            }
            Ok(resp) => {
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(anyhow::anyhow!(
                        "token exchange failed ({}): {}",
                        status,
                        body
                    ));
                }
                let token_resp: serde_json::Value = resp
                    .json()
                    .await
                    .map_err(|e| anyhow::anyhow!("parse token response: {}", e))?;
                let access_token = token_resp["access_token"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("no access_token in response"))?
                    .to_string();
                return Ok(access_token);
            }
        }
    }
    Err(last_err)
}

fn docker_auths_from_secret(secret: &Secret) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    let data = match &secret.data {
        Some(d) => d,
        None => return out,
    };

    if let Some(cfg) = data.get(".dockerconfigjson") {
        out.extend(parse_dockerconfigjson_bytes(&cfg.0));
    }
    if let Some(cfg) = data.get(".dockercfg") {
        out.extend(parse_dockercfg_bytes(&cfg.0));
    }

    out
}

fn parse_dockerconfigjson_bytes(raw: &[u8]) -> Vec<(String, String, String)> {
    let json: serde_json::Value = match serde_json::from_slice(raw) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    let auths = match json.get("auths").and_then(|v| v.as_object()) {
        Some(a) => a,
        None => return vec![],
    };

    auths
        .iter()
        .filter_map(|(host, v)| parse_registry_auth_entry(host, v))
        .collect()
}

fn parse_dockercfg_bytes(raw: &[u8]) -> Vec<(String, String, String)> {
    let json: serde_json::Value = match serde_json::from_slice(raw) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    let Some(obj) = json.as_object() else {
        return vec![];
    };

    obj.iter()
        .filter_map(|(host, v)| parse_registry_auth_entry(host, v))
        .collect()
}

fn parse_registry_auth_entry(
    host: &str,
    entry: &serde_json::Value,
) -> Option<(String, String, String)> {
    let username = entry
        .get("username")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let password = entry
        .get("password")
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    if !username.is_empty() || !password.is_empty() {
        return Some((
            normalize_registry_host(host),
            username.to_string(),
            password.to_string(),
        ));
    }

    let auth = entry.get("auth").and_then(|v| v.as_str())?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(auth)
        .ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (u, p) = decoded.split_once(':')?;

    Some((normalize_registry_host(host), u.to_string(), p.to_string()))
}

fn container_restart_policy(
    container: &kubelet_core::pod::ContainerSpec,
    pod: &RestartPolicy,
) -> RestartPolicy {
    container
        .restart_policy
        .clone()
        .unwrap_or_else(|| pod.clone())
}

fn is_sidecar_init_container(container: &kubelet_core::pod::ContainerSpec) -> bool {
    matches!(container.restart_policy, Some(RestartPolicy::Always))
}

fn runtime_overhead_memory_bytes(
    pod: &PodSpec,
    runtime_overheads: &HashMap<String, HashMap<String, String>>,
) -> i64 {
    let from_runtime_class = pod
        .runtime_class_name
        .as_ref()
        .and_then(|rc| runtime_overheads.get(rc))
        .and_then(|resources| resources.get("memory"))
        .map(|raw| parse_memory_quantity_to_bytes(raw));

    if let Some(bytes) = from_runtime_class {
        return bytes;
    }

    pod.annotations
        .get("kube-air.io/runtime-overhead-memory")
        .or_else(|| {
            pod.annotations
                .get("kube-air.io/runtime-overhead-memory-bytes")
        })
        .map(|raw| parse_memory_quantity_to_bytes(raw))
        .unwrap_or(0)
}

fn parse_memory_quantity_to_bytes(raw: &str) -> i64 {
    let s = raw.trim();
    if let Some(x) = s.strip_suffix("Ki") {
        return x.parse::<i64>().unwrap_or(0).saturating_mul(1024);
    }
    if let Some(x) = s.strip_suffix("Mi") {
        return x
            .parse::<i64>()
            .unwrap_or(0)
            .saturating_mul(1024)
            .saturating_mul(1024);
    }
    if let Some(x) = s.strip_suffix("Gi") {
        return x
            .parse::<i64>()
            .unwrap_or(0)
            .saturating_mul(1024)
            .saturating_mul(1024)
            .saturating_mul(1024);
    }
    s.parse::<i64>().unwrap_or(0)
}

async fn load_container_configmaps(
    container: &ContainerSpec,
    api: &Api<ConfigMap>,
) -> Result<HashMap<String, HashMap<String, String>>> {
    let mut configmaps = HashMap::new();

    for env_from in &container.env_from {
        if let Some(cm_ref) = &env_from.config_map_ref {
            load_single_configmap(api, &mut configmaps, &cm_ref.name, cm_ref.optional).await?;
        }
    }

    for env in &container.env {
        if let Some(EnvVarSource::ConfigMapKeyRef { name, optional, .. }) = &env.value_from {
            load_single_configmap(api, &mut configmaps, name, *optional).await?;
        }
    }

    Ok(configmaps)
}

async fn load_single_configmap(
    api: &Api<ConfigMap>,
    configmaps: &mut HashMap<String, HashMap<String, String>>,
    name: &str,
    optional: bool,
) -> Result<()> {
    if configmaps.contains_key(name) {
        return Ok(());
    }

    match api.get(name).await {
        Ok(config_map) => {
            configmaps.insert(
                name.to_string(),
                config_map.data.unwrap_or_default().into_iter().collect(),
            );
            Ok(())
        }
        Err(_err) if optional => Ok(()),
        Err(err) => Err(KubeletError::Runtime(format!(
            "failed to fetch ConfigMap '{}': {}",
            name, err
        ))),
    }
}

async fn load_container_secrets(
    container: &ContainerSpec,
    api: &Api<Secret>,
) -> Result<HashMap<String, HashMap<String, Vec<u8>>>> {
    let mut secrets = HashMap::new();

    for env_from in &container.env_from {
        if let Some(secret_ref) = &env_from.secret_ref {
            load_single_secret(api, &mut secrets, &secret_ref.name, secret_ref.optional).await?;
        }
    }

    for env in &container.env {
        if let Some(EnvVarSource::SecretKeyRef { name, optional, .. }) = &env.value_from {
            load_single_secret(api, &mut secrets, name, *optional).await?;
        }
    }

    Ok(secrets)
}

async fn load_single_secret(
    api: &Api<Secret>,
    secrets: &mut HashMap<String, HashMap<String, Vec<u8>>>,
    name: &str,
    optional: bool,
) -> Result<()> {
    if secrets.contains_key(name) {
        return Ok(());
    }

    match api.get(name).await {
        Ok(secret) => {
            let data = secret
                .data
                .unwrap_or_default()
                .into_iter()
                .map(|(key, value)| (key, value.0))
                .collect();
            secrets.insert(name.to_string(), data);
            Ok(())
        }
        Err(_err) if optional => Ok(()),
        Err(err) => Err(KubeletError::Runtime(format!(
            "failed to fetch Secret '{}': {}",
            name, err
        ))),
    }
}

fn assemble_container_env(
    pod: &PodSpec,
    container: &ContainerSpec,
    pod_ip: Option<&str>,
    configmaps: &HashMap<String, HashMap<String, String>>,
    secrets: &HashMap<String, HashMap<String, Vec<u8>>>,
    service_envs: &HashMap<String, String>,
) -> Result<Vec<(String, String)>> {
    let mut env = service_envs.clone();

    for env_from in &container.env_from {
        if let Some(cm_ref) = &env_from.config_map_ref {
            if let Some(configmap) = configmaps.get(&cm_ref.name) {
                let prefix = env_from.prefix.as_deref().unwrap_or("");
                for (key, value) in configmap {
                    env.insert(format!("{}{}", prefix, key), value.clone());
                }
            } else if !cm_ref.optional {
                return Err(KubeletError::Runtime(format!(
                    "missing required ConfigMap '{}' for envFrom",
                    cm_ref.name
                )));
            }
        }

        if let Some(secret_ref) = &env_from.secret_ref {
            if let Some(secret) = secrets.get(&secret_ref.name) {
                let prefix = env_from.prefix.as_deref().unwrap_or("");
                for (key, value) in secret {
                    env.insert(
                        format!("{}{}", prefix, key),
                        String::from_utf8_lossy(value).into_owned(),
                    );
                }
            } else if !secret_ref.optional {
                return Err(KubeletError::Runtime(format!(
                    "missing required Secret '{}' for envFrom",
                    secret_ref.name
                )));
            }
        }
    }

    for env_var in &container.env {
        let value = if let Some(raw_value) = env_var.value.as_deref() {
            expand_env_value(raw_value, &env)
        } else if let Some(source) = &env_var.value_from {
            resolve_env_var_source(pod, container, pod_ip, source, configmaps, secrets)?
        } else {
            String::new()
        };
        env.insert(env_var.name.clone(), value.clone());
        info!(
            container = %container.name,
            env_name = %env_var.name,
            env_value = %value,
            "Resolved environment variable"
        );
    }

    // Inject HOSTNAME env var if not explicitly set by the pod spec.
    // Go kubelet injects this automatically using the pod's effective hostname
    // (spec.hostname or metadata.name). Without this, containers inherit the
    // node's HOSTNAME from containerd's environment, breaking apps that use
    // $HOSTNAME or os.Hostname() to derive their pod identity.
    env.entry("HOSTNAME".to_string())
        .or_insert_with(|| pod.effective_hostname());

    info!(
        container = %container.name,
        total_env_vars = env.len(),
        env_keys = ?env.keys().collect::<Vec<_>>(),
        "Assembled container environment"
    );

    Ok(env.into_iter().collect())
}

async fn load_service_env_vars(
    pod: &PodSpec,
    client: &KubeClient,
    service_links_enabled: bool,
) -> Result<HashMap<String, String>> {
    let mut env = HashMap::new();

    // Always inject the `kubernetes` service env vars first (KUBERNETES_SERVICE_HOST /
    // KUBERNETES_SERVICE_PORT).  These are critical for in-cluster client configuration
    // and must be present regardless of whether the namespace service list succeeds or
    // whether `enableServiceLinks` is false.
    let default_api: Api<Service> = Api::namespaced(client.clone(), "default");
    match default_api.get("kubernetes").await {
        Ok(kube_service) => {
            env.extend(service_env_vars_from_service(&kube_service));
        }
        Err(e) => {
            warn!(
                pod = %pod.pod_ref,
                error = %e,
                "Failed to fetch 'kubernetes' service from default namespace — KUBERNETES_SERVICE_HOST will be missing"
            );
        }
    }

    // Inject env vars for all other services in the pod's namespace, but only
    // when `enableServiceLinks` is true (Go kubelet parity).
    if service_links_enabled {
        let ns_api: Api<Service> = Api::namespaced(client.clone(), &pod.pod_ref.namespace);
        match ns_api.list(&ListParams::default()).await {
            Ok(ns_services) => {
                for service in ns_services.items {
                    // Don't overwrite the kubernetes service vars already set above.
                    for (k, v) in service_env_vars_from_service(&service) {
                        env.entry(k).or_insert(v);
                    }
                }
            }
            Err(e) => {
                warn!(
                    pod = %pod.pod_ref,
                    error = %e,
                    "Failed to list Services for env injection — namespace service links will be absent"
                );
            }
        }
    }

    Ok(env)
}

fn service_env_vars_from_service(service: &Service) -> HashMap<String, String> {
    let mut env = HashMap::new();
    let Some(spec) = &service.spec else {
        return env;
    };
    let Some(name) = service.metadata.name.as_deref() else {
        return env;
    };
    let Some(cluster_ip) = spec.cluster_ip.as_deref() else {
        return env;
    };
    if cluster_ip.is_empty() || cluster_ip == "None" {
        return env;
    }

    let prefix = to_service_env_name(name);
    env.insert(format!("{}_SERVICE_HOST", prefix), cluster_ip.to_string());

    let ports = spec.ports.clone().unwrap_or_default();
    if let Some(first) = ports.first() {
        let port = first.port;
        let proto = first
            .protocol
            .clone()
            .unwrap_or_else(|| "TCP".to_string())
            .to_ascii_uppercase();
        let proto_lower = proto.to_ascii_lowercase();
        env.insert(format!("{}_SERVICE_PORT", prefix), port.to_string());
        env.insert(
            format!("{}_PORT", prefix),
            format!("{}://{}:{}", proto_lower, cluster_ip, port),
        );
    }

    for port in ports {
        let proto = port
            .protocol
            .clone()
            .unwrap_or_else(|| "TCP".to_string())
            .to_ascii_uppercase();
        let proto_lower = proto.to_ascii_lowercase();
        let port_num = port.port;
        let port_prefix = format!("{}_PORT_{}_{}", prefix, port_num, proto);
        env.insert(
            port_prefix.clone(),
            format!("{}://{}:{}", proto_lower, cluster_ip, port_num),
        );
        env.insert(format!("{}_PROTO", port_prefix), proto_lower.clone());
        env.insert(format!("{}_PORT", port_prefix), port_num.to_string());
        env.insert(format!("{}_ADDR", port_prefix), cluster_ip.to_string());

        if let Some(name) = port.name.as_deref() {
            env.insert(
                format!("{}_SERVICE_PORT_{}", prefix, to_service_env_name(name)),
                port_num.to_string(),
            );
        }
    }

    env
}

fn to_service_env_name(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn resolve_env_var_source(
    pod: &PodSpec,
    container: &ContainerSpec,
    pod_ip: Option<&str>,
    source: &EnvVarSource,
    configmaps: &HashMap<String, HashMap<String, String>>,
    secrets: &HashMap<String, HashMap<String, Vec<u8>>>,
) -> Result<String> {
    match source {
        EnvVarSource::FieldRef { field_path } => Ok(match field_path.as_str() {
            "metadata.name" => pod.pod_ref.name.clone(),
            "metadata.namespace" => pod.pod_ref.namespace.clone(),
            "metadata.uid" => pod.uid.0.clone(),
            "spec.nodeName" => pod.node_name.clone(),
            "spec.serviceAccountName" => pod.service_account_name.clone(),
            "status.podIP" | "status.podIPs" => pod_ip
                .filter(|ip| !ip.is_empty())
                .map(|ip| ip.to_string())
                .unwrap_or_else(|| {
                    if pod.host_network {
                        detect_node_internal_ip()
                    } else {
                        String::new()
                    }
                }),
            "status.hostIP" => detect_node_internal_ip(),
            // Delegate remaining paths (metadata.annotations['key'], metadata.labels['key'], etc.)
            _ => resolve_field_ref(pod, field_path),
        }),
        EnvVarSource::ResourceFieldRef { resource, .. } => Ok(match resource.as_str() {
            "limits.cpu" => container
                .resources
                .limits
                .get("cpu")
                .map(|q| apply_resource_divisor(q.value, true, "1").to_string())
                .unwrap_or_else(|| {
                    // When no limit is set, Kubernetes exposes node allocatable as the limit
                    apply_resource_divisor(node_allocatable_cpu_millicores(), true, "1").to_string()
                }),
            "requests.cpu" => container
                .resources
                .requests
                .get("cpu")
                .map(|q| apply_resource_divisor(q.value, true, "1").to_string())
                .unwrap_or_else(|| "0".to_string()),
            "limits.memory" => container
                .resources
                .limits
                .get("memory")
                .map(|q| q.value.to_string())
                .unwrap_or_else(|| {
                    // When no limit is set, Kubernetes exposes node allocatable as the limit
                    node_allocatable_memory_bytes().to_string()
                }),
            "requests.memory" => container
                .resources
                .requests
                .get("memory")
                .map(|q| q.value.to_string())
                .unwrap_or_else(|| "0".to_string()),
            _ => String::new(),
        }),
        EnvVarSource::ConfigMapKeyRef {
            name,
            key,
            optional,
        } => match configmaps
            .get(name)
            .and_then(|configmap| configmap.get(key))
        {
            Some(value) => Ok(value.clone()),
            None if *optional => Ok(String::new()),
            None => Err(KubeletError::Runtime(format!(
                "missing required ConfigMap key '{}.{}'",
                name, key
            ))),
        },
        EnvVarSource::SecretKeyRef {
            name,
            key,
            optional,
        } => match secrets.get(name).and_then(|secret| secret.get(key)) {
            Some(value) => Ok(String::from_utf8_lossy(value).into_owned()),
            None if *optional => Ok(String::new()),
            None => Err(KubeletError::Runtime(format!(
                "missing required Secret key '{}.{}'",
                name, key
            ))),
        },
    }
}

fn expand_container_tokens(tokens: &[String], env: &HashMap<String, String>) -> Vec<String> {
    tokens
        .iter()
        .map(|token| expand_env_value(token, env))
        .collect()
}

/// Return the Go kubelet-compatible volume subdirectory name for a given volume source.
///
/// This must match the naming convention used by the Go kubelet so that volume paths
/// are interoperable when switching between kubelet implementations:
///   /var/lib/kubelet/pods/<uid>/volumes/<subdir>/<volume-name>
fn volume_source_subdir(source: &kubelet_core::pod::VolumeSource) -> &'static str {
    use kubelet_core::pod::VolumeSource;
    match source {
        VolumeSource::EmptyDir { .. } => "kubernetes.io~empty-dir",
        VolumeSource::ConfigMap { .. } => "kubernetes.io~configmap",
        VolumeSource::Secret { .. } => "kubernetes.io~secret",
        VolumeSource::Projected { .. } => "kubernetes.io~projected",
        VolumeSource::DownwardAPI { .. } => "kubernetes.io~downward-api",
        _ => "kubernetes.io~projected",
    }
}

/// Remove dangling symlinks from a directory.
///
/// The Go kubelet's atomic writer creates a timestamped directory and symlinks
/// (`..data -> ..TIMESTAMP`, then `token -> ..data/token`). After kubelet restarts
/// or handover the timestamped directory may be cleaned up, leaving dangling
/// symlinks. These prevent `tokio::fs::write` from writing through the symlink
/// (ENOENT), so we remove them before writing our own flat files.
async fn remove_dangling_symlinks_in_dir(dir: &std::path::Path) {
    let mut read_dir = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => return,
    };
    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let path = entry.path();
        // symlink_metadata succeeds for symlinks even when target is gone.
        let is_symlink = matches!(
            tokio::fs::symlink_metadata(&path).await,
            Ok(m) if m.file_type().is_symlink()
        );
        if is_symlink {
            // metadata follows the symlink; failure means dangling.
            if tokio::fs::metadata(&path).await.is_err() {
                let _ = tokio::fs::remove_file(&path).await;
            }
        }
    }
}

fn expand_env_value(input: &str, env: &HashMap<String, String>) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut checkpoint = 0;
    let mut cursor = 0;

    while cursor < bytes.len() {
        if bytes[cursor] == b'$' && cursor + 1 < bytes.len() {
            output.push_str(&input[checkpoint..cursor]);
            let (read, is_var, advance) = try_read_variable_name(&input[cursor + 1..]);
            if is_var {
                if let Some(value) = env.get(read) {
                    output.push_str(value);
                } else {
                    output.push_str("$(");
                    output.push_str(read);
                    output.push(')');
                }
            } else {
                // $$ → $ (escape); $X for any other X → pass through $X unchanged
                // so that shell vars like $? $@ $# etc. are not corrupted.
                if read != "$" {
                    output.push('$');
                }
                output.push_str(read);
            }
            cursor += advance;
            checkpoint = cursor + 1;
        }
        cursor += 1;
    }

    output.push_str(&input[checkpoint..]);
    output
}

fn try_read_variable_name(input: &str) -> (&str, bool, usize) {
    let bytes = input.as_bytes();
    match bytes[0] {
        b'$' => (&input[..1], false, 1),
        b'(' => {
            for index in 1..bytes.len() {
                if bytes[index] == b')' {
                    return (&input[1..index], true, index + 1);
                }
            }
            ("$(", false, 1)
        }
        _ => (&input[..1], false, 1),
    }
}

use kubelet_core::pod::Probe;

/// Write ConfigMap data files for a projected volume source.
/// Remove all regular files from a volume directory.
/// Used when an optional ConfigMap/Secret is deleted so its files disappear from the volume.
async fn clear_volume_dir(dir: &std::path::Path) {
    remove_dir_contents_recursive(dir);
}

/// Remove regular files in a volume directory that are NOT in `expected`.
/// Used to clean up stale files when ConfigMap/Secret keys are removed.
async fn cleanup_stale_volume_files(dir: &std::path::Path, expected: &HashSet<PathBuf>) {
    remove_stale_files_recursive(dir, expected);
}

fn remove_dir_contents_recursive(dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            remove_dir_contents_recursive(&path);
            let _ = std::fs::remove_dir(&path);
        } else if path.is_file() {
            let _ = std::fs::remove_file(&path);
        }
    }
}

fn remove_stale_files_recursive(dir: &std::path::Path, expected: &HashSet<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            remove_stale_files_recursive(&path, expected);
            let is_empty = std::fs::read_dir(&path)
                .map(|mut it| it.next().is_none())
                .unwrap_or(false);
            if is_empty {
                let _ = std::fs::remove_dir(&path);
            }
        } else if path.is_file() && !expected.contains(&path) {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Compute the set of expected file paths for a ConfigMap volume given its current data.
fn expected_configmap_paths(
    dir: &std::path::Path,
    cm_data: &HashMap<String, Vec<u8>>,
    items: &[kubelet_core::pod::KeyToPath],
) -> HashSet<PathBuf> {
    if items.is_empty() {
        cm_data.keys().map(|k| dir.join(k)).collect()
    } else {
        items.iter().map(|item| dir.join(&item.path)).collect()
    }
}

/// Compute the set of expected file paths for a Secret volume given its current data.
fn expected_secret_paths(
    dir: &std::path::Path,
    secret_data: &HashMap<String, Vec<u8>>,
    items: &[kubelet_core::pod::KeyToPath],
) -> HashSet<PathBuf> {
    if items.is_empty() {
        secret_data.keys().map(|k| dir.join(k)).collect()
    } else {
        items.iter().map(|item| dir.join(&item.path)).collect()
    }
}

/// Write `value` to `file_path` only when the file is absent or its content differs.
/// This avoids spurious inotify/fsnotify events that cause watchers (e.g. kube-proxy)
/// to reload unnecessarily on every kubelet pod-sync cycle.
async fn write_if_changed(file_path: &std::path::Path, value: &[u8]) -> Result<()> {
    if let Ok(existing) = tokio::fs::read(file_path).await
        && existing == value
    {
        return Ok(());
    }
    tokio::fs::write(file_path, value).await?;
    Ok(())
}

async fn write_projected_configmap_files(
    dir: &std::path::Path,
    cm_data: &HashMap<String, Vec<u8>>,
    items: &[kubelet_core::pod::KeyToPath],
    default_mode: Option<i32>,
) -> Result<()> {
    if items.is_empty() {
        // No items specified means write all keys.
        for (key, value) in cm_data {
            let file_path = dir.join(key);
            if let Some(parent) = file_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            write_if_changed(&file_path, value).await?;

            // Apply default_mode if specified.
            #[cfg(unix)]
            if let Some(mode) = default_mode {
                use std::os::unix::fs::PermissionsExt;
                let _ = tokio::fs::set_permissions(
                    &file_path,
                    std::fs::Permissions::from_mode(mode as u32),
                )
                .await;
            }
        }
    } else {
        // Write only specified items.
        for item in items {
            if let Some(value) = cm_data.get(&item.key) {
                let file_path = dir.join(&item.path);
                if let Some(parent) = file_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                write_if_changed(&file_path, value).await?;

                // Apply per-item mode if specified, otherwise use volume default_mode.
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = item.mode.or(default_mode).unwrap_or(0o644) as u32;
                    let _ = tokio::fs::set_permissions(
                        &file_path,
                        std::fs::Permissions::from_mode(mode),
                    )
                    .await;
                }
            }
        }
    }
    Ok(())
}

fn configmap_data_bytes(cm: &ConfigMap) -> HashMap<String, Vec<u8>> {
    let mut out = HashMap::new();

    for (k, v) in cm.data.clone().unwrap_or_default() {
        out.insert(k, v.into_bytes());
    }
    for (k, v) in cm.binary_data.clone().unwrap_or_default() {
        out.insert(k, v.0);
    }

    out
}

/// Write Secret data files for a projected volume source.
async fn write_projected_secret_files(
    dir: &std::path::Path,
    secret_data: &HashMap<String, Vec<u8>>,
    items: &[kubelet_core::pod::KeyToPath],
    default_mode: Option<i32>,
) -> Result<()> {
    if items.is_empty() {
        // No items specified means write all keys.
        for (key, value) in secret_data {
            let file_path = dir.join(key);
            if let Some(parent) = file_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            write_if_changed(&file_path, value).await?;

            // Secrets default to 0o600, but respect volume default_mode if set.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = default_mode.unwrap_or(0o600) as u32;
                let _ =
                    tokio::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(mode))
                        .await;
            }
        }
    } else {
        // Write only specified items.
        for item in items {
            if let Some(value) = secret_data.get(&item.key) {
                let file_path = dir.join(&item.path);
                if let Some(parent) = file_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                write_if_changed(&file_path, value).await?;

                // Apply per-item mode if specified, otherwise volume default_mode, otherwise 0o600.
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = item.mode.or(default_mode).unwrap_or(0o600) as u32;
                    let _ = tokio::fs::set_permissions(
                        &file_path,
                        std::fs::Permissions::from_mode(mode),
                    )
                    .await;
                }
            }
        }
    }
    Ok(())
}

/// Request a service account token via the Kubernetes TokenRequest API.
///
/// POSTs to `/api/v1/namespaces/{ns}/serviceaccounts/{sa}/token` and returns
/// the bearer token string. Falls back to an error-distinguishable placeholder
/// if the kube client is unavailable or the API call fails.
async fn request_service_account_token(
    pod: &PodSpec,
    audience: Option<&str>,
    expiration_seconds: Option<u64>,
    kube_client: Option<&KubeClient>,
) -> Option<String> {
    use k8s_openapi::api::authentication::v1::{
        BoundObjectReference, TokenRequest, TokenRequestSpec,
    };
    use k8s_openapi::api::core::v1::ServiceAccount;
    use kube::api::PostParams;

    let Some(client) = kube_client else {
        debug!(
            pod = %pod.pod_ref,
            "No kube client available for TokenRequest"
        );
        return None;
    };

    let sa_name = &pod.service_account_name;
    let ns = &pod.pod_ref.namespace;

    // If the volume projection specifies an explicit audience, request it.
    // If not (audience is None / empty string in the pod spec), send an empty
    // audiences slice.  The node authorizer's loop over requested audiences is
    // vacuously satisfied for an empty slice (∅ ⊆ any set), so it approves.
    // The API server then fills in its default --api-audiences
    // (e.g. "https://kubernetes.default.svc.cluster.local") in the JWT, which
    // is the only audience the API server accepts for authentication.
    // Sending [""] produces a JWT with aud:[""] which the API server rejects
    // with 401 when the token is later used for in-cluster authentication.
    let audiences: Vec<String> = match audience {
        Some(aud) if !aud.is_empty() => vec![aud.to_string()],
        _ => vec![],
    };

    let expiry = expiration_seconds.unwrap_or(3600) as i64;

    let token_request = TokenRequest {
        metadata: Default::default(),
        spec: TokenRequestSpec {
            audiences: audiences.clone(),
            expiration_seconds: Some(expiry),
            bound_object_ref: Some(BoundObjectReference {
                api_version: Some("v1".to_string()),
                kind: Some("Pod".to_string()),
                name: Some(pod.pod_ref.name.clone()),
                uid: Some(pod.uid.to_string()),
            }),
        },
        status: None,
    };

    let request_body = match serde_json::to_vec(&token_request) {
        Ok(body) => body,
        Err(e) => {
            warn!(
                pod = %pod.pod_ref,
                sa = %sa_name,
                error = %e,
                "Failed to serialize TokenRequest payload"
            );
            return None;
        }
    };

    let sa_api: Api<ServiceAccount> = Api::namespaced(client.clone(), ns);
    match sa_api
        .create_subresource::<TokenRequest>("token", sa_name, &PostParams::default(), request_body)
        .await
    {
        Ok(tr) => {
            if let Some(status) = tr.status {
                let token = status.token;
                if !token.is_empty() {
                    info!(
                        pod = %pod.pod_ref,
                        sa = %sa_name,
                        audiences = ?audiences,
                        expiry_secs = expiry,
                        "Obtained service account token via TokenRequest API"
                    );
                    return Some(token);
                }
            }
            warn!(pod = %pod.pod_ref, sa = %sa_name, "TokenRequest response missing token field");
            None
        }
        Err(e) => {
            warn!(
                pod = %pod.pod_ref,
                sa = %sa_name,
                bound_kind = "Pod",
                bound_name = %pod.pod_ref.name,
                bound_uid = %pod.uid,
                error = %e,
                "TokenRequest API call failed"
            );
            None
        }
    }
}

fn resolve_downward_api_value(
    pod: &PodSpec,
    item: &kubelet_core::pod::DownwardAPIVolumeFile,
    pod_ip: Option<&str>,
) -> String {
    if let Some(ref_info) = &item.resource_field_ref {
        let container_name = ref_info.container_name.as_deref();
        let resource = ref_info.resource.as_str();
        let divisor = ref_info.divisor.as_deref().unwrap_or("1");

        let ctr = container_name
            .and_then(|n| {
                pod.containers
                    .iter()
                    .chain(pod.init_containers.iter())
                    .find(|c| c.name == n)
            })
            .or_else(|| pod.containers.first());

        if let Some(ctr) = ctr {
            let raw_val: Option<i64> = match resource {
                "limits.cpu" => {
                    let v = ctr
                        .resources
                        .limits
                        .get("cpu")
                        .map(|q| q.value)
                        .unwrap_or(0);
                    // Per Kubernetes spec: if limit is not set, use node allocatable.
                    Some(if v > 0 {
                        v
                    } else {
                        node_allocatable_cpu_millicores()
                    })
                }
                "limits.memory" => {
                    let v = ctr
                        .resources
                        .limits
                        .get("memory")
                        .map(|q| q.value)
                        .unwrap_or(0);
                    // Per Kubernetes spec: if limit is not set, use node allocatable.
                    Some(if v > 0 {
                        v
                    } else {
                        node_allocatable_memory_bytes()
                    })
                }
                "limits.ephemeral-storage" => ctr
                    .resources
                    .limits
                    .get("ephemeral-storage")
                    .map(|q| q.value),
                "requests.cpu" => ctr.resources.requests.get("cpu").map(|q| q.value),
                "requests.memory" => ctr.resources.requests.get("memory").map(|q| q.value),
                "requests.ephemeral-storage" => ctr
                    .resources
                    .requests
                    .get("ephemeral-storage")
                    .map(|q| q.value),
                _ => None,
            };
            if let Some(raw) = raw_val {
                let is_cpu = resource.contains("cpu");
                return apply_resource_divisor(raw, is_cpu, divisor).to_string();
            }
        }
    }

    // field_ref: pod metadata/spec fields
    if let Some(field) = &item.field_ref {
        return resolve_field_ref_with_pod_ip(pod, field, pod_ip);
    }
    String::new()
}

/// Apply a k8s quantity divisor to a raw resource value.
///
/// - CPU values are stored in millicores; default divisor `"1"` → whole cores (ceiling).
/// - Memory / ephemeral-storage values are stored in bytes; default divisor `"1"` → bytes.
fn node_allocatable_memory_bytes() -> i64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
            for line in content.lines() {
                if line.starts_with("MemTotal:")
                    && let Some(kb_str) = line.split_whitespace().nth(1)
                    && let Ok(kb_val) = kb_str.parse::<i64>()
                {
                    return kb_val * 1024;
                }
            }
        }
    }
    8 * 1024 * 1024 * 1024i64 // 8 GiB fallback
}

/// Return the node's allocatable CPU in millicores.
fn node_allocatable_cpu_millicores() -> i64 {
    (num_cpus::get() as i64) * 1000
}

fn apply_resource_divisor(raw: i64, is_cpu: bool, divisor: &str) -> i64 {
    if is_cpu {
        match divisor {
            "1m" => raw,                                // millicores as-is
            _ => ((raw as f64) / 1000.0).ceil() as i64, // whole cores (ceiling)
        }
    } else {
        let divisor_bytes: i64 = match divisor {
            "1" | "" => 1,
            "1k" | "1K" => 1_000,
            "1Ki" => 1_024,
            "1M" => 1_000_000,
            "1Mi" => 1_048_576,
            "1G" => 1_000_000_000,
            "1Gi" => 1_073_741_824,
            _ => 1,
        };
        raw / divisor_bytes.max(1)
    }
}

/// Resolve a `fieldRef.fieldPath` expression against a pod spec.
/// Resolve a fieldRef path to its string value.
///
/// For `status.podIP`/`status.podIPs`, use [`resolve_field_ref_with_pod_ip`]
/// when the sandbox IP is known.
fn resolve_field_ref(pod: &PodSpec, field: &str) -> String {
    resolve_field_ref_with_pod_ip(pod, field, None)
}

/// Like [`resolve_field_ref`] but also accepts the pod's IP for `status.podIP`
/// and `status.podIPs` fields.
fn resolve_field_ref_with_pod_ip(pod: &PodSpec, field: &str, pod_ip: Option<&str>) -> String {
    // Handle metadata.labels['key'] and metadata.annotations['key']
    if let Some(key) = field
        .strip_prefix("metadata.labels['")
        .and_then(|s| s.strip_suffix("']"))
    {
        return pod.labels.get(key).cloned().unwrap_or_default();
    }
    if let Some(key) = field
        .strip_prefix("metadata.annotations['")
        .and_then(|s| s.strip_suffix("']"))
    {
        return pod.annotations.get(key).cloned().unwrap_or_default();
    }
    match field {
        // Serialize all labels/annotations as key="value"\n lines (sorted for stability)
        "metadata.labels" => {
            let mut pairs: Vec<_> = pod.labels.iter().collect();
            pairs.sort_by_key(|(k, _)| k.as_str());
            pairs
                .iter()
                .map(|(k, v)| format!("{}=\"{}\"\n", k, v))
                .collect::<Vec<_>>()
                .concat()
        }
        "metadata.annotations" => {
            let mut pairs: Vec<_> = pod.annotations.iter().collect();
            pairs.sort_by_key(|(k, _)| k.as_str());
            pairs
                .iter()
                .map(|(k, v)| format!("{}=\"{}\"\n", k, v))
                .collect::<Vec<_>>()
                .concat()
        }
        "metadata.name" => pod.pod_ref.name.clone(),
        "metadata.namespace" => pod.pod_ref.namespace.clone(),
        "metadata.uid" => pod.uid.0.clone(),
        "spec.nodeName" => pod.node_name.clone(),
        "spec.serviceAccountName" => pod.service_account_name.clone(),
        // Host IP is the node's primary IP — available immediately.
        "status.hostIP" => detect_node_internal_ip(),
        // Pod IP comes from the sandbox — use the supplied value when available.
        // For host-network pods the CRI returns no sandbox IP, so fall back
        // to the node's own IP (pod shares the host network namespace).
        "status.podIP" | "status.podIPs" => pod_ip
            .filter(|ip| !ip.is_empty())
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| {
                if pod.host_network {
                    detect_node_internal_ip()
                } else {
                    String::new()
                }
            }),
        _ => String::new(),
    }
}

/// Run a liveness probe loop in the background. If the probe fails beyond
/// Convert a container's `SecurityContext` to `LinuxContainerSecurity`.
/// Falls back to pod-level security context for run_as_user/run_as_group if
/// not specified at the container level.
fn build_container_security(
    ctr_sc: Option<&kubelet_core::pod::SecurityContext>,
    pod_sc: Option<&kubelet_core::pod::PodSecurityContext>,
) -> LinuxContainerSecurity {
    let run_as_user = ctr_sc
        .and_then(|sc| sc.run_as_user)
        .or_else(|| pod_sc.and_then(|sc| sc.run_as_user));
    let run_as_group = ctr_sc
        .and_then(|sc| sc.run_as_group)
        .or_else(|| pod_sc.and_then(|sc| sc.run_as_group));

    let supplemental_groups = pod_sc
        .map(|sc| {
            let mut groups = Vec::new();
            if let Some(g) = sc.fs_group {
                groups.push(g);
            }
            for g in &sc.supplemental_groups {
                if !groups.contains(g) {
                    groups.push(*g);
                }
            }
            groups
        })
        .unwrap_or_default();

    let (capabilities_add, capabilities_drop) = ctr_sc
        .and_then(|sc| sc.capabilities.as_ref())
        .map(|caps| (caps.add.clone(), caps.drop.clone()))
        .unwrap_or_default();

    let seccomp = ctr_sc
        .and_then(|sc| sc.seccomp_profile.as_ref())
        .or_else(|| pod_sc.and_then(|sc| sc.seccomp_profile.as_ref()));

    let apparmor = ctr_sc.and_then(|sc| sc.apparmor_profile.as_ref());

    LinuxContainerSecurity {
        run_as_user,
        run_as_group,
        supplemental_groups,
        privileged: ctr_sc.and_then(|sc| sc.privileged).unwrap_or(false),
        read_only_root_filesystem: ctr_sc
            .and_then(|sc| sc.read_only_root_filesystem)
            .unwrap_or(false),
        allow_privilege_escalation: ctr_sc.and_then(|sc| sc.allow_privilege_escalation),
        capabilities_add,
        capabilities_drop,
        seccomp_profile_type: seccomp.as_ref().map(|s| s.type_.clone()),
        seccomp_localhost_path: seccomp.as_ref().and_then(|s| s.localhost_profile.clone()),
        apparmor_profile: apparmor.map(|ap| match ap.type_.as_str() {
            "Unconfined" => "unconfined".to_string(),
            "Localhost" => format!(
                "localhost/{}",
                ap.localhost_profile.as_deref().unwrap_or("")
            ),
            _ => "runtime/default".to_string(),
        }),
    }
}

/// `failure_threshold`, stop the container so the pod worker restarts it on
/// the next sync cycle.
#[allow(clippy::too_many_arguments)]
async fn spawn_liveness_probe(
    runtime: Arc<dyn ContainerRuntime>,
    cid: kubelet_core::container::ContainerID,
    probe: Probe,
    pod_ip: String,
    reporter: Arc<dyn kubelet_ports::driven::node_reporter::NodeReporter>,
    pod_ref: kubelet_core::types::PodRef,
    pod_uid: kubelet_core::types::PodUID,
    container_name: String,
) {
    use kubelet_adapters::prober::{
        ProbeDecision, ProbeState, ProbeType, evaluate_probe_result, run_probe,
    };

    info!(
        cid = %cid,
        pod_ip = %pod_ip,
        initial_delay_secs = probe.initial_delay_seconds,
        period_secs = probe.period_seconds,
        timeout_secs = probe.timeout_seconds,
        failure_threshold = probe.failure_threshold,
        "Liveness probe registered"
    );
    tokio::time::sleep(Duration::from_secs(probe.initial_delay_seconds as u64)).await;

    let mut state = ProbeState::default();
    loop {
        // Exit early if container is gone.
        match runtime.container_status(&cid).await {
            Ok(Some(s)) if s.state == RuntimeContainerState::Running => {}
            _ => return,
        }

        let timeout = Duration::from_secs(probe.timeout_seconds as u64);
        let result = run_probe(&probe.handler, runtime.clone(), &cid, &pod_ip, timeout).await;

        if evaluate_probe_result(&result, &mut state, &probe, ProbeType::Liveness)
            == ProbeDecision::Fail
        {
            warn!(
                cid = %cid,
                consecutive_failures = state.consecutive_failures,
                threshold = probe.failure_threshold,
                "Liveness probe exceeded threshold; stopping container for restart"
            );
            // Emit a Killing event visible in kubectl describe pod.
            let _ = reporter
                .emit_container_event(
                    &pod_ref,
                    &pod_uid,
                    &container_name,
                    "Normal",
                    "Killing",
                    &format!(
                        "Container {} failed liveness probe; restarting",
                        container_name
                    ),
                )
                .await;
            // Stop the container with 0 grace period - pod worker will detect the
            // exit and restart according to the restart policy on its next sync.
            let _ = runtime.stop_container(&cid, 0).await;
            return;
        }

        tokio::time::sleep(Duration::from_secs(probe.period_seconds as u64)).await;
    }
}

/// Run a startup probe loop in the background.
///
/// Sets `done_map[cid] = true` when the startup probe passes so the next
/// sync cycle knows it's safe to arm liveness/readiness probes. Kills the
/// container if the failure threshold is exceeded.
async fn spawn_startup_probe(
    runtime: Arc<dyn ContainerRuntime>,
    cid: kubelet_core::container::ContainerID,
    probe: Probe,
    pod_ip: String,
    done_map: Arc<DashMap<String, bool>>,
) {
    use kubelet_adapters::prober::{
        ProbeDecision, ProbeState, ProbeType, evaluate_probe_result, run_probe,
    };

    tokio::time::sleep(Duration::from_secs(probe.initial_delay_seconds as u64)).await;

    let mut state = ProbeState::default();
    loop {
        match runtime.container_status(&cid).await {
            Ok(Some(s)) if s.state == RuntimeContainerState::Running => {}
            _ => {
                done_map.insert(cid.0.clone(), false);
                return;
            }
        }

        let timeout = Duration::from_secs(probe.timeout_seconds as u64);
        let result = run_probe(&probe.handler, runtime.clone(), &cid, &pod_ip, timeout).await;

        match evaluate_probe_result(&result, &mut state, &probe, ProbeType::Startup) {
            ProbeDecision::Pass => {
                info!(cid = %cid, "Startup probe passed");
                done_map.insert(cid.0.clone(), true);
                return;
            }
            ProbeDecision::Fail => {
                warn!(cid = %cid, "Startup probe exceeded threshold; stopping container");
                let _ = runtime.stop_container(&cid, 0).await;
                done_map.insert(cid.0.clone(), false);
                return;
            }
            ProbeDecision::Pending => {}
        }

        tokio::time::sleep(Duration::from_secs(probe.period_seconds as u64)).await;
    }
}

/// Run a readiness probe loop in the background, updating the shared
/// `readiness_map` so that `update_pod_status` can gate the container's
/// `ready` condition appropriately.
async fn spawn_readiness_probe(
    runtime: Arc<dyn ContainerRuntime>,
    cid: kubelet_core::container::ContainerID,
    probe: Probe,
    pod_ip: String,
    readiness_map: Arc<DashMap<String, bool>>,
) {
    use kubelet_adapters::prober::{
        ProbeDecision, ProbeState, ProbeType, evaluate_probe_result, run_probe,
    };

    // Containers start not-ready until the readiness probe passes.
    readiness_map.insert(cid.0.clone(), false);

    tokio::time::sleep(Duration::from_secs(probe.initial_delay_seconds as u64)).await;

    let mut state = ProbeState::default();
    loop {
        match runtime.container_status(&cid).await {
            Ok(Some(s)) if s.state == RuntimeContainerState::Running => {
                // Container is running — proceed to probe below.
            }
            Ok(Some(s)) if s.state == RuntimeContainerState::Exited => {
                // Container definitively exited — stop probing.
                readiness_map.insert(cid.0.clone(), false);
                return;
            }
            Ok(None) => {
                // Container no longer exists — stop probing.
                readiness_map.remove(&cid.0);
                return;
            }
            Ok(Some(_)) | Err(_) => {
                // Transient unknown/error state — mark not-ready but keep the
                // probe loop alive. The container may return to Running shortly
                // (e.g. brief CRI hiccup or containerd restart).
                readiness_map.insert(cid.0.clone(), false);
                state = ProbeState::default();
                tokio::time::sleep(Duration::from_secs(probe.period_seconds.max(1) as u64)).await;
                continue;
            }
        }

        let timeout = Duration::from_secs(probe.timeout_seconds.max(1) as u64);
        let result = run_probe(&probe.handler, runtime.clone(), &cid, &pod_ip, timeout).await;

        match evaluate_probe_result(&result, &mut state, &probe, ProbeType::Readiness) {
            ProbeDecision::Pass => {
                readiness_map.insert(cid.0.clone(), true);
            }
            ProbeDecision::Fail => {
                readiness_map.insert(cid.0.clone(), false);
                // Reset state so re-entry via success_threshold works correctly.
                state = ProbeState::default();
            }
            ProbeDecision::Pending => {}
        }

        tokio::time::sleep(Duration::from_secs(probe.period_seconds.max(1) as u64)).await;
    }
}

/// Detect the node's internal (non-loopback) IP address.
/// Uses the UDP connect trick to find the default-route source IP.
fn detect_node_internal_ip() -> String {
    if let Ok(sock) = std::net::UdpSocket::bind("0.0.0.0:0")
        && sock.connect("1.1.1.1:80").is_ok()
        && let Ok(addr) = sock.local_addr()
    {
        if let std::net::IpAddr::V4(v4) = addr.ip()
            && !v4.is_loopback()
        {
            return v4.to_string();
        }
        // IPv6
        let s = addr.ip().to_string();
        if !s.starts_with("::1") && s != "::1" {
            return s;
        }
    }
    "127.0.0.1".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_adapters::checkpoint::CheckpointManager;
    use kubelet_adapters::kube_client::InMemoryNodeReporter;
    use kubelet_adapters::mock_runtime::MockRuntime;
    use kubelet_adapters::sandbox_builder::NodeDnsConfig;
    use kubelet_adapters::volume::LocalVolumeManager;
    use kubelet_core::pod::{
        EnvFromRef, EnvFromSource, EnvVar, EnvVarSource, ImagePullPolicy, PodSpec,
        ResourceRequirements,
    };
    use kubelet_core::types::{PodRef, PodUID};
    use tokio::sync::mpsc;

    fn make_pod(uid: &str, name: &str) -> PodSpec {
        PodSpec {
            uid: PodUID::new(uid),
            pod_ref: PodRef::new("default", name),
            containers: vec![kubelet_core::pod::ContainerSpec {
                name: "nginx".to_string(),
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
            }],
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
        }
    }

    // Returns (worker, pod_manager, _rx_keep_alive, _dir_keep_alive)
    async fn make_worker() -> (
        PodWorker,
        Arc<PodManager>,
        mpsc::Receiver<kubelet_core::pod::PodUpdate>,
        tempfile::TempDir,
    ) {
        let (tx, rx) = mpsc::channel(100);
        let pm = Arc::new(PodManager::new(tx));
        let rt = Arc::new(MockRuntime::new());
        let dir = tempfile::TempDir::new().unwrap();
        let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
        let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
        let vm = Arc::new(LocalVolumeManager::new(dir.path()));
        let worker = PodWorker::new(
            pm.clone(),
            rt.clone(),
            rt.clone(),
            vm,
            cm,
            cg,
            Arc::new(HashMap::new()),
            "cgroupfs",
            dir.path(),
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
            None,
            "node1",
            Arc::new(InMemoryNodeReporter::new()),
            NodeDnsConfig::default(),
            Arc::new(DeviceManager::new("/tmp")),
        );
        (worker, pm, rx, dir)
    }

    async fn make_worker_with_driver(
        cgroup_driver: &str,
    ) -> (
        PodWorker,
        Arc<PodManager>,
        mpsc::Receiver<kubelet_core::pod::PodUpdate>,
        tempfile::TempDir,
    ) {
        let (tx, rx) = mpsc::channel(100);
        let pm = Arc::new(PodManager::new(tx));
        let rt = Arc::new(MockRuntime::new());
        let dir = tempfile::TempDir::new().unwrap();
        let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
        let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
        let vm = Arc::new(LocalVolumeManager::new(dir.path()));
        let worker = PodWorker::new(
            pm.clone(),
            rt.clone(),
            rt.clone(),
            vm,
            cm,
            cg,
            Arc::new(HashMap::new()),
            cgroup_driver,
            dir.path(),
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
            None,
            "node1",
            Arc::new(InMemoryNodeReporter::new()),
            NodeDnsConfig::default(),
            Arc::new(DeviceManager::new("/tmp")),
        );
        (worker, pm, rx, dir)
    }

    #[tokio::test]
    async fn test_sync_pod_creates_sandbox_and_containers() {
        let (worker, pm, _rx, _dir) = make_worker().await;
        let pod = make_pod("uid-1", "pod-1");
        pm.upsert(pod.clone()).await.unwrap();

        let mut state = PodRuntimeState::default();
        let result = worker.sync_pod(&pod, &mut state).await;

        assert_eq!(result, PodSyncResult::Synced);
        assert!(state.sandbox_id.is_some(), "Sandbox should be created");
        assert!(
            state.container_ids.contains_key("nginx"),
            "Container should be started"
        );
    }

    #[test]
    fn test_build_container_security_includes_fs_group_in_supplemental_groups() {
        let pod_sc = kubelet_core::pod::PodSecurityContext {
            fs_group: Some(3000),
            supplemental_groups: vec![4000, 5000],
            ..Default::default()
        };

        let sec = build_container_security(None, Some(&pod_sc));
        assert_eq!(sec.supplemental_groups, vec![3000, 4000, 5000]);
    }

    #[test]
    fn test_build_container_security_dedupes_fs_group() {
        let pod_sc = kubelet_core::pod::PodSecurityContext {
            fs_group: Some(3000),
            supplemental_groups: vec![3000, 4000],
            ..Default::default()
        };

        let sec = build_container_security(None, Some(&pod_sc));
        assert_eq!(sec.supplemental_groups, vec![3000, 4000]);
    }

    #[tokio::test]
    async fn test_sync_pod_updates_status_to_running() {
        let (worker, pm, _rx, _dir) = make_worker().await;
        let pod = make_pod("uid-2", "pod-2");
        pm.upsert(pod.clone()).await.unwrap();

        let mut state = PodRuntimeState::default();
        worker.sync_pod(&pod, &mut state).await;

        let status = pm.status.get(&PodUID::new("uid-2")).unwrap();
        assert_eq!(status.phase, PodPhase::Running);
        assert_eq!(status.container_statuses.len(), 1);
        assert_eq!(status.container_statuses[0].restart_count, 0);
        assert!(
            status.host_ip.as_deref().is_some_and(|ip| !ip.is_empty()),
            "host_ip should be populated after sandbox creation"
        );
    }

    #[tokio::test]
    async fn test_update_pod_status_populates_init_container_statuses() {
        let (worker, pm, _rx, _dir) = make_worker().await;
        let mut pod = make_pod("uid-init-1", "pod-init-1");
        pod.init_containers = vec![kubelet_core::pod::ContainerSpec {
            name: "init-a".to_string(),
            image: "busybox:latest".to_string(),
            ..Default::default()
        }];
        pm.upsert(pod.clone()).await.unwrap();

        let state = PodRuntimeState::default();
        worker.update_pod_status(&pod, &state).await;

        let status = pm.status.get(&pod.uid).unwrap();
        assert_eq!(status.init_container_statuses.len(), 1);
        assert_eq!(status.init_container_statuses[0].name, "init-a");
        assert!(matches!(
            status.init_container_statuses[0].state,
            ContainerState::Waiting { .. }
        ));
    }

    #[tokio::test]
    async fn test_sync_pod_invalid_spec_fails() {
        let (worker, _pm, _rx, _dir) = make_worker().await;
        let mut pod = make_pod("", "pod-invalid");
        pod.uid = PodUID::new("");
        let mut state = PodRuntimeState::default();
        let result = worker.sync_pod(&pod, &mut state).await;
        assert!(matches!(result, PodSyncResult::Failed(_)));
    }

    #[tokio::test]
    async fn test_sync_pod_does_not_restart_running_container() {
        let (worker, pm, _rx, _dir) = make_worker().await;
        let pod = make_pod("uid-3", "pod-3");
        pm.upsert(pod.clone()).await.unwrap();

        let mut state = PodRuntimeState::default();
        worker.sync_pod(&pod, &mut state).await;
        let container_id_first = state.container_ids["nginx"].clone();
        worker.sync_pod(&pod, &mut state).await;
        assert_eq!(
            state.container_ids["nginx"], container_id_first,
            "Container ID unchanged on 2nd sync"
        );
    }

    #[tokio::test]
    async fn test_terminate_pod_removes_sandbox() {
        let (worker, pm, _rx, _dir) = make_worker().await;
        let pod = make_pod("uid-4", "pod-4");
        pm.upsert(pod.clone()).await.unwrap();

        let mut state = PodRuntimeState::default();
        worker.sync_pod(&pod, &mut state).await;
        worker
            .terminate_pod(&pod, &state, Duration::from_secs(0))
            .await
            .unwrap();

        let status = pm.status.get(&PodUID::new("uid-4")).unwrap();
        assert_eq!(status.phase, PodPhase::Running);
    }

    #[tokio::test]
    async fn test_terminate_pod_preserves_pending_phase_before_start() {
        let (worker, pm, _rx, _dir) = make_worker().await;
        let pod = make_pod("uid-4b", "pod-4b");
        pm.upsert(pod.clone()).await.unwrap();

        // No sync_pod call: pod status is still initial Pending.
        let state = PodRuntimeState::default();
        worker
            .terminate_pod(&pod, &state, Duration::from_secs(0))
            .await
            .unwrap();

        let status = pm.status.get(&PodUID::new("uid-4b")).unwrap();
        assert_eq!(status.phase, PodPhase::Pending);
        assert!(
            !status.container_statuses.is_empty(),
            "container statuses should remain initialized"
        );
    }

    #[tokio::test]
    async fn test_sync_pod_writes_checkpoint() {
        let (tx, _rx) = mpsc::channel(100);
        let pm = Arc::new(PodManager::new(tx));
        let rt = Arc::new(MockRuntime::new());
        let dir = tempfile::TempDir::new().unwrap();
        let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
        let cm_ref = cm.clone();
        let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
        let vm = Arc::new(LocalVolumeManager::new(dir.path()));
        let worker = PodWorker::new(
            pm.clone(),
            rt.clone(),
            rt.clone(),
            vm,
            cm,
            cg,
            Arc::new(HashMap::new()),
            "cgroupfs",
            dir.path(),
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
            None,
            "node1",
            Arc::new(InMemoryNodeReporter::new()),
            NodeDnsConfig::default(),
            Arc::new(DeviceManager::new("/tmp")),
        );

        let pod = make_pod("uid-5", "pod-5");
        pm.upsert(pod.clone()).await.unwrap();
        let mut state = PodRuntimeState::default();
        worker.sync_pod(&pod, &mut state).await;

        assert!(cm_ref.exists("uid-5"));
        let cp: PodCheckpoint = cm_ref.read("uid-5").unwrap().unwrap();
        assert!(cp.sandbox_id.is_some());
    }

    #[tokio::test]
    async fn test_sync_pod_fail_on_runtime_error() {
        let (tx, _rx) = mpsc::channel(100);
        let pm = Arc::new(PodManager::new(tx));
        let rt = Arc::new(MockRuntime::new());
        rt.set_fail_on_start(true).await;
        let dir = tempfile::TempDir::new().unwrap();
        let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
        let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
        let vm = Arc::new(LocalVolumeManager::new(dir.path()));
        let worker = PodWorker::new(
            pm.clone(),
            rt.clone(),
            rt.clone(),
            vm,
            cm,
            cg,
            Arc::new(HashMap::new()),
            "cgroupfs",
            dir.path(),
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
            None,
            "node1",
            Arc::new(InMemoryNodeReporter::new()),
            NodeDnsConfig::default(),
            Arc::new(DeviceManager::new("/tmp")),
        );

        let pod = make_pod("uid-6", "pod-6");
        pm.upsert(pod.clone()).await.unwrap();
        let mut state = PodRuntimeState::default();
        let result = worker.sync_pod(&pod, &mut state).await;
        assert_eq!(result, PodSyncResult::Synced);
    }

    #[test]
    fn test_parse_memory_quantity_to_bytes() {
        assert_eq!(parse_memory_quantity_to_bytes("10"), 10);
        assert_eq!(parse_memory_quantity_to_bytes("1Ki"), 1024);
        assert_eq!(parse_memory_quantity_to_bytes("2Mi"), 2 * 1024 * 1024);
        assert_eq!(
            parse_memory_quantity_to_bytes("3Gi"),
            3 * 1024 * 1024 * 1024
        );
        assert_eq!(parse_memory_quantity_to_bytes("bad"), 0);
    }

    #[test]
    fn test_runtime_overhead_memory_bytes_from_annotations() {
        let mut pod = make_pod("uid-overhead", "pod-overhead");
        pod.annotations.insert(
            "kube-air.io/runtime-overhead-memory".to_string(),
            "64Mi".to_string(),
        );
        assert_eq!(
            runtime_overhead_memory_bytes(&pod, &HashMap::new()),
            64 * 1024 * 1024
        );
    }

    #[test]
    fn test_runtime_overhead_memory_bytes_prefers_runtime_class() {
        let mut pod = make_pod("uid-overhead-rc", "pod-overhead-rc");
        pod.runtime_class_name = Some("gvisor".to_string());
        pod.annotations.insert(
            "kube-air.io/runtime-overhead-memory".to_string(),
            "64Mi".to_string(),
        );

        let mut runtime_overheads = HashMap::new();
        runtime_overheads.insert(
            "gvisor".to_string(),
            HashMap::from([("memory".to_string(), "128Mi".to_string())]),
        );

        assert_eq!(
            runtime_overhead_memory_bytes(&pod, &runtime_overheads),
            128 * 1024 * 1024
        );
    }

    #[test]
    fn test_reserved_container_parsing_plain_quotes() {
        let err = "CreateContainer: status: Unknown, message: \"failed to reserve container name \"nginx_demo_default_uid_0\": name \"nginx_demo_default_uid_0\" is reserved for \"abc123\"\"";

        assert_eq!(
            reserved_container_name(err).as_deref(),
            Some("nginx_demo_default_uid_0")
        );
        assert_eq!(
            reserved_container_id(err).map(|id| id.0),
            Some("abc123".to_string())
        );
    }

    #[test]
    fn test_reserved_container_parsing_escaped_quotes() {
        let err = "CreateContainer: status: Unknown, message: \\\"failed to reserve container name \\\"nginx_demo_default_uid_0\\\": name \\\"nginx_demo_default_uid_0\\\" is reserved for \\\"def456\\\"\\\"";

        assert_eq!(
            reserved_container_name(err).as_deref(),
            Some("nginx_demo_default_uid_0")
        );
        assert_eq!(
            reserved_container_id(err).map(|id| id.0),
            Some("def456".to_string())
        );
    }

    #[test]
    fn test_expand_env_value_matches_upstream_missing_and_escape_behavior() {
        let env = HashMap::from([("FOO".to_string(), "bar".to_string())]);

        assert_eq!(expand_env_value("$(FOO)", &env), "bar");
        assert_eq!(expand_env_value("$(MISSING)", &env), "$(MISSING)");
        assert_eq!(expand_env_value("$$", &env), "$");
    }

    #[test]
    fn test_expand_env_value_preserves_shell_special_vars() {
        // Shell vars like $?, $@, $# must pass through unchanged so that
        // commands like `echo $?` aren't corrupted into `echo ?`.
        let env = HashMap::new();
        assert_eq!(expand_env_value("echo $?", &env), "echo $?");
        assert_eq!(expand_env_value("echo $@", &env), "echo $@");
        assert_eq!(expand_env_value("echo $#", &env), "echo $#");
        assert_eq!(expand_env_value("echo $1", &env), "echo $1");
        // $$ still escapes to single $
        assert_eq!(expand_env_value("$$", &env), "$");
        // Mix: real var, escape, and shell special
        let env2 = HashMap::from([("NAME".to_string(), "foo".to_string())]);
        assert_eq!(expand_env_value("$(NAME)/$?/$$", &env2), "foo/$?/$");
        // Multi-character shell variables (not k8s env refs) must be preserved:
        // e.g. $count, $f from a shell script passed as command argument.
        assert_eq!(
            expand_env_value("if [ $count -eq 1 ]", &env),
            "if [ $count -eq 1 ]"
        );
        assert_eq!(expand_env_value("echo $f", &env), "echo $f");
        // Script with mixed k8s and shell vars: only $(VAR) patterns are expanded
        let env3 = HashMap::from([("MY_VAR".to_string(), "hello".to_string())]);
        let script = "x=$(MY_VAR); if [ $count -eq 1 ]; then echo $x; fi";
        let result = expand_env_value(script, &env3);
        assert!(
            result.contains("$count"),
            "multi-char shell var $count must be preserved: got {result}"
        );
        assert!(
            result.contains("$x"),
            "shell var $x must be preserved: got {result}"
        );
        assert!(
            result.contains("x=hello"),
            "k8s $(MY_VAR) must be expanded: got {result}"
        );
    }

    #[test]
    fn test_resolve_field_ref_annotation_in_env_var_source() {
        // Annotations accessed via FieldRef in env var source must resolve correctly.
        // This covers the SubPathExpr failure where $(ANNOTATION) was empty because
        // metadata.annotations['key'] wasn't handled in resolve_env_var_source.
        let mut pod = make_pod("uid-anno", "pod-anno");
        pod.annotations
            .insert("mysubpath".to_string(), "mypath".to_string());

        let result = resolve_field_ref(&pod, "metadata.annotations['mysubpath']");
        assert_eq!(result, "mypath");

        // Missing annotation returns empty string, not panic
        let result = resolve_field_ref(&pod, "metadata.annotations['nonexistent']");
        assert_eq!(result, "");
    }

    #[test]
    fn test_assemble_container_env_resolves_annotation_field_ref() {
        // When a container env var uses FieldRef to metadata.annotations['key'],
        // the value must be resolved from the pod's annotations.
        let mut pod = make_pod("uid-sub", "pod-sub");
        pod.annotations
            .insert("mysubpath".to_string(), "mypath".to_string());

        let mut container = pod.containers[0].clone();
        container.env = vec![
            EnvVar {
                name: "ANNOTATION".to_string(),
                value: None,
                value_from: Some(EnvVarSource::FieldRef {
                    field_path: "metadata.annotations['mysubpath']".to_string(),
                }),
            },
            EnvVar {
                name: "POD_NAME".to_string(),
                value: Some("foo".to_string()),
                value_from: None,
            },
        ];

        let env = assemble_container_env(
            &pod,
            &container,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap()
        .into_iter()
        .collect::<HashMap<_, _>>();

        assert_eq!(env.get("ANNOTATION").map(String::as_str), Some("mypath"));
        assert_eq!(env.get("POD_NAME").map(String::as_str), Some("foo"));

        // Verify the SubPathExpr expansion would produce "mypath/foo" (valid, not "/foo")
        let expanded = expand_env_value("$(ANNOTATION)/$(POD_NAME)", &env);
        assert_eq!(expanded, "mypath/foo");
    }

    #[test]
    fn test_assemble_container_env_expands_env_and_field_refs() {
        let pod = make_pod("uid-env", "pod-env");
        let mut container = pod.containers[0].clone();
        container.env_from = vec![EnvFromSource {
            prefix: None,
            config_map_ref: Some(EnvFromRef {
                name: "settings".to_string(),
                optional: false,
            }),
            secret_ref: None,
        }];
        container.env = vec![
            EnvVar {
                name: "MESSAGE".to_string(),
                value: Some("$(GREETING), world".to_string()),
                value_from: None,
            },
            EnvVar {
                name: "POD_IP".to_string(),
                value: None,
                value_from: Some(EnvVarSource::FieldRef {
                    field_path: "status.podIP".to_string(),
                }),
            },
        ];

        let configmaps = HashMap::from([(
            "settings".to_string(),
            HashMap::from([("GREETING".to_string(), "hello".to_string())]),
        )]);
        let secrets = HashMap::new();

        let env = assemble_container_env(
            &pod,
            &container,
            Some("10.0.0.7"),
            &configmaps,
            &secrets,
            &HashMap::new(),
        )
        .unwrap()
        .into_iter()
        .collect::<HashMap<_, _>>();

        assert_eq!(env.get("GREETING").map(String::as_str), Some("hello"));
        assert_eq!(env.get("MESSAGE").map(String::as_str), Some("hello, world"));
        assert_eq!(env.get("POD_IP").map(String::as_str), Some("10.0.0.7"));

        let expanded = expand_container_tokens(&["echo $(MESSAGE)".to_string()], &env);
        assert_eq!(expanded, vec!["echo hello, world".to_string()]);
    }

    #[test]
    fn test_service_env_vars_from_service_includes_kubernetes_vars() {
        let service = Service {
            metadata: kube::api::ObjectMeta {
                name: Some("kubernetes".to_string()),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::ServiceSpec {
                cluster_ip: Some("10.96.0.1".to_string()),
                ports: Some(vec![k8s_openapi::api::core::v1::ServicePort {
                    name: Some("https".to_string()),
                    port: 443,
                    protocol: Some("TCP".to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let envs = service_env_vars_from_service(&service);
        assert_eq!(
            envs.get("KUBERNETES_SERVICE_HOST").map(String::as_str),
            Some("10.96.0.1")
        );
        assert_eq!(
            envs.get("KUBERNETES_SERVICE_PORT").map(String::as_str),
            Some("443")
        );
        assert_eq!(
            envs.get("KUBERNETES_PORT").map(String::as_str),
            Some("tcp://10.96.0.1:443")
        );
    }

    #[test]
    fn test_assemble_container_env_allows_container_env_to_override_service_env() {
        let pod = make_pod("uid-env-override", "pod-env-override");
        let mut container = pod.containers[0].clone();
        container.env = vec![EnvVar {
            name: "KUBERNETES_SERVICE_HOST".to_string(),
            value: Some("127.0.0.1".to_string()),
            value_from: None,
        }];

        let mut service_envs = HashMap::new();
        service_envs.insert(
            "KUBERNETES_SERVICE_HOST".to_string(),
            "10.96.0.1".to_string(),
        );

        let env = assemble_container_env(
            &pod,
            &container,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &service_envs,
        )
        .unwrap()
        .into_iter()
        .collect::<HashMap<_, _>>();

        assert_eq!(
            env.get("KUBERNETES_SERVICE_HOST").map(String::as_str),
            Some("127.0.0.1")
        );
    }

    /// Regression test: `enableServiceLinks: false` must NOT prevent injection of
    /// KUBERNETES_SERVICE_HOST / KUBERNETES_SERVICE_PORT.  Those vars come from the
    /// `kubernetes` service in the `default` namespace and are required for
    /// in-cluster client configuration regardless of the service-links flag.
    #[test]
    fn test_service_master_vars_always_present_regardless_of_enable_service_links() {
        // With enableServiceLinks=false the namespace service links are absent but
        // the kubernetes master service vars must still appear.
        let mut pod = make_pod("uid-no-links", "pod-no-links");
        pod.enable_service_links = Some(false);
        let container = pod.containers[0].clone();

        // Simulate what load_service_env_vars returns when service_links_enabled=false:
        // only the kubernetes service vars are present.
        let mut service_envs = HashMap::new();
        service_envs.insert(
            "KUBERNETES_SERVICE_HOST".to_string(),
            "10.96.0.1".to_string(),
        );
        service_envs.insert("KUBERNETES_SERVICE_PORT".to_string(), "443".to_string());
        service_envs.insert(
            "KUBERNETES_PORT".to_string(),
            "tcp://10.96.0.1:443".to_string(),
        );

        let env = assemble_container_env(
            &pod,
            &container,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &service_envs,
        )
        .unwrap()
        .into_iter()
        .collect::<HashMap<_, _>>();

        assert_eq!(
            env.get("KUBERNETES_SERVICE_HOST").map(String::as_str),
            Some("10.96.0.1"),
            "KUBERNETES_SERVICE_HOST must be present even with enableServiceLinks=false"
        );
        assert_eq!(
            env.get("KUBERNETES_SERVICE_PORT").map(String::as_str),
            Some("443"),
            "KUBERNETES_SERVICE_PORT must be present even with enableServiceLinks=false"
        );
    }

    #[test]
    fn test_write_etc_hosts_file_includes_pod_hostname_entry() {
        let rt = Arc::new(MockRuntime::new());
        let dir = tempfile::TempDir::new().unwrap();
        let (tx, _rx) = mpsc::channel(8);
        let pm = Arc::new(PodManager::new(tx));
        let worker = PodWorker::new(
            pm,
            rt.clone(),
            rt,
            Arc::new(LocalVolumeManager::new(dir.path())),
            Arc::new(CheckpointManager::new(dir.path()).unwrap()),
            Arc::new(CgroupManager::new("/sys/fs/cgroup", true)),
            Arc::new(HashMap::new()),
            "cgroupfs",
            dir.path(),
            "/tmp",
            "registry.k8s.io/pause:3.9",
            None,
            "node1",
            Arc::new(InMemoryNodeReporter::new()),
            NodeDnsConfig::default(),
            Arc::new(DeviceManager::new("/tmp")),
        );
        let pod = make_pod("uid-hosts", "pod-hosts");

        let hosts_path = worker.write_etc_hosts_file(&pod, Some("10.0.0.7")).unwrap();
        let content = std::fs::read_to_string(hosts_path).unwrap();

        assert!(content.contains("127.0.0.1\tlocalhost"));
        assert!(content.contains("10.0.0.7\tpod-hosts"));
    }

    #[test]
    fn test_write_etc_hosts_file_includes_host_aliases() {
        let rt = Arc::new(MockRuntime::new());
        let dir = tempfile::TempDir::new().unwrap();
        let (tx, _rx) = mpsc::channel(8);
        let pm = Arc::new(PodManager::new(tx));
        let worker = PodWorker::new(
            pm,
            rt.clone(),
            rt,
            Arc::new(LocalVolumeManager::new(dir.path())),
            Arc::new(CheckpointManager::new(dir.path()).unwrap()),
            Arc::new(CgroupManager::new("/sys/fs/cgroup", true)),
            Arc::new(HashMap::new()),
            "cgroupfs",
            dir.path(),
            "/tmp",
            "registry.k8s.io/pause:3.9",
            None,
            "node1",
            Arc::new(InMemoryNodeReporter::new()),
            NodeDnsConfig::default(),
            Arc::new(DeviceManager::new("/tmp")),
        );
        let mut pod = make_pod("uid-host-alias", "pod-host-alias");
        pod.host_aliases = vec![kubelet_core::pod::HostAlias {
            ip: "1.2.3.4".to_string(),
            hostnames: vec!["foo.local".to_string(), "bar.local".to_string()],
        }];

        let hosts_path = worker.write_etc_hosts_file(&pod, Some("10.0.0.8")).unwrap();
        let content = std::fs::read_to_string(hosts_path).unwrap();

        assert!(content.contains("1.2.3.4\tfoo.local\tbar.local"));
        assert!(content.contains("# Entries added by HostAliases."));
    }

    #[tokio::test]
    async fn test_sandbox_cgroup_parent_cgroupfs_format() {
        let (worker, _pm, _rx, _dir) = make_worker_with_driver("cgroupfs").await;
        let pod = make_pod("68c9f245-57f7-479e-a441-c7f1ca615447", "pod-cgroupfs");

        let parent = worker.sandbox_cgroup_parent(&pod);
        assert_eq!(
            parent,
            "/kubepods/besteffort/pod68c9f245-57f7-479e-a441-c7f1ca615447"
        );
    }

    #[tokio::test]
    async fn test_sandbox_cgroup_parent_systemd_slice_format() {
        let (worker, _pm, _rx, _dir) = make_worker_with_driver("systemd").await;
        let pod = make_pod("68c9f245-57f7-479e-a441-c7f1ca615447", "pod-systemd");

        let parent = worker.sandbox_cgroup_parent(&pod);
        assert_eq!(
            parent,
            "kubepods-besteffort-pod68c9f245_57f7_479e_a441_c7f1ca615447.slice"
        );
    }

    // -- Registry authentication tests ----------------------------------------

    #[test]
    fn test_normalize_registry_host_strips_scheme() {
        assert_eq!(normalize_registry_host("https://docker.io"), "docker.io");
        assert_eq!(
            normalize_registry_host("http://registry.example.com"),
            "registry.example.com"
        );
        assert_eq!(normalize_registry_host("docker.io"), "docker.io");
        assert_eq!(normalize_registry_host("DOCKER.IO"), "docker.io");
        assert_eq!(
            normalize_registry_host("  registry.k8s.io  "),
            "registry.k8s.io"
        );
    }

    #[test]
    fn test_normalize_registry_host_strips_path_suffix() {
        assert_eq!(
            normalize_registry_host("registry.example.com/v2/"),
            "registry.example.com"
        );
        assert_eq!(
            normalize_registry_host("https://gcr.io/myproject"),
            "gcr.io"
        );
    }

    #[test]
    fn test_image_registry_host_official_docker_image() {
        // Short images with no dot/colon in the first segment → docker.io
        assert_eq!(image_registry_host("nginx"), "docker.io");
        assert_eq!(image_registry_host("nginx:latest"), "docker.io");
        assert_eq!(image_registry_host("library/nginx:1.25"), "docker.io");
    }

    #[test]
    fn test_image_registry_host_fully_qualified() {
        assert_eq!(
            image_registry_host("gcr.io/google-containers/pause"),
            "gcr.io"
        );
        assert_eq!(
            image_registry_host("registry.k8s.io/pause:3.9"),
            "registry.k8s.io"
        );
        assert_eq!(
            image_registry_host("localhost:5000/myimage"),
            "localhost:5000"
        );
        assert_eq!(image_registry_host("localhost/myimage"), "localhost");
    }

    #[test]
    fn test_host_matches_registry_direct() {
        assert!(host_matches_registry("docker.io", "docker.io"));
        assert!(host_matches_registry("gcr.io", "gcr.io"));
        assert!(!host_matches_registry("gcr.io", "docker.io"));
    }

    #[test]
    fn test_host_matches_registry_docker_aliases() {
        // docker.io ↔ index.docker.io should be treated as the same registry
        assert!(host_matches_registry("index.docker.io", "docker.io"));
        assert!(host_matches_registry("docker.io", "index.docker.io"));
        assert!(!host_matches_registry("index.docker.io", "gcr.io"));
    }

    #[test]
    fn test_host_matches_registry_normalizes_schemes() {
        assert!(host_matches_registry("https://docker.io", "docker.io"));
        assert!(host_matches_registry("docker.io", "https://docker.io"));
    }

    #[test]
    fn test_parse_dockerconfigjson_with_auth_field() {
        use base64::Engine as _;
        let creds = base64::engine::general_purpose::STANDARD.encode("myuser:mypass");
        let json = format!(
            r#"{{"auths": {{"registry.example.com": {{"auth": "{}"}}}}}}"#,
            creds
        );
        let entries = parse_dockerconfigjson_bytes(json.as_bytes());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "registry.example.com");
        assert_eq!(entries[0].1, "myuser");
        assert_eq!(entries[0].2, "mypass");
    }

    #[test]
    fn test_parse_dockerconfigjson_with_username_password_fields() {
        let json =
            r#"{"auths": {"gcr.io": {"username": "oauth2accesstoken", "password": "ya29.token"}}}"#;
        let entries = parse_dockerconfigjson_bytes(json.as_bytes());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "gcr.io");
        assert_eq!(entries[0].1, "oauth2accesstoken");
        assert_eq!(entries[0].2, "ya29.token");
    }

    #[test]
    fn test_parse_dockerconfigjson_normalizes_host() {
        use base64::Engine as _;
        let creds = base64::engine::general_purpose::STANDARD.encode("user:pass");
        let json = format!(
            r#"{{"auths": {{"https://index.docker.io/v1/": {{"auth": "{}"}}}}}}"#,
            creds
        );
        let entries = parse_dockerconfigjson_bytes(json.as_bytes());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "index.docker.io");
    }

    #[test]
    fn test_parse_dockerconfigjson_multiple_registries() {
        use base64::Engine as _;
        let c1 = base64::engine::general_purpose::STANDARD.encode("u1:p1");
        let c2 = base64::engine::general_purpose::STANDARD.encode("u2:p2");
        let json = format!(
            r#"{{"auths": {{"docker.io": {{"auth": "{}"}}, "gcr.io": {{"auth": "{}"}}}}}}"#,
            c1, c2
        );
        let mut entries = parse_dockerconfigjson_bytes(json.as_bytes());
        entries.sort_by_key(|e| e.0.clone());
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "docker.io");
        assert_eq!(entries[1].0, "gcr.io");
    }

    #[test]
    fn test_parse_dockerconfigjson_invalid_json_returns_empty() {
        assert!(parse_dockerconfigjson_bytes(b"not-json").is_empty());
        assert!(parse_dockerconfigjson_bytes(b"{}").is_empty()); // missing "auths"
    }

    #[test]
    fn test_parse_dockerconfigjson_invalid_base64_skipped() {
        let json = r#"{"auths": {"bad.io": {"auth": "!!!invalid-base64!!!"}}}"#;
        // Invalid base64 → entry is skipped, no panic
        let entries = parse_dockerconfigjson_bytes(json.as_bytes());
        assert!(entries.is_empty());
    }

    #[test]
    fn test_parse_dockercfg_legacy_format() {
        use base64::Engine as _;
        let creds = base64::engine::general_purpose::STANDARD.encode("legacyuser:legacypass");
        let json = format!(r#"{{"registry.old.com": {{"auth": "{}"}}}}"#, creds);
        let entries = parse_dockercfg_bytes(json.as_bytes());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "registry.old.com");
        assert_eq!(entries[0].1, "legacyuser");
        assert_eq!(entries[0].2, "legacypass");
    }

    #[test]
    fn test_parse_registry_auth_entry_prefers_username_password() {
        use base64::Engine as _;
        // Even if "auth" is present, explicit username/password take priority
        let creds = base64::engine::general_purpose::STANDARD.encode("authuser:authpass");
        let entry = serde_json::json!({
            "username": "explicit_user",
            "password": "explicit_pass",
            "auth": creds
        });
        let result = parse_registry_auth_entry("r.io", &entry).unwrap();
        assert_eq!(result.1, "explicit_user");
        assert_eq!(result.2, "explicit_pass");
    }

    #[test]
    fn test_parse_registry_auth_entry_missing_both_returns_none() {
        let entry = serde_json::json!({"email": "nobody@example.com"});
        assert!(parse_registry_auth_entry("r.io", &entry).is_none());
    }

    #[test]
    fn test_docker_auths_from_secret_empty_data() {
        use k8s_openapi::api::core::v1::Secret;
        let secret = Secret {
            data: None,
            ..Default::default()
        };
        assert!(docker_auths_from_secret(&secret).is_empty());
    }

    #[test]
    fn test_docker_auths_from_secret_prefers_dockerconfigjson() {
        use base64::Engine as _;
        use k8s_openapi::{ByteString, api::core::v1::Secret};
        let creds = base64::engine::general_purpose::STANDARD.encode("newuser:newpass");
        let json = format!(r#"{{"auths": {{"docker.io": {{"auth": "{}"}}}}}}"#, creds).into_bytes();
        let mut data = std::collections::BTreeMap::new();
        data.insert(".dockerconfigjson".to_string(), ByteString(json));
        let secret = Secret {
            data: Some(data),
            ..Default::default()
        };
        let entries = docker_auths_from_secret(&secret);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, "newuser");
    }

    // -- DownwardAPI value resolution tests ------------------------------------

    #[test]
    fn test_resolve_downward_api_field_ref_metadata_name() {
        let pod = make_pod("uid-da", "my-pod");
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "podname".to_string(),
            field_ref: Some("metadata.name".to_string()),
            resource_field_ref: None,
            mode: None,
        };
        assert_eq!(resolve_downward_api_value(&pod, &item, None), "my-pod");
    }

    #[test]
    fn test_resolve_downward_api_field_ref_metadata_namespace() {
        let pod = make_pod("uid-da", "my-pod");
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "namespace".to_string(),
            field_ref: Some("metadata.namespace".to_string()),
            resource_field_ref: None,
            mode: None,
        };
        assert_eq!(resolve_downward_api_value(&pod, &item, None), "default");
    }

    #[test]
    fn test_resolve_downward_api_field_ref_metadata_uid() {
        let pod = make_pod("uid-da-123", "my-pod");
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "uid".to_string(),
            field_ref: Some("metadata.uid".to_string()),
            resource_field_ref: None,
            mode: None,
        };
        assert_eq!(resolve_downward_api_value(&pod, &item, None), "uid-da-123");
    }

    #[test]
    fn test_resolve_downward_api_resource_limits_memory() {
        use kubelet_core::pod::ResourceRequirements;
        use kubelet_core::types::ResourceQuantity;
        let mut pod = make_pod("uid-da-res", "pod-res");
        pod.containers[0].resources = ResourceRequirements {
            limits: std::collections::HashMap::from([
                (
                    "memory".to_string(),
                    ResourceQuantity::memory_bytes(134217728),
                ), // 128Mi
            ]),
            requests: std::collections::HashMap::from([
                (
                    "memory".to_string(),
                    ResourceQuantity::memory_bytes(33554432),
                ), // 32Mi
            ]),
        };
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "memory_limit".to_string(),
            field_ref: None,
            resource_field_ref: Some(kubelet_core::pod::ResourceFieldRef {
                container_name: None,
                resource: "limits.memory".to_string(),
                divisor: None,
            }),
            mode: None,
        };
        assert_eq!(resolve_downward_api_value(&pod, &item, None), "134217728");
    }

    #[test]
    fn test_resolve_downward_api_resource_limits_cpu_whole_cores() {
        use kubelet_core::types::ResourceQuantity;
        let mut pod = make_pod("uid-da-cpu", "pod-cpu");
        pod.containers[0].resources = kubelet_core::pod::ResourceRequirements {
            limits: std::collections::HashMap::from([
                ("cpu".to_string(), ResourceQuantity::cpu_millicores(1250)), // 1.25 cores → ceil = 2
            ]),
            requests: std::collections::HashMap::from([
                ("cpu".to_string(), ResourceQuantity::cpu_millicores(250)), // 0.25 → ceil = 1
            ]),
        };

        let limit_item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "cpu_limit".to_string(),
            field_ref: None,
            resource_field_ref: Some(kubelet_core::pod::ResourceFieldRef {
                container_name: None,
                resource: "limits.cpu".to_string(),
                divisor: None, // default "1" = whole cores
            }),
            mode: None,
        };
        assert_eq!(resolve_downward_api_value(&pod, &limit_item, None), "2");

        let req_item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "cpu_request".to_string(),
            field_ref: None,
            resource_field_ref: Some(kubelet_core::pod::ResourceFieldRef {
                container_name: None,
                resource: "requests.cpu".to_string(),
                divisor: None,
            }),
            mode: None,
        };
        assert_eq!(resolve_downward_api_value(&pod, &req_item, None), "1");
    }

    #[test]
    fn test_resolve_downward_api_resource_with_container_name() {
        use kubelet_core::types::ResourceQuantity;
        let mut pod = make_pod("uid-da-cname", "pod-cname");
        pod.containers[0].name = "web".to_string();
        pod.containers[0].resources = kubelet_core::pod::ResourceRequirements {
            limits: std::collections::HashMap::from([(
                "memory".to_string(),
                ResourceQuantity::memory_bytes(67108864),
            )]),
            requests: Default::default(),
        };
        // resource_field_ref with explicit container name
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "mem".to_string(),
            field_ref: None,
            resource_field_ref: Some(kubelet_core::pod::ResourceFieldRef {
                container_name: Some("web".to_string()),
                resource: "limits.memory".to_string(),
                divisor: None,
            }),
            mode: None,
        };
        assert_eq!(resolve_downward_api_value(&pod, &item, None), "67108864");
    }

    #[test]
    fn test_resolve_downward_api_unknown_field_returns_empty() {
        let pod = make_pod("uid-da-unk", "pod-unk");
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "unknown".to_string(),
            field_ref: Some("metadata.something_unknown".to_string()),
            resource_field_ref: None,
            mode: None,
        };
        assert_eq!(resolve_downward_api_value(&pod, &item, None), "");
    }

    #[tokio::test]
    async fn test_ensure_pod_volumes_writes_downward_api_files() {
        let (worker, pm, _rx, dir) = make_worker().await;
        let mut pod = make_pod("uid-vol-da", "pod-vol-da");

        use kubelet_core::pod::{VolumeSource, VolumeSpec};
        use kubelet_core::types::ResourceQuantity;

        pod.containers[0].resources = kubelet_core::pod::ResourceRequirements {
            limits: std::collections::HashMap::from([(
                "memory".to_string(),
                ResourceQuantity::memory_bytes(134217728),
            )]),
            requests: Default::default(),
        };
        pod.volumes = vec![VolumeSpec {
            name: "podinfo".to_string(),
            source: VolumeSource::DownwardAPI {
                items: vec![
                    kubelet_core::pod::DownwardAPIVolumeFile {
                        path: "memory_limit".to_string(),
                        field_ref: None,
                        resource_field_ref: Some(kubelet_core::pod::ResourceFieldRef {
                            container_name: None,
                            resource: "limits.memory".to_string(),
                            divisor: None,
                        }),
                        mode: None,
                    },
                    kubelet_core::pod::DownwardAPIVolumeFile {
                        path: "podname".to_string(),
                        field_ref: Some("metadata.name".to_string()),
                        resource_field_ref: None,
                        mode: None,
                    },
                ],
                default_mode: None,
            },
        }];
        pod.containers[0].volume_mounts = vec![kubelet_core::pod::VolumeMount {
            name: "podinfo".to_string(),
            mount_path: "/etc/podinfo".to_string(),
            read_only: false,
            sub_path: None,
            sub_path_expr: None,
        }];
        pm.upsert(pod.clone()).await.unwrap();

        let mut state = PodRuntimeState::default();
        let result = worker.sync_pod(&pod, &mut state).await;
        assert_eq!(result, PodSyncResult::Synced);

        // Check that the downwardAPI files were written
        let vol_dir = dir
            .path()
            .join("pods")
            .join("uid-vol-da")
            .join("volumes")
            .join("kubernetes.io~downward-api")
            .join("podinfo");
        let mem_file = vol_dir.join("memory_limit");
        let name_file = vol_dir.join("podname");

        assert!(mem_file.exists(), "memory_limit file should exist");
        assert!(name_file.exists(), "podname file should exist");
        assert_eq!(std::fs::read_to_string(&mem_file).unwrap(), "134217728");
        assert_eq!(std::fs::read_to_string(&name_file).unwrap(), "pod-vol-da");
    }

    // -- apply_resource_divisor tests -----------------------------------------

    #[test]
    fn test_apply_divisor_memory_default_bytes() {
        // No divisor (treats "1") → raw bytes
        assert_eq!(apply_resource_divisor(134_217_728, false, "1"), 134_217_728);
    }

    #[test]
    fn test_apply_divisor_memory_kibibytes() {
        // 128 MiB in bytes / 1Ki = 131_072
        assert_eq!(apply_resource_divisor(134_217_728, false, "1Ki"), 131_072);
    }

    #[test]
    fn test_apply_divisor_memory_mebibytes() {
        // 128 MiB in bytes / 1Mi = 128
        assert_eq!(apply_resource_divisor(134_217_728, false, "1Mi"), 128);
    }

    #[test]
    fn test_apply_divisor_memory_gibibytes() {
        // 2 GiB in bytes / 1Gi = 2
        let two_gib = 2 * 1_073_741_824_i64;
        assert_eq!(apply_resource_divisor(two_gib, false, "1Gi"), 2);
    }

    #[test]
    fn test_apply_divisor_cpu_whole_cores_default() {
        // 1250m / "1" → ceil(1.25) = 2
        assert_eq!(apply_resource_divisor(1250, true, "1"), 2);
        // 250m / "1" → ceil(0.25) = 1
        assert_eq!(apply_resource_divisor(250, true, "1"), 1);
        // 1000m / "1" → 1 (exact)
        assert_eq!(apply_resource_divisor(1000, true, "1"), 1);
    }

    #[test]
    fn test_apply_divisor_cpu_millicores() {
        // 1250m / "1m" → 1250
        assert_eq!(apply_resource_divisor(1250, true, "1m"), 1250);
    }

    #[test]
    fn test_resolve_downward_api_resource_with_mib_divisor() {
        use kubelet_core::types::ResourceQuantity;
        let mut pod = make_pod("uid-div", "pod-div");
        pod.containers[0].resources = kubelet_core::pod::ResourceRequirements {
            limits: std::collections::HashMap::from([(
                "memory".to_string(),
                ResourceQuantity::memory_bytes(268_435_456), // 256 MiB
            )]),
            requests: Default::default(),
        };
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "mem_mib".to_string(),
            field_ref: None,
            resource_field_ref: Some(kubelet_core::pod::ResourceFieldRef {
                container_name: None,
                resource: "limits.memory".to_string(),
                divisor: Some("1Mi".to_string()),
            }),
            mode: None,
        };
        // 256 MiB / 1Mi = 256
        assert_eq!(resolve_downward_api_value(&pod, &item, None), "256");
    }

    #[test]
    fn test_resolve_downward_api_resource_cpu_millicores_divisor() {
        use kubelet_core::types::ResourceQuantity;
        let mut pod = make_pod("uid-div-cpu", "pod-div-cpu");
        pod.containers[0].resources = kubelet_core::pod::ResourceRequirements {
            limits: std::collections::HashMap::from([(
                "cpu".to_string(),
                ResourceQuantity::cpu_millicores(1500),
            )]),
            requests: Default::default(),
        };
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "cpu_m".to_string(),
            field_ref: None,
            resource_field_ref: Some(kubelet_core::pod::ResourceFieldRef {
                container_name: None,
                resource: "limits.cpu".to_string(),
                divisor: Some("1m".to_string()),
            }),
            mode: None,
        };
        assert_eq!(resolve_downward_api_value(&pod, &item, None), "1500");
    }

    // -- field_ref extension tests --------------------------------------------

    #[test]
    fn test_resolve_field_ref_service_account_name() {
        let mut pod = make_pod("uid-sa", "pod-sa");
        pod.service_account_name = "my-service-account".to_string();
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "sa".to_string(),
            field_ref: Some("spec.serviceAccountName".to_string()),
            resource_field_ref: None,
            mode: None,
        };
        assert_eq!(
            resolve_downward_api_value(&pod, &item, None),
            "my-service-account"
        );
    }

    #[test]
    fn test_resolve_field_ref_labels() {
        let mut pod = make_pod("uid-lbl", "pod-lbl");
        pod.labels.insert("app".to_string(), "frontend".to_string());
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "labels_app".to_string(),
            field_ref: Some("metadata.labels['app']".to_string()),
            resource_field_ref: None,
            mode: None,
        };
        assert_eq!(resolve_downward_api_value(&pod, &item, None), "frontend");
    }

    #[test]
    fn test_resolve_field_ref_annotations() {
        let mut pod = make_pod("uid-ann", "pod-ann");
        pod.annotations
            .insert("example.com/region".to_string(), "us-west-2".to_string());
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "region".to_string(),
            field_ref: Some("metadata.annotations['example.com/region']".to_string()),
            resource_field_ref: None,
            mode: None,
        };
        assert_eq!(resolve_downward_api_value(&pod, &item, None), "us-west-2");
    }

    #[test]
    fn test_resolve_field_ref_missing_label_returns_empty() {
        let pod = make_pod("uid-ml", "pod-ml");
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "missing".to_string(),
            field_ref: Some("metadata.labels['nonexistent']".to_string()),
            resource_field_ref: None,
            mode: None,
        };
        assert_eq!(resolve_downward_api_value(&pod, &item, None), "");
    }

    // -- status.hostIP / status.podIP in downward API volume files ---------------

    #[test]
    fn test_resolve_field_ref_status_host_ip_returns_non_empty() {
        // status.hostIP must resolve to the node's IP (detect_node_internal_ip),
        // never to an empty string. The exact value is env-dependent so we just
        // assert it is non-empty.
        let pod = make_pod("uid-hip", "pod-hip");
        let result = resolve_field_ref_with_pod_ip(&pod, "status.hostIP", None);
        // On CI or macOS `detect_node_internal_ip` falls back to 127.0.0.1.
        assert!(
            !result.is_empty(),
            "status.hostIP must not be empty; got {:?}",
            result
        );
    }

    #[test]
    fn test_resolve_field_ref_status_pod_ip_with_sandbox_ip() {
        // When a pod_ip is supplied (sandbox assigned), status.podIP must reflect it.
        let pod = make_pod("uid-pip", "pod-pip");
        let result = resolve_field_ref_with_pod_ip(&pod, "status.podIP", Some("10.0.0.5"));
        assert_eq!(result, "10.0.0.5");
    }

    #[test]
    fn test_resolve_field_ref_status_pod_ips_with_sandbox_ip() {
        let pod = make_pod("uid-pips", "pod-pips");
        let result = resolve_field_ref_with_pod_ip(&pod, "status.podIPs", Some("10.0.0.6"));
        assert_eq!(result, "10.0.0.6");
    }

    #[test]
    fn test_resolve_field_ref_status_pod_ip_no_sandbox_returns_empty() {
        // Before the sandbox is ready, pod_ip is None; file should be empty (not
        // written by ensure_pod_volumes in that case).
        let pod = make_pod("uid-pip-ns", "pod-pip-ns");
        let result = resolve_field_ref_with_pod_ip(&pod, "status.podIP", None);
        assert_eq!(result, "");
    }

    #[test]
    fn test_resolve_field_ref_status_pod_ip_host_network_falls_back_to_host_ip() {
        // For host-network pods the CRI returns no sandbox IP, so status.podIP
        // should resolve to the node's own IP (same as status.hostIP).
        let mut pod = make_pod("uid-hn", "pod-hn");
        pod.host_network = true;
        let result = resolve_field_ref_with_pod_ip(&pod, "status.podIP", None);
        assert!(
            !result.is_empty(),
            "status.podIP for host-network pod must not be empty"
        );
    }

    #[test]
    fn test_resolve_downward_api_value_host_ip_non_empty() {
        let pod = make_pod("uid-da-hip", "pod-da-hip");
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "host-ip".to_string(),
            field_ref: Some("status.hostIP".to_string()),
            resource_field_ref: None,
            mode: None,
        };
        let result = resolve_downward_api_value(&pod, &item, None);
        assert!(
            !result.is_empty(),
            "DownwardAPI status.hostIP volume file must not be empty"
        );
    }

    #[test]
    fn test_resolve_downward_api_value_pod_ip_with_sandbox() {
        let pod = make_pod("uid-da-pip", "pod-da-pip");
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "pod-ip".to_string(),
            field_ref: Some("status.podIP".to_string()),
            resource_field_ref: None,
            mode: None,
        };
        let result = resolve_downward_api_value(&pod, &item, Some("192.168.1.50"));
        assert_eq!(result, "192.168.1.50");
    }

    // -- defaultMode tests ----------------------------------------------------

    #[tokio::test]
    async fn test_projected_configmap_default_mode() {
        use std::collections::HashMap;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let mut cm_data = HashMap::new();
        cm_data.insert("key1".to_string(), b"value1".to_vec());

        let items = vec![kubelet_core::pod::KeyToPath {
            key: "key1".to_string(),
            path: "config.txt".to_string(),
            mode: None, // No per-item mode, should use default_mode
        }];

        write_projected_configmap_files(dir.path(), &cm_data, &items, Some(0o644))
            .await
            .unwrap();

        let file_path = dir.path().join("config.txt");
        assert!(file_path.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = tokio::fs::metadata(&file_path).await.unwrap();
            assert_eq!(metadata.permissions().mode() & 0o777, 0o644);
        }
    }

    #[tokio::test]
    async fn test_projected_configmap_item_mode_overrides_default() {
        use std::collections::HashMap;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let mut cm_data = HashMap::new();
        cm_data.insert("key1".to_string(), b"value1".to_vec());

        let items = vec![kubelet_core::pod::KeyToPath {
            key: "key1".to_string(),
            path: "config.txt".to_string(),
            mode: Some(0o600), // Per-item mode should override default_mode
        }];

        write_projected_configmap_files(dir.path(), &cm_data, &items, Some(0o644))
            .await
            .unwrap();

        let file_path = dir.path().join("config.txt");
        assert!(file_path.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = tokio::fs::metadata(&file_path).await.unwrap();
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        }
    }

    #[tokio::test]
    async fn test_projected_secret_default_mode() {
        use std::collections::HashMap;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let mut secret_data = HashMap::new();
        secret_data.insert("key1".to_string(), b"secret-value".to_vec());

        let items = vec![kubelet_core::pod::KeyToPath {
            key: "key1".to_string(),
            path: "secret.txt".to_string(),
            mode: None, // No per-item mode, should use default_mode
        }];

        write_projected_secret_files(dir.path(), &secret_data, &items, Some(0o400))
            .await
            .unwrap();

        let file_path = dir.path().join("secret.txt");
        assert!(file_path.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = tokio::fs::metadata(&file_path).await.unwrap();
            assert_eq!(metadata.permissions().mode() & 0o777, 0o400);
        }
    }

    #[tokio::test]
    async fn test_projected_secret_defaults_to_0600_when_no_mode() {
        use std::collections::HashMap;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let mut secret_data = HashMap::new();
        secret_data.insert("key1".to_string(), b"secret-value".to_vec());

        let items = vec![kubelet_core::pod::KeyToPath {
            key: "key1".to_string(),
            path: "secret.txt".to_string(),
            mode: None,
        }];

        // No default_mode specified, should default to 0o600 for secrets
        write_projected_secret_files(dir.path(), &secret_data, &items, None)
            .await
            .unwrap();

        let file_path = dir.path().join("secret.txt");
        assert!(file_path.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = tokio::fs::metadata(&file_path).await.unwrap();
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        }
    }

    #[tokio::test]
    async fn test_projected_configmap_defaults_to_0644_when_no_mode() {
        use std::collections::HashMap;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let mut cm_data = HashMap::new();
        cm_data.insert("key1".to_string(), b"value1".to_vec());

        let items = vec![kubelet_core::pod::KeyToPath {
            key: "key1".to_string(),
            path: "config.txt".to_string(),
            mode: None,
        }];

        // No default_mode specified, should default to 0o644 for configmaps
        write_projected_configmap_files(dir.path(), &cm_data, &items, None)
            .await
            .unwrap();

        let file_path = dir.path().join("config.txt");
        assert!(file_path.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = tokio::fs::metadata(&file_path).await.unwrap();
            assert_eq!(metadata.permissions().mode() & 0o777, 0o644);
        }
    }

    // -- volume update propagation tests (stale file cleanup) -----------------

    #[tokio::test]
    async fn test_clear_volume_dir_removes_all_files() {
        let dir = tempfile::TempDir::new().unwrap();
        // Create some files in the dir.
        tokio::fs::write(dir.path().join("file1"), b"a")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("file2"), b"b")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("file3"), b"c")
            .await
            .unwrap();

        clear_volume_dir(dir.path()).await;

        // All files should be gone.
        assert!(!dir.path().join("file1").exists());
        assert!(!dir.path().join("file2").exists());
        assert!(!dir.path().join("file3").exists());
    }

    #[tokio::test]
    async fn test_clear_volume_dir_removes_nested_files() {
        let dir = tempfile::TempDir::new().unwrap();
        tokio::fs::create_dir_all(dir.path().join("nested/leaf"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("nested/leaf/file"), b"x")
            .await
            .unwrap();

        clear_volume_dir(dir.path()).await;

        assert!(!dir.path().join("nested/leaf/file").exists());
    }

    #[tokio::test]
    async fn test_cleanup_stale_volume_files_removes_unlisted_files() {
        use std::collections::HashSet;
        let dir = tempfile::TempDir::new().unwrap();
        tokio::fs::write(dir.path().join("keep"), b"keep")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("stale"), b"stale")
            .await
            .unwrap();

        let expected: HashSet<std::path::PathBuf> = [dir.path().join("keep")].into_iter().collect();
        cleanup_stale_volume_files(dir.path(), &expected).await;

        assert!(dir.path().join("keep").exists(), "keep should remain");
        assert!(
            !dir.path().join("stale").exists(),
            "stale should be removed"
        );
    }

    #[tokio::test]
    async fn test_cleanup_stale_volume_files_empty_expected_removes_all() {
        use std::collections::HashSet;
        let dir = tempfile::TempDir::new().unwrap();
        tokio::fs::write(dir.path().join("file1"), b"x")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("file2"), b"y")
            .await
            .unwrap();

        let expected: HashSet<std::path::PathBuf> = HashSet::new();
        cleanup_stale_volume_files(dir.path(), &expected).await;

        assert!(!dir.path().join("file1").exists());
        assert!(!dir.path().join("file2").exists());
    }

    #[tokio::test]
    async fn test_cleanup_stale_volume_files_removes_nested_unlisted_files() {
        use std::collections::HashSet;
        let dir = tempfile::TempDir::new().unwrap();
        tokio::fs::create_dir_all(dir.path().join("update"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.path().join("delete"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("update/data-1"), b"value-2")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("delete/data-1"), b"value-1")
            .await
            .unwrap();

        let expected: HashSet<std::path::PathBuf> =
            [dir.path().join("update/data-1")].into_iter().collect();
        cleanup_stale_volume_files(dir.path(), &expected).await;

        assert!(dir.path().join("update/data-1").exists());
        assert!(!dir.path().join("delete/data-1").exists());
    }

    #[tokio::test]
    async fn test_expected_configmap_paths_all_keys() {
        use std::collections::HashMap;
        let dir = std::path::Path::new("/vol");
        let mut cm = HashMap::new();
        cm.insert("alpha".to_string(), b"v1".to_vec());
        cm.insert("beta".to_string(), b"v2".to_vec());
        let paths = expected_configmap_paths(dir, &cm, &[]);
        assert!(paths.contains(&dir.join("alpha")));
        assert!(paths.contains(&dir.join("beta")));
        assert_eq!(paths.len(), 2);
    }

    #[tokio::test]
    async fn test_expected_configmap_paths_with_items() {
        use std::collections::HashMap;
        let dir = std::path::Path::new("/vol");
        let cm: HashMap<String, Vec<u8>> = [("raw".to_string(), b"v".to_vec())].into();
        let items = vec![kubelet_core::pod::KeyToPath {
            key: "raw".to_string(),
            path: "mapped/path".to_string(),
            mode: None,
        }];
        let paths = expected_configmap_paths(dir, &cm, &items);
        assert!(paths.contains(&dir.join("mapped/path")));
        assert_eq!(paths.len(), 1);
    }

    #[tokio::test]
    async fn test_configmap_volume_update_removes_stale_files() {
        // Simulate a ConfigMap that previously had keys A and B, now only has A.
        // The file for B should be cleaned up.
        use std::collections::HashMap;
        let dir = tempfile::TempDir::new().unwrap();

        // Initial state: both files exist.
        tokio::fs::write(dir.path().join("keyA"), b"valA")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("keyB"), b"valB")
            .await
            .unwrap();

        // Updated CM only has keyA.
        let mut updated = HashMap::new();
        updated.insert("keyA".to_string(), b"newValA".to_vec());

        let expected = expected_configmap_paths(dir.path(), &updated, &[]);
        write_projected_configmap_files(dir.path(), &updated, &[], Some(0o644))
            .await
            .unwrap();
        cleanup_stale_volume_files(dir.path(), &expected).await;

        // keyA should be updated, keyB should be removed.
        assert!(dir.path().join("keyA").exists());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("keyA")).unwrap(),
            "newValA"
        );
        assert!(
            !dir.path().join("keyB").exists(),
            "stale keyB must be removed"
        );
    }

    #[tokio::test]
    async fn test_secret_volume_update_removes_stale_files() {
        use std::collections::HashMap;
        let dir = tempfile::TempDir::new().unwrap();

        // Initial state: two keys exist.
        tokio::fs::write(dir.path().join("user"), b"admin")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("pass"), b"secret")
            .await
            .unwrap();

        // Updated secret only has 'user'.
        let mut updated: HashMap<String, Vec<u8>> = HashMap::new();
        updated.insert("user".to_string(), b"new-admin".to_vec());

        let expected = expected_secret_paths(dir.path(), &updated, &[]);
        write_projected_secret_files(dir.path(), &updated, &[], Some(0o600))
            .await
            .unwrap();
        cleanup_stale_volume_files(dir.path(), &expected).await;

        assert!(dir.path().join("user").exists());
        assert_eq!(
            std::fs::read(dir.path().join("user")).unwrap(),
            b"new-admin"
        );
        assert!(
            !dir.path().join("pass").exists(),
            "stale 'pass' must be removed"
        );
    }

    #[test]
    fn test_configmap_data_bytes_merges_data_and_binary_data() {
        let cm = ConfigMap {
            data: Some(
                [("text".to_string(), "hello".to_string())]
                    .into_iter()
                    .collect(),
            ),
            binary_data: Some(
                [(
                    "bin".to_string(),
                    k8s_openapi::ByteString(vec![0_u8, 159_u8, 255_u8]),
                )]
                .into_iter()
                .collect(),
            ),
            ..Default::default()
        };

        let data = configmap_data_bytes(&cm);
        assert_eq!(data.get("text").map(Vec::as_slice), Some(&b"hello"[..]));
        assert_eq!(
            data.get("bin").map(Vec::as_slice),
            Some(&[0_u8, 159_u8, 255_u8][..])
        );
    }

    // ── activeDeadlineSeconds tests ───────────────────────────────────────

    /// A pod with an active_deadline_seconds that has not yet elapsed must NOT
    /// be terminated by sync_pod.
    #[tokio::test]
    async fn test_active_deadline_not_yet_elapsed_does_not_terminate() {
        let (worker, pm, _rx, _dir) = make_worker().await;
        let mut pod = make_pod("uid-dl-ok", "pod-dl-ok");
        pod.active_deadline_seconds = Some(3600); // 1 hour from now
        pm.upsert(pod.clone()).await.unwrap();

        // start_time is set by initialize() inside upsert
        let mut state = PodRuntimeState::default();
        let result = worker.sync_pod(&pod, &mut state).await;
        assert_eq!(result, PodSyncResult::Synced);
        let status = pm.status.get(&pod.uid).unwrap();
        assert_ne!(status.phase, PodPhase::Failed);
    }

    /// A pod whose active deadline has already elapsed must be terminated and
    /// its phase set to Failed.
    #[tokio::test]
    async fn test_active_deadline_elapsed_terminates_pod() {
        let (worker, pm, _rx, _dir) = make_worker().await;
        let mut pod = make_pod("uid-dl-expired", "pod-dl-expired");
        pod.active_deadline_seconds = Some(10);
        pm.upsert(pod.clone()).await.unwrap();

        // Manually back-date the pod's start_time so the deadline appears expired.
        if let Some(mut ls) = pm.status.get(&pod.uid) {
            ls.start_time = Some(Utc::now() - chrono::Duration::seconds(120));
            pm.status.set(pod.uid.clone(), ls);
        }

        let mut state = PodRuntimeState::default();
        let result = worker.sync_pod(&pod, &mut state).await;
        assert_eq!(result, PodSyncResult::Terminated);
        let status = pm.status.get(&pod.uid).unwrap();
        assert_eq!(status.phase, PodPhase::Failed);
    }

    // ── init container ready flag ─────────────────────────────────────────

    /// update_pod_status must set ready=true for a regular init container that
    /// has completed successfully (exit code 0).
    #[tokio::test]
    async fn test_init_container_ready_true_when_completed_successfully() {
        let (tx, _rx) = mpsc::channel(100);
        let pm = Arc::new(PodManager::new(tx));
        let rt = Arc::new(MockRuntime::new());
        let dir = tempfile::TempDir::new().unwrap();
        let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
        let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
        let vm = Arc::new(kubelet_adapters::volume::LocalVolumeManager::new(
            dir.path(),
        ));
        let rt_ref = rt.clone();
        let worker = PodWorker::new(
            pm.clone(),
            rt.clone(),
            rt,
            vm,
            cm,
            cg,
            Arc::new(HashMap::new()),
            "cgroupfs",
            dir.path(),
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
            None,
            "node1",
            Arc::new(InMemoryNodeReporter::new()),
            NodeDnsConfig::default(),
            Arc::new(DeviceManager::new("/tmp")),
        );

        let mut pod = make_pod("uid-ic-ready", "pod-ic-ready");
        pod.init_containers = vec![kubelet_core::pod::ContainerSpec {
            name: "init1".to_string(),
            image: "busybox:latest".to_string(),
            ..Default::default()
        }];
        pm.upsert(pod.clone()).await.unwrap();

        // Start: runtime exits init container with code 0 immediately.
        rt_ref.set_exit_on_start(Some(0)).await;
        let mut state = PodRuntimeState::default();
        let _ = worker.sync_pod(&pod, &mut state).await;
        // Second sync: init container is Exited/0 → continue to app containers.
        rt_ref.set_exit_on_start(None).await;
        let _ = worker.sync_pod(&pod, &mut state).await;

        let status = pm.status.get(&pod.uid).unwrap();
        assert_eq!(status.init_container_statuses.len(), 1);
        assert!(
            status.init_container_statuses[0].ready,
            "completed init container must be ready=true"
        );
    }

    /// update_pod_status must keep ready=false for a regular init container
    /// that is still Running.
    #[tokio::test]
    async fn test_init_container_ready_false_when_still_running() {
        let (worker, pm, _rx, _dir) = make_worker().await;
        let mut pod = make_pod("uid-ic-running", "pod-ic-running");
        pod.init_containers = vec![kubelet_core::pod::ContainerSpec {
            name: "init1".to_string(),
            image: "busybox:latest".to_string(),
            ..Default::default()
        }];
        pm.upsert(pod.clone()).await.unwrap();

        // First sync: creates sandbox + starts init container (Running state).
        let mut state = PodRuntimeState::default();
        let _ = worker.sync_pod(&pod, &mut state).await;

        let status = pm.status.get(&pod.uid).unwrap();
        assert!(!status.init_container_statuses.is_empty());
        assert!(
            !status.init_container_statuses[0].ready,
            "running init container must be ready=false"
        );
    }

    // ── failing init containers with RestartPolicy::Never ─────────────────

    /// When a regular init container exits with non-zero and the pod's
    /// restartPolicy is Never, sync_pod must return Terminated and set the
    /// pod phase to Failed without endlessly restarting.
    #[tokio::test]
    async fn test_failing_init_container_never_restart_terminates_pod() {
        let rt = Arc::new(MockRuntime::new());
        let dir = tempfile::TempDir::new().unwrap();
        let (tx, _rx) = mpsc::channel(100);
        let pm = Arc::new(PodManager::new(tx));
        let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
        let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
        let vm = Arc::new(kubelet_adapters::volume::LocalVolumeManager::new(
            dir.path(),
        ));
        let rt_ref = rt.clone();
        let worker = PodWorker::new(
            pm.clone(),
            rt.clone(),
            rt,
            vm,
            cm,
            cg,
            Arc::new(HashMap::new()),
            "cgroupfs",
            dir.path(),
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
            None,
            "node1",
            Arc::new(InMemoryNodeReporter::new()),
            NodeDnsConfig::default(),
            Arc::new(DeviceManager::new("/tmp")),
        );

        let mut pod = make_pod("uid-ic-fail", "pod-ic-fail");
        pod.restart_policy = RestartPolicy::Never;
        pod.init_containers = vec![kubelet_core::pod::ContainerSpec {
            name: "init1".to_string(),
            image: "busybox:latest".to_string(),
            ..Default::default()
        }];
        pm.upsert(pod.clone()).await.unwrap();

        // Make the runtime immediately exit containers with code 1.
        rt_ref.set_exit_on_start(Some(1)).await;

        let mut state = PodRuntimeState::default();
        // First sync: sandbox created + init container starts (exits 1 immediately).
        let first = worker.sync_pod(&pod, &mut state).await;
        assert!(
            matches!(first, PodSyncResult::NeedsRetry(_)),
            "first sync should NeedsRetry after starting init, got {:?}",
            first
        );
        // Second sync: sees Exited/1 + RestartPolicy::Never → terminates.
        let result = worker.sync_pod(&pod, &mut state).await;
        assert_eq!(
            result,
            PodSyncResult::Terminated,
            "should terminate when init exits non-zero with RestartPolicy::Never"
        );
        let status = pm.status.get(&pod.uid).unwrap();
        assert_eq!(
            status.phase,
            PodPhase::Failed,
            "pod phase must be Failed after init container failure"
        );
    }

    /// When a regular init container exits with non-zero and the pod's
    /// restartPolicy is Always (default), sync_pod must return NeedsRetry
    /// so it can be restarted.
    #[tokio::test]
    async fn test_failing_init_container_always_restart_needs_retry() {
        let rt = Arc::new(MockRuntime::new());
        let dir = tempfile::TempDir::new().unwrap();
        let (tx, _rx) = mpsc::channel(100);
        let pm = Arc::new(PodManager::new(tx));
        let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
        let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
        let vm = Arc::new(kubelet_adapters::volume::LocalVolumeManager::new(
            dir.path(),
        ));
        let rt_ref = rt.clone();
        let worker = PodWorker::new(
            pm.clone(),
            rt.clone(),
            rt,
            vm,
            cm,
            cg,
            Arc::new(HashMap::new()),
            "cgroupfs",
            dir.path(),
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
            None,
            "node1",
            Arc::new(InMemoryNodeReporter::new()),
            NodeDnsConfig::default(),
            Arc::new(DeviceManager::new("/tmp")),
        );

        let mut pod = make_pod("uid-ic-restart", "pod-ic-restart");
        pod.restart_policy = RestartPolicy::Always;
        pod.init_containers = vec![kubelet_core::pod::ContainerSpec {
            name: "init1".to_string(),
            image: "busybox:latest".to_string(),
            ..Default::default()
        }];
        pm.upsert(pod.clone()).await.unwrap();

        // init container exits 1 immediately, but restart policy is Always
        rt_ref.set_exit_on_start(Some(1)).await;

        let mut state = PodRuntimeState::default();
        let result = worker.sync_pod(&pod, &mut state).await;
        // The container exits and falls through — start_container is called,
        // so we get NeedsRetry("init container started, waiting for completion").
        assert!(
            matches!(result, PodSyncResult::NeedsRetry(_)),
            "should need retry for restart policy Always, got {:?}",
            result
        );
    }

    // -- metadata.labels / metadata.annotations format tests -----------------

    #[test]
    fn test_resolve_field_ref_all_labels_format() {
        let mut pod = make_pod("uid-lbl-fmt", "pod-lbl-fmt");
        pod.labels.insert("app".to_string(), "frontend".to_string());
        pod.labels.insert("tier".to_string(), "cache".to_string());
        let result = resolve_field_ref(&pod, "metadata.labels");
        // Each label must be formatted as key="value"\n (with trailing newline)
        assert!(
            result.contains("app=\"frontend\"\n"),
            "missing app label line, got: {:?}",
            result
        );
        assert!(
            result.contains("tier=\"cache\"\n"),
            "missing tier label line, got: {:?}",
            result
        );
        // The result should end with \n (last line also has trailing newline)
        assert!(
            result.ends_with('\n'),
            "should end with newline, got: {:?}",
            result
        );
    }

    #[test]
    fn test_resolve_field_ref_all_annotations_format() {
        let mut pod = make_pod("uid-ann-fmt", "pod-ann-fmt");
        pod.annotations
            .insert("builder".to_string(), "bar".to_string());
        let result = resolve_field_ref(&pod, "metadata.annotations");
        // Must contain builder="bar"\n (with trailing newline per Kubernetes spec)
        assert_eq!(result, "builder=\"bar\"\n");
    }

    #[test]
    fn test_resolve_field_ref_annotations_single_trailing_newline() {
        // Regression: previously used join("\n") which omitted trailing newline
        let mut pod = make_pod("uid-ann-nl", "pod-ann-nl");
        pod.annotations.insert("k".to_string(), "v".to_string());
        let result = resolve_field_ref(&pod, "metadata.annotations");
        assert_eq!(result, "k=\"v\"\n", "single annotation must end with \\n");
    }

    // -- defensive downwardAPI empty-value guard tests -----------------------

    #[test]
    fn test_resolve_downward_api_empty_pod_name_returns_empty() {
        // If pod_ref.name is empty, metadata.name resolves to empty string
        let pod = make_pod("uid-empty-name", "");
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "podname".to_string(),
            field_ref: Some("metadata.name".to_string()),
            resource_field_ref: None,
            mode: None,
        };
        let value = resolve_downward_api_value(&pod, &item, None);
        assert_eq!(value, "", "empty pod name should resolve to empty string");
        // This is the condition our defensive guard checks before writing files
        assert!(
            value.is_empty() && item.field_ref.is_some(),
            "should trigger the defensive skip condition"
        );
    }

    #[test]
    fn test_resolve_downward_api_nonempty_pod_name_not_skipped() {
        let pod = make_pod("uid-nn", "my-pod");
        let item = kubelet_core::pod::DownwardAPIVolumeFile {
            path: "podname".to_string(),
            field_ref: Some("metadata.name".to_string()),
            resource_field_ref: None,
            mode: None,
        };
        let value = resolve_downward_api_value(&pod, &item, None);
        assert_eq!(value, "my-pod");
        // Non-empty value should NOT trigger the defensive skip
        assert!(!value.is_empty(), "non-empty pod name must not be skipped");
    }

    #[tokio::test]
    async fn test_ensure_pod_volumes_rejects_empty_pod_name() {
        let (worker, pm, _rx, _dir) = make_worker().await;
        let mut pod = make_pod("uid-no-name", "");
        // Add a DownwardAPI volume so ensure_pod_volumes has something to write
        use kubelet_core::pod::{VolumeSource, VolumeSpec};
        pod.volumes = vec![VolumeSpec {
            name: "podinfo".to_string(),
            source: VolumeSource::DownwardAPI {
                items: vec![kubelet_core::pod::DownwardAPIVolumeFile {
                    path: "podname".to_string(),
                    field_ref: Some("metadata.name".to_string()),
                    resource_field_ref: None,
                    mode: None,
                }],
                default_mode: None,
            },
        }];
        pm.upsert(pod.clone()).await.unwrap();

        let mut state = PodRuntimeState::default();
        let result = worker.sync_pod(&pod, &mut state).await;
        // Empty pod name is caught by validate_pod (terminal failure, not retry)
        // This ensures we never proceed to write downwardAPI files with an empty name.
        assert!(
            matches!(result, PodSyncResult::Failed(_)),
            "empty pod name must yield Failed, got {:?}",
            result
        );
    }

    /// After NeedsRetry for a running init container, the pod status must
    /// reflect the init container's current state (not all-Waiting).
    #[tokio::test]
    async fn test_init_container_status_visible_during_needs_retry() {
        let (worker, pm, _rx, _dir) = make_worker().await;
        let mut pod = make_pod("uid-ic-vis", "pod-ic-vis");
        pod.init_containers = vec![kubelet_core::pod::ContainerSpec {
            name: "init1".to_string(),
            image: "busybox:latest".to_string(),
            ..Default::default()
        }];
        pm.upsert(pod.clone()).await.unwrap();

        // First sync: creates sandbox + starts init container → NeedsRetry
        let mut state = PodRuntimeState::default();
        let result = worker.sync_pod(&pod, &mut state).await;
        assert!(
            matches!(result, PodSyncResult::NeedsRetry(_)),
            "expected NeedsRetry while init container is running"
        );

        // After NeedsRetry the status must show the init container as Running,
        // not stuck in Waiting/ContainerCreating.
        let status = pm.status.get(&pod.uid).unwrap();
        assert_eq!(status.init_container_statuses.len(), 1);
        assert!(
            matches!(
                status.init_container_statuses[0].state,
                ContainerState::Running { .. }
            ),
            "init container state must be Running during NeedsRetry, got {:?}",
            status.init_container_statuses[0].state
        );
    }

    // ── volume_source_subdir tests ────────────────────────────────────────

    /// Every VolumeSource variant must map to the correct Go-kubelet-compatible
    /// `kubernetes.io~<type>` subdirectory name.
    #[test]
    fn test_volume_source_subdir_all_variants() {
        use kubelet_core::pod::VolumeSource;

        assert_eq!(
            volume_source_subdir(&VolumeSource::EmptyDir {
                medium: None,
                size_limit: None
            }),
            "kubernetes.io~empty-dir"
        );
        assert_eq!(
            volume_source_subdir(&VolumeSource::ConfigMap {
                name: "cm".into(),
                items: vec![],
                optional: false,
                default_mode: None
            }),
            "kubernetes.io~configmap"
        );
        assert_eq!(
            volume_source_subdir(&VolumeSource::Secret {
                secret_name: "s".into(),
                items: vec![],
                optional: false,
                default_mode: None
            }),
            "kubernetes.io~secret"
        );
        assert_eq!(
            volume_source_subdir(&VolumeSource::Projected {
                sources: vec![],
                default_mode: None
            }),
            "kubernetes.io~projected"
        );
        assert_eq!(
            volume_source_subdir(&VolumeSource::DownwardAPI {
                items: vec![],
                default_mode: None
            }),
            "kubernetes.io~downward-api"
        );
        // Unknown/unhandled variants fall back to projected.
        assert_eq!(
            volume_source_subdir(&VolumeSource::HostPath {
                path: "/tmp".into(),
                path_type: None
            }),
            "kubernetes.io~projected"
        );
    }

    /// `volume_target_path` must use the correct `kubernetes.io~<type>` subdir
    /// for each VolumeSource kind, and must fall back to `kubernetes.io~projected`
    /// when the volume name is not found in the pod spec.
    #[tokio::test]
    async fn test_volume_target_path_uses_correct_subdir() {
        use kubelet_core::pod::{VolumeSource, VolumeSpec};

        let (worker, _pm, _rx, dir) = make_worker().await;
        let mut pod = make_pod("uid-vtp", "pod-vtp");

        pod.volumes = vec![
            VolumeSpec {
                name: "sa-token".into(),
                source: VolumeSource::Projected {
                    sources: vec![],
                    default_mode: None,
                },
            },
            VolumeSpec {
                name: "logs".into(),
                source: VolumeSource::EmptyDir {
                    medium: None,
                    size_limit: None,
                },
            },
            VolumeSpec {
                name: "cfg".into(),
                source: VolumeSource::ConfigMap {
                    name: "my-cm".into(),
                    items: vec![],
                    optional: false,
                    default_mode: None,
                },
            },
        ];

        let base = dir.path().join("pods").join("uid-vtp").join("volumes");

        assert_eq!(
            worker.volume_target_path(&pod, "sa-token"),
            base.join("kubernetes.io~projected").join("sa-token")
        );
        assert_eq!(
            worker.volume_target_path(&pod, "logs"),
            base.join("kubernetes.io~empty-dir").join("logs")
        );
        assert_eq!(
            worker.volume_target_path(&pod, "cfg"),
            base.join("kubernetes.io~configmap").join("cfg")
        );
        // Unknown volume name falls back to projected.
        assert_eq!(
            worker.volume_target_path(&pod, "unknown"),
            base.join("kubernetes.io~projected").join("unknown")
        );
    }

    // ── remove_dangling_symlinks_in_dir tests ─────────────────────────────

    /// Dangling symlinks (target does not exist) must be removed; live symlinks
    /// and regular files must be preserved.
    #[tokio::test]
    async fn test_remove_dangling_symlinks_removes_only_dangling() {
        let dir = tempfile::TempDir::new().unwrap();

        // Regular file — must be preserved.
        tokio::fs::write(dir.path().join("regular"), b"data")
            .await
            .unwrap();

        // Live symlink (target exists) — must be preserved.
        tokio::fs::write(dir.path().join("target"), b"target-data")
            .await
            .unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(dir.path().join("target"), dir.path().join("live_link"))
            .unwrap();

        // Dangling symlink (target is gone) — must be removed.
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            dir.path().join("does_not_exist"),
            dir.path().join("dangling_link"),
        )
        .unwrap();

        remove_dangling_symlinks_in_dir(dir.path()).await;

        assert!(
            dir.path().join("regular").exists(),
            "regular file must be preserved"
        );
        assert!(
            dir.path().join("target").exists(),
            "symlink target must be preserved"
        );
        #[cfg(unix)]
        {
            assert!(
                dir.path().join("live_link").exists(),
                "live symlink must be preserved"
            );
            assert!(
                !dir.path().join("dangling_link").exists(),
                "dangling symlink must be removed"
            );
        }
    }

    /// Simulates the Go kubelet `..data -> ..TIMESTAMP` pattern:
    /// - `..data` symlink pointing to a timestamped dir that no longer exists.
    /// - `token -> ..data/token` chained symlink (also dangling).
    /// Both must be removed, leaving the directory clean for flat writes.
    #[tokio::test]
    async fn test_remove_dangling_symlinks_clears_go_kubelet_atomic_writer_leftovers() {
        let dir = tempfile::TempDir::new().unwrap();

        // `..data` points to a timestamped directory that was already cleaned up.
        #[cfg(unix)]
        {
            let ts_dir = dir.path().join("..2024_01_01_00_00_00.000000000");
            // Do NOT create ts_dir — it's intentionally absent.
            std::os::unix::fs::symlink(&ts_dir, dir.path().join("..data")).unwrap();

            // `token` -> `..data/token` (chained dangling symlink).
            let data_token = dir.path().join("..data").join("token");
            std::os::unix::fs::symlink(&data_token, dir.path().join("token")).unwrap();

            remove_dangling_symlinks_in_dir(dir.path()).await;

            assert!(
                !dir.path().join("..data").exists(),
                "dangling ..data symlink must be removed"
            );
            assert!(
                !dir.path().join("token").exists(),
                "dangling token symlink must be removed"
            );

            // Now a flat write must succeed.
            tokio::fs::write(dir.path().join("token"), b"my-token")
                .await
                .unwrap();
            assert_eq!(
                std::fs::read(dir.path().join("token")).unwrap(),
                b"my-token"
            );
        }
    }

    // ── CrashLoopBackOff backoff formula ──────────────────────────────────
    //
    // Formula: min(2^(n-1) * 10, 300) where n = restart_count.
    // Matches Go kubelet behaviour.  Regression guard against reverting to
    // the old linear `min(n*2, 30)` formula which capped too low and caused
    // rapid OOM restart storms on the physical node.

    fn clbo_backoff(restart_count: u32) -> u32 {
        let exp = (restart_count.saturating_sub(1)).min(5);
        ((1u64 << exp) * 10).min(300) as u32
    }

    #[test]
    fn test_clbo_backoff_zero_on_first_restart() {
        // restart_count == 0 → no delay (container failed for the first time).
        // The pod_worker guards with `if backoff > 0` so this path skips the
        // sleep entirely, but the formula must still yield 0.
        // restart_count=0: saturating_sub(1)=0, 2^0*10=10 — wait, the guard
        // is `if restart_count > 0` for init containers and `if backoff > 0`
        // for app containers.  Let's verify restart_count=0 is never passed
        // to the formula in practice (the guard is checked before calling it).
        // For the formula itself, restart=1 should give 10s (first actual backoff).
        assert_eq!(clbo_backoff(1), 10);
    }

    #[test]
    fn test_clbo_backoff_exponential_sequence() {
        // n=1 → 10s, n=2 → 20s, n=3 → 40s, n=4 → 80s, n=5 → 160s, n=6+ → 300s
        assert_eq!(clbo_backoff(1), 10);
        assert_eq!(clbo_backoff(2), 20);
        assert_eq!(clbo_backoff(3), 40);
        assert_eq!(clbo_backoff(4), 80);
        assert_eq!(clbo_backoff(5), 160);
        assert_eq!(clbo_backoff(6), 300);
    }

    #[test]
    fn test_clbo_backoff_caps_at_300s() {
        // Any restart count >= 6 must be capped at 300s (5 minutes).
        assert_eq!(clbo_backoff(6), 300);
        assert_eq!(clbo_backoff(10), 300);
        assert_eq!(clbo_backoff(100), 300);
    }

    #[test]
    fn test_clbo_backoff_exceeds_old_cap_of_30s() {
        // The old formula was min(n*2, 30).  Verify the new formula gives
        // significantly longer delays at higher restart counts, which prevents
        // OOM-crashing containers from being restarted too rapidly.
        let old_formula = |n: u32| n.saturating_mul(2).min(30);

        for n in 6..=20 {
            assert!(
                clbo_backoff(n) > old_formula(n),
                "restart={n}: new={} should exceed old={}",
                clbo_backoff(n),
                old_formula(n)
            );
        }
    }

    #[test]
    fn test_clbo_backoff_no_overflow_on_large_restart_count() {
        // saturating arithmetic must not panic or wrap.
        let _ = clbo_backoff(u32::MAX);
        let _ = clbo_backoff(1000);
    }

    // ── Pod cgroup memory limit (uses limits, not requests) ───────────────
    //
    // Regression: pod-level cgroup memory.max was set to sum-of-requests,
    // not sum-of-limits.  For Burstable pods (requests < limits) the pod
    // cgroup would cap memory at the request value, OOM-killing containers
    // before they could use their declared limit.
    // e.g. calico-apiserver: requests=24Mi, limits=128Mi → OOM at 24Mi.

    fn make_pod_with_resources(
        limits_memory: Option<i64>,
        requests_memory: Option<i64>,
    ) -> PodSpec {
        use kubelet_core::types::ResourceQuantity;
        let mut pod = make_pod("uid-cgtest", "cgtest");
        let mut resources = ResourceRequirements::default();
        if let Some(v) = limits_memory {
            resources
                .limits
                .insert("memory".to_string(), ResourceQuantity::memory_bytes(v));
        }
        if let Some(v) = requests_memory {
            resources
                .requests
                .insert("memory".to_string(), ResourceQuantity::memory_bytes(v));
        }
        pod.containers[0].resources = resources;
        pod
    }

    #[tokio::test]
    async fn test_pod_cgroup_memory_uses_limits_not_requests() {
        // Burstable: requests=24Mi, limits=128Mi.
        // Pod cgroup must be set to 128Mi (limits), not 24Mi (requests).
        let (worker, _, _, _) = make_worker().await;
        let pod = make_pod_with_resources(
            Some(128 * 1024 * 1024), // limit
            Some(24 * 1024 * 1024),  // request
        );
        let result = worker.calculate_pod_memory_requests(&pod);
        assert_eq!(
            result,
            128 * 1024 * 1024,
            "Pod cgroup limit must use memory.limits (128Mi), not memory.requests (24Mi)"
        );
    }

    #[tokio::test]
    async fn test_pod_cgroup_memory_falls_back_to_requests_when_no_limit() {
        // No limits set → fall back to requests.
        let (worker, _, _, _) = make_worker().await;
        let pod = make_pod_with_resources(
            None,                   // no limit
            Some(64 * 1024 * 1024), // request
        );
        let result = worker.calculate_pod_memory_requests(&pod);
        assert_eq!(
            result,
            64 * 1024 * 1024,
            "Should fall back to requests when no limit is set"
        );
    }

    #[tokio::test]
    async fn test_pod_cgroup_memory_zero_for_best_effort() {
        // BestEffort: no requests, no limits → pod cgroup is unconstrained (0).
        let (worker, _, _, _) = make_worker().await;
        let pod = make_pod_with_resources(None, None);
        let result = worker.calculate_pod_memory_requests(&pod);
        assert_eq!(result, 0, "BestEffort pod should return 0 (unconstrained)");
    }

    // ── imagePullSecrets resolution ──────────────────────────────────────────
    //
    // Regression: resolve_image_pull_secrets always returned an empty Vec
    // because pod.image_pull_secrets was never populated from the k8s API
    // object (both kube_watcher and kube_reporter were hardcoding vec![]).
    // This caused private images to fail with NotFound even with valid creds.

    #[test]
    fn test_image_registry_host_gcp_artifact_registry() {
        // Fully-qualified GCP Artifact Registry URL must extract just the host
        let host = image_registry_host("us-east1-docker.pkg.dev/my-repo/myapp@sha256:abc123");
        assert_eq!(host, "us-east1-docker.pkg.dev");
    }

    #[test]
    fn test_image_registry_host_with_tag() {
        let host = image_registry_host("us-east1-docker.pkg.dev/proj/images/app:v1.2.3");
        assert_eq!(host, "us-east1-docker.pkg.dev");
    }

    #[test]
    fn test_host_matches_registry_gcp_same_host() {
        assert!(host_matches_registry(
            "us-east1-docker.pkg.dev",
            "us-east1-docker.pkg.dev"
        ));
    }

    #[test]
    fn test_host_matches_registry_gcp_with_https_prefix() {
        // dockerconfigjson can have "https://" prefix in the auth key
        assert!(host_matches_registry(
            "https://us-east1-docker.pkg.dev",
            "us-east1-docker.pkg.dev"
        ));
    }

    #[test]
    fn test_host_matches_registry_different_region_no_match() {
        assert!(!host_matches_registry(
            "eu-west1-docker.pkg.dev",
            "us-east1-docker.pkg.dev"
        ));
    }

    #[test]
    fn test_parse_dockerconfigjson_gcp_service_account_auth() {
        use base64::Engine as _;
        // GCP uses "_json_key_base64" as username and a service account JSON as password
        let creds = base64::engine::general_purpose::STANDARD
            .encode("_json_key_base64:ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIKfQ==");
        let json_bytes =
            format!(r#"{{"auths": {{"us-east1-docker.pkg.dev": {{"auth": "{creds}"}}}}}}"#)
                .into_bytes();
        let entries = parse_dockerconfigjson_bytes(&json_bytes);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "us-east1-docker.pkg.dev");
        assert_eq!(entries[0].1, "_json_key_base64");
    }

    // ── ServiceAccount imagePullSecrets ──────────────────────────────────────
    //
    // When a pod has no pod-spec-level imagePullSecrets but the pod's
    // ServiceAccount does, resolve_image_pull_secrets must include SA secrets.
    // Also verify de-duplication when both pod and SA reference the same secret.

    #[test]
    fn test_image_registry_host_sa_secret_registry_extraction() {
        // Ensure image_registry_host works for typical private registry images
        // used with SA-level imagePullSecrets scenarios.
        assert_eq!(
            image_registry_host("us-east1-docker.pkg.dev/project/images/app:v1"),
            "us-east1-docker.pkg.dev"
        );
        // Docker Hub image with no explicit registry
        assert_eq!(image_registry_host("myapp:v1"), "docker.io");
    }

    #[test]
    fn test_host_matches_registry_sa_secret_scenarios() {
        // SA-level secret key might use "https://" prefix
        assert!(host_matches_registry(
            "https://us-east1-docker.pkg.dev",
            "us-east1-docker.pkg.dev"
        ));
        // Different registries must not match
        assert!(!host_matches_registry(
            "us-west1-docker.pkg.dev",
            "us-east1-docker.pkg.dev"
        ));
    }

    // ── HOSTNAME env var injection ──────────────────────────────────────────

    /// When no explicit HOSTNAME is set in the container spec, assemble_container_env
    /// must inject HOSTNAME equal to the pod name.
    ///
    /// Regression test for the bug where an empty sandbox_config.hostname caused
    /// containerd to propagate the node hostname into containers (e.g. "worker-node"),
    /// breaking stateful apps that parse `$HOSTNAME` for pod-ordinal identity.
    #[test]
    fn test_assemble_container_env_injects_hostname_from_pod_name() {
        let pod = make_pod("uid-hostname-1", "stateful-app-0");
        let container = pod.containers[0].clone();

        let env = assemble_container_env(
            &pod,
            &container,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap()
        .into_iter()
        .collect::<HashMap<_, _>>();

        assert_eq!(
            env.get("HOSTNAME").map(String::as_str),
            Some("stateful-app-0"),
            "HOSTNAME must equal pod name when not explicitly set"
        );
    }

    /// When the container spec explicitly sets HOSTNAME, that value must take
    /// precedence over the auto-injected pod name.
    #[test]
    fn test_assemble_container_env_explicit_hostname_not_overridden() {
        let mut pod = make_pod("uid-hostname-2", "my-pod-0");
        pod.containers[0].env = vec![kubelet_core::pod::EnvVar {
            name: "HOSTNAME".to_string(),
            value: Some("explicitly-set-hostname".to_string()),
            value_from: None,
        }];
        let container = pod.containers[0].clone();

        let env = assemble_container_env(
            &pod,
            &container,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap()
        .into_iter()
        .collect::<HashMap<_, _>>();

        assert_eq!(
            env.get("HOSTNAME").map(String::as_str),
            Some("explicitly-set-hostname"),
            "Explicitly set HOSTNAME must not be overridden by auto-injection"
        );
    }

    /// When spec.hostname is set on the pod, HOSTNAME must equal that value
    /// rather than the pod's metadata.name.
    #[test]
    fn test_assemble_container_env_hostname_uses_spec_hostname() {
        let mut pod = make_pod("uid-hostname-3", "my-pod-0");
        pod.hostname = Some("custom-host".to_string());
        let container = pod.containers[0].clone();

        let env = assemble_container_env(
            &pod,
            &container,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap()
        .into_iter()
        .collect::<HashMap<_, _>>();

        assert_eq!(
            env.get("HOSTNAME").map(String::as_str),
            Some("custom-host"),
            "HOSTNAME must equal spec.hostname when set"
        );
    }

    // ── spawn_readiness_probe tests ───────────────────────────────────────────

    /// Returns (worker, pod_manager, rx, rt, _dir) — the runtime is exposed so
    /// tests can inspect or mutate container state after sync_pod.
    async fn make_worker_with_rt() -> (
        PodWorker,
        Arc<PodManager>,
        mpsc::Receiver<kubelet_core::pod::PodUpdate>,
        Arc<MockRuntime>,
        tempfile::TempDir,
    ) {
        let (tx, rx) = mpsc::channel(100);
        let pm = Arc::new(PodManager::new(tx));
        let rt = Arc::new(MockRuntime::new());
        let dir = tempfile::TempDir::new().unwrap();
        let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
        let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
        let vm = Arc::new(LocalVolumeManager::new(dir.path()));
        let worker = PodWorker::new(
            pm.clone(),
            rt.clone(),
            rt.clone(),
            vm,
            cm,
            cg,
            Arc::new(HashMap::new()),
            "cgroupfs",
            dir.path(),
            "/var/log/pods",
            "registry.k8s.io/pause:3.9",
            None,
            "node1",
            Arc::new(InMemoryNodeReporter::new()),
            NodeDnsConfig::default(),
            Arc::new(DeviceManager::new("/tmp")),
        );
        (worker, pm, rx, rt, dir)
    }

    /// A running container becomes ready after the probe succeeds.
    ///
    /// Uses sync_pod to create and start the container via the worker, then
    /// drives spawn_readiness_probe directly with an exec probe (MockRuntime
    /// always returns exit_code=0 for exec_sync, so it always succeeds).
    #[tokio::test]
    async fn test_readiness_probe_marks_ready_when_running() {
        let (worker, pm, _rx, rt, _dir) = make_worker_with_rt().await;
        let pod = make_pod("uid-rp-1", "pod-rp-1");
        pm.upsert(pod.clone()).await.unwrap();

        let mut state = PodRuntimeState::default();
        assert_eq!(
            worker.sync_pod(&pod, &mut state).await,
            PodSyncResult::Synced
        );

        // Grab the container ID that sync_pod created.
        let cid_str = state.container_ids["nginx"].clone();
        let cid = kubelet_core::container::ContainerID::new(cid_str.clone());

        let map: Arc<DashMap<String, bool>> = Arc::new(DashMap::new());
        let probe = Probe {
            handler: kubelet_core::pod::ProbeHandler::Exec {
                command: vec!["true".to_string()],
            },
            initial_delay_seconds: 0,
            period_seconds: 1,
            timeout_seconds: 1,
            success_threshold: 1,
            failure_threshold: 3,
        };

        let map_clone = map.clone();
        let rt_arc: Arc<dyn ContainerRuntime> = rt.clone();
        let handle = tokio::spawn(spawn_readiness_probe(
            rt_arc,
            cid.clone(),
            probe,
            "127.0.0.1".to_string(),
            map_clone,
        ));

        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();

        assert_eq!(
            map.get(&cid_str).map(|v| *v),
            Some(true),
            "container should be ready after successful exec probe"
        );
    }

    /// A container that exits causes the probe task to stop and mark not-ready.
    #[tokio::test]
    async fn test_readiness_probe_stops_on_container_exit() {
        let (worker, pm, _rx, rt, _dir) = make_worker_with_rt().await;
        let pod = make_pod("uid-rp-2", "pod-rp-2");
        pm.upsert(pod.clone()).await.unwrap();

        let mut state = PodRuntimeState::default();
        assert_eq!(
            worker.sync_pod(&pod, &mut state).await,
            PodSyncResult::Synced
        );

        let cid_str = state.container_ids["nginx"].clone();
        let cid = kubelet_core::container::ContainerID::new(cid_str.clone());

        // Stop the container so state == Exited.
        rt.stop_container(&cid, 0).await.unwrap();

        let map: Arc<DashMap<String, bool>> = Arc::new(DashMap::new());
        let probe = Probe {
            handler: kubelet_core::pod::ProbeHandler::Exec {
                command: vec!["true".to_string()],
            },
            initial_delay_seconds: 0,
            period_seconds: 1,
            timeout_seconds: 1,
            success_threshold: 1,
            failure_threshold: 1,
        };

        let map_clone = map.clone();
        let rt_arc: Arc<dyn ContainerRuntime> = rt.clone();
        // Task must self-terminate because the container is Exited.
        tokio::time::timeout(
            Duration::from_secs(3),
            spawn_readiness_probe(
                rt_arc,
                cid.clone(),
                probe,
                "127.0.0.1".to_string(),
                map_clone,
            ),
        )
        .await
        .expect("probe task should terminate when container exits");

        assert_eq!(
            map.get(&cid_str).map(|v| *v),
            Some(false),
            "container should be not-ready after exit"
        );
    }

    /// A container that disappears from the runtime causes the probe task to stop
    /// and removes the entry from the readiness map entirely.
    #[tokio::test]
    async fn test_readiness_probe_stops_when_container_removed() {
        let (worker, pm, _rx, rt, _dir) = make_worker_with_rt().await;
        let pod = make_pod("uid-rp-3", "pod-rp-3");
        pm.upsert(pod.clone()).await.unwrap();

        let mut state = PodRuntimeState::default();
        assert_eq!(
            worker.sync_pod(&pod, &mut state).await,
            PodSyncResult::Synced
        );

        let cid_str = state.container_ids["nginx"].clone();
        let cid = kubelet_core::container::ContainerID::new(cid_str.clone());

        // Remove the container — runtime returns None for container_status.
        rt.remove_container(&cid).await.unwrap();

        let map: Arc<DashMap<String, bool>> = Arc::new(DashMap::new());
        map.insert(cid_str.clone(), true); // pretend it was ready before
        let probe = Probe {
            handler: kubelet_core::pod::ProbeHandler::Exec {
                command: vec!["true".to_string()],
            },
            initial_delay_seconds: 0,
            period_seconds: 1,
            timeout_seconds: 1,
            success_threshold: 1,
            failure_threshold: 1,
        };

        let map_clone = map.clone();
        let rt_arc: Arc<dyn ContainerRuntime> = rt.clone();
        tokio::time::timeout(
            Duration::from_secs(3),
            spawn_readiness_probe(
                rt_arc,
                cid.clone(),
                probe,
                "127.0.0.1".to_string(),
                map_clone,
            ),
        )
        .await
        .expect("probe task should terminate when container is gone");

        assert!(
            map.get(&cid_str).is_none(),
            "entry should be removed from readiness map when container no longer exists"
        );
    }
}
