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

//! Runtime manager -- orchestrates the pod worker pool.
//!
//! Spawns and manages one PodWorker task per pod, routes pod updates to workers,
//! and handles worker lifecycle (crash recovery, reaping).

pub mod pleg;

use crate::pod_worker::{PodRuntimeState, PodSyncResult, PodWorker};
use crate::runtime_manager::pleg::GenericPleg;
use kube::Client as KubeClient;
use kubelet_adapters::cgroup::CgroupManager;
use kubelet_adapters::checkpoint::CheckpointManager;
use kubelet_adapters::device_manager::DeviceManager;
use kubelet_adapters::sandbox_builder::NodeDnsConfig;
use kubelet_core::pod::manager::PodManager;
use kubelet_core::pod::{PodOperation, PodUpdate};
use kubelet_core::types::PodUID;
use kubelet_ports::driven::container_runtime::{ContainerRuntime, ImageManager};
use kubelet_ports::driven::node_reporter::NodeReporter;
use kubelet_ports::driven::storage::VolumeManager;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

#[derive(Debug, Default, Clone, Copy)]
struct PodSyncSlot {
    running: bool,
    pending: bool,
    /// How many consecutive NeedsRetry results this pod has had.
    /// Used to compute exponential back-off so a pod that is persistently
    /// failing (e.g. containerd down) doesn't spin at full speed but also
    /// retries automatically without waiting for an external watch event.
    retry_count: u32,
}

/// Manages per-pod worker tasks.
pub struct RuntimeManager {
    pod_manager: Arc<PodManager>,
    worker: Arc<PodWorker>,
    reporter: Arc<dyn NodeReporter>,
    pleg: Arc<Mutex<GenericPleg>>,
    /// Per-pod runtime state (sandbox IDs, container IDs, restart counts)
    pod_states: Arc<Mutex<HashMap<PodUID, PodRuntimeState>>>,
    /// Per-pod sync execution flags. Ensures one active sync per pod and
    /// coalesces bursts of updates into one trailing sync.
    sync_slots: Arc<Mutex<HashMap<PodUID, PodSyncSlot>>>,
}

impl RuntimeManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pod_manager: Arc<PodManager>,
        runtime: Arc<dyn ContainerRuntime>,
        image_manager: Arc<dyn ImageManager>,
        volume_manager: Arc<dyn VolumeManager>,
        reporter: Arc<dyn NodeReporter>,
        checkpoint_mgr: Arc<CheckpointManager>,
        cgroup_mgr: Arc<CgroupManager>,
        runtime_overheads: Arc<HashMap<String, HashMap<String, String>>>,
        cgroup_driver: impl Into<String>,
        root_dir: impl Into<PathBuf>,
        log_dir: impl Into<String>,
        pod_infra_container_image: impl Into<String>,
        kube_client: Option<KubeClient>,
        node_name: impl Into<String>,
        node_dns: NodeDnsConfig,
        device_manager: Arc<DeviceManager>,
    ) -> Self {
        let pleg = GenericPleg::new(runtime.clone(), Duration::from_secs(1));
        let cgroup_driver = cgroup_driver.into();
        let worker = Arc::new(PodWorker::new(
            pod_manager.clone(),
            runtime,
            image_manager,
            volume_manager,
            checkpoint_mgr,
            cgroup_mgr,
            runtime_overheads,
            cgroup_driver,
            root_dir,
            log_dir,
            pod_infra_container_image,
            kube_client,
            node_name,
            reporter.clone(),
            node_dns,
            device_manager,
        ));
        Self {
            pod_manager,
            worker,
            reporter,
            pleg: Arc::new(Mutex::new(pleg)),
            pod_states: Arc::new(Mutex::new(HashMap::new())),
            sync_slots: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Process a pod update event.
    pub async fn handle_update(&self, update: PodUpdate) {
        match update.op {
            PodOperation::Add | PodOperation::Update | PodOperation::Reconcile => {
                self.sync_pod(update.pod).await;
            }
            PodOperation::Remove => {
                self.delete_pod(update.pod).await;
            }
        }
    }

    async fn sync_pod(&self, pod: kubelet_core::pod::PodSpec) {
        let uid = pod.uid.clone();
        let worker = self.worker.clone();
        let reporter = self.reporter.clone();
        let pod_manager = self.pod_manager.clone();
        let states = self.pod_states.clone();
        let sync_slots = self.sync_slots.clone();

        // Coalesce concurrent update triggers for the same pod.
        let should_spawn = {
            let mut slots = sync_slots.lock().await;
            let slot = slots.entry(uid.clone()).or_default();
            if slot.running {
                slot.pending = true;
                false
            } else {
                slot.running = true;
                true
            }
        };

        if !should_spawn {
            debug!(pod_uid = %uid, "Coalescing pod sync while another sync is running");
            return;
        }

        // Run sync in a separate task so we don't block the update loop
        tokio::spawn(async move {
            let mut current_pod = pod;

            loop {
                let mut state = {
                    let mut locked = states.lock().await;
                    locked.remove(&uid).unwrap_or_default()
                };

                match worker.sync_pod(&current_pod, &mut state).await {
                    PodSyncResult::Synced => {
                        let mut locked = states.lock().await;
                        locked.insert(uid.clone(), state);
                        drop(locked);
                        // Clear retry back-off counter on success.
                        {
                            let mut slots = sync_slots.lock().await;
                            if let Some(slot) = slots.get_mut(&uid) {
                                slot.retry_count = 0;
                            }
                        }
                        if let Some(lifecycle_state) = pod_manager.status.get(&uid) {
                            if let Err(e) = reporter
                                .report_pod_status(&current_pod.pod_ref, &uid, &lifecycle_state)
                                .await
                            {
                                warn!(pod = %current_pod.pod_ref, error = %e, "Failed to report pod status");
                            }
                        }
                        debug!(pod = %current_pod.pod_ref, "Pod synced successfully");
                    }
                    PodSyncResult::NeedsRetry(reason) => {
                        let mut locked = states.lock().await;
                        locked.insert(uid.clone(), state);
                        drop(locked);
                        warn!(pod = %current_pod.pod_ref, reason, "Pod sync needs retry");
                        // Report current status so the API server always sees progress,
                        // even while waiting for init containers or image pulls.
                        if let Some(lifecycle_state) = pod_manager.status.get(&uid) {
                            if let Err(e) = reporter
                                .report_pod_status(&current_pod.pod_ref, &uid, &lifecycle_state)
                                .await
                            {
                                warn!(pod = %current_pod.pod_ref, error = %e, "Failed to report pod status on retry");
                            }
                        }
                        // Exponential back-off: 500ms, 1s, 2s, 4s, … capped at 30s.
                        // This ensures the pod retries autonomously even when the API
                        // server is unreachable (e.g. kube-vip hasn't come up yet),
                        // without spinning at full speed.
                        let retry_count = {
                            let mut slots = sync_slots.lock().await;
                            let slot = slots.entry(uid.clone()).or_default();
                            slot.retry_count = slot.retry_count.saturating_add(1);
                            slot.retry_count
                        };
                        let backoff = Duration::from_millis(
                            500u64.saturating_mul(1u64 << retry_count.min(6).saturating_sub(1)),
                        )
                        .min(Duration::from_secs(30));
                        tokio::time::sleep(backoff).await;

                        // If a new PodUpdate arrived while we were sleeping, use it
                        // (it may have fresher spec). Otherwise loop with current pod.
                        let next_pod = {
                            let mut slots = sync_slots.lock().await;
                            if let Some(slot) = slots.get_mut(&uid) {
                                if slot.pending {
                                    slot.pending = false;
                                    pod_manager.get(&uid)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        };
                        if let Some(p) = next_pod {
                            current_pod = p;
                        }
                        continue;
                    }
                    PodSyncResult::Failed(reason) => {
                        let mut locked = states.lock().await;
                        locked.insert(uid.clone(), state);
                        error!(pod = %current_pod.pod_ref, reason, "Pod sync failed permanently");
                    }
                    PodSyncResult::Terminated => {
                        let mut locked = states.lock().await;
                        locked.remove(&uid);
                        drop(locked);
                        // Report final status (e.g. Failed/DeadlineExceeded) before cleaning up.
                        if let Some(lifecycle_state) = pod_manager.status.get(&uid) {
                            if let Err(e) = reporter
                                .report_pod_status(&current_pod.pod_ref, &uid, &lifecycle_state)
                                .await
                            {
                                warn!(pod = %current_pod.pod_ref, error = %e, "Failed to report final pod status after termination");
                            }
                        }
                        // Do NOT force-delete the pod from the API server here. This path is
                        // only reached for natural terminations (init container failure,
                        // activeDeadlineSeconds). The test framework and GC need to observe
                        // the Failed phase. Explicit deletes are handled via PodOperation::Remove.
                    }
                }

                let maybe_next_pod = {
                    let mut slots = sync_slots.lock().await;
                    if let Some(slot) = slots.get_mut(&uid) {
                        if slot.pending {
                            slot.pending = false;
                            pod_manager.get(&uid)
                        } else {
                            slots.remove(&uid);
                            None
                        }
                    } else {
                        None
                    }
                };

                let Some(next_pod) = maybe_next_pod else {
                    break;
                };
                current_pod = next_pod;
                debug!(pod_uid = %uid, "Running coalesced trailing pod sync");
            }

            // Best effort: clear stale slot if control flow exits unexpectedly.
            let mut slots = sync_slots.lock().await;
            slots.remove(&uid);
        });
    }

    /// Run an independent PLEG poll loop, calling `poll_pleg` every `period`.
    ///
    /// Spawning this as a background task ensures container exits are detected
    /// promptly (within ~1 second) rather than waiting for the next API-server
    /// relist tick (which can be 30+ seconds).
    pub async fn run_pleg_loop(self: Arc<Self>, period: Duration) {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            self.poll_pleg().await;
        }
    }

    async fn poll_pleg(&self) {
        let events = {
            let mut pleg = self.pleg.lock().await;
            match pleg.relist().await {
                Ok(events) => events,
                Err(e) => {
                    warn!(error = %e, "PLEG relist failed");
                    return;
                }
            }
        };

        for event in events {
            debug!(
                pod_uid = %event.pod_uid,
                container = %event.container_name,
                event = ?event.event_type,
                "PLEG event"
            );
            let uid = PodUID::new(event.pod_uid.clone());
            if let Some(pod) = self.pod_manager.get(&uid) {
                self.sync_pod(pod).await;
            }
        }
    }

    pub async fn pleg_healthy(&self, max_staleness: Duration) -> bool {
        self.pleg.lock().await.is_healthy(max_staleness)
    }

    async fn delete_pod(&self, pod: kubelet_core::pod::PodSpec) {
        let worker = self.worker.clone();
        let states = self.pod_states.clone();
        let reporter = self.reporter.clone();
        let pod_manager = self.pod_manager.clone();
        let sync_slots = self.sync_slots.clone();

        tokio::spawn(async move {
            let state = {
                let mut locked = states.lock().await;
                locked.remove(&pod.uid).unwrap_or_default()
            };

            // Terminate the pod
            if let Err(e) = worker
                .terminate_pod(
                    &pod,
                    &state,
                    Duration::from_secs(pod.termination_grace_period_seconds),
                )
                .await
            {
                error!(pod = %pod.pod_ref, error = %e, "Pod termination failed");
            }

            // Wait for any concurrent sync_pod task to drain before running fallback
            // cleanup.  A sync_pod task that was holding the state when we took it above
            // may still be in the middle of creating containers.  If we run
            // cleanup_pod_containerd_by_uid before that task finishes, the newly-created
            // containers will be missed and remain in containerd as leaked records.
            // Polling the sync_slot until it is gone ensures we see all containers that
            // the concurrent sync could have created.
            let sync_drain_deadline = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                let still_running = {
                    let slots = sync_slots.lock().await;
                    slots.get(&pod.uid).map(|s| s.running).unwrap_or(false)
                };
                if !still_running || std::time::Instant::now() >= sync_drain_deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            // Fallback cleanup: if a concurrent sync_pod held the state when we took
            // it above, terminate_pod would have gotten an empty state and left orphaned
            // containerd records. Scan by pod UID and remove any stragglers.
            worker
                .cleanup_pod_containerd_by_uid(&pod.uid.0, &pod.pod_ref.to_string())
                .await;

            // Report final status to API server
            if let Some(lifecycle_state) = pod_manager.status.get(&pod.uid) {
                if let Err(e) = reporter
                    .report_pod_status(&pod.pod_ref, &pod.uid, &lifecycle_state)
                    .await
                {
                    warn!(pod = %pod.pod_ref, error = %e, "Failed to report final pod status after termination");
                }
            }

            // Force-delete the pod from the API server so it disappears from kubectl/e2e framework
            if let Err(e) = reporter.delete_pod(&pod.pod_ref, &pod.uid).await {
                warn!(pod = %pod.pod_ref, error = %e, "Failed to force-delete pod from API server");
            }

            info!(pod = %pod.pod_ref, "Pod cleanup completed");
        });
    }

    /// Get current pod runtime states (for diagnostics / API).
    pub async fn pod_states_snapshot(&self) -> HashMap<PodUID, PodRuntimeState> {
        self.pod_states.lock().await.clone()
    }

    /// Number of pods currently being managed.
    pub async fn active_pod_count(&self) -> usize {
        self.pod_states.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_adapters::checkpoint::CheckpointManager;
    use kubelet_adapters::mock_runtime::MockRuntime;
    use kubelet_adapters::volume::LocalVolumeManager;
    use kubelet_core::pod::{
        ContainerSpec, ImagePullPolicy, PodSpec, ResourceRequirements, RestartPolicy,
    };
    use kubelet_core::types::{PodRef, PodUID};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    async fn make_manager() -> (
        RuntimeManager,
        Arc<PodManager>,
        mpsc::Receiver<kubelet_core::pod::PodUpdate>,
        TempDir,
    ) {
        let (tx, rx) = mpsc::channel(1000);
        let pm = Arc::new(PodManager::new(tx));
        let rt = Arc::new(MockRuntime::new());
        let dir = TempDir::new().unwrap();
        let cm = Arc::new(CheckpointManager::new(dir.path()).unwrap());
        let cg = Arc::new(CgroupManager::new("/sys/fs/cgroup", true));
        let vm = Arc::new(LocalVolumeManager::new(dir.path()));
        let manager = RuntimeManager::new(
            pm.clone(),
            rt.clone(),
            rt.clone(),
            vm,
            Arc::new(kubelet_adapters::kube_client::InMemoryNodeReporter::default()),
            cm,
            cg,
            Arc::new(HashMap::new()),
            "cgroupfs",
            dir.path(),
            "/tmp/logs",
            "registry.k8s.io/pause:3.9",
            None,
            "node1",
            kubelet_adapters::sandbox_builder::NodeDnsConfig::default(),
            Arc::new(kubelet_adapters::device_manager::DeviceManager::new(
                dir.path(),
            )),
        );
        (manager, pm, rx, dir)
    }

    fn make_pod(uid: &str, name: &str) -> PodSpec {
        PodSpec {
            uid: PodUID::new(uid),
            pod_ref: PodRef::new("default", name),
            containers: vec![ContainerSpec {
                name: "app".to_string(),
                image: "alpine:latest".to_string(),
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
            termination_grace_period_seconds: 5,
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

    #[tokio::test]
    async fn test_handle_add_syncs_pod() {
        let (manager, pm, _rx, _dir) = make_manager().await;
        let pod = make_pod("uid-rm-1", "pod-1");
        pm.upsert(pod.clone()).await.unwrap();

        manager
            .handle_update(PodUpdate {
                pod,
                op: PodOperation::Add,
            })
            .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let count = manager.active_pod_count().await;
            if count >= 1 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                assert!(
                    count >= 1,
                    "expected at least one active pod state, got {}",
                    count
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn test_handle_remove_clears_state() {
        let (manager, pm, _rx, _dir) = make_manager().await;
        let pod = make_pod("uid-rm-2", "pod-2");
        pm.upsert(pod.clone()).await.unwrap();

        manager
            .handle_update(PodUpdate {
                pod: pod.clone(),
                op: PodOperation::Add,
            })
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        manager
            .handle_update(PodUpdate {
                pod,
                op: PodOperation::Remove,
            })
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(manager.active_pod_count().await, 0);
    }

    #[tokio::test]
    async fn test_multiple_pods_tracked_independently() {
        let (manager, pm, _rx, _dir) = make_manager().await;
        for i in 0..3 {
            let pod = make_pod(&format!("uid-{}", i), &format!("pod-{}", i));
            pm.upsert(pod.clone()).await.unwrap();
            manager
                .handle_update(PodUpdate {
                    pod,
                    op: PodOperation::Add,
                })
                .await;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let count = manager.active_pod_count().await;
            if count >= 2 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                assert!(
                    count >= 2,
                    "expected at least two independently tracked pod states, got {}",
                    count
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn test_pleg_reports_healthy_after_updates() {
        let (manager, pm, _rx, _dir) = make_manager().await;
        let pod = make_pod("uid-rm-pleg-1", "pod-pleg-1");
        pm.upsert(pod.clone()).await.unwrap();

        // PLEG health is updated via poll_pleg (called by run_pleg_loop),
        // not directly by handle_update.  Trigger a poll explicitly.
        manager.poll_pleg().await;

        assert!(manager.pleg_healthy(Duration::from_secs(3)).await);
    }

    /// Regression test: reconcile_all must not deadlock when the number of
    /// desired pods exceeds the update channel capacity.
    ///
    /// Previously `reconcile_all` was called inline inside the `tokio::select!`
    /// reconcile arm in lib.rs.  When the pod count exceeded the channel
    /// capacity the arm blocked trying to send, starving `update_rx.recv()`,
    /// and permanently deadlocking — new pods were never processed.
    ///
    /// The fix spawns `reconcile_all` in a separate task so the main loop
    /// continues draining the channel while reconcile sends are in flight.
    /// This test verifies that the spawned pattern delivers all Reconcile
    /// events for a pod count well above the channel capacity.
    #[tokio::test]
    async fn test_reconcile_all_does_not_deadlock_with_many_pods() {
        const POD_COUNT: usize = 100;

        // Use a channel large enough for setup upserts, then drain it clean
        // before testing reconcile so the initial Add events don't interfere.
        let (setup_tx, mut setup_rx) = mpsc::channel(POD_COUNT + 10);
        let pm = Arc::new(PodManager::new(setup_tx));

        for i in 0..POD_COUNT {
            let pod = make_pod(&format!("uid-deadlock-{}", i), &format!("pod-{}", i));
            pm.upsert(pod).await.unwrap();
        }
        // Drain all Add events from setup so the channel is empty.
        while setup_rx.try_recv().is_ok() {}

        // Use a small-capacity channel for the reconcile phase to prove the
        // spawned-task approach handles backpressure correctly.  Without
        // spawning, reconcile_all would block after filling this channel and
        // never return.
        const SMALL_CAP: usize = 8;
        let (small_tx, mut small_rx) = mpsc::channel(SMALL_CAP);
        let pm2 = Arc::new(PodManager::new(small_tx));
        // Transfer the desired pods to pm2 one batch at a time, draining
        // between batches so the small channel never fills during setup.
        for pod in pm.list() {
            // Drain any pending events before each send to keep space free.
            while small_rx.try_recv().is_ok() {}
            pm2.upsert(pod).await.unwrap();
        }
        while small_rx.try_recv().is_ok() {}

        // Spawn reconcile_all as a task — this is the pattern from the fix.
        // It runs concurrently with the drainer below, so backpressure from
        // the small channel is relieved without deadlocking the caller.
        let pm2_clone = pm2.clone();
        let reconcile = tokio::spawn(async move {
            pm2_clone.reconcile_all().await.unwrap();
        });

        // Drain Reconcile events — simulates the runtime loop's recv arm.
        let mut received = 0usize;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match tokio::time::timeout(Duration::from_millis(50), small_rx.recv()).await {
                Ok(Some(_)) => {
                    received += 1;
                    if received == POD_COUNT {
                        break;
                    }
                }
                Ok(None) => break, // channel closed
                Err(_) => {
                    // recv timed out — check whether reconcile finished
                    if tokio::time::Instant::now() >= deadline {
                        break;
                    }
                }
            }
        }

        let reconcile_result = tokio::time::timeout(Duration::from_secs(2), reconcile).await;
        assert!(
            reconcile_result.is_ok(),
            "reconcile_all task timed out — possible deadlock regression"
        );

        assert_eq!(
            received, POD_COUNT,
            "expected {} Reconcile events, got {} — some pods were not reconciled",
            POD_COUNT, received
        );
    }
}
