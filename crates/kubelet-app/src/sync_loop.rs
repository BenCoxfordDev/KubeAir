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

//! Main pod sync loop - drives reconciliation of desired vs actual pod state.
//!
//! Mirrors pkg/kubelet/kubelet.go syncLoop / syncLoopIteration.

use kubelet_core::error::Result;
use kubelet_core::pod::PodUpdate;
use kubelet_core::pod::manager::PodManager;
use kubelet_core::pod::sync::{SyncAction, determine_sync_action, validate_pod};
use kubelet_ports::driven::container_runtime::ContainerRuntime;
use kubelet_ports::driven::node_reporter::NodeReporter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::metrics::{POD_START_TOTAL, POD_SYNC_DURATION, RUNNING_CONTAINER_COUNT};

/// Configuration for the sync loop.
pub struct SyncLoopConfig {
    pub reconcile_interval: Duration,
    pub max_concurrent_syncs: usize,
}

impl Default for SyncLoopConfig {
    fn default() -> Self {
        Self {
            reconcile_interval: Duration::from_secs(60),
            max_concurrent_syncs: 5,
        }
    }
}

/// Run the pod sync loop, processing updates from the channel.
pub async fn run_sync_loop(
    pod_manager: Arc<PodManager>,
    runtime: Arc<dyn ContainerRuntime>,
    reporter: Arc<dyn NodeReporter>,
    mut update_rx: mpsc::Receiver<PodUpdate>,
    config: SyncLoopConfig,
) -> Result<()> {
    info!("Pod sync loop started");

    let mut reconcile_interval = tokio::time::interval(config.reconcile_interval);

    loop {
        tokio::select! {
            maybe_update = update_rx.recv() => {
                match maybe_update {
                    None => {
                        info!("Sync loop: update channel closed, exiting");
                        break;
                    }
                    Some(update) => {
                        let action = determine_sync_action(&update);
                        debug!(
                            pod = %update.pod.pod_ref,
                            action = ?action,
                            "Processing pod update"
                        );

                        let start = std::time::Instant::now();
                        let result = handle_pod_update(
                            &update,
                            action,
                            &*runtime,
                            &*reporter,
                            &pod_manager,
                        ).await;

                        let duration = start.elapsed().as_secs_f64();
                        POD_SYNC_DURATION
                            .with_label_values(&["sync"])
                            .observe(duration);

                        if let Err(e) = result {
                            error!(
                                pod = %update.pod.pod_ref,
                                error = %e,
                                "Pod sync failed"
                            );
                            POD_START_TOTAL.with_label_values(&["failure"]).inc();
                        }
                    }
                }
            }
            _ = reconcile_interval.tick() => {
                debug!("Periodic reconciliation tick");
                update_running_metrics(&*runtime).await;
                if let Err(e) = pod_manager.reconcile_all().await {
                    error!(error = %e, "Reconcile all failed");
                }
            }
        }
    }

    Ok(())
}

/// Handle a single pod update event.
async fn handle_pod_update(
    update: &PodUpdate,
    action: SyncAction,
    _runtime: &dyn ContainerRuntime,
    reporter: &dyn NodeReporter,
    pod_manager: &PodManager,
) -> Result<()> {
    if let Err(e) = validate_pod(&update.pod) {
        warn!(pod = %update.pod.pod_ref, error = %e, "Invalid pod spec, skipping");
        return Ok(());
    }

    match action {
        SyncAction::Create => {
            info!(pod = %update.pod.pod_ref, "Creating pod");
            // In a full implementation: pull images, run sandbox, start containers
            POD_START_TOTAL.with_label_values(&["success"]).inc();
        }
        SyncAction::Update => {
            info!(pod = %update.pod.pod_ref, "Updating pod");
        }
        SyncAction::Delete => {
            info!(pod = %update.pod.pod_ref, "Deleting pod");
        }
        SyncAction::Reconcile => {
            debug!(pod = %update.pod.pod_ref, "Reconciling pod");
        }
    }

    // Report status to API server
    if let Some(state) = pod_manager.status.get(&update.pod.uid) {
        reporter
            .report_pod_status(&update.pod.pod_ref, &update.pod.uid, &state)
            .await?;
    }

    Ok(())
}

/// Update Prometheus metrics with current runtime container counts.
async fn update_running_metrics(runtime: &dyn ContainerRuntime) {
    match runtime.list_containers().await {
        Ok(containers) => {
            let running = containers
                .iter()
                .filter(|c| c.state == kubelet_core::container::RuntimeContainerState::Running)
                .count();
            RUNNING_CONTAINER_COUNT.set(running as i64);
        }
        Err(e) => {
            warn!(error = %e, "Failed to list containers for metrics");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_adapters::kube_client::InMemoryNodeReporter;
    use kubelet_adapters::mock_runtime::MockRuntime;
    use kubelet_core::pod::{
        ContainerSpec, ImagePullPolicy, PodSpec, PodUpdate, ResourceRequirements, RestartPolicy,
    };
    use kubelet_core::types::{PodRef, PodUID};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn valid_pod(uid: &str, name: &str) -> PodSpec {
        PodSpec {
            uid: PodUID::new(uid),
            pod_ref: PodRef::new("default", name),
            containers: vec![ContainerSpec {
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
            generation: None,
        }
    }

    #[tokio::test]
    async fn test_sync_loop_processes_add_event() {
        let (tx, rx) = mpsc::channel(10);
        let pod_manager = Arc::new(PodManager::new(tx.clone()));
        let runtime = Arc::new(MockRuntime::new());
        let reporter = Arc::new(InMemoryNodeReporter::new());

        let pod = valid_pod("uid-sl-1", "my-pod");
        pod_manager.upsert(pod.clone()).await.unwrap();

        let pm = pod_manager.clone();
        let rt = runtime.clone();
        let rp = reporter.clone();

        // Run loop briefly
        let handle = tokio::spawn(async move {
            let config = SyncLoopConfig {
                reconcile_interval: Duration::from_secs(3600), // don't tick
                max_concurrent_syncs: 1,
            };
            run_sync_loop(pm, rt, rp, rx, config).await
        });

        // Give the loop time to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Pod status should have been initialized
        let state = pod_manager.status.get(&PodUID::new("uid-sl-1"));
        assert!(state.is_some());

        // Reporter should have received a pod status report
        assert!(reporter.pod_report_count().await >= 1);

        handle.abort();
    }

    #[tokio::test]
    async fn test_sync_loop_closes_on_channel_drop() {
        let (tx, _rx) = mpsc::channel(10);
        let pod_manager = Arc::new(PodManager::new(tx));
        let runtime = Arc::new(MockRuntime::new());
        let reporter = Arc::new(InMemoryNodeReporter::new());

        // Drop the pod manager's sender (tx is dropped here by not cloning it)
        // The rx is passed to the loop; when rx's sender side is all dropped, loop exits.

        // We need to drop tx - since pod_manager holds it internally, we test by
        // sending then dropping the tx side manually:
        let (tx2, rx2) = mpsc::channel::<PodUpdate>(1);
        drop(tx2);

        let config = SyncLoopConfig {
            reconcile_interval: Duration::from_secs(3600),
            max_concurrent_syncs: 1,
        };

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            run_sync_loop(pod_manager, runtime, reporter, rx2, config),
        )
        .await;

        assert!(
            result.is_ok(),
            "Loop should exit cleanly when channel closes"
        );
    }
}
