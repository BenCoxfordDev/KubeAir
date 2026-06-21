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

//! Pod manager - central registry of desired and actual pod states.
//!
//! This is the heart of the domain: it tracks what pods _should_ exist,
//! what pods _actually_ exist (via runtime), and drives reconciliation.

use crate::error::{KubeletError, Result};
use crate::pod::status::PodStatusManager;
use crate::pod::{PodOperation, PodSpec, PodUpdate};
use crate::types::PodUID;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// How long a removed pod stays accessible (e.g. for log fetches) after being removed.
const REMOVED_POD_TTL: Duration = Duration::from_secs(300);

/// Manages the set of pods assigned to this node.
pub struct PodManager {
    /// Desired pods (from API server / static files)
    desired: Arc<DashMap<PodUID, PodSpec>>,
    /// Recently removed pods kept for log fetches
    recently_removed: Arc<DashMap<PodUID, (PodSpec, Instant)>>,
    /// Status tracker
    pub status: Arc<PodStatusManager>,
    /// Channel for pod update events
    update_tx: mpsc::Sender<PodUpdate>,
}

impl PodManager {
    pub fn new(update_tx: mpsc::Sender<PodUpdate>) -> Self {
        Self {
            desired: Arc::new(DashMap::new()),
            recently_removed: Arc::new(DashMap::new()),
            status: Arc::new(PodStatusManager::new()),
            update_tx,
        }
    }

    /// Add or update a desired pod.
    pub async fn upsert(&self, pod: PodSpec) -> Result<()> {
        let mut pod = pod;
        let uid = pod.uid.clone();
        let is_new = !self.desired.contains_key(&uid);

        // Reconcile host_aliases between equivalent static/mirror pod entries
        // that may have different names (e.g. kube-vip vs kube-vip-<node>) and UIDs.
        if is_new {
            let existing = self.desired.iter().find(|r| {
                r.pod_ref.namespace == pod.pod_ref.namespace
                    && pod_names_equivalent(r.value(), &pod)
                    && r.uid != uid
            });
            if let Some(existing) = existing {
                let existing_uid = existing.uid.clone();
                let existing_aliases = existing.host_aliases.clone();
                drop(existing);

                // Incoming pod has aliases: this is the file-source authoritative spec.
                // Propagate its aliases to the existing equivalent (API-server) pod and
                // do NOT insert this pod as a separate entry. Returning early prevents
                // a second pod worker from being spawned for the same logical pod on
                // every file-config poll cycle. Only send an Update event when the
                // aliases actually changed (avoids spurious restarts).
                if !pod.host_aliases.is_empty() {
                    if existing_aliases != pod.host_aliases
                        && let Some(mut entry) = self.desired.get_mut(&existing_uid)
                    {
                        entry.host_aliases = pod.host_aliases.clone();
                        let updated_pod = entry.clone();
                        drop(entry);
                        info!(
                            pod = %updated_pod.pod_ref,
                            uid = %existing_uid,
                            host_aliases_count = updated_pod.host_aliases.len(),
                            "Merged host_aliases from file source into existing pod entry"
                        );
                        self.update_tx
                            .send(PodUpdate {
                                pod: updated_pod,
                                op: PodOperation::Update,
                            })
                            .await
                            .map_err(|e| {
                                KubeletError::Internal(format!("pod update channel closed: {e}"))
                            })?;
                    }
                    return Ok(());
                }

                // Incoming pod has no aliases, existing does: copy aliases into incoming
                // before insertion so either representation preserves HostAliases.
                if pod.host_aliases.is_empty() && !existing_aliases.is_empty() {
                    pod.host_aliases = existing_aliases;
                    info!(
                        pod = %pod.pod_ref,
                        uid = %uid,
                        host_aliases_count = pod.host_aliases.len(),
                        "Propagated host_aliases from existing equivalent pod entry"
                    );
                }
            }
        }

        // On updates (is_new=false): if the incoming spec carries no host_aliases but the
        // currently-stored spec does, preserve the stored aliases. API-server periodic
        // re-syncs carry the mirror pod spec which has empty host_aliases, and would
        // otherwise silently clear the aliases that were merged from the static file source.
        if !is_new
            && pod.host_aliases.is_empty()
            && let Some(existing) = self.desired.get(&uid)
            && !existing.host_aliases.is_empty()
        {
            pod.host_aliases = existing.host_aliases.clone();
            debug!(
                pod = %pod.pod_ref,
                uid = %uid,
                host_aliases_count = pod.host_aliases.len(),
                "Preserved host_aliases on update (incoming spec had none)"
            );
        }

        self.desired.insert(uid.clone(), pod.clone());

        let op = if is_new {
            info!(pod = %pod.pod_ref, uid = %uid, "Pod added");
            PodOperation::Add
        } else {
            info!(pod = %pod.pod_ref, uid = %uid, "Pod updated");
            PodOperation::Update
        };

        if is_new {
            self.status.initialize(&pod);
        }

        self.update_tx
            .send(PodUpdate { pod, op })
            .await
            .map_err(|e| KubeletError::Internal(format!("pod update channel closed: {e}")))?;

        Ok(())
    }

    /// Remove a pod (graceful teardown).
    ///
    /// `fallback_spec` is used when the pod UID is not in the desired set (i.e.
    /// the pod arrived with `deletionTimestamp` already set and was never added).
    /// In that case we still forward the Remove to the runtime so it can
    /// stop any running containers and force-delete the API object.
    pub async fn remove(&self, uid: &PodUID, fallback_spec: Option<PodSpec>) -> Result<()> {
        let pod = if let Some((_, pod)) = self.desired.remove(uid) {
            info!(pod = %pod.pod_ref, uid = %uid, "Pod removed");
            // Keep a short-lived cache entry so log requests can still find the pod.
            self.recently_removed
                .insert(uid.clone(), (pod.clone(), Instant::now()));
            self.evict_stale_removed();
            pod
        } else if let Some(spec) = fallback_spec {
            // Pod was never tracked (arrived already terminating). Forward the
            // Remove to the runtime so it force-deletes the API object and
            // stops any leaked containers.
            warn!(pod = %spec.pod_ref, uid = %uid, "Remove for untracked pod; forwarding to runtime for cleanup");
            spec
        } else {
            warn!(uid = %uid, "Attempted to remove unknown pod");
            return Ok(());
        };
        self.update_tx
            .send(PodUpdate {
                pod,
                op: PodOperation::Remove,
            })
            .await
            .map_err(|e| KubeletError::Internal(format!("pod update channel closed: {e}")))?;
        Ok(())
    }

    fn evict_stale_removed(&self) {
        self.recently_removed
            .retain(|_, (_, inserted)| inserted.elapsed() < REMOVED_POD_TTL);
    }

    /// Get a pod by UID.
    pub fn get(&self, uid: &PodUID) -> Option<PodSpec> {
        self.desired.get(uid).map(|r| r.clone())
    }

    /// List all desired pods.
    pub fn list(&self) -> Vec<PodSpec> {
        self.desired.iter().map(|r| r.value().clone()).collect()
    }

    /// Count of desired pods.
    pub fn count(&self) -> usize {
        self.desired.len()
    }

    /// Get a pod by namespace and name.
    ///
    /// Also checks recently-removed pods so that log requests can succeed
    /// for a short window after the pod is deleted from the API server.
    pub fn get_by_name(&self, namespace: &str, name: &str) -> Option<PodSpec> {
        if let Some(pod) = self
            .desired
            .iter()
            .find(|r| r.pod_ref.namespace == namespace && r.pod_ref.name == name)
            .map(|r| r.value().clone())
        {
            return Some(pod);
        }
        self.recently_removed
            .iter()
            .find(|r| {
                let (spec, inserted) = r.value();
                inserted.elapsed() < REMOVED_POD_TTL
                    && spec.pod_ref.namespace == namespace
                    && spec.pod_ref.name == name
            })
            .map(|r| r.value().0.clone())
    }

    /// Trigger a full reconciliation pass.
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!(
            "Triggering reconciliation of all {} pods",
            self.desired.len()
        );
        for entry in self.desired.iter() {
            self.update_tx
                .send(PodUpdate {
                    pod: entry.value().clone(),
                    op: PodOperation::Reconcile,
                })
                .await
                .map_err(|e| KubeletError::Internal(format!("pod update channel closed: {e}")))?;
        }
        Ok(())
    }
}

fn strip_node_suffix<'a>(name: &'a str, node_name: &str) -> &'a str {
    if node_name.is_empty() {
        return name;
    }
    let suffix = format!("-{node_name}");
    name.strip_suffix(&suffix).unwrap_or(name)
}

fn pod_names_equivalent(existing: &PodSpec, incoming: &PodSpec) -> bool {
    if existing.pod_ref.name == incoming.pod_ref.name {
        return true;
    }

    let existing_base = strip_node_suffix(&existing.pod_ref.name, &existing.node_name);
    let incoming_base = strip_node_suffix(&incoming.pod_ref.name, &incoming.node_name);
    existing_base == incoming_base
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pod::{ImagePullPolicy, RestartPolicy};
    use crate::types::{PodRef, PodUID};

    fn make_pod(uid: &str, name: &str) -> PodSpec {
        PodSpec {
            uid: PodUID::new(uid),
            pod_ref: PodRef::new("default", name),
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
        }
    }

    #[tokio::test]
    async fn test_upsert_and_get() {
        let (tx, _rx) = mpsc::channel(100);
        let manager = PodManager::new(tx);
        let pod = make_pod("uid-1", "pod-1");
        manager.upsert(pod.clone()).await.unwrap();

        let found = manager.get(&PodUID::new("uid-1"));
        assert!(found.is_some());
        assert_eq!(found.unwrap().pod_ref.name, "pod-1");
    }

    #[tokio::test]
    async fn test_remove_pod() {
        let (tx, _rx) = mpsc::channel(100);
        let manager = PodManager::new(tx);
        manager.upsert(make_pod("uid-2", "pod-2")).await.unwrap();
        manager.remove(&PodUID::new("uid-2"), None).await.unwrap();
        assert!(manager.get(&PodUID::new("uid-2")).is_none());
    }

    #[tokio::test]
    async fn test_list_all_pods() {
        let (tx, _rx) = mpsc::channel(100);
        let manager = PodManager::new(tx);
        for i in 0..5 {
            manager
                .upsert(make_pod(&format!("uid-{}", i), &format!("pod-{}", i)))
                .await
                .unwrap();
        }
        assert_eq!(manager.list().len(), 5);
        assert_eq!(manager.count(), 5);
    }

    #[tokio::test]
    async fn test_upsert_sends_add_then_update() {
        let (tx, mut rx) = mpsc::channel(100);
        let manager = PodManager::new(tx);
        let pod = make_pod("uid-3", "pod-3");

        manager.upsert(pod.clone()).await.unwrap();
        let update1 = rx.recv().await.unwrap();
        assert_eq!(update1.op, PodOperation::Add);

        manager.upsert(pod.clone()).await.unwrap();
        let update2 = rx.recv().await.unwrap();
        assert_eq!(update2.op, PodOperation::Update);
    }

    #[tokio::test]
    async fn test_reconcile_all_sends_reconcile_for_each_pod() {
        let (tx, mut rx) = mpsc::channel(100);
        let manager = PodManager::new(tx);
        for i in 0..3 {
            manager
                .upsert(make_pod(&format!("uid-r{}", i), &format!("pod-r{}", i)))
                .await
                .unwrap();
        }
        // drain add events
        for _ in 0..3 {
            rx.recv().await.unwrap();
        }

        manager.reconcile_all().await.unwrap();
        let mut reconcile_count = 0;
        for _ in 0..3 {
            let upd = rx.recv().await.unwrap();
            assert_eq!(upd.op, PodOperation::Reconcile);
            reconcile_count += 1;
        }
        assert_eq!(reconcile_count, 3);
    }

    #[tokio::test]
    async fn test_upsert_merges_host_aliases_for_suffixed_mirror_name() {
        let (tx, mut rx) = mpsc::channel(100);
        let manager = PodManager::new(tx);

        let mut mirror = make_pod("uid-mirror", "kube-vip-node1");
        mirror.pod_ref = PodRef::new("kube-system", "kube-vip-node1");
        mirror.node_name = "node1".to_string();
        manager.upsert(mirror).await.unwrap();
        let first = rx.recv().await.unwrap();
        assert_eq!(first.op, PodOperation::Add);

        let mut static_pod = make_pod("uid-static", "kube-vip");
        static_pod.pod_ref = PodRef::new("kube-system", "kube-vip");
        static_pod.node_name = "node1".to_string();
        static_pod.host_aliases = vec![crate::pod::HostAlias {
            ip: "127.0.0.1".to_string(),
            hostnames: vec!["kubernetes".to_string()],
        }];

        manager.upsert(static_pod).await.unwrap();
        let merged = rx.recv().await.unwrap();
        assert_eq!(merged.op, PodOperation::Update);
        assert_eq!(manager.count(), 1);

        let found = manager.get(&PodUID::new("uid-mirror")).unwrap();
        assert_eq!(found.host_aliases.len(), 1);
        assert_eq!(found.host_aliases[0].ip, "127.0.0.1");
        assert_eq!(found.host_aliases[0].hostnames, vec!["kubernetes"]);
    }

    #[tokio::test]
    async fn test_upsert_propagates_host_aliases_into_new_suffixed_entry() {
        let (tx, mut rx) = mpsc::channel(100);
        let manager = PodManager::new(tx);

        let mut static_pod = make_pod("uid-static", "kube-vip");
        static_pod.pod_ref = PodRef::new("kube-system", "kube-vip");
        static_pod.node_name = "node1".to_string();
        static_pod.host_aliases = vec![crate::pod::HostAlias {
            ip: "127.0.0.1".to_string(),
            hostnames: vec!["kubernetes".to_string()],
        }];
        manager.upsert(static_pod).await.unwrap();
        let first = rx.recv().await.unwrap();
        assert_eq!(first.op, PodOperation::Add);

        let mut mirror = make_pod("uid-mirror", "kube-vip-node1");
        mirror.pod_ref = PodRef::new("kube-system", "kube-vip-node1");
        mirror.node_name = "node1".to_string();
        manager.upsert(mirror).await.unwrap();
        let second = rx.recv().await.unwrap();
        assert_eq!(second.op, PodOperation::Add);
        assert_eq!(second.pod.host_aliases.len(), 1);
        assert_eq!(second.pod.host_aliases[0].ip, "127.0.0.1");
        assert_eq!(second.pod.host_aliases[0].hostnames, vec!["kubernetes"]);
    }

    #[tokio::test]
    async fn test_upsert_preserves_host_aliases_on_update() {
        // Simulate the production scenario: API-server mirror pod arrives first (no aliases),
        // file source pod patches it (returns early), then a periodic API re-sync arrives
        // (is_new=false, no aliases). The update path must preserve the merged aliases so
        // write_etc_hosts_file sees host_aliases_count > 0.
        let (tx, mut rx) = mpsc::channel(100);
        let manager = PodManager::new(tx);

        // Step 1: API-server mirror pod arrives with no aliases.
        let mut mirror = make_pod("uid-mirror", "kube-vip-node1");
        mirror.pod_ref = PodRef::new("kube-system", "kube-vip-node1");
        mirror.node_name = "node1".to_string();
        manager.upsert(mirror).await.unwrap();
        let ev1 = rx.recv().await.unwrap();
        assert_eq!(ev1.op, PodOperation::Add);
        assert_eq!(
            manager
                .get(&PodUID::new("uid-mirror"))
                .unwrap()
                .host_aliases
                .len(),
            0
        );

        // Step 2: File source pod (with aliases) patches the mirror pod.
        let mut static_pod = make_pod("uid-static", "kube-vip");
        static_pod.pod_ref = PodRef::new("kube-system", "kube-vip");
        static_pod.node_name = "node1".to_string();
        static_pod.host_aliases = vec![crate::pod::HostAlias {
            ip: "127.0.0.1".to_string(),
            hostnames: vec!["kubernetes".to_string()],
        }];
        manager.upsert(static_pod).await.unwrap();
        let ev2 = rx.recv().await.unwrap();
        assert_eq!(ev2.op, PodOperation::Update); // mirror pod was patched
        assert_eq!(
            manager
                .get(&PodUID::new("uid-mirror"))
                .unwrap()
                .host_aliases
                .len(),
            1
        );

        // Step 3: Periodic API re-sync arrives with the mirror pod spec (no aliases).
        // This is the bug scenario: is_new=false, incoming has empty host_aliases.
        // The fix must preserve the previously-merged aliases.
        let mut resync = make_pod("uid-mirror", "kube-vip-node1");
        resync.pod_ref = PodRef::new("kube-system", "kube-vip-node1");
        resync.node_name = "node1".to_string();
        // host_aliases intentionally empty — mirrors the API server mirror pod
        manager.upsert(resync).await.unwrap();
        let ev3 = rx.recv().await.unwrap();
        assert_eq!(ev3.op, PodOperation::Update);
        // Aliases must be preserved despite the incoming spec having none.
        assert_eq!(
            ev3.pod.host_aliases.len(),
            1,
            "host_aliases must be preserved on re-sync"
        );
        assert_eq!(ev3.pod.host_aliases[0].ip, "127.0.0.1");
        let stored = manager.get(&PodUID::new("uid-mirror")).unwrap();
        assert_eq!(
            stored.host_aliases.len(),
            1,
            "stored entry must retain aliases after re-sync"
        );

        // Step 4: File source re-polls (is_new=true for uid-static since it was never inserted).
        // It should return early without inserting a second worker entry.
        let mut static_pod2 = make_pod("uid-static", "kube-vip");
        static_pod2.pod_ref = PodRef::new("kube-system", "kube-vip");
        static_pod2.node_name = "node1".to_string();
        static_pod2.host_aliases = vec![crate::pod::HostAlias {
            ip: "127.0.0.1".to_string(),
            hostnames: vec!["kubernetes".to_string()],
        }];
        manager.upsert(static_pod2).await.unwrap();
        // No event should be emitted because aliases are unchanged (early return, no patch needed).
        assert!(
            rx.try_recv().is_err(),
            "no event expected when aliases unchanged on re-poll"
        );
        // Still only 1 pod in the desired map (uid-mirror), no dual insertion.
        assert_eq!(
            manager.count(),
            1,
            "only one pod entry should exist after re-poll"
        );
    }
}
