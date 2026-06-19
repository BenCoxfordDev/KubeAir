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

//! Image puller with exponential backoff and pull policy enforcement.
//!
//! Mirrors pkg/kubelet/images/image_manager.go.
//!
//! Pull policies:
//!   Always       -- always pull (even if present locally).
//!   IfNotPresent -- only pull if not already cached.
//!   Never        -- never pull; fail if missing.
//!
//! On pull failure, backs off exponentially: 10s, 20s, 40s … max 5m.
//! Per-image backoff state is tracked across pod sync cycles.

use kubelet_core::error::{KubeletError, Result};
use kubelet_core::pod::ImagePullPolicy;
use kubelet_ports::driven::container_runtime::{ImageManager, ImagePullSecret};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

// -- Backoff state -------------------------------------------------------------

const INITIAL_BACKOFF: Duration = Duration::from_secs(10);
const MAX_BACKOFF: Duration = Duration::from_secs(300); // 5 minutes

#[derive(Debug, Clone)]
struct ImageBackoff {
    next_retry: Instant,
    current_backoff: Duration,
    failure_count: u32,
}

impl ImageBackoff {
    fn new() -> Self {
        Self {
            next_retry: Instant::now(),
            current_backoff: INITIAL_BACKOFF,
            failure_count: 0,
        }
    }

    fn should_retry(&self) -> bool {
        Instant::now() >= self.next_retry
    }

    fn record_failure(&mut self) {
        self.failure_count += 1;
        self.next_retry = Instant::now() + self.current_backoff;
        self.current_backoff = std::cmp::min(self.current_backoff * 2, MAX_BACKOFF);
    }

    fn record_success(&mut self) {
        self.failure_count = 0;
        self.current_backoff = INITIAL_BACKOFF;
        self.next_retry = Instant::now();
    }
}

// -- Image puller --------------------------------------------------------------

pub struct ImagePuller {
    image_manager: Arc<dyn ImageManager>,
    backoffs: Mutex<HashMap<String, ImageBackoff>>,
}

impl ImagePuller {
    pub fn new(image_manager: Arc<dyn ImageManager>) -> Self {
        Self {
            image_manager,
            backoffs: Mutex::new(HashMap::new()),
        }
    }

    /// Ensure an image is available locally, respecting pull policy and backoff.
    pub async fn ensure_image(
        &self,
        image: &str,
        policy: &ImagePullPolicy,
        pull_secrets: Vec<ImagePullSecret>,
    ) -> Result<String> {
        match policy {
            ImagePullPolicy::Never => {
                // Check if present; fail if missing.
                match self.image_manager.image_status(image).await? {
                    Some(info) => {
                        debug!(image, "Image found locally (Never policy)");
                        Ok(info.id)
                    }
                    None => Err(KubeletError::Runtime(format!(
                        "image '{}' not found locally and pull policy is Never",
                        image
                    ))),
                }
            }

            ImagePullPolicy::IfNotPresent => {
                // Check cache first.
                match self.image_manager.image_status(image).await? {
                    Some(info) => {
                        debug!(image, "Image found in cache (IfNotPresent)");
                        Ok(info.id)
                    }
                    None => self.pull_with_backoff(image, pull_secrets).await,
                }
            }

            ImagePullPolicy::Always => self.pull_with_backoff(image, pull_secrets).await,
        }
    }

    async fn pull_with_backoff(
        &self,
        image: &str,
        pull_secrets: Vec<ImagePullSecret>,
    ) -> Result<String> {
        // Check backoff.
        {
            let backoffs = self.backoffs.lock().await;
            if let Some(bo) = backoffs.get(image) {
                if !bo.should_retry() {
                    let wait = bo.next_retry.duration_since(Instant::now());
                    return Err(KubeletError::Runtime(format!(
                        "image '{}' is in pull backoff (retry in {:?}, failures: {})",
                        image, wait, bo.failure_count
                    )));
                }
            }
        }

        info!(image, "Pulling image");
        match self.image_manager.pull_image(image, pull_secrets).await {
            Ok(image_ref) => {
                info!(image, image_ref = %image_ref, "Image pull succeeded");
                let mut backoffs = self.backoffs.lock().await;
                if let Some(bo) = backoffs.get_mut(image) {
                    bo.record_success();
                }
                Ok(image_ref)
            }
            Err(e) => {
                warn!(image, error = %e, "Image pull failed");
                let mut backoffs = self.backoffs.lock().await;
                backoffs
                    .entry(image.to_string())
                    .or_insert_with(ImageBackoff::new)
                    .record_failure();
                Err(e)
            }
        }
    }

    /// Clear backoff for an image (e.g. when pod is deleted).
    pub async fn clear_backoff(&self, image: &str) {
        self.backoffs.lock().await.remove(image);
    }

    /// Return the current failure count for an image.
    pub async fn failure_count(&self, image: &str) -> u32 {
        self.backoffs
            .lock()
            .await
            .get(image)
            .map(|b| b.failure_count)
            .unwrap_or(0)
    }

    pub async fn backoff_count(&self) -> usize {
        self.backoffs.lock().await.len()
    }
}

// -- Pod garbage collector -----------------------------------------------------

/// Removes terminated pods beyond the retention threshold.
/// Mirrors pkg/kubelet/pod_gc.go.
pub struct PodGarbageCollector {
    /// Max number of terminated pods to keep per node.
    max_terminated_pods: usize,
    /// Directory where pod data is stored.
    pods_dir: std::path::PathBuf,
}

impl PodGarbageCollector {
    pub fn new(pods_dir: impl Into<std::path::PathBuf>, max_terminated_pods: usize) -> Self {
        Self {
            max_terminated_pods,
            pods_dir: pods_dir.into(),
        }
    }

    /// Collect terminated pods beyond retention.
    /// `terminated_pods` = list of (pod_uid, termination_time) sorted newest first.
    pub fn collect(&self, terminated_pods: &[(String, std::time::SystemTime)]) -> Vec<String> {
        // Sort by termination time (oldest first).
        let mut sorted = terminated_pods.to_vec();
        sorted.sort_by_key(|(_, t)| *t);

        // Keep the newest `max_terminated_pods`; mark the rest for GC.
        let to_gc: Vec<String> = if sorted.len() > self.max_terminated_pods {
            sorted[..sorted.len() - self.max_terminated_pods]
                .iter()
                .map(|(uid, _)| uid.clone())
                .collect()
        } else {
            vec![]
        };
        to_gc
    }

    /// Remove the on-disk data for a pod.
    pub fn remove_pod_dir(&self, pod_uid: &str) -> Result<()> {
        let pod_dir = self.pods_dir.join(pod_uid);
        if pod_dir.exists() {
            std::fs::remove_dir_all(&pod_dir).map_err(|e| {
                KubeletError::Storage(format!("pod GC: remove dir for {}: {}", pod_uid, e))
            })?;
            info!(pod_uid, "Pod directory removed by GC");
        }
        Ok(())
    }
}

// -- Mirror pod manager --------------------------------------------------------

/// Creates and manages mirror pods in the API server for static pods.
/// Mirrors pkg/kubelet/config/config.go mirrorPodManager.
pub struct MirrorPodManager {
    node_name: String,
    client: Option<kube::Client>,
}

impl MirrorPodManager {
    pub async fn new(node_name: impl Into<String>) -> Self {
        let node_name = node_name.into();
        let client = kube::Client::try_default().await.ok();
        Self { node_name, client }
    }

    /// Create a mirror pod in the API server for a static pod.
    pub async fn create_mirror_pod(
        &self,
        static_pod_uid: &str,
        name: &str,
        namespace: &str,
        spec_json: serde_json::Value,
    ) -> Result<()> {
        let Some(client) = &self.client else {
            debug!(pod = %name, "Standalone: skip mirror pod creation");
            return Ok(());
        };

        let mirror_annotation = format!("{}/{}.{}", static_pod_uid, name, namespace);
        let pod_obj = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "annotations": {
                    "kubernetes.io/config.mirror": mirror_annotation,
                    "kubernetes.io/config.source": "file",
                    "kubernetes.io/config.hash": static_pod_uid
                },
                "ownerReferences": []
            },
            "spec": spec_json
        });

        let pods: kube::Api<k8s_openapi::api::core::v1::Pod> =
            kube::Api::namespaced(client.clone(), namespace);

        match pods
            .create(
                &kube::api::PostParams::default(),
                &serde_json::from_value(pod_obj).unwrap(),
            )
            .await
        {
            Ok(_) => {
                info!(pod = %name, ns = %namespace, "Mirror pod created");
                Ok(())
            }
            Err(kube::Error::Api(e)) if e.code == 409 => {
                // Already exists -- that's fine.
                debug!(pod = %name, "Mirror pod already exists");
                Ok(())
            }
            Err(e) => Err(KubeletError::Runtime(format!("create mirror pod: {}", e))),
        }
    }

    /// Delete the mirror pod when the static pod is removed.
    pub async fn delete_mirror_pod(&self, name: &str, namespace: &str) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        let pods: kube::Api<k8s_openapi::api::core::v1::Pod> =
            kube::Api::namespaced(client.clone(), namespace);

        match pods.delete(name, &kube::api::DeleteParams::default()).await {
            Ok(_) => {
                info!(pod = %name, "Mirror pod deleted");
                Ok(())
            }
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()), // already gone
            Err(e) => Err(KubeletError::Runtime(format!("delete mirror pod: {}", e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_runtime::MockRuntime;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    #[tokio::test]
    async fn test_image_puller_never_policy_missing_fails() {
        let runtime = Arc::new(MockRuntime::new());
        let puller = ImagePuller::new(runtime);
        let result = puller
            .ensure_image("nonexistent:latest", &ImagePullPolicy::Never, vec![])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_image_puller_always_pulls() {
        let runtime = Arc::new(MockRuntime::new());
        let puller = ImagePuller::new(runtime);
        let result = puller
            .ensure_image("nginx:latest", &ImagePullPolicy::Always, vec![])
            .await;
        // MockRuntime.pull_image succeeds.
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_backoff_increments_on_failure() {
        let runtime = Arc::new(MockRuntime::new_failing()); // always fails
        let puller = ImagePuller::new(runtime);
        let _ = puller
            .ensure_image("bad:image", &ImagePullPolicy::Always, vec![])
            .await;
        assert_eq!(puller.failure_count("bad:image").await, 1);
        // Second attempt should be in backoff.
        let result = puller
            .ensure_image("bad:image", &ImagePullPolicy::Always, vec![])
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_pod_gc_collects_oldest_beyond_threshold() {
        let dir = tempfile::TempDir::new().unwrap();
        let gc = PodGarbageCollector::new(dir.path(), 2);
        let now = SystemTime::now();
        let terminated = vec![
            ("uid-1".to_string(), now - Duration::from_secs(300)),
            ("uid-2".to_string(), now - Duration::from_secs(200)),
            ("uid-3".to_string(), now - Duration::from_secs(100)),
            ("uid-4".to_string(), now - Duration::from_secs(50)),
        ];
        let to_gc = gc.collect(&terminated);
        assert_eq!(to_gc.len(), 2);
        assert!(to_gc.contains(&"uid-1".to_string()));
        assert!(to_gc.contains(&"uid-2".to_string()));
    }

    #[test]
    fn test_pod_gc_within_threshold_nothing_collected() {
        let dir = tempfile::TempDir::new().unwrap();
        let gc = PodGarbageCollector::new(dir.path(), 10);
        let now = SystemTime::now();
        let terminated = vec![("uid-1".to_string(), now), ("uid-2".to_string(), now)];
        assert!(gc.collect(&terminated).is_empty());
    }

    #[test]
    fn test_pod_gc_remove_dir() {
        let dir = tempfile::TempDir::new().unwrap();
        let pod_dir = dir.path().join("uid-test");
        std::fs::create_dir_all(&pod_dir).unwrap();
        let gc = PodGarbageCollector::new(dir.path(), 0);
        gc.remove_pod_dir("uid-test").unwrap();
        assert!(!pod_dir.exists());
    }
}
