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

//! Image garbage collection manager.
//!
//! Mirrors pkg/kubelet/images/image_gc_manager.go.
//!
//! Policy:
//! - If disk usage exceeds `high_threshold`, free images until below `low_threshold`.
//! - Images used by running pods are never GC'd (pinned set).
//! - Images are freed in order: oldest last-used first (LRU).
//! - Images newer than `min_age` are never GC'd.

use chrono::{DateTime, Duration, Utc};
use kubelet_core::container::ImageInfo;
use kubelet_core::error::{KubeletError, Result};
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};

/// A tracked image entry with usage metadata.
#[derive(Debug, Clone)]
pub struct TrackedImage {
    pub info: ImageInfo,
    /// When this image was last used to start a container.
    pub last_used: DateTime<Utc>,
    /// Whether the image is currently in use by a running container.
    pub in_use: bool,
}

/// Configuration for the image GC manager.
#[derive(Debug, Clone)]
pub struct ImageGcConfig {
    /// Disk usage % that triggers GC (0.0-1.0).
    pub high_threshold: f64,
    /// Disk usage % to reduce to after GC (0.0-1.0).
    pub low_threshold: f64,
    /// Minimum age before an image can be GC'd.
    pub min_age: Duration,
}

impl Default for ImageGcConfig {
    fn default() -> Self {
        Self {
            high_threshold: 0.85,
            low_threshold: 0.80,
            min_age: Duration::seconds(120),
        }
    }
}

/// Image GC manager: decides which images to delete.
pub struct ImageGcManager {
    config: ImageGcConfig,
    images: HashMap<String, TrackedImage>,
    pinned: HashSet<String>, // image IDs currently in use
}

impl ImageGcManager {
    pub fn new(config: ImageGcConfig) -> Self {
        Self {
            config,
            images: HashMap::new(),
            pinned: HashSet::new(),
        }
    }

    /// Record that an image was pulled / is available.
    pub fn track_image(&mut self, info: ImageInfo) {
        let id = info.id.clone();
        self.images.entry(id).or_insert_with(|| TrackedImage {
            info,
            last_used: Utc::now(),
            in_use: false,
        });
    }

    /// Record that a container started using an image.
    pub fn mark_in_use(&mut self, image_id: &str) {
        self.pinned.insert(image_id.to_string());
        if let Some(img) = self.images.get_mut(image_id) {
            img.last_used = Utc::now();
            img.in_use = true;
        }
    }

    /// Record that no container is using this image anymore.
    pub fn mark_not_in_use(&mut self, image_id: &str) {
        self.pinned.remove(image_id);
        if let Some(img) = self.images.get_mut(image_id) {
            img.in_use = false;
        }
    }

    /// Remove an image record (after actual deletion).
    pub fn remove_image(&mut self, image_id: &str) {
        self.images.remove(image_id);
        self.pinned.remove(image_id);
    }

    /// Sync the tracked set with what's actually on disk.
    pub fn sync(&mut self, available: Vec<ImageInfo>) {
        let available_ids: HashSet<String> = available.iter().map(|i| i.id.clone()).collect();
        // Remove images no longer present
        self.images.retain(|id, _| available_ids.contains(id));
        // Track new images
        for img in available {
            self.track_image(img);
        }
    }

    /// Calculate GC candidates given current disk usage.
    /// Returns a list of image IDs to delete, in deletion order (LRU first).
    pub fn gc_candidates(&self, disk_used_bytes: u64, disk_total_bytes: u64) -> GcPlan {
        let used_fraction = disk_used_bytes as f64 / disk_total_bytes as f64;

        if used_fraction <= self.config.high_threshold {
            return GcPlan {
                to_delete: vec![],
                bytes_to_free: 0,
            };
        }

        let target_usage = self.config.low_threshold * disk_total_bytes as f64;
        let current_usage = disk_used_bytes as f64;
        let bytes_to_free = (current_usage - target_usage).max(0.0) as u64;

        // Collect eligible images (not in use, old enough)
        let min_age = self.config.min_age;
        let now = Utc::now();
        let mut eligible: Vec<&TrackedImage> = self
            .images
            .values()
            .filter(|img| !self.pinned.contains(&img.info.id) && (now - img.last_used) > min_age)
            .collect();

        // Sort by last_used ascending (oldest first)
        eligible.sort_by_key(|img| img.last_used);

        let mut to_delete = Vec::new();
        let mut freed = 0u64;

        for img in eligible {
            if freed >= bytes_to_free {
                break;
            }
            freed += img.info.size_bytes;
            to_delete.push(img.info.id.clone());
        }

        GcPlan {
            to_delete,
            bytes_to_free,
        }
    }

    /// Total bytes consumed by tracked images.
    pub fn total_image_bytes(&self) -> u64 {
        self.images.values().map(|i| i.info.size_bytes).sum()
    }

    /// Number of tracked images.
    pub fn image_count(&self) -> usize {
        self.images.len()
    }

    /// Number of pinned (in-use) images.
    pub fn pinned_count(&self) -> usize {
        self.pinned.len()
    }
}

/// Plan returned by gc_candidates.
#[derive(Debug, Clone)]
pub struct GcPlan {
    pub to_delete: Vec<String>,
    pub bytes_to_free: u64,
}

impl GcPlan {
    pub fn is_empty(&self) -> bool {
        self.to_delete.is_empty()
    }
}

// -- helpers -------------------------------------------------------------------

fn make_image(id: &str, size_bytes: u64) -> ImageInfo {
    ImageInfo {
        id: id.to_string(),
        repo_tags: vec![format!("{}:latest", id)],
        repo_digests: vec![],
        size_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn default_mgr() -> ImageGcManager {
        ImageGcManager::new(ImageGcConfig {
            high_threshold: 0.85,
            low_threshold: 0.80,
            min_age: Duration::zero(), // no age restriction for tests
        })
    }

    #[test]
    fn test_track_and_count() {
        let mut mgr = default_mgr();
        mgr.track_image(make_image("img-a", 100_000_000));
        mgr.track_image(make_image("img-b", 200_000_000));
        assert_eq!(mgr.image_count(), 2);
        assert_eq!(mgr.total_image_bytes(), 300_000_000);
    }

    #[test]
    fn test_no_gc_below_high_threshold() {
        let mut mgr = default_mgr();
        mgr.track_image(make_image("img-a", 10_000_000));
        let plan = mgr.gc_candidates(80 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024); // 80%
        assert!(plan.is_empty());
    }

    #[test]
    fn test_gc_above_high_threshold() {
        let mut mgr = default_mgr();
        for i in 0..5 {
            let img = make_image(&format!("img-{}", i), 10 * 1024 * 1024 * 1024);
            mgr.track_image(img);
        }
        // 90Gi used of 100Gi = 90% -> above 85%
        let plan = mgr.gc_candidates(90 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024);
        assert!(!plan.is_empty());
        assert!(plan.bytes_to_free > 0);
    }

    #[test]
    fn test_pinned_images_not_gc_d() {
        let mut mgr = default_mgr();
        mgr.track_image(make_image("pinned", 10 * 1024 * 1024 * 1024));
        mgr.mark_in_use("pinned");

        let plan = mgr.gc_candidates(90 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024);
        assert!(!plan.to_delete.contains(&"pinned".to_string()));
    }

    #[test]
    fn test_oldest_image_deleted_first() {
        let mut mgr = ImageGcManager::new(ImageGcConfig {
            high_threshold: 0.85,
            low_threshold: 0.80,
            min_age: Duration::zero(),
        });

        mgr.track_image(make_image("old", 5 * 1024 * 1024 * 1024));
        mgr.track_image(make_image("new", 5 * 1024 * 1024 * 1024));

        // Make old image older
        mgr.images.get_mut("old").unwrap().last_used = Utc::now() - Duration::hours(24);

        let plan = mgr.gc_candidates(90 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024);
        assert!(!plan.is_empty());
        assert_eq!(plan.to_delete[0], "old");
    }

    #[test]
    fn test_mark_not_in_use_unpins() {
        let mut mgr = default_mgr();
        mgr.track_image(make_image("img", 5 * 1024 * 1024 * 1024));
        mgr.mark_in_use("img");
        assert_eq!(mgr.pinned_count(), 1);
        mgr.mark_not_in_use("img");
        assert_eq!(mgr.pinned_count(), 0);
    }

    #[test]
    fn test_sync_removes_deleted_images() {
        let mut mgr = default_mgr();
        mgr.track_image(make_image("img-a", 100));
        mgr.track_image(make_image("img-b", 200));
        // Sync with only img-a present
        mgr.sync(vec![make_image("img-a", 100)]);
        assert_eq!(mgr.image_count(), 1);
        assert!(!mgr.images.contains_key("img-b"));
    }

    #[test]
    fn test_sync_adds_new_images() {
        let mut mgr = default_mgr();
        mgr.sync(vec![make_image("new-img", 500)]);
        assert_eq!(mgr.image_count(), 1);
    }

    #[test]
    fn test_remove_image() {
        let mut mgr = default_mgr();
        mgr.track_image(make_image("del", 100));
        mgr.remove_image("del");
        assert_eq!(mgr.image_count(), 0);
    }

    #[test]
    fn test_gc_stops_at_low_threshold() {
        let mut mgr = ImageGcManager::new(ImageGcConfig {
            high_threshold: 0.80,
            low_threshold: 0.70,
            min_age: Duration::zero(),
        });

        // Add 10 x 5Gi images
        for i in 0..10 {
            let mut img = make_image(&format!("img-{}", i), 5 * 1024 * 1024 * 1024);
            mgr.track_image(img);
            // Make each progressively newer
            mgr.images.get_mut(&format!("img-{}", i)).unwrap().last_used =
                Utc::now() - Duration::hours((10 - i) as i64);
        }

        let total = 100 * 1024 * 1024 * 1024u64;
        let used = 85 * 1024 * 1024 * 1024u64; // 85% -> above 80% threshold

        let plan = mgr.gc_candidates(used, total);
        // Need to free 15Gi (85% -> 70%) = 3 x 5Gi images
        assert_eq!(plan.to_delete.len(), 3);
    }
}
