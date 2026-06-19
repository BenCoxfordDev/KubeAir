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

//! Active deadline controller.
//!
//! Watches for pods with `activeDeadlineSeconds` set and terminates them
//! if they exceed their deadline. Mirrors pkg/kubelet/active_deadline.go.

use chrono::{DateTime, Duration, Utc};
use kubelet_core::types::PodUID;
use std::collections::HashMap;

/// Tracks active deadline state for pods.
pub struct ActiveDeadlineController {
    /// Map of pod UID -> (start_time, deadline_seconds)
    tracking: HashMap<PodUID, (DateTime<Utc>, u64)>,
}

impl ActiveDeadlineController {
    pub fn new() -> Self {
        Self {
            tracking: HashMap::new(),
        }
    }

    /// Register a pod with an active deadline.
    pub fn register(&mut self, uid: PodUID, start_time: DateTime<Utc>, deadline_seconds: u64) {
        self.tracking.insert(uid, (start_time, deadline_seconds));
    }

    /// Deregister a pod (called when pod completes or is removed).
    pub fn deregister(&mut self, uid: &PodUID) {
        self.tracking.remove(uid);
    }

    /// Returns the set of pod UIDs that have exceeded their active deadline.
    pub fn expired_pods(&self) -> Vec<PodUID> {
        let now = Utc::now();
        self.tracking
            .iter()
            .filter(|(_, (start, deadline_secs))| {
                now > *start + Duration::seconds(*deadline_secs as i64)
            })
            .map(|(uid, _)| uid.clone())
            .collect()
    }

    /// Returns time remaining for a pod's deadline (None if not tracked or expired).
    pub fn time_remaining(&self, uid: &PodUID) -> Option<Duration> {
        self.tracking.get(uid).map(|(start, deadline_secs)| {
            let deadline = *start + Duration::seconds(*deadline_secs as i64);
            let remaining = deadline - Utc::now();
            if remaining < Duration::zero() {
                Duration::zero()
            } else {
                remaining
            }
        })
    }

    /// Number of pods being tracked.
    pub fn tracked_count(&self) -> usize {
        self.tracking.len()
    }
}

impl Default for ActiveDeadlineController {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::types::PodUID;

    #[test]
    fn test_register_and_track() {
        let mut ctrl = ActiveDeadlineController::new();
        ctrl.register(PodUID::new("uid-1"), Utc::now(), 60);
        assert_eq!(ctrl.tracked_count(), 1);
    }

    #[test]
    fn test_deregister_removes_pod() {
        let mut ctrl = ActiveDeadlineController::new();
        let uid = PodUID::new("uid-2");
        ctrl.register(uid.clone(), Utc::now(), 60);
        ctrl.deregister(&uid);
        assert_eq!(ctrl.tracked_count(), 0);
    }

    #[test]
    fn test_not_expired_for_future_deadline() {
        let mut ctrl = ActiveDeadlineController::new();
        ctrl.register(PodUID::new("uid-3"), Utc::now(), 3600); // 1 hour
        assert!(ctrl.expired_pods().is_empty());
    }

    #[test]
    fn test_expired_for_past_deadline() {
        let mut ctrl = ActiveDeadlineController::new();
        let uid = PodUID::new("uid-4");
        let past_start = Utc::now() - Duration::seconds(120);
        ctrl.register(uid.clone(), past_start, 60); // deadline was 60s after 120s ago
        let expired = ctrl.expired_pods();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], uid);
    }

    #[test]
    fn test_time_remaining_for_tracked_pod() {
        let mut ctrl = ActiveDeadlineController::new();
        ctrl.register(PodUID::new("uid-5"), Utc::now(), 3600);
        let remaining = ctrl.time_remaining(&PodUID::new("uid-5")).unwrap();
        assert!(remaining > Duration::seconds(3500));
        assert!(remaining <= Duration::seconds(3600));
    }

    #[test]
    fn test_time_remaining_zero_for_expired() {
        let mut ctrl = ActiveDeadlineController::new();
        let uid = PodUID::new("uid-6");
        ctrl.register(uid.clone(), Utc::now() - Duration::seconds(120), 60);
        let remaining = ctrl.time_remaining(&uid).unwrap();
        assert_eq!(remaining, Duration::zero());
    }

    #[test]
    fn test_time_remaining_none_for_untracked() {
        let ctrl = ActiveDeadlineController::new();
        assert!(ctrl.time_remaining(&PodUID::new("unknown")).is_none());
    }

    #[test]
    fn test_multiple_pods_only_expired_returned() {
        let mut ctrl = ActiveDeadlineController::new();
        ctrl.register(PodUID::new("alive"), Utc::now(), 3600);
        ctrl.register(PodUID::new("dead"), Utc::now() - Duration::seconds(200), 60);
        let expired = ctrl.expired_pods();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], PodUID::new("dead"));
    }
}
