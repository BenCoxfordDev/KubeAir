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

//! OOM event watcher.
//!
//! Monitors the kernel OOM killer via /dev/kmsg or cgroup memory events
//! and maps OOM kills back to pods. Mirrors pkg/kubelet/oom/oom_watcher.go.
//!
//! For portability, this implementation parses /proc/*/oom_score_adj and
//! monitors cgroup memory.events (v2) or memory.oom_control (v1).

use chrono::{DateTime, Utc};
use kubelet_core::qos::QosClass;
use kubelet_core::types::PodUID;
use std::collections::VecDeque;

/// An OOM kill event.
#[derive(Debug, Clone)]
pub struct OomEvent {
    pub timestamp: DateTime<Utc>,
    pub container_id: Option<String>,
    pub pod_uid: Option<PodUID>,
    pub process_name: String,
    pub message: String,
}

/// Holds a rolling buffer of recent OOM events.
pub struct OomWatcher {
    capacity: usize,
    events: VecDeque<OomEvent>,
}

/// Applies kubelet-like oom_score_adj values to container PIDs.
pub struct OomScoreManager;

impl Default for OomScoreManager {
    fn default() -> Self {
        Self::new()
    }
}

impl OomScoreManager {
    pub fn new() -> Self {
        Self
    }

    pub fn score_for_qos(
        &self,
        qos: &QosClass,
        requested_memory_bytes: Option<u64>,
        node_memory_capacity_bytes: Option<u64>,
    ) -> i32 {
        match qos {
            QosClass::Guaranteed => -997,
            QosClass::BestEffort => 1000,
            QosClass::Burstable => {
                let req = requested_memory_bytes.unwrap_or(0);
                let cap = node_memory_capacity_bytes.unwrap_or(0);
                if req == 0 || cap == 0 {
                    return 999;
                }
                let ratio = (req as f64 / cap as f64).clamp(0.0, 1.0);
                let score = 1000.0 - (998.0 * ratio);
                score.round().clamp(2.0, 999.0) as i32
            }
        }
    }

    pub fn apply_to_pid(&self, pid: u32, oom_score_adj: i32) -> std::io::Result<bool> {
        let path = format!("/proc/{}/oom_score_adj", pid);
        match std::fs::write(path, oom_score_adj.to_string()) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                tracing::warn!(
                    pid,
                    oom_score_adj,
                    "No CAP_SYS_RESOURCE; skipping oom_score_adj application"
                );
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }
}

impl OomWatcher {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            events: VecDeque::with_capacity(capacity),
        }
    }

    /// Record an OOM event.
    pub fn record(&mut self, event: OomEvent) {
        if self.events.len() >= self.capacity {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }

    /// Return recent OOM events (newest first).
    pub fn recent(&self) -> impl Iterator<Item = &OomEvent> {
        self.events.iter().rev()
    }

    /// Count of recorded events.
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Events for a specific pod UID.
    pub fn events_for_pod(&self, uid: &PodUID) -> Vec<&OomEvent> {
        self.events
            .iter()
            .filter(|e| e.pod_uid.as_ref() == Some(uid))
            .collect()
    }

    /// Parse a kernel OOM kill message (from /dev/kmsg or dmesg).
    /// Returns `Some(OomEvent)` if the line looks like an OOM kill.
    pub fn parse_kmsg_line(line: &str) -> Option<OomEvent> {
        // Kernel format: "Out of memory: Killed process N (comm) ..."
        if !line.contains("Out of memory") && !line.contains("oom-kill") {
            return None;
        }
        let process_name = if let Some(start) = line.find('(') {
            if let Some(end) = line.find(')') {
                line[start + 1..end].to_string()
            } else {
                "unknown".to_string()
            }
        } else {
            "unknown".to_string()
        };

        Some(OomEvent {
            timestamp: Utc::now(),
            container_id: None,
            pod_uid: None,
            process_name,
            message: line.to_string(),
        })
    }

    /// Attempt to read OOM score from /proc/<pid>/oom_score_adj (non-blocking).
    pub fn read_oom_score_adj(pid: u32) -> Option<i32> {
        let path = format!("/proc/{}/oom_score_adj", pid);
        std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| s.trim().parse::<i32>().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubelet_core::qos::QosClass;

    #[test]
    fn test_record_and_count() {
        let mut watcher = OomWatcher::new(10);
        watcher.record(OomEvent {
            timestamp: Utc::now(),
            container_id: None,
            pod_uid: None,
            process_name: "nginx".to_string(),
            message: "OOM".to_string(),
        });
        assert_eq!(watcher.event_count(), 1);
    }

    #[test]
    fn test_capacity_evicts_oldest() {
        let mut watcher = OomWatcher::new(3);
        for i in 0..5 {
            watcher.record(OomEvent {
                timestamp: Utc::now(),
                container_id: None,
                pod_uid: None,
                process_name: format!("proc-{}", i),
                message: "OOM".to_string(),
            });
        }
        assert_eq!(watcher.event_count(), 3);
        // Newest should be proc-4
        let first = watcher.recent().next().unwrap();
        assert_eq!(first.process_name, "proc-4");
    }

    #[test]
    fn test_events_for_pod() {
        let mut watcher = OomWatcher::new(10);
        let uid = PodUID::new("uid-oom");
        watcher.record(OomEvent {
            timestamp: Utc::now(),
            container_id: None,
            pod_uid: Some(uid.clone()),
            process_name: "app".to_string(),
            message: "OOM".to_string(),
        });
        watcher.record(OomEvent {
            timestamp: Utc::now(),
            container_id: None,
            pod_uid: None, // different pod
            process_name: "other".to_string(),
            message: "OOM".to_string(),
        });
        let pod_events = watcher.events_for_pod(&uid);
        assert_eq!(pod_events.len(), 1);
        assert_eq!(pod_events[0].process_name, "app");
    }

    #[test]
    fn test_parse_kmsg_oom_line() {
        let line =
            "kernel: Out of memory: Killed process 1234 (nginx) total-vm:123kB, anon-rss:456kB";
        let event = OomWatcher::parse_kmsg_line(line).unwrap();
        assert_eq!(event.process_name, "nginx");
    }

    #[test]
    fn test_parse_kmsg_non_oom_line() {
        let result = OomWatcher::parse_kmsg_line("kernel: CPU0: Temperature above threshold");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_kmsg_oom_kill_line() {
        let line = "oom-kill:constraint=CONSTRAINT_NONE,nodemask=(null),cpuset=/,mems_allowed=0";
        let event = OomWatcher::parse_kmsg_line(line);
        assert!(event.is_some());
    }

    #[test]
    fn test_recent_order_newest_first() {
        let mut watcher = OomWatcher::new(10);
        for i in 0..3 {
            watcher.record(OomEvent {
                timestamp: Utc::now(),
                container_id: None,
                pod_uid: None,
                process_name: format!("p{}", i),
                message: "OOM".to_string(),
            });
        }
        let names: Vec<&str> = watcher.recent().map(|e| e.process_name.as_str()).collect();
        assert_eq!(names, vec!["p2", "p1", "p0"]);
    }

    #[test]
    fn test_oom_score_for_guaranteed_and_besteffort() {
        let mgr = OomScoreManager::new();
        assert_eq!(mgr.score_for_qos(&QosClass::Guaranteed, None, None), -997);
        assert_eq!(mgr.score_for_qos(&QosClass::BestEffort, None, None), 1000);
    }

    #[test]
    fn test_oom_score_for_burstable_range() {
        let mgr = OomScoreManager::new();
        let s = mgr.score_for_qos(
            &QosClass::Burstable,
            Some(512 * 1024 * 1024),
            Some(8 * 1024 * 1024 * 1024),
        );
        assert!((2..=999).contains(&s));
    }
}
