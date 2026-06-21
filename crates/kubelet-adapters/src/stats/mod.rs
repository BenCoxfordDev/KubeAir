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

//! cgroup-based container and node statistics.
//!
//! Reads real CPU and memory metrics from cgroup v2 for use in:
//!   - /stats/summary (metrics-server)
//!   - Prometheus metrics
//!   - Eviction decisions
//!
//! cgroup v2 paths:
//!   cpu: /sys/fs/cgroup/kubepods.slice/.../cpu.stat
//!   memory: /sys/fs/cgroup/kubepods.slice/.../memory.current
//!   disk: statfs() on mount points
//!
//! Mirrors pkg/kubelet/stats/cri_stats_provider.go.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tracing::{debug, warn};

// -- Container stats -----------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContainerStatSnapshot {
    pub timestamp: Option<SystemTime>,
    /// CPU usage in nanocores (instantaneous rate).
    pub cpu_usage_nano_cores: u64,
    /// Cumulative CPU usage in nanoseconds.
    pub cpu_usage_core_nano_seconds: u64,
    /// Working set memory bytes (non-reclaimable).
    pub memory_working_set_bytes: u64,
    /// RSS memory bytes.
    pub memory_rss_bytes: u64,
    /// Total memory usage bytes.
    pub memory_usage_bytes: u64,
}

/// Pod-level aggregate stats.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PodStatSnapshot {
    pub pod_uid: String,
    pub pod_name: String,
    pub pod_namespace: String,
    pub timestamp: Option<SystemTime>,
    pub containers: Vec<(String, ContainerStatSnapshot)>,
    /// Network: bytes received / transmitted.
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
    pub ephemeral_storage_used_bytes: u64,
    pub overhead_cpu_millicores: u64,
    pub overhead_memory_bytes: u64,
}

/// Node-level stats.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeStatSnapshot {
    pub timestamp: Option<SystemTime>,
    pub cpu_usage_nano_cores: u64,
    pub cpu_usage_core_nano_seconds: u64,
    pub memory_available_bytes: u64,
    pub memory_usage_bytes: u64,
    pub memory_working_set_bytes: u64,
    pub memory_rss_bytes: u64,
    pub fs_available_bytes: u64,
    pub fs_used_bytes: u64,
    pub fs_capacity_bytes: u64,
    pub fs_inodes_free: u64,
    pub fs_inodes_used: u64,
    pub runtime_image_fs_available_bytes: u64,
    pub runtime_image_fs_used_bytes: u64,
}

// -- cgroup v2 reader ----------------------------------------------------------

pub struct CgroupStatsReader {
    cgroup_root: PathBuf,
}

impl CgroupStatsReader {
    pub fn new(cgroup_root: impl Into<PathBuf>) -> Self {
        Self {
            cgroup_root: cgroup_root.into(),
        }
    }

    /// Read container stats from cgroup v2.
    pub fn read_container_stats(
        &self,
        pod_qos: &str, // "guaranteed" | "burstable" | "besteffort"
        pod_uid: &str,
        container_id: &str,
    ) -> ContainerStatSnapshot {
        let pod_slice = format!(
            "kubepods.slice/kubepods-{}.slice/kubepods-{}-pod{}.slice",
            pod_qos,
            pod_qos,
            pod_uid.replace('-', "_")
        );
        let ctr_scope = format!("cri-containerd-{}.scope", container_id);
        let cgroup_path = self.cgroup_root.join(&pod_slice).join(&ctr_scope);

        let cpu = self.read_cpu_stat(&cgroup_path);
        let mem = self.read_memory_stat(&cgroup_path);

        ContainerStatSnapshot {
            timestamp: Some(SystemTime::now()),
            cpu_usage_nano_cores: cpu.0,
            cpu_usage_core_nano_seconds: cpu.1,
            memory_working_set_bytes: mem.0,
            memory_rss_bytes: mem.1,
            memory_usage_bytes: mem.2,
        }
    }

    /// Read node-level stats.
    pub fn read_node_stats(&self, root_fs_path: &str, image_fs_path: &str) -> NodeStatSnapshot {
        // CPU from /proc/stat.
        let (cpu_nano_cores, cpu_cum_ns) = self.read_proc_cpu();
        // Memory from /proc/meminfo.
        let (mem_avail, mem_total, mem_rss) = self.read_proc_meminfo();
        // Disk from statfs.
        let (fs_avail, fs_used, fs_cap, inodes_free, inodes_used) = self.read_disk(root_fs_path);
        let (img_avail, img_used, _, _, _) = self.read_disk(image_fs_path);

        NodeStatSnapshot {
            timestamp: Some(SystemTime::now()),
            cpu_usage_nano_cores: cpu_nano_cores,
            cpu_usage_core_nano_seconds: cpu_cum_ns,
            memory_available_bytes: mem_avail,
            memory_usage_bytes: mem_total.saturating_sub(mem_avail),
            memory_working_set_bytes: mem_total.saturating_sub(mem_avail),
            memory_rss_bytes: mem_rss,
            fs_available_bytes: fs_avail,
            fs_used_bytes: fs_used,
            fs_capacity_bytes: fs_cap,
            fs_inodes_free: inodes_free,
            fs_inodes_used: inodes_used,
            runtime_image_fs_available_bytes: img_avail,
            runtime_image_fs_used_bytes: img_used,
        }
    }

    fn read_cpu_stat(&self, cgroup_path: &Path) -> (u64, u64) {
        let content = match std::fs::read_to_string(cgroup_path.join("cpu.stat")) {
            Ok(c) => c,
            Err(_) => return (0, 0),
        };
        let mut usage_usec: u64 = 0;
        for line in content.lines() {
            if line.starts_with("usage_usec ") {
                usage_usec = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
            }
        }
        let cumulative_ns = usage_usec * 1000; // µs -> ns
        // Instantaneous rate: we'd need two samples; return 0 for now (needs sampling loop).
        (0, cumulative_ns)
    }

    fn read_memory_stat(&self, cgroup_path: &Path) -> (u64, u64, u64) {
        let current = std::fs::read_to_string(cgroup_path.join("memory.current"))
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);

        let swap = std::fs::read_to_string(cgroup_path.join("memory.swap.current"))
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);

        // Parse memory.stat for rss and inactive_file.
        let content = std::fs::read_to_string(cgroup_path.join("memory.stat")).unwrap_or_default();
        let mut rss: u64 = 0;
        let mut inactive_file: u64 = 0;
        for line in content.lines() {
            let mut parts = line.split_whitespace();
            match parts.next() {
                Some("anon") => rss = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0),
                Some("inactive_file") => {
                    inactive_file = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0)
                }
                _ => {}
            }
        }
        let working_set = current.saturating_sub(inactive_file);
        (working_set, rss, current)
    }

    fn read_proc_cpu(&self) -> (u64, u64) {
        // Read /proc/stat line "cpu  user nice system idle ..."
        let content = std::fs::read_to_string("/proc/stat").unwrap_or_default();
        let cpu_line = content
            .lines()
            .find(|l| l.starts_with("cpu "))
            .unwrap_or("");
        let values: Vec<u64> = cpu_line
            .split_whitespace()
            .skip(1)
            .filter_map(|v| v.parse().ok())
            .collect();
        // user + nice + system + irq + softirq (in jiffies = 10ms units)
        let total_jiffies: u64 = values.iter().sum();
        let user_hz = 100u64; // HZ=100 on most Linux
        let cumulative_ns = total_jiffies * 1_000_000_000 / user_hz;
        (0, cumulative_ns) // instantaneous requires two samples
    }

    fn read_proc_meminfo(&self) -> (u64, u64, u64) {
        let content = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
        let mut mem_total: u64 = 0;
        let mut mem_available: u64 = 0;
        let mut mem_rss: u64 = 0;
        for line in content.lines() {
            let mut parts = line.split_whitespace();
            match parts.next() {
                Some("MemTotal:") => {
                    mem_total = parts
                        .next()
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0)
                        * 1024
                }
                Some("MemAvailable:") => {
                    mem_available = parts
                        .next()
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0)
                        * 1024
                }
                Some("Active(anon):") => {
                    mem_rss = parts
                        .next()
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0)
                        * 1024
                }
                _ => {}
            }
        }
        (mem_available, mem_total, mem_rss)
    }

    fn read_disk(&self, path: &str) -> (u64, u64, u64, u64, u64) {
        #[cfg(unix)]
        {
            use std::mem::MaybeUninit;
            let c_path = std::ffi::CString::new(path).unwrap_or_default();
            let mut stat: MaybeUninit<libc::statvfs> = MaybeUninit::uninit();
            let ret = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
            if ret == 0 {
                let stat = unsafe { stat.assume_init() };
                #[cfg(target_os = "linux")]
                let (available, used, total, inodes_free, inodes_used) = {
                    let block_size = stat.f_frsize;
                    let total = stat.f_blocks * block_size;
                    let available = stat.f_bavail * block_size;
                    let used = total.saturating_sub(stat.f_bfree * block_size);
                    let inodes_total = stat.f_files;
                    let inodes_free = stat.f_ffree;
                    let inodes_used = inodes_total.saturating_sub(inodes_free);
                    (available, used, total, inodes_free, inodes_used)
                };
                #[cfg(not(target_os = "linux"))]
                let (available, used, total, inodes_free, inodes_used) = {
                    let block_size = stat.f_frsize;
                    let total = u64::from(stat.f_blocks) * block_size;
                    let available = u64::from(stat.f_bavail) * block_size;
                    let used = total.saturating_sub(u64::from(stat.f_bfree) * block_size);
                    let inodes_total = u64::from(stat.f_files);
                    let inodes_free = u64::from(stat.f_ffree);
                    let inodes_used = inodes_total.saturating_sub(inodes_free);
                    (available, used, total, inodes_free, inodes_used)
                };
                return (available, used, total, inodes_free, inodes_used);
            }
        }
        (0, 0, 0, 0, 0)
    }
}

// -- Disk pressure evaluator ---------------------------------------------------

/// Check if a filesystem is under pressure based on eviction thresholds.
pub fn disk_pressure(fs_path: &str, available_threshold_percent: f64) -> bool {
    #[cfg(unix)]
    {
        use std::mem::MaybeUninit;
        let c_path = std::ffi::CString::new(fs_path).ok();
        if let Some(c_path) = c_path {
            let mut stat: MaybeUninit<libc::statvfs> = MaybeUninit::uninit();
            let ret = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
            if ret == 0 {
                let stat = unsafe { stat.assume_init() };
                if stat.f_blocks > 0 {
                    let avail_pct = stat.f_bavail as f64 / stat.f_blocks as f64 * 100.0;
                    return avail_pct < available_threshold_percent;
                }
            }
        }
    }
    false
}

/// Check if memory is under pressure.
pub fn memory_pressure(available_threshold_bytes: u64) -> bool {
    let content = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    for line in content.lines() {
        if line.starts_with("MemAvailable:")
            && let Some(kb) = line
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok())
        {
            return kb * 1024 < available_threshold_bytes;
        }
    }
    false
}

/// Build a /stats/summary-compatible JSON response.
pub fn build_stats_summary(
    node_name: &str,
    node_stats: &NodeStatSnapshot,
    pod_stats: &[PodStatSnapshot],
) -> serde_json::Value {
    let ts = node_stats
        .timestamp
        .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|d| chrono::DateTime::<chrono::Utc>::from(SystemTime::UNIX_EPOCH + d).to_rfc3339())
        .unwrap_or_default();

    let pod_entries: Vec<serde_json::Value> = pod_stats
        .iter()
        .map(|ps| {
            let containers: Vec<serde_json::Value> = ps
                .containers
                .iter()
                .map(|(name, cs)| {
                    serde_json::json!({
                        "name": name,
                        "startTime": ts,
                        "cpu": {
                            "time": ts,
                            "usageNanoCores": cs.cpu_usage_nano_cores,
                            "usageCoreNanoSeconds": cs.cpu_usage_core_nano_seconds
                        },
                        "memory": {
                            "time": ts,
                            "usageBytes": cs.memory_usage_bytes,
                            "workingSetBytes": cs.memory_working_set_bytes,
                            "rssBytes": cs.memory_rss_bytes
                        }
                    })
                })
                .collect();

            serde_json::json!({
                "podRef": {
                    "name": ps.pod_name,
                    "namespace": ps.pod_namespace,
                    "uid": ps.pod_uid
                },
                "startTime": ts,
                "containers": containers,
                "network": {
                    "time": ts,
                    "rxBytes": ps.network_rx_bytes,
                    "txBytes": ps.network_tx_bytes
                },
                "volume": [],
                "ephemeralStorage": {
                    "time": ts,
                    "usedBytes": ps.ephemeral_storage_used_bytes
                },
                "podOverhead": {
                    "time": ts,
                    "cpuMillicores": ps.overhead_cpu_millicores,
                    "memoryBytes": ps.overhead_memory_bytes
                }
            })
        })
        .collect();

    serde_json::json!({
        "node": {
            "nodeName": node_name,
            "startTime": ts,
            "cpu": {
                "time": ts,
                "usageNanoCores": node_stats.cpu_usage_nano_cores,
                "usageCoreNanoSeconds": node_stats.cpu_usage_core_nano_seconds
            },
            "memory": {
                "time": ts,
                "availableBytes": node_stats.memory_available_bytes,
                "usageBytes": node_stats.memory_usage_bytes,
                "workingSetBytes": node_stats.memory_working_set_bytes,
                "rssBytes": node_stats.memory_rss_bytes
            },
            "fs": {
                "time": ts,
                "availableBytes": node_stats.fs_available_bytes,
                "usedBytes": node_stats.fs_used_bytes,
                "capacityBytes": node_stats.fs_capacity_bytes,
                "inodesUsed": node_stats.fs_inodes_used,
                "inodesFree": node_stats.fs_inodes_free
            },
            "runtime": {
                "imageFs": {
                    "time": ts,
                    "availableBytes": node_stats.runtime_image_fs_available_bytes,
                    "usedBytes": node_stats.runtime_image_fs_used_bytes
                }
            },
            "rlimit": {
                "time": ts,
                "maxpid": 32768,
                "curproc": 0
            },
            "systemContainers": []
        },
        "pods": pod_entries
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_stats_summary_structure() {
        let node = NodeStatSnapshot {
            timestamp: Some(SystemTime::now()),
            cpu_usage_nano_cores: 100_000,
            cpu_usage_core_nano_seconds: 1_000_000_000,
            memory_available_bytes: 4 * 1024 * 1024 * 1024,
            memory_usage_bytes: 2 * 1024 * 1024 * 1024,
            memory_working_set_bytes: 1024 * 1024 * 1024,
            memory_rss_bytes: 512 * 1024 * 1024,
            fs_available_bytes: 50 * 1024 * 1024 * 1024,
            fs_used_bytes: 10 * 1024 * 1024 * 1024,
            fs_capacity_bytes: 100 * 1024 * 1024 * 1024,
            fs_inodes_free: 1_000_000,
            fs_inodes_used: 100_000,
            runtime_image_fs_available_bytes: 20 * 1024 * 1024 * 1024,
            runtime_image_fs_used_bytes: 5 * 1024 * 1024 * 1024,
        };
        let summary = build_stats_summary("node1", &node, &[]);
        assert_eq!(summary["node"]["nodeName"], "node1");
        assert!(
            summary["node"]["cpu"]["usageCoreNanoSeconds"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(summary["pods"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_build_stats_summary_with_pods() {
        let node = NodeStatSnapshot::default();
        let pods = vec![PodStatSnapshot {
            pod_uid: "uid-1".to_string(),
            pod_name: "nginx".to_string(),
            pod_namespace: "default".to_string(),
            containers: vec![(
                "nginx".to_string(),
                ContainerStatSnapshot {
                    memory_working_set_bytes: 10 * 1024 * 1024,
                    ..Default::default()
                },
            )],
            ..Default::default()
        }];
        let summary = build_stats_summary("node1", &node, &pods);
        assert_eq!(summary["pods"].as_array().unwrap().len(), 1);
        let pod = &summary["pods"][0];
        assert_eq!(pod["podRef"]["name"], "nginx");
        assert_eq!(
            pod["containers"][0]["memory"]["workingSetBytes"],
            10 * 1024 * 1024
        );
        assert_eq!(pod["podOverhead"]["memoryBytes"], 0);
    }

    #[test]
    fn test_memory_pressure_reads_proc_meminfo() {
        // Should not panic on any system.
        let _under_pressure = memory_pressure(u64::MAX);
    }

    #[test]
    fn test_cgroup_reader_graceful_on_missing_paths() {
        let reader = CgroupStatsReader::new("/nonexistent/cgroup");
        let stats = reader.read_container_stats("besteffort", "uid-1", "container-abc");
        assert_eq!(stats.cpu_usage_nano_cores, 0);
        assert_eq!(stats.memory_usage_bytes, 0);
    }
}
