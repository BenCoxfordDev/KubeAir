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

//! Real eviction pressure signals using statfs + /proc/meminfo.
//!
//! Mirrors pkg/kubelet/eviction/eviction_manager.go signal collection.
//!
//! Signals evaluated:
//!   memory.available     -- /proc/meminfo MemAvailable
//!   nodefs.available     -- statfs on /var/lib/kubelet (or rootfs)
//!   nodefs.inodesFree    -- statfs inodes on rootfs
//!   imagefs.available    -- statfs on container image filesystem
//!   pid.available        -- /proc/sys/kernel/pid_max - running PIDs

use std::path::Path;
use tracing::debug;

/// Current resource signal values for the node.
#[derive(Debug, Clone, Default)]
pub struct ResourceSignals {
    /// MemAvailable from /proc/meminfo in bytes.
    pub memory_available_bytes: u64,
    /// Root filesystem available bytes (for nodefs.available).
    pub nodefs_available_bytes: u64,
    /// Root filesystem available inodes (for nodefs.inodesFree).
    pub nodefs_inodes_free: u64,
    /// Image filesystem available bytes (for imagefs.available).
    pub imagefs_available_bytes: u64,
    /// PID count headroom (pid_max - current pids).
    pub pid_available: u64,
    /// Total root filesystem bytes.
    pub nodefs_total_bytes: u64,
    /// Total image filesystem bytes.
    pub imagefs_total_bytes: u64,
}

/// Collect current resource signals from the system.
pub fn collect_signals(rootfs_path: &str, imagefs_path: &str) -> ResourceSignals {
    let memory_available = read_mem_available();
    let (nodefs_avail, nodefs_inodes, nodefs_total) = read_statfs(rootfs_path);
    let (imagefs_avail, _, imagefs_total) = read_statfs(imagefs_path);
    let pid_avail = read_pid_available();

    let signals = ResourceSignals {
        memory_available_bytes: memory_available,
        nodefs_available_bytes: nodefs_avail,
        nodefs_inodes_free: nodefs_inodes,
        imagefs_available_bytes: imagefs_avail,
        pid_available: pid_avail,
        nodefs_total_bytes: nodefs_total,
        imagefs_total_bytes: imagefs_total,
    };

    debug!(
        memory_available_mb = signals.memory_available_bytes / 1024 / 1024,
        nodefs_available_gb = signals.nodefs_available_bytes / 1024 / 1024 / 1024,
        "Resource signals collected"
    );

    signals
}

fn read_mem_available() -> u64 {
    let content = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    for line in content.lines() {
        if line.starts_with("MemAvailable:") {
            return line
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok())
                .map(|kb| kb * 1024)
                .unwrap_or(0);
        }
    }
    0
}

fn read_statfs(path: &str) -> (u64, u64, u64) {
    #[cfg(unix)]
    {
        use std::mem::MaybeUninit;
        let c_path = std::ffi::CString::new(path).unwrap_or_default();
        let mut stat: MaybeUninit<libc::statvfs> = MaybeUninit::uninit();
        let ret = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
        if ret == 0 {
            let stat = unsafe { stat.assume_init() };
            #[cfg(target_os = "linux")]
            let (avail, inodes_free, total) = {
                let bs = stat.f_frsize;
                (stat.f_bavail * bs, stat.f_ffree, stat.f_blocks * bs)
            };
            #[cfg(not(target_os = "linux"))]
            let (avail, inodes_free, total) = {
                let bs = stat.f_frsize;
                (
                    u64::from(stat.f_bavail) * bs,
                    u64::from(stat.f_ffree),
                    u64::from(stat.f_blocks) * bs,
                )
            };
            return (avail, inodes_free, total);
        }
    }
    (0, 0, 0)
}

fn read_pid_available() -> u64 {
    let pid_max = std::fs::read_to_string("/proc/sys/kernel/pid_max")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(32768);

    let current_pids = std::fs::read_dir("/proc")
        .ok()
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().parse::<u64>().is_ok())
                .count() as u64
        })
        .unwrap_or(0);

    pid_max.saturating_sub(current_pids)
}

/// Evaluate whether a threshold is exceeded given current signals.
///
/// `threshold` is in the same format as evictionHard:
///   "100Mi"  -> absolute bytes
///   "10%"    -> percentage of total capacity
pub fn threshold_exceeded(signal_value: u64, total: u64, threshold: &str) -> bool {
    if let Some(pct_str) = threshold.strip_suffix('%') {
        let pct: f64 = pct_str.parse().unwrap_or(0.0);
        if total == 0 {
            return false;
        }
        let threshold_bytes = (pct / 100.0 * total as f64) as u64;
        signal_value < threshold_bytes
    } else {
        let threshold_bytes = parse_bytes(threshold);
        signal_value < threshold_bytes
    }
}

fn parse_bytes(s: &str) -> u64 {
    if let Some(n) = s.strip_suffix("Ki") {
        return n.parse::<u64>().unwrap_or(0) * 1024;
    }
    if let Some(n) = s.strip_suffix("Mi") {
        return n.parse::<u64>().unwrap_or(0) * 1024 * 1024;
    }
    if let Some(n) = s.strip_suffix("Gi") {
        return n.parse::<u64>().unwrap_or(0) * 1024 * 1024 * 1024;
    }
    s.parse().unwrap_or(0)
}

/// Determine which eviction signals are exceeded.
#[derive(Debug, Clone, Default)]
pub struct PressureConditions {
    pub memory_pressure: bool,
    pub disk_pressure: bool,
    pub pid_pressure: bool,
}

pub fn evaluate_pressure(
    signals: &ResourceSignals,
    eviction_hard: &std::collections::HashMap<String, String>,
) -> PressureConditions {
    let mut conditions = PressureConditions::default();

    for (signal, threshold) in eviction_hard {
        let exceeded = match signal.as_str() {
            "memory.available" => threshold_exceeded(
                signals.memory_available_bytes,
                signals.memory_available_bytes + signals.nodefs_total_bytes, // approx total mem
                threshold,
            ),
            "nodefs.available" => threshold_exceeded(
                signals.nodefs_available_bytes,
                signals.nodefs_total_bytes,
                threshold,
            ),
            "nodefs.inodesFree" => threshold_exceeded(
                signals.nodefs_inodes_free,
                1_000_000, // approx inodes total
                threshold,
            ),
            "imagefs.available" => threshold_exceeded(
                signals.imagefs_available_bytes,
                signals.imagefs_total_bytes,
                threshold,
            ),
            "pid.available" => signals.pid_available < threshold.parse().unwrap_or(1000),
            _ => false,
        };

        if exceeded {
            match signal.as_str() {
                "memory.available" => conditions.memory_pressure = true,
                "nodefs.available" | "nodefs.inodesFree" | "imagefs.available" => {
                    conditions.disk_pressure = true
                }
                "pid.available" => conditions.pid_pressure = true,
                _ => {}
            }
        }
    }

    conditions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_threshold_exceeded_absolute_bytes() {
        // 50 MiB available, threshold 100 MiB -> exceeded
        assert!(threshold_exceeded(50 * 1024 * 1024, 0, "100Mi"));
        // 200 MiB available, threshold 100 MiB -> not exceeded
        assert!(!threshold_exceeded(200 * 1024 * 1024, 0, "100Mi"));
    }

    #[test]
    fn test_threshold_exceeded_percentage() {
        // 5% available of 100 GiB, threshold 10% -> exceeded
        let total = 100 * 1024 * 1024 * 1024u64;
        let avail = total / 20; // 5%
        assert!(threshold_exceeded(avail, total, "10%"));
        // 15% available -> not exceeded
        let avail15 = total * 15 / 100;
        assert!(!threshold_exceeded(avail15, total, "10%"));
    }

    #[test]
    fn test_evaluate_pressure_memory() {
        let mut eviction_hard = std::collections::HashMap::new();
        eviction_hard.insert("memory.available".to_string(), "200Mi".to_string());
        let signals = ResourceSignals {
            memory_available_bytes: 50 * 1024 * 1024, // 50 MiB < 200 MiB threshold
            ..Default::default()
        };
        let conditions = evaluate_pressure(&signals, &eviction_hard);
        assert!(conditions.memory_pressure);
    }

    #[test]
    fn test_evaluate_pressure_no_pressure() {
        let mut eviction_hard = std::collections::HashMap::new();
        eviction_hard.insert("memory.available".to_string(), "100Mi".to_string());
        let signals = ResourceSignals {
            memory_available_bytes: 4 * 1024 * 1024 * 1024, // 4 GiB > 100 MiB
            ..Default::default()
        };
        let conditions = evaluate_pressure(&signals, &eviction_hard);
        assert!(!conditions.memory_pressure);
    }

    #[test]
    fn test_collect_signals_no_panic() {
        // Should not panic even if paths don't exist.
        let _ = collect_signals("/var/lib/kubelet", "/var/lib/containerd");
    }

    #[test]
    fn test_parse_bytes_formats() {
        assert_eq!(parse_bytes("100Mi"), 100 * 1024 * 1024);
        assert_eq!(parse_bytes("1Gi"), 1024 * 1024 * 1024);
        assert_eq!(parse_bytes("500Ki"), 500 * 1024);
    }
}
