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

//! cgroup v2 resource enforcement.
//!
//! Manages container resource limits by writing to the cgroupfs hierarchy at
//! /sys/fs/cgroup/ (cgroup v2 unified hierarchy).
//!
//! Mirrors pkg/kubelet/cm/cgroup_manager_linux.go.

use kubelet_core::error::{KubeletError, Result};
use kubelet_core::qos::QosClass;
use kubelet_core::types::PodUID;
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

// -- cgroup hierarchy ----------------------------------------------------------

/// cgroup v2 path structure for Kubernetes.
///
/// Root: /sys/fs/cgroup/
/// Kubepods slice: kubepods.slice/
///   QoS classes: kubepods-guaranteed.slice/, kubepods-burstable.slice/, kubepods-besteffort.slice/
///     Pod cgroups: kubepods-<qos>-pod<uid>.slice/
///       Container cgroups: cri-containerd-<ctr-id>.scope/
pub struct CgroupPath {
    pub root: PathBuf,
}

impl CgroupPath {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn kubepods(&self) -> PathBuf {
        self.root.join("kubepods.slice")
    }

    pub fn qos_slice(&self, qos: &QosClass) -> PathBuf {
        let name = match qos {
            QosClass::Guaranteed => "kubepods-guaranteed.slice",
            QosClass::Burstable => "kubepods-burstable.slice",
            QosClass::BestEffort => "kubepods-besteffort.slice",
        };
        self.kubepods().join(name)
    }

    pub fn pod_slice(&self, qos: &QosClass, uid: &PodUID) -> PathBuf {
        let safe_uid = uid.0.replace('-', "_");
        let name = match qos {
            QosClass::Guaranteed => format!("kubepods-guaranteed-pod{}.slice", safe_uid),
            QosClass::Burstable => format!("kubepods-burstable-pod{}.slice", safe_uid),
            QosClass::BestEffort => format!("kubepods-besteffort-pod{}.slice", safe_uid),
        };
        self.qos_slice(qos).join(name)
    }

    pub fn container_scope(&self, qos: &QosClass, uid: &PodUID, container_id: &str) -> PathBuf {
        self.pod_slice(qos, uid)
            .join(format!("cri-containerd-{}.scope", container_id))
    }
}

// -- Resource limits -----------------------------------------------------------

/// Resource limits to apply to a cgroup.
#[derive(Debug, Clone, Default)]
pub struct CgroupResources {
    /// CPU quota in microseconds per period (cpu.max: quota period).
    pub cpu_quota_us: Option<i64>,
    /// CPU period in microseconds.
    pub cpu_period_us: Option<u64>,
    /// CPU shares (cpu.weight in cgroup v2, 1-10000).
    pub cpu_shares: Option<u64>,
    /// Memory limit in bytes (memory.max).
    pub memory_limit_bytes: Option<i64>,
    /// Memory soft limit in bytes (memory.high).
    pub memory_high_bytes: Option<i64>,
    /// Memory swap limit (memory.swap.max).
    pub memory_swap_bytes: Option<i64>,
    /// PID limit (pids.max).
    pub pids_max: Option<i64>,
    /// OOM score adjustment (oom_score_adj in /proc/<pid>/).
    pub oom_score_adj: Option<i32>,
}

impl CgroupResources {
    /// Build resources for a QoS class with reasonable defaults.
    pub fn for_qos(qos: &QosClass) -> Self {
        match qos {
            QosClass::BestEffort => Self {
                cpu_shares: Some(2), // minimum (cgroup v2 weight)
                oom_score_adj: Some(1000),
                ..Default::default()
            },
            QosClass::Burstable => Self {
                cpu_shares: Some(1024),
                oom_score_adj: Some(500),
                ..Default::default()
            },
            QosClass::Guaranteed => Self {
                cpu_shares: Some(1024),
                oom_score_adj: Some(-997),
                ..Default::default()
            },
        }
    }
}

// -- cgroup manager ------------------------------------------------------------

/// Manages cgroup resources for pods and containers.
pub struct CgroupManager {
    paths: CgroupPath,
    /// Whether we're in dry-run mode (log but don't write to /sys/fs/cgroup/).
    dry_run: bool,
}

impl CgroupManager {
    pub fn new(cgroup_root: impl Into<PathBuf>, dry_run: bool) -> Self {
        Self {
            paths: CgroupPath::new(cgroup_root),
            dry_run,
        }
    }

    /// Create cgroup directories for a pod.
    pub async fn create_pod_cgroup(&self, qos: &QosClass, uid: &PodUID) -> Result<()> {
        let path = self.paths.pod_slice(qos, uid);
        debug!(path = %path.display(), qos = ?qos, "Creating pod cgroup");
        if !self.dry_run {
            tokio::fs::create_dir_all(&path).await?;
        }
        Ok(())
    }

    /// Create pod cgroup with memory limit (including overhead).
    ///
    /// Calculates the pod-level memory limit from container memory requests plus RuntimeClass overhead.
    /// Mirrors pkg/kubelet/cm/cgroup_manager_linux.go::podCgroupMemoryLimit().
    pub async fn create_pod_cgroup_with_memory_limit(
        &self,
        qos: &QosClass,
        uid: &PodUID,
        container_memory_bytes: i64,
        overhead_memory_bytes: i64,
    ) -> Result<()> {
        let path = self.paths.pod_slice(qos, uid);
        debug!(
            path = %path.display(),
            qos = ?qos,
            container_memory = container_memory_bytes,
            overhead_memory = overhead_memory_bytes,
            "Creating pod cgroup with memory limit"
        );

        if !self.dry_run {
            tokio::fs::create_dir_all(&path).await?;

            // Pod-level memory.max = sum of container memory requests + overhead
            let pod_memory_limit = container_memory_bytes.saturating_add(overhead_memory_bytes);
            if pod_memory_limit > 0 {
                let limit_val = pod_memory_limit.to_string();
                self.write_cgroup_file(&path, "memory.max", &limit_val)
                    .await?;
                debug!(
                    path = %path.display(),
                    memory_bytes = pod_memory_limit,
                    "Applied pod memory limit"
                );
            }
        }
        Ok(())
    }

    /// Remove cgroup directory for a pod.
    pub async fn remove_pod_cgroup(&self, qos: &QosClass, uid: &PodUID) -> Result<()> {
        let path = self.paths.pod_slice(qos, uid);
        if !self.dry_run && path.exists() {
            std::fs::remove_dir(&path).map_err(KubeletError::Io)?;
        }
        Ok(())
    }

    /// Apply resource limits to a container cgroup.
    pub async fn apply_resources(
        &self,
        qos: &QosClass,
        uid: &PodUID,
        container_id: &str,
        resources: &CgroupResources,
    ) -> Result<()> {
        let cgroup_path = self.paths.container_scope(qos, uid, container_id);
        debug!(path = %cgroup_path.display(), "Applying cgroup resources");

        if self.dry_run {
            return Ok(());
        }

        tokio::fs::create_dir_all(&cgroup_path).await?;

        // cpu.max: "quota period" or "max period"
        if let (Some(quota), Some(period)) = (resources.cpu_quota_us, resources.cpu_period_us) {
            let cpu_max = if quota < 0 {
                format!("max {}", period)
            } else {
                format!("{} {}", quota, period)
            };
            self.write_cgroup_file(&cgroup_path, "cpu.max", &cpu_max)
                .await?;
        }

        // cpu.weight (replaces cpu.shares in cgroup v2, range 1-10000)
        if let Some(shares) = resources.cpu_shares {
            // Convert from cgroup v1 shares (1024=normal) to v2 weight (100=normal)
            let weight = ((shares as f64 / 1024.0) * 100.0).clamp(1.0, 10000.0) as u64;
            self.write_cgroup_file(&cgroup_path, "cpu.weight", &weight.to_string())
                .await?;
        }

        // memory.max
        if let Some(limit) = resources.memory_limit_bytes {
            let val = if limit < 0 {
                "max".to_string()
            } else {
                limit.to_string()
            };
            self.write_cgroup_file(&cgroup_path, "memory.max", &val)
                .await?;
        }

        // memory.high (soft limit)
        if let Some(high) = resources.memory_high_bytes {
            let val = if high < 0 {
                "max".to_string()
            } else {
                high.to_string()
            };
            self.write_cgroup_file(&cgroup_path, "memory.high", &val)
                .await?;
        }

        // pids.max
        if let Some(pids) = resources.pids_max {
            let val = if pids < 0 {
                "max".to_string()
            } else {
                pids.to_string()
            };
            self.write_cgroup_file(&cgroup_path, "pids.max", &val)
                .await?;
        }

        Ok(())
    }

    /// Read a cgroup file value.
    pub async fn read_cgroup_file(&self, cgroup_path: &Path, file: &str) -> Result<String> {
        let path = cgroup_path.join(file);
        let content = tokio::fs::read_to_string(&path).await?;
        Ok(content.trim().to_string())
    }

    /// Write a value to a cgroup control file.
    async fn write_cgroup_file(&self, cgroup_path: &Path, file: &str, value: &str) -> Result<()> {
        let path = cgroup_path.join(file);
        debug!(path = %path.display(), value, "Writing cgroup file");
        tokio::fs::write(&path, value).await.map_err(|e| {
            // Non-fatal: cgroup writes can fail in test environments
            KubeletError::Io(e)
        })
    }

    pub fn cgroup_root(&self) -> &Path {
        &self.paths.root
    }
}

// -- cpu_shares -> v2 weight conversion ----------------------------------------

/// Convert cgroup v1 cpu.shares to cgroup v2 cpu.weight.
pub fn shares_to_weight(shares: u64) -> u64 {
    ((shares as f64 / 1024.0) * 100.0).clamp(1.0, 10000.0) as u64
}

/// Convert cgroup v2 cpu.weight back to approximate v1 shares.
pub fn weight_to_shares(weight: u64) -> u64 {
    ((weight as f64 / 100.0) * 1024.0).max(2.0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cgroup_path_structure() {
        let paths = CgroupPath::new("/sys/fs/cgroup");
        assert_eq!(
            paths.kubepods().to_str().unwrap(),
            "/sys/fs/cgroup/kubepods.slice"
        );
    }

    #[test]
    fn test_qos_slice_names() {
        let paths = CgroupPath::new("/sys/fs/cgroup");
        assert!(paths
            .qos_slice(&QosClass::Guaranteed)
            .to_str()
            .unwrap()
            .contains("guaranteed"));
        assert!(paths
            .qos_slice(&QosClass::Burstable)
            .to_str()
            .unwrap()
            .contains("burstable"));
        assert!(paths
            .qos_slice(&QosClass::BestEffort)
            .to_str()
            .unwrap()
            .contains("besteffort"));
    }

    #[test]
    fn test_pod_slice_contains_uid() {
        let paths = CgroupPath::new("/sys/fs/cgroup");
        let uid = PodUID::new("abc-123-def");
        let slice = paths.pod_slice(&QosClass::Guaranteed, &uid);
        let slice_str = slice.to_str().unwrap();
        assert!(slice_str.contains("abc") && slice_str.contains("guaranteed"));
    }

    #[test]
    fn test_container_scope() {
        let paths = CgroupPath::new("/sys/fs/cgroup");
        let uid = PodUID::new("test-uid");
        let scope = paths.container_scope(&QosClass::Burstable, &uid, "deadbeef123");
        assert!(scope
            .to_str()
            .unwrap()
            .contains("cri-containerd-deadbeef123"));
    }

    #[test]
    fn test_shares_to_weight_normal() {
        // 1024 shares -> 100 weight (normal CPU)
        assert_eq!(shares_to_weight(1024), 100);
    }

    #[test]
    fn test_shares_to_weight_min() {
        // 2 shares -> minimum weight = 1
        assert_eq!(shares_to_weight(2), 1);
    }

    #[test]
    fn test_weight_to_shares_normal() {
        // 100 weight -> 1024 shares
        assert_eq!(weight_to_shares(100), 1024);
    }

    #[test]
    fn test_cgroup_resources_for_best_effort() {
        let r = CgroupResources::for_qos(&QosClass::BestEffort);
        assert_eq!(r.cpu_shares, Some(2));
        assert_eq!(r.oom_score_adj, Some(1000));
        assert!(r.memory_limit_bytes.is_none());
    }

    #[test]
    fn test_cgroup_resources_for_guaranteed() {
        let r = CgroupResources::for_qos(&QosClass::Guaranteed);
        assert_eq!(r.oom_score_adj, Some(-997));
    }

    #[tokio::test]
    async fn test_create_pod_cgroup_dry_run() {
        let mgr = CgroupManager::new("/sys/fs/cgroup", true /* dry_run */);
        let uid = PodUID::new("uid-cg-1");
        mgr.create_pod_cgroup(&QosClass::Burstable, &uid)
            .await
            .unwrap();
        // In dry-run, no actual directories are created -- just verifying no panic
    }

    #[tokio::test]
    async fn test_apply_resources_dry_run() {
        let mgr = CgroupManager::new("/sys/fs/cgroup", true);
        let uid = PodUID::new("uid-cg-2");
        let resources = CgroupResources {
            cpu_quota_us: Some(50_000),
            cpu_period_us: Some(100_000),
            cpu_shares: Some(1024),
            memory_limit_bytes: Some(512 * 1024 * 1024),
            ..Default::default()
        };
        mgr.apply_resources(&QosClass::Guaranteed, &uid, "ctr-abc", &resources)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_and_remove_pod_cgroup_real() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let mgr = CgroupManager::new(dir.path(), false);
        let uid = PodUID::new("uid-cg-3");

        mgr.create_pod_cgroup(&QosClass::BestEffort, &uid)
            .await
            .unwrap();
        let cgroup_path = mgr.paths.pod_slice(&QosClass::BestEffort, &uid);
        assert!(cgroup_path.exists());

        mgr.remove_pod_cgroup(&QosClass::BestEffort, &uid)
            .await
            .unwrap();
        assert!(!cgroup_path.exists());
    }
}
