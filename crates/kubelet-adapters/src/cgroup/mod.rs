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
use kubelet_core::pod::ResourceRequirements;
use kubelet_core::qos::QosClass;
use kubelet_core::types::PodUID;
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

/// Default CFS CPU period in microseconds (100 ms, standard Kubernetes value).
pub const DEFAULT_CPU_PERIOD_US: u64 = 100_000;
/// Minimum positive CFS CPU quota accepted by Linux cgroup v2 (1 ms).
pub const MIN_CPU_QUOTA_US: i64 = 1_000;
const MIN_CPU_SHARES: i64 = 2;
const MAX_CPU_SHARES: i64 = 262_144;

// -- cgroup hierarchy ----------------------------------------------------------

/// cgroup driver naming convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CgroupDriver {
    /// systemd driver:
    /// `kubepods.slice/kubepods-<qos>.slice/kubepods-<qos>-pod<uid>.slice/cri-containerd-<ctr-id>.scope`
    #[default]
    Systemd,
    /// cgroupfs driver:
    /// `kubepods/<qos>/pod<uid>/<ctr-id>`
    Cgroupfs,
}

impl CgroupDriver {
    /// Parse a kubelet `cgroup_driver` configuration string (`"systemd"` or `"cgroupfs"`).
    pub fn parse(driver: &str) -> Self {
        if driver.eq_ignore_ascii_case("systemd") {
            Self::Systemd
        } else {
            Self::Cgroupfs
        }
    }
}

/// cgroup v2 path structure for Kubernetes.
///
/// Root: `/sys/fs/cgroup/`
/// - systemd driver:
///   `kubepods.slice/kubepods-<qos>.slice/kubepods-<qos>-pod<uid>.slice/cri-containerd-<ctr-id>.scope/`
/// - cgroupfs driver:
///   `kubepods/<qos>/pod<uid>/<ctr-id>/`
pub struct CgroupPath {
    pub root: PathBuf,
    pub driver: CgroupDriver,
}

impl CgroupPath {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_driver(root, CgroupDriver::Systemd)
    }

    pub fn with_driver(root: impl Into<PathBuf>, driver: CgroupDriver) -> Self {
        Self {
            root: root.into(),
            driver,
        }
    }

    pub fn kubepods(&self) -> PathBuf {
        match self.driver {
            CgroupDriver::Systemd => self.root.join("kubepods.slice"),
            CgroupDriver::Cgroupfs => self.root.join("kubepods"),
        }
    }

    pub fn qos_slice(&self, qos: &QosClass) -> PathBuf {
        let name = match (self.driver, qos) {
            (CgroupDriver::Systemd, QosClass::Guaranteed) => "kubepods-guaranteed.slice",
            (CgroupDriver::Systemd, QosClass::Burstable) => "kubepods-burstable.slice",
            (CgroupDriver::Systemd, QosClass::BestEffort) => "kubepods-besteffort.slice",
            (CgroupDriver::Cgroupfs, QosClass::Guaranteed) => "guaranteed",
            (CgroupDriver::Cgroupfs, QosClass::Burstable) => "burstable",
            (CgroupDriver::Cgroupfs, QosClass::BestEffort) => "besteffort",
        };
        self.kubepods().join(name)
    }

    pub fn pod_slice(&self, qos: &QosClass, uid: &PodUID) -> PathBuf {
        let name = match self.driver {
            CgroupDriver::Systemd => {
                let safe_uid = uid.0.replace('-', "_");
                match qos {
                    QosClass::Guaranteed => format!("kubepods-guaranteed-pod{}.slice", safe_uid),
                    QosClass::Burstable => format!("kubepods-burstable-pod{}.slice", safe_uid),
                    QosClass::BestEffort => format!("kubepods-besteffort-pod{}.slice", safe_uid),
                }
            }
            CgroupDriver::Cgroupfs => format!("pod{}", uid.0),
        };
        self.qos_slice(qos).join(name)
    }

    pub fn container_scope(&self, qos: &QosClass, uid: &PodUID, container_id: &str) -> PathBuf {
        let leaf = match self.driver {
            CgroupDriver::Systemd => format!("cri-containerd-{}.scope", container_id),
            CgroupDriver::Cgroupfs => container_id.to_string(),
        };
        self.pod_slice(qos, uid).join(leaf)
    }

    /// Compute a container's cgroup path for an explicit [`CgroupDriver`].
    pub fn container_scope_for_driver(
        &self,
        driver: CgroupDriver,
        qos: &QosClass,
        uid: &PodUID,
        container_id: &str,
    ) -> PathBuf {
        Self::with_driver(&self.root, driver).container_scope(qos, uid, container_id)
    }
}

/// Extract the relative cgroup v2 path from `/proc/<pid>/cgroup` contents.
///
/// In cgroup v2 unified hierarchy, the entry has the form `0::<path>` (e.g.
/// `0::/kubepods/guaranteed/pod<uid>/<ctr-id>`). Returns `None` for the root
/// cgroup (`0::/`) or if no v2 entry is present.
pub fn parse_proc_cgroup_v2_path(proc_cgroup: &str) -> Option<&str> {
    for line in proc_cgroup.lines() {
        if let Some(raw_path) = line.trim().strip_prefix("0::") {
            let rel = raw_path.trim_start_matches('/');
            if !rel.is_empty() {
                return Some(rel);
            }
        }
    }
    None
}

/// Resolve a running container's actual cgroup v2 directory via `/proc/<pid>/cgroup`.
pub fn cgroup_path_for_pid(cgroup_root: &Path, pid: u32) -> Option<PathBuf> {
    let proc_path = format!("/proc/{}/cgroup", pid);
    let content = std::fs::read_to_string(proc_path).ok()?;
    let rel = parse_proc_cgroup_v2_path(&content)?;
    Some(cgroup_root.join(rel))
}

// -- Resource limits -----------------------------------------------------------

/// Resource limits to apply to a cgroup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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

    /// Convert a container's Kubernetes [`ResourceRequirements`] and pod [`QosClass`]
    /// into cgroup v2 resource limits.
    ///
    /// - **BestEffort**: returns all `None` (no cgroup writes needed).
    /// - **Guaranteed**: sets `cpu_shares` from CPU requests (falling back to CPU
    ///   limits if requests are omitted), `cpu_quota_us`/`cpu_period_us` from CPU
    ///   limits, and `memory_limit_bytes` from memory limits.
    /// - **Burstable**: sets `cpu_shares` from CPU requests if present; sets
    ///   `cpu_quota_us`/`cpu_period_us` and `memory_limit_bytes` only if limits
    ///   are present.
    pub fn from_container_spec(resources: &ResourceRequirements, qos: &QosClass) -> Self {
        if *qos == QosClass::BestEffort {
            return Self::default();
        }

        let cpu_req_millis = resources
            .requests
            .get("cpu")
            .map(|q| q.value)
            .filter(|&v| v > 0)
            .or_else(|| {
                if *qos == QosClass::Guaranteed {
                    resources
                        .limits
                        .get("cpu")
                        .map(|q| q.value)
                        .filter(|&v| v > 0)
                } else {
                    None
                }
            });

        let cpu_shares = cpu_req_millis.map(|milli_cpu| {
            ((milli_cpu.saturating_mul(1024)) / 1000).clamp(MIN_CPU_SHARES, MAX_CPU_SHARES) as u64
        });

        let (cpu_quota_us, cpu_period_us) = match resources
            .limits
            .get("cpu")
            .map(|q| q.value)
            .filter(|&v| v > 0)
        {
            Some(milli_cpu) => {
                // Linux cgroup v2 rejects cpu.max quotas below 1,000 us (1 ms).
                let quota = ((milli_cpu.saturating_mul(DEFAULT_CPU_PERIOD_US as i64)) / 1000)
                    .max(MIN_CPU_QUOTA_US);
                (Some(quota), Some(DEFAULT_CPU_PERIOD_US))
            }
            None => (None, None),
        };

        let memory_limit_bytes = resources
            .limits
            .get("memory")
            .map(|q| q.value)
            .filter(|&v| v > 0);

        Self {
            cpu_quota_us,
            cpu_period_us,
            cpu_shares,
            memory_limit_bytes,
            ..Default::default()
        }
    }

    /// Returns `true` when no cgroup control files need to be written.
    pub fn is_empty(&self) -> bool {
        self.cpu_quota_us.is_none()
            && self.cpu_shares.is_none()
            && self.memory_limit_bytes.is_none()
            && self.memory_high_bytes.is_none()
            && self.pids_max.is_none()
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
        Self::with_driver(cgroup_root, CgroupDriver::Systemd, dry_run)
    }

    pub fn with_driver(
        cgroup_root: impl Into<PathBuf>,
        driver: CgroupDriver,
        dry_run: bool,
    ) -> Self {
        Self {
            paths: CgroupPath::with_driver(cgroup_root, driver),
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

            // Only apply a pod-level memory.max when every container has a hard memory limit.
            // When container_memory_bytes <= 0, the pod is unconstrained even if runtime overhead is set.
            if container_memory_bytes > 0 {
                let pod_memory_limit =
                    container_memory_bytes.saturating_add(overhead_memory_bytes.max(0));
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

    /// Resolve the cgroup directory for a container, preferring `/proc/<pid>/cgroup`
    /// when a runtime-reported PID resolves to an existing directory under the
    /// configured cgroup root, and falling back to the configured [`CgroupDriver`]
    /// hierarchy otherwise.
    pub fn resolve_container_cgroup_path(
        &self,
        driver: CgroupDriver,
        qos: &QosClass,
        uid: &PodUID,
        container_id: &str,
        pid: Option<u32>,
    ) -> PathBuf {
        if let Some(pid) = pid
            && pid != std::process::id()
            && let Some(path) = cgroup_path_for_pid(&self.paths.root, pid)
            && path.exists()
        {
            return path;
        }
        self.paths
            .container_scope_for_driver(driver, qos, uid, container_id)
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
        self.apply_resources_to_path(&cgroup_path, resources).await
    }

    /// Apply resource limits directly to a resolved cgroup directory path.
    pub async fn apply_resources_to_path(
        &self,
        cgroup_path: &Path,
        resources: &CgroupResources,
    ) -> Result<()> {
        debug!(path = %cgroup_path.display(), "Applying cgroup resources");

        if self.dry_run || resources.is_empty() {
            return Ok(());
        }

        tokio::fs::create_dir_all(cgroup_path).await?;

        // cpu.max: "quota period" or "max period"
        if let (Some(quota), Some(period)) = (resources.cpu_quota_us, resources.cpu_period_us) {
            let cpu_max = if quota < 0 {
                format!("max {}", period)
            } else {
                format!("{} {}", quota, period)
            };
            self.write_cgroup_file(cgroup_path, "cpu.max", &cpu_max)
                .await?;
        }

        // cpu.weight (replaces cpu.shares in cgroup v2, range 1-10000)
        if let Some(shares) = resources.cpu_shares {
            let weight = shares_to_weight(shares);
            self.write_cgroup_file(cgroup_path, "cpu.weight", &weight.to_string())
                .await?;
        }

        // memory.max
        if let Some(limit) = resources.memory_limit_bytes {
            let val = if limit < 0 {
                "max".to_string()
            } else {
                limit.to_string()
            };
            self.write_cgroup_file(cgroup_path, "memory.max", &val)
                .await?;
        }

        // memory.high (soft limit)
        if let Some(high) = resources.memory_high_bytes {
            let val = if high < 0 {
                "max".to_string()
            } else {
                high.to_string()
            };
            self.write_cgroup_file(cgroup_path, "memory.high", &val)
                .await?;
        }

        // pids.max
        if let Some(pids) = resources.pids_max {
            let val = if pids < 0 {
                "max".to_string()
            } else {
                pids.to_string()
            };
            self.write_cgroup_file(cgroup_path, "pids.max", &val)
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

/// Convert cgroup v1 `cpu.shares` (2..=262144) to cgroup v2 `cpu.weight` (1..=10000)
/// using the OCI/Kubernetes quadratic log2 formula (runc >= 1.3.2 / crun >= 1.23):
///
/// `weight = ceil(10^((l^2 + 125*l - 126) / 612))`, where `l = log2(shares)`.
///
/// Maps 2 -> 1, 20 (20m) -> 6, 51 (50m) -> 11, 1024 (1 CPU) -> 100, 262144 -> 10000.
pub fn shares_to_weight(shares: u64) -> u64 {
    if shares <= 2 {
        return 1;
    }
    if shares >= 262_144 {
        return 10_000;
    }
    let l = (shares as f64).log2();
    let exponent = (l * l + 125.0 * l - 126.0) / 612.0;
    10.0_f64.powf(exponent).ceil().clamp(1.0, 10_000.0) as u64
}

/// Convert cgroup v2 `cpu.weight` (1..=10000) back to approximate v1 `cpu.shares` (2..=262144)
/// by inverting the OCI/Kubernetes quadratic log2 mapping.
pub fn weight_to_shares(weight: u64) -> u64 {
    if weight <= 1 {
        return 2;
    }
    if weight >= 10_000 {
        return 262_144;
    }
    let y = (weight as f64).log10();
    let l = (-125.0 + (125.0 * 125.0 + 4.0 * (126.0 + 612.0 * y)).sqrt()) / 2.0;
    2.0_f64.powf(l).round().clamp(2.0, 262_144.0) as u64
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
        assert!(
            paths
                .qos_slice(&QosClass::Guaranteed)
                .to_str()
                .unwrap()
                .contains("guaranteed")
        );
        assert!(
            paths
                .qos_slice(&QosClass::Burstable)
                .to_str()
                .unwrap()
                .contains("burstable")
        );
        assert!(
            paths
                .qos_slice(&QosClass::BestEffort)
                .to_str()
                .unwrap()
                .contains("besteffort")
        );
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
        assert!(
            scope
                .to_str()
                .unwrap()
                .contains("cri-containerd-deadbeef123")
        );
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
    fn test_shares_to_weight_kubernetes_mapping() {
        // 20m -> 20 shares -> weight 6 (matches k8s InPlace conformance expected ["1", "6"])
        assert_eq!(shares_to_weight(20), 6);
        // 50m -> 51 shares -> weight 11
        assert_eq!(shares_to_weight(51), 11);
        // 256 cores -> max weight 10000
        assert_eq!(shares_to_weight(262_144), 10_000);
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

    #[test]
    fn test_from_container_spec_best_effort() {
        let reqs = ResourceRequirements::default();
        let cg = CgroupResources::from_container_spec(&reqs, &QosClass::BestEffort);
        assert_eq!(cg, CgroupResources::default());
        assert!(cg.is_empty());
    }

    #[test]
    fn test_from_container_spec_guaranteed() {
        use kubelet_core::types::ResourceQuantity;
        use std::collections::HashMap;

        let mut requests = HashMap::new();
        requests.insert("cpu".to_string(), ResourceQuantity::cpu_millicores(50));
        requests.insert(
            "memory".to_string(),
            ResourceQuantity::memory_bytes(52_428_800),
        );
        let limits = requests.clone();
        let reqs = ResourceRequirements { requests, limits };

        let cg = CgroupResources::from_container_spec(&reqs, &QosClass::Guaranteed);
        assert_eq!(cg.cpu_shares, Some(51)); // (50 * 1024) / 1000 = 51
        assert_eq!(cg.cpu_quota_us, Some(5_000)); // (50 * 100_000) / 1000 = 5000
        assert_eq!(cg.cpu_period_us, Some(DEFAULT_CPU_PERIOD_US));
        assert_eq!(cg.memory_limit_bytes, Some(52_428_800));
        assert!(!cg.is_empty());
    }

    #[test]
    fn test_from_container_spec_burstable_requests_and_limits() {
        use kubelet_core::types::ResourceQuantity;
        use std::collections::HashMap;

        let mut requests = HashMap::new();
        requests.insert("cpu".to_string(), ResourceQuantity::cpu_millicores(250));
        requests.insert(
            "memory".to_string(),
            ResourceQuantity::memory_bytes(64 * 1024 * 1024),
        );
        let mut limits = HashMap::new();
        limits.insert("cpu".to_string(), ResourceQuantity::cpu_millicores(500));
        limits.insert(
            "memory".to_string(),
            ResourceQuantity::memory_bytes(128 * 1024 * 1024),
        );
        let reqs = ResourceRequirements { requests, limits };

        let cg = CgroupResources::from_container_spec(&reqs, &QosClass::Burstable);
        assert_eq!(cg.cpu_shares, Some(256)); // (250 * 1024) / 1000 = 256
        assert_eq!(cg.cpu_quota_us, Some(50_000));
        assert_eq!(cg.cpu_period_us, Some(DEFAULT_CPU_PERIOD_US));
        assert_eq!(cg.memory_limit_bytes, Some(128 * 1024 * 1024));
    }

    #[test]
    fn test_from_container_spec_burstable_requests_only() {
        use kubelet_core::types::ResourceQuantity;
        use std::collections::HashMap;

        let mut requests = HashMap::new();
        requests.insert("cpu".to_string(), ResourceQuantity::cpu_millicores(500));
        requests.insert(
            "memory".to_string(),
            ResourceQuantity::memory_bytes(64 * 1024 * 1024),
        );
        let reqs = ResourceRequirements {
            requests,
            limits: HashMap::new(),
        };

        let cg = CgroupResources::from_container_spec(&reqs, &QosClass::Burstable);
        assert_eq!(cg.cpu_shares, Some(512));
        assert!(cg.cpu_quota_us.is_none());
        assert!(cg.cpu_period_us.is_none());
        assert!(cg.memory_limit_bytes.is_none());
    }

    #[test]
    fn test_from_container_spec_cpu_shares_clamping() {
        use kubelet_core::types::ResourceQuantity;
        use std::collections::HashMap;

        // 1m -> (1 * 1024) / 1000 = 1 -> clamped to min 2
        let mut req_min = HashMap::new();
        req_min.insert("cpu".to_string(), ResourceQuantity::cpu_millicores(1));
        let cg_min = CgroupResources::from_container_spec(
            &ResourceRequirements {
                requests: req_min,
                limits: HashMap::new(),
            },
            &QosClass::Burstable,
        );
        assert_eq!(cg_min.cpu_shares, Some(2));

        // 1_000_000m (1000 cores) -> clamped to max 262_144
        let mut req_max = HashMap::new();
        req_max.insert(
            "cpu".to_string(),
            ResourceQuantity::cpu_millicores(1_000_000),
        );
        let cg_max = CgroupResources::from_container_spec(
            &ResourceRequirements {
                requests: req_max,
                limits: HashMap::new(),
            },
            &QosClass::Burstable,
        );
        assert_eq!(cg_max.cpu_shares, Some(262_144));
    }

    #[tokio::test]
    async fn test_apply_resources_writes_files_from_container_spec() {
        use kubelet_core::types::ResourceQuantity;
        use std::collections::HashMap;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let mgr = CgroupManager::new(dir.path(), false);
        let uid = PodUID::new("uid-cg-apply");

        let mut requests = HashMap::new();
        requests.insert("cpu".to_string(), ResourceQuantity::cpu_millicores(50));
        requests.insert(
            "memory".to_string(),
            ResourceQuantity::memory_bytes(52_428_800),
        );
        let reqs = ResourceRequirements {
            requests: requests.clone(),
            limits: requests,
        };

        let cg_res = CgroupResources::from_container_spec(&reqs, &QosClass::Guaranteed);
        mgr.apply_resources(&QosClass::Guaranteed, &uid, "ctr-50m", &cg_res)
            .await
            .unwrap();

        let scope = mgr
            .paths
            .container_scope(&QosClass::Guaranteed, &uid, "ctr-50m");
        assert_eq!(
            mgr.read_cgroup_file(&scope, "cpu.max").await.unwrap(),
            "5000 100000"
        );
        assert_eq!(
            mgr.read_cgroup_file(&scope, "memory.max").await.unwrap(),
            "52428800"
        );
        assert_eq!(
            mgr.read_cgroup_file(&scope, "cpu.weight").await.unwrap(),
            "11"
        );
    }

    #[tokio::test]
    async fn test_apply_resources_best_effort_noop() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let mgr = CgroupManager::new(dir.path(), false);
        let uid = PodUID::new("uid-cg-be");

        let cg_res = CgroupResources::from_container_spec(
            &ResourceRequirements::default(),
            &QosClass::BestEffort,
        );
        mgr.apply_resources(&QosClass::BestEffort, &uid, "ctr-be", &cg_res)
            .await
            .unwrap();

        let scope = mgr
            .paths
            .container_scope(&QosClass::BestEffort, &uid, "ctr-be");
        assert!(!scope.exists());
    }

    #[test]
    fn test_cgroupfs_path_structure() {
        let paths = CgroupPath::with_driver("/sys/fs/cgroup", CgroupDriver::Cgroupfs);
        let uid = PodUID::new("abc-123-def");
        assert_eq!(
            paths.container_scope(&QosClass::Guaranteed, &uid, "ctr-1"),
            PathBuf::from("/sys/fs/cgroup/kubepods/guaranteed/podabc-123-def/ctr-1")
        );
        assert_eq!(
            paths.container_scope(&QosClass::Burstable, &uid, "ctr-2"),
            PathBuf::from("/sys/fs/cgroup/kubepods/burstable/podabc-123-def/ctr-2")
        );
        assert_eq!(
            paths.container_scope(&QosClass::BestEffort, &uid, "ctr-3"),
            PathBuf::from("/sys/fs/cgroup/kubepods/besteffort/podabc-123-def/ctr-3")
        );
    }

    #[test]
    fn test_systemd_path_structure() {
        let paths = CgroupPath::with_driver("/sys/fs/cgroup", CgroupDriver::Systemd);
        let uid = PodUID::new("abc-123-def");
        assert_eq!(
            paths.container_scope(&QosClass::Guaranteed, &uid, "ctr-1"),
            PathBuf::from(
                "/sys/fs/cgroup/kubepods.slice/kubepods-guaranteed.slice/kubepods-guaranteed-podabc_123_def.slice/cri-containerd-ctr-1.scope"
            )
        );
    }

    #[test]
    fn test_parse_proc_cgroup_v2_path() {
        assert_eq!(
            parse_proc_cgroup_v2_path("0::/kubepods/guaranteed/podabc-123/ctr1\n"),
            Some("kubepods/guaranteed/podabc-123/ctr1")
        );
        assert_eq!(
            parse_proc_cgroup_v2_path(
                "12:pids:/kubepods\n0::/kubepods.slice/kubepods-burstable.slice/cri-containerd-abc.scope\n"
            ),
            Some("kubepods.slice/kubepods-burstable.slice/cri-containerd-abc.scope")
        );
        assert_eq!(parse_proc_cgroup_v2_path("0::/\n"), None);
        assert_eq!(parse_proc_cgroup_v2_path(""), None);
    }

    #[test]
    fn test_resolve_container_cgroup_path_ignores_kubelet_pid() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let mgr = CgroupManager::with_driver(dir.path(), CgroupDriver::Cgroupfs, false);
        let uid = PodUID::new("uid-cg-self-pid");
        if let Some(kubelet_cgroup) = cgroup_path_for_pid(dir.path(), std::process::id()) {
            std::fs::create_dir_all(&kubelet_cgroup).unwrap();
        }

        let expected = mgr.paths.container_scope_for_driver(
            CgroupDriver::Cgroupfs,
            &QosClass::Burstable,
            &uid,
            "ctr-1",
        );
        assert_eq!(
            mgr.resolve_container_cgroup_path(
                CgroupDriver::Cgroupfs,
                &QosClass::Burstable,
                &uid,
                "ctr-1",
                Some(std::process::id()),
            ),
            expected
        );
    }

    #[test]
    fn test_from_container_spec_clamps_small_cpu_quota_to_minimum() {
        use kubelet_core::types::ResourceQuantity;
        use std::collections::HashMap;

        for millis in [1, 5, 9, 10] {
            let mut limits = HashMap::new();
            limits.insert("cpu".to_string(), ResourceQuantity::cpu_millicores(millis));
            limits.insert(
                "memory".to_string(),
                ResourceQuantity::memory_bytes(32 * 1024 * 1024),
            );
            let reqs = ResourceRequirements {
                requests: limits.clone(),
                limits,
            };

            let cg = CgroupResources::from_container_spec(&reqs, &QosClass::Guaranteed);
            assert_eq!(
                cg.cpu_quota_us,
                Some(MIN_CPU_QUOTA_US),
                "CPU limit {millis}m must clamp quota to >= {MIN_CPU_QUOTA_US} us"
            );
            assert_eq!(cg.cpu_period_us, Some(DEFAULT_CPU_PERIOD_US));
            assert_eq!(cg.memory_limit_bytes, Some(32 * 1024 * 1024));
        }
    }

    #[tokio::test]
    async fn test_create_pod_cgroup_unconstrained_with_overhead_writes_no_memory_max() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let mgr = CgroupManager::with_driver(dir.path(), CgroupDriver::Cgroupfs, false);
        let uid = PodUID::new("uid-cg-overhead");

        // Unconstrained pod (container_memory_bytes == 0) with non-zero RuntimeClass overhead
        // must NOT write memory.max.
        mgr.create_pod_cgroup_with_memory_limit(&QosClass::Burstable, &uid, 0, 16 * 1024 * 1024)
            .await
            .unwrap();

        let pod_dir = dir.path().join("kubepods/burstable/poduid-cg-overhead");
        assert!(pod_dir.exists());
        assert!(
            !pod_dir.join("memory.max").exists(),
            "unconstrained pod must not get a hard memory.max from runtime overhead alone"
        );
    }
}
