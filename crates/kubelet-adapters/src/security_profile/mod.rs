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

//! Security profile enforcement -- seccomp and AppArmor.
//!
//! Mirrors pkg/security/ in the Go kubelet.
//!
//! Seccomp:
//!   Filters system calls made by containers.
//!   Profiles: "RuntimeDefault", "Unconfined", "Localhost/<path>".
//!   Applied via CRI SecurityContext.seccomp_profile.
//!
//! AppArmor:
//!   Mandatory access control via Linux kernel AppArmor LSM.
//!   Applied via CRI SecurityContext.apparmor_profile or container annotations
//!   (legacy: container.apparmor.security.beta.kubernetes.io/<name>).

use kubelet_core::error::{KubeletError, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

// -- Seccomp -------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SeccompProfileType {
    /// No seccomp profile applied.
    Unconfined,
    /// Use the container runtime's default seccomp profile.
    RuntimeDefault,
    /// Use a custom profile from the kubelet's seccomp profile root.
    Localhost(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeccompProfile {
    pub profile_type: SeccompProfileType,
}

impl SeccompProfile {
    pub fn runtime_default() -> Self {
        Self {
            profile_type: SeccompProfileType::RuntimeDefault,
        }
    }

    pub fn unconfined() -> Self {
        Self {
            profile_type: SeccompProfileType::Unconfined,
        }
    }

    pub fn localhost(path: impl Into<String>) -> Self {
        Self {
            profile_type: SeccompProfileType::Localhost(path.into()),
        }
    }

    /// Convert to the CRI seccomp profile type integer.
    pub fn to_cri_type(&self) -> i32 {
        match &self.profile_type {
            SeccompProfileType::Unconfined => 0,
            SeccompProfileType::RuntimeDefault => 1,
            SeccompProfileType::Localhost(_) => 2,
        }
    }
}

pub struct SeccompEnforcer {
    /// Root directory for localhost seccomp profiles.
    profile_root: PathBuf,
}

impl SeccompEnforcer {
    pub fn new(profile_root: impl Into<PathBuf>) -> Self {
        Self {
            profile_root: profile_root.into(),
        }
    }

    /// Resolve and validate a seccomp profile.
    /// For Localhost profiles, checks the file exists in profile_root.
    pub fn validate(&self, profile: &SeccompProfile) -> Result<()> {
        if let SeccompProfileType::Localhost(path) = &profile.profile_type {
            // Prevent path traversal.
            if path.contains("..") {
                return Err(KubeletError::Security(format!(
                    "seccomp localhost profile path '{}' contains '..'",
                    path
                )));
            }
            let full_path = self.profile_root.join(path);
            if !full_path.exists() {
                return Err(KubeletError::Security(format!(
                    "seccomp localhost profile not found: {}",
                    full_path.display()
                )));
            }
            debug!(path = %full_path.display(), "Validated localhost seccomp profile");
        }
        Ok(())
    }

    /// Load the seccomp profile JSON for a Localhost profile.
    pub fn load_profile(&self, path: &str) -> Result<String> {
        if path.contains("..") {
            return Err(KubeletError::Security(
                "path traversal in seccomp profile path".to_string(),
            ));
        }
        let full_path = self.profile_root.join(path);
        std::fs::read_to_string(&full_path).map_err(|e| {
            KubeletError::Security(format!(
                "read seccomp profile '{}': {}",
                full_path.display(),
                e
            ))
        })
    }

    /// Determine the effective seccomp profile for a container.
    /// Pod-level profile is used if container-level is not set.
    pub fn effective_profile(
        container: Option<&SeccompProfile>,
        pod: Option<&SeccompProfile>,
    ) -> SeccompProfile {
        container
            .or(pod)
            .cloned()
            .unwrap_or(SeccompProfile::runtime_default())
    }
}

// -- AppArmor ------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AppArmorProfile {
    /// No AppArmor profile.
    Unconfined,
    /// Use the container runtime's default AppArmor profile.
    RuntimeDefault,
    /// Use a named AppArmor profile loaded on the node.
    Localhost(String),
}

impl AppArmorProfile {
    /// Parse from a legacy annotation value like:
    ///   "runtime/default" | "unconfined" | "localhost/<profile-name>"
    pub fn from_annotation(s: &str) -> Self {
        match s {
            "unconfined" => Self::Unconfined,
            "runtime/default" => Self::RuntimeDefault,
            s if s.starts_with("localhost/") => {
                Self::Localhost(s["localhost/".len()..].to_string())
            }
            _ => Self::RuntimeDefault,
        }
    }

    /// Render to a CRI apparmor profile string.
    pub fn to_cri_string(&self) -> String {
        match self {
            Self::Unconfined => "unconfined".to_string(),
            Self::RuntimeDefault => "runtime/default".to_string(),
            Self::Localhost(name) => format!("localhost/{}", name),
        }
    }
}

pub struct AppArmorEnforcer;

impl AppArmorEnforcer {
    /// Check if AppArmor is enabled on this kernel.
    pub fn is_enabled() -> bool {
        std::path::Path::new("/sys/kernel/security/apparmor").exists()
    }

    /// Check if a named AppArmor profile is loaded.
    pub fn profile_is_loaded(profile_name: &str) -> bool {
        // Profiles are listed in /sys/kernel/security/apparmor/profiles.
        if let Ok(content) = std::fs::read_to_string("/sys/kernel/security/apparmor/profiles") {
            content
                .lines()
                .any(|line| line.split_whitespace().next() == Some(profile_name))
        } else {
            false
        }
    }

    /// Validate an AppArmor profile for a container.
    pub fn validate(profile: &AppArmorProfile) -> Result<()> {
        if !Self::is_enabled() {
            match profile {
                AppArmorProfile::Unconfined | AppArmorProfile::RuntimeDefault => return Ok(()),
                AppArmorProfile::Localhost(name) => {
                    warn!(profile = %name, "AppArmor not enabled on this node; localhost profile cannot be applied");
                    return Ok(()); // Non-fatal: warn but allow.
                }
            }
        }
        if let AppArmorProfile::Localhost(name) = profile {
            if !Self::profile_is_loaded(name) {
                return Err(KubeletError::Security(format!(
                    "AppArmor profile '{}' is not loaded on this node",
                    name
                )));
            }
        }
        Ok(())
    }
}

// -- In-place pod resource resize (k8s 1.27+) ---------------------------------

/// Tracks the resize state for a pod's containers.
/// Mirrors pkg/kubelet/resize/ -- ResourcesSpec vs ResourcesStatus comparison.

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ResizeStatus {
    /// Resources are at the desired level.
    Proposed,
    /// Resize is in progress.
    InProgress,
    /// Resize could not be completed (resource unavailable).
    Deferred,
    /// Resize is infeasible.
    Infeasible,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerResizeRequest {
    pub container_name: String,
    pub cpu_request: Option<String>,
    pub cpu_limit: Option<String>,
    pub memory_request: Option<String>,
    pub memory_limit: Option<String>,
}

pub struct ResizeManager;

impl ResizeManager {
    /// Compute the diff between desired and current resources for a container.
    pub fn needs_resize(
        desired_cpu: Option<&str>,
        current_cpu: Option<&str>,
        desired_mem: Option<&str>,
        current_mem: Option<&str>,
    ) -> bool {
        desired_cpu != current_cpu || desired_mem != current_mem
    }

    /// Apply a resize via CRI UpdateContainerResources.
    /// Returns ResizeStatus indicating the outcome.
    pub fn apply_resize(req: &ContainerResizeRequest) -> ResizeStatus {
        // In a real implementation: call cri_client.update_container_resources(...)
        // Then update the cgroup via cgroup_manager.apply_resources(...)
        info!(
            container = %req.container_name,
            cpu_limit = ?req.cpu_limit,
            memory_limit = ?req.memory_limit,
            "Applying in-place container resize"
        );
        ResizeStatus::InProgress
    }
}

// -- Hugepages -----------------------------------------------------------------

/// Hugepage size supported by the kernel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HugepageSize {
    pub size_kb: u64,
    pub total_pages: u64,
    pub free_pages: u64,
}

impl HugepageSize {
    pub fn size_bytes(&self) -> u64 {
        self.size_kb * 1024
    }
    pub fn total_bytes(&self) -> u64 {
        self.size_kb * 1024 * self.total_pages
    }
    pub fn free_bytes(&self) -> u64 {
        self.size_kb * 1024 * self.free_pages
    }
}

/// Discover hugepage sizes from /sys/kernel/mm/hugepages/.
pub fn discover_hugepages() -> Vec<HugepageSize> {
    let base = std::path::Path::new("/sys/kernel/mm/hugepages");
    if !base.exists() {
        return vec![];
    }
    let mut result = vec![];
    let Ok(entries) = std::fs::read_dir(base) else {
        return vec![];
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Directory names like "hugepages-2048kB"
        if !name.starts_with("hugepages-") {
            continue;
        }
        let size_str = &name["hugepages-".len()..];
        let size_kb = parse_hugepage_size_kb(size_str);
        if size_kb == 0 {
            continue;
        }
        let path = entry.path();
        let total_pages = read_hugepage_count(&path, "nr_hugepages");
        let free_pages = read_hugepage_count(&path, "free_hugepages");
        result.push(HugepageSize {
            size_kb,
            total_pages,
            free_pages,
        });
    }
    result.sort_by_key(|h| h.size_kb);
    result
}

fn parse_hugepage_size_kb(s: &str) -> u64 {
    if let Some(n) = s.strip_suffix("kB") {
        n.parse().unwrap_or(0)
    } else if let Some(n) = s.strip_suffix("MB") {
        n.parse::<u64>().unwrap_or(0) * 1024
    } else if let Some(n) = s.strip_suffix("GB") {
        n.parse::<u64>().unwrap_or(0) * 1024 * 1024
    } else {
        0
    }
}

fn read_hugepage_count(dir: &std::path::Path, file: &str) -> u64 {
    std::fs::read_to_string(dir.join(file))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

// -- Swap support --------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SwapBehavior {
    /// No swap (default, fail_swap_on = true).
    NoSwap,
    /// Allow limited swap for Burstable pods.
    LimitedSwap,
    /// Allow unlimited swap (not recommended).
    UnlimitedSwap,
}

pub struct SwapManager {
    behavior: SwapBehavior,
}

impl SwapManager {
    pub fn new(behavior: SwapBehavior, fail_swap_on: bool) -> Result<Self> {
        let swap_available = Self::is_swap_available();
        if fail_swap_on && swap_available {
            return Err(KubeletError::Config(
                "Swap is enabled on this node; set failSwapOn=false to allow".to_string(),
            ));
        }
        Ok(Self { behavior })
    }

    pub fn is_swap_available() -> bool {
        std::fs::read_to_string("/proc/swaps")
            .map(|s| s.lines().count() > 1) // header line + at least one swap
            .unwrap_or(false)
    }

    /// Compute memory.swap.max for a container cgroup (cgroup v2).
    pub fn swap_limit_bytes(
        &self,
        memory_limit_bytes: i64,
        _qos: kubelet_core::qos::QosClass,
    ) -> i64 {
        match &self.behavior {
            SwapBehavior::NoSwap => 0,
            SwapBehavior::LimitedSwap => {
                // Allow swap = memory_limit (2x total = memory + swap).
                memory_limit_bytes
            }
            SwapBehavior::UnlimitedSwap => i64::MAX,
        }
    }

    pub fn behavior(&self) -> &SwapBehavior {
        &self.behavior
    }
}

// -- Pod readiness gates -------------------------------------------------------

/// A readiness gate condition -- must be True before the pod is Ready.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessGate {
    pub condition_type: String,
}

/// Check if all readiness gates are satisfied.
pub fn all_readiness_gates_met(
    gates: &[ReadinessGate],
    pod_conditions: &std::collections::HashMap<String, bool>,
) -> bool {
    gates.iter().all(|gate| {
        pod_conditions
            .get(&gate.condition_type)
            .copied()
            .unwrap_or(false)
    })
}

// -- Tests ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // -- Seccomp tests ---------------------------------------------------------

    #[test]
    fn test_seccomp_profile_cri_types() {
        assert_eq!(SeccompProfile::unconfined().to_cri_type(), 0);
        assert_eq!(SeccompProfile::runtime_default().to_cri_type(), 1);
        assert_eq!(SeccompProfile::localhost("audit.json").to_cri_type(), 2);
    }

    #[test]
    fn test_seccomp_enforcer_rejects_path_traversal() {
        let dir = TempDir::new().unwrap();
        let enforcer = SeccompEnforcer::new(dir.path());
        let profile = SeccompProfile::localhost("../../etc/passwd");
        assert!(enforcer.validate(&profile).is_err());
    }

    #[test]
    fn test_seccomp_enforcer_accepts_runtime_default() {
        let dir = TempDir::new().unwrap();
        let enforcer = SeccompEnforcer::new(dir.path());
        assert!(enforcer
            .validate(&SeccompProfile::runtime_default())
            .is_ok());
    }

    #[test]
    fn test_seccomp_effective_profile_container_wins() {
        let container = SeccompProfile::unconfined();
        let pod = SeccompProfile::runtime_default();
        let effective = SeccompEnforcer::effective_profile(Some(&container), Some(&pod));
        assert_eq!(effective.profile_type, SeccompProfileType::Unconfined);
    }

    #[test]
    fn test_seccomp_effective_profile_falls_back_to_pod() {
        let pod = SeccompProfile::runtime_default();
        let effective = SeccompEnforcer::effective_profile(None, Some(&pod));
        assert_eq!(effective.profile_type, SeccompProfileType::RuntimeDefault);
    }

    // -- AppArmor tests --------------------------------------------------------

    #[test]
    fn test_apparmor_from_annotation_unconfined() {
        let p = AppArmorProfile::from_annotation("unconfined");
        assert_eq!(p, AppArmorProfile::Unconfined);
    }

    #[test]
    fn test_apparmor_from_annotation_localhost() {
        let p = AppArmorProfile::from_annotation("localhost/my-profile");
        assert_eq!(p, AppArmorProfile::Localhost("my-profile".to_string()));
    }

    #[test]
    fn test_apparmor_to_cri_string() {
        assert_eq!(AppArmorProfile::Unconfined.to_cri_string(), "unconfined");
        assert_eq!(
            AppArmorProfile::RuntimeDefault.to_cri_string(),
            "runtime/default"
        );
        assert_eq!(
            AppArmorProfile::Localhost("test".to_string()).to_cri_string(),
            "localhost/test"
        );
    }

    // -- Readiness gates -------------------------------------------------------

    #[test]
    fn test_all_readiness_gates_met_when_all_true() {
        let gates = vec![
            ReadinessGate {
                condition_type: "gate-1".to_string(),
            },
            ReadinessGate {
                condition_type: "gate-2".to_string(),
            },
        ];
        let conditions = [("gate-1".to_string(), true), ("gate-2".to_string(), true)]
            .into_iter()
            .collect();
        assert!(all_readiness_gates_met(&gates, &conditions));
    }

    #[test]
    fn test_readiness_gates_not_met_when_one_false() {
        let gates = vec![
            ReadinessGate {
                condition_type: "gate-1".to_string(),
            },
            ReadinessGate {
                condition_type: "gate-2".to_string(),
            },
        ];
        let conditions = [("gate-1".to_string(), true), ("gate-2".to_string(), false)]
            .into_iter()
            .collect();
        assert!(!all_readiness_gates_met(&gates, &conditions));
    }

    #[test]
    fn test_readiness_gates_not_met_when_missing() {
        let gates = vec![ReadinessGate {
            condition_type: "missing-gate".to_string(),
        }];
        let conditions = std::collections::HashMap::new();
        assert!(!all_readiness_gates_met(&gates, &conditions));
    }

    // -- Resize ----------------------------------------------------------------

    #[test]
    fn test_needs_resize_detects_change() {
        assert!(ResizeManager::needs_resize(
            Some("500m"),
            Some("250m"),
            None,
            None
        ));
    }

    #[test]
    fn test_no_resize_when_equal() {
        assert!(!ResizeManager::needs_resize(
            Some("500m"),
            Some("500m"),
            None,
            None
        ));
    }

    // -- Swap -----------------------------------------------------------------

    #[test]
    fn test_swap_limit_no_swap() {
        let mgr = SwapManager {
            behavior: SwapBehavior::NoSwap,
        };
        assert_eq!(
            mgr.swap_limit_bytes(512 * 1024 * 1024, kubelet_core::qos::QosClass::Burstable),
            0
        );
    }

    #[test]
    fn test_swap_limit_limited() {
        let mgr = SwapManager {
            behavior: SwapBehavior::LimitedSwap,
        };
        assert_eq!(
            mgr.swap_limit_bytes(256 * 1024 * 1024, kubelet_core::qos::QosClass::Burstable),
            256 * 1024 * 1024
        );
    }

    // -- Hugepages -------------------------------------------------------------

    #[test]
    fn test_parse_hugepage_size_kb() {
        assert_eq!(parse_hugepage_size_kb("2048kB"), 2048);
        assert_eq!(parse_hugepage_size_kb("1MB"), 1024);
        assert_eq!(parse_hugepage_size_kb("1GB"), 1024 * 1024);
    }

    #[test]
    fn test_hugepage_size_bytes() {
        let h = HugepageSize {
            size_kb: 2048,
            total_pages: 10,
            free_pages: 5,
        };
        assert_eq!(h.size_bytes(), 2 * 1024 * 1024);
        assert_eq!(h.total_bytes(), 20 * 1024 * 1024);
        assert_eq!(h.free_bytes(), 10 * 1024 * 1024);
    }
}
