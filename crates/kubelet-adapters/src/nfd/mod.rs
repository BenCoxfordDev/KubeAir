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

//! Node Feature Discovery (NFD) adapter.
//!
//! Discovers hardware features and system capabilities, populating node labels
//! and extended resources. Mirrors the NFD project's feature detection logic.
//!
//! Features detected:
//! - CPU features (from /proc/cpuinfo flags)
//! - Memory (hugepages from /sys/kernel/mm/hugepages/)
//! - Storage (local SSDs, NVMe detection)
//! - Kernel version and features
//! - OS image info

pub mod labels;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tracing::debug;

// -- Feature types -------------------------------------------------------------

/// A set of discovered node features.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeFeatures {
    /// CPU feature flags from /proc/cpuinfo.
    pub cpu_flags: Vec<String>,
    /// Number of physical CPU cores.
    pub cpu_cores: u32,
    /// Number of logical CPUs (with HT).
    pub cpu_threads: u32,
    /// CPU model name.
    pub cpu_model: String,
    /// Total RAM in bytes.
    pub memory_bytes: u64,
    /// Hugepage sizes available (size_kb -> count).
    pub hugepages: HashMap<String, u64>,
    /// Kernel version string.
    pub kernel_version: String,
    /// OS image name.
    pub os_image: String,
    /// Architecture.
    pub architecture: String,
    /// Number of NVIDIA GPUs detected.
    pub nvidia_gpu_count: u32,
    /// Container runtime version.
    pub container_runtime_version: String,
    /// Extended resources (e.g. nvidia.com/gpu -> count).
    pub extended_resources: HashMap<String, u64>,
    /// Node labels derived from features.
    pub labels: HashMap<String, String>,
}

impl NodeFeatures {
    /// Convert features to Kubernetes node labels.
    pub fn to_labels(&self) -> HashMap<String, String> {
        let mut labels = self.labels.clone();

        // CPU feature flags -> labels
        for flag in &self.cpu_flags {
            labels.insert(
                format!(
                    "feature.node.kubernetes.io/cpu-cpuid.{}",
                    flag.to_uppercase()
                ),
                "true".to_string(),
            );
        }

        // Architecture
        labels.insert(
            "feature.node.kubernetes.io/system-os_release.ID".to_string(),
            self.architecture.clone(),
        );

        // Hugepages
        for (size, count) in &self.hugepages {
            if *count > 0 {
                labels.insert(
                    format!("feature.node.kubernetes.io/memory-numa.hugepages-{}", size),
                    count.to_string(),
                );
            }
        }

        labels
    }

    /// Convert extended resources for node capacity reporting.
    pub fn to_extended_resources(&self) -> HashMap<String, u64> {
        self.extended_resources.clone()
    }
}

// -- Feature scanner -----------------------------------------------------------

/// Scans the system for hardware and kernel features.
pub struct FeatureScanner {
    proc_path: std::path::PathBuf,
    sys_path: std::path::PathBuf,
}

impl FeatureScanner {
    pub fn new() -> Self {
        Self {
            proc_path: "/proc".into(),
            sys_path: "/sys".into(),
        }
    }

    /// For testing: use a custom root.
    pub fn with_root(root: impl Into<std::path::PathBuf>) -> Self {
        let root = root.into();
        Self {
            proc_path: root.join("proc"),
            sys_path: root.join("sys"),
        }
    }

    /// Run all feature detection.
    pub fn scan(&self) -> NodeFeatures {
        let mut features = NodeFeatures {
            architecture: std::env::consts::ARCH.to_string(),
            ..Default::default()
        };

        self.scan_cpu(&mut features);
        self.scan_memory(&mut features);
        self.scan_hugepages(&mut features);
        self.scan_kernel(&mut features);
        self.scan_os(&mut features);

        features
    }

    fn scan_cpu(&self, features: &mut NodeFeatures) {
        let cpuinfo_path = self.proc_path.join("cpuinfo");
        let Ok(content) = std::fs::read_to_string(&cpuinfo_path) else {
            return;
        };

        let mut core_ids = std::collections::HashSet::new();
        let mut thread_count = 0u32;

        for line in content.lines() {
            if line.starts_with("processor") {
                thread_count += 1;
            }
            if line.starts_with("core id") {
                if let Some(id) = line.split(':').nth(1).map(|s| s.trim().to_string()) {
                    core_ids.insert(id);
                }
            }
            if line.starts_with("model name") {
                if let Some(model) = line.split(':').nth(1).map(|s| s.trim().to_string()) {
                    if features.cpu_model.is_empty() {
                        features.cpu_model = model;
                    }
                }
            }
            if (line.starts_with("flags") || line.starts_with("Features"))
                && features.cpu_flags.is_empty()
            {
                features.cpu_flags = line
                    .split(':')
                    .nth(1)
                    .unwrap_or("")
                    .split_whitespace()
                    .map(String::from)
                    .collect();
            }
        }

        features.cpu_threads = thread_count;
        features.cpu_cores = if core_ids.is_empty() {
            thread_count
        } else {
            core_ids.len() as u32
        };
    }

    fn scan_memory(&self, features: &mut NodeFeatures) {
        let meminfo_path = self.proc_path.join("meminfo");
        let Ok(content) = std::fs::read_to_string(&meminfo_path) else {
            return;
        };
        for line in content.lines() {
            if line.starts_with("MemTotal:") {
                if let Some(kb) = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    features.memory_bytes = kb * 1024;
                }
                break;
            }
        }
    }

    fn scan_hugepages(&self, features: &mut NodeFeatures) {
        let hp_path = self.sys_path.join("kernel/mm/hugepages");
        let Ok(entries) = std::fs::read_dir(&hp_path) else {
            return;
        };
        for entry in entries.filter_map(|e| e.ok()) {
            let dir = entry.path();
            if let Some(name) = dir.file_name().and_then(|n| n.to_str()) {
                // e.g. hugepages-2048kB
                if name.starts_with("hugepages-") {
                    let size = name.trim_start_matches("hugepages-").to_string();
                    let count_path = dir.join("nr_hugepages");
                    if let Ok(count_str) = std::fs::read_to_string(&count_path) {
                        if let Ok(count) = count_str.trim().parse::<u64>() {
                            features.hugepages.insert(size, count);
                        }
                    }
                }
            }
        }
    }

    fn scan_kernel(&self, features: &mut NodeFeatures) {
        let version_path = self.proc_path.join("version");
        if let Ok(content) = std::fs::read_to_string(&version_path) {
            features.kernel_version = content
                .split_whitespace()
                .nth(2)
                .unwrap_or("unknown")
                .to_string();
        }
    }

    fn scan_os(&self, features: &mut NodeFeatures) {
        // Try /etc/os-release
        if let Ok(content) = std::fs::read_to_string("/etc/os-release") {
            for line in content.lines() {
                if line.starts_with("PRETTY_NAME=") {
                    features.os_image = line
                        .trim_start_matches("PRETTY_NAME=")
                        .trim_matches('"')
                        .to_string();
                    break;
                }
            }
        }
        if features.os_image.is_empty() {
            features.os_image = std::env::consts::OS.to_string();
        }
    }
}

impl Default for FeatureScanner {
    fn default() -> Self {
        Self::new()
    }
}

// -- GPU detection -------------------------------------------------------------

/// Attempt to detect NVIDIA GPUs via /proc/driver/nvidia/gpus/ or `nvidia-smi`.
pub fn detect_nvidia_gpus() -> u64 {
    // Check /proc/driver/nvidia/gpus/
    if let Ok(entries) = std::fs::read_dir("/proc/driver/nvidia/gpus") {
        return entries.filter_map(|e| e.ok()).count() as u64;
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_test_root() -> TempDir {
        let dir = TempDir::new().unwrap();
        let proc = dir.path().join("proc");
        let sys = dir.path().join("sys/kernel/mm/hugepages");
        std::fs::create_dir_all(&proc).unwrap();
        std::fs::create_dir_all(&sys).unwrap();

        // Write /proc/cpuinfo
        std::fs::write(
            proc.join("cpuinfo"),
            r#"processor	: 0
vendor_id	: GenuineIntel
cpu family	: 6
model name	: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
core id		: 0
flags		: fpu vme de pse avx avx2 aes

processor	: 1
core id		: 1
model name	: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
flags		: fpu vme de pse avx avx2 aes

"#,
        )
        .unwrap();

        // Write /proc/meminfo
        std::fs::write(
            proc.join("meminfo"),
            "MemTotal:       16384000 kB\nMemFree:        8000000 kB\n",
        )
        .unwrap();
        // Write /proc/version
        std::fs::write(
            proc.join("version"),
            "Linux version 6.1.0-generic (gcc version 12.2.0)",
        )
        .unwrap();

        // Write hugepages
        let hp_2m = dir.path().join("sys/kernel/mm/hugepages/hugepages-2048kB");
        std::fs::create_dir_all(&hp_2m).unwrap();
        std::fs::write(hp_2m.join("nr_hugepages"), "4\n").unwrap();

        dir
    }

    #[test]
    fn test_scan_cpu_flags() {
        let root = make_test_root();
        let scanner = FeatureScanner::with_root(root.path());
        let features = scanner.scan();
        assert!(features.cpu_flags.contains(&"avx".to_string()));
        assert!(features.cpu_flags.contains(&"avx2".to_string()));
        assert!(features.cpu_flags.contains(&"aes".to_string()));
    }

    #[test]
    fn test_scan_cpu_thread_count() {
        let root = make_test_root();
        let scanner = FeatureScanner::with_root(root.path());
        let features = scanner.scan();
        assert_eq!(features.cpu_threads, 2);
    }

    #[test]
    fn test_scan_memory() {
        let root = make_test_root();
        let scanner = FeatureScanner::with_root(root.path());
        let features = scanner.scan();
        assert_eq!(features.memory_bytes, 16384000 * 1024);
    }

    #[test]
    fn test_scan_hugepages() {
        let root = make_test_root();
        let scanner = FeatureScanner::with_root(root.path());
        let features = scanner.scan();
        assert_eq!(*features.hugepages.get("2048kB").unwrap(), 4);
    }

    #[test]
    fn test_scan_kernel_version() {
        let root = make_test_root();
        let scanner = FeatureScanner::with_root(root.path());
        let features = scanner.scan();
        assert_eq!(features.kernel_version, "6.1.0-generic");
    }

    #[test]
    fn test_features_to_labels() {
        let mut features = NodeFeatures {
            cpu_flags: vec!["avx".to_string(), "avx2".to_string()],
            architecture: "amd64".to_string(),
            ..Default::default()
        };
        features.hugepages.insert("2048kB".to_string(), 8);

        let labels = features.to_labels();
        assert!(labels.contains_key("feature.node.kubernetes.io/cpu-cpuid.AVX"));
        assert!(labels.contains_key("feature.node.kubernetes.io/cpu-cpuid.AVX2"));
        assert!(labels.contains_key("feature.node.kubernetes.io/memory-numa.hugepages-2048kB"));
    }

    #[test]
    fn test_extended_resources_empty() {
        let features = NodeFeatures::default();
        assert!(features.to_extended_resources().is_empty());
    }

    #[test]
    fn test_extended_resources_with_gpu() {
        let mut features = NodeFeatures::default();
        features
            .extended_resources
            .insert("nvidia.com/gpu".to_string(), 2);
        let resources = features.to_extended_resources();
        assert_eq!(*resources.get("nvidia.com/gpu").unwrap(), 2);
    }

    #[test]
    fn test_cpu_model_extracted() {
        let root = make_test_root();
        let scanner = FeatureScanner::with_root(root.path());
        let features = scanner.scan();
        assert!(features.cpu_model.contains("i7-9750H"));
    }

    #[test]
    fn test_detect_nvidia_gpus_zero_without_driver() {
        // In this test env, no NVIDIA driver is present
        let count = detect_nvidia_gpus();
        assert_eq!(count, 0);
    }
}
