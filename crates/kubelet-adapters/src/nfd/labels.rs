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

//! NFD (Node Feature Discovery) label writer.
//!
//! After scanning node features, writes labels + extended resources to the Node
//! object via a PATCH to the API server.
//!
//! Labels use the prefix: feature.node.kubernetes.io/
//! Extended resources use the prefix: feature.node.kubernetes.io/ (capacity)

use super::{FeatureScanner, NodeFeatures};
use kubelet_core::error::{KubeletError, Result};
use std::collections::HashMap;
use tracing::info;

/// Convert detected node features into Kubernetes node labels.
pub fn features_to_labels(features: &NodeFeatures) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    let prefix = "feature.node.kubernetes.io";

    // CPU flags (subset of useful ones).
    for flag in &features.cpu_flags {
        let normalized = flag.replace('_', "-");
        labels.insert(format!("{}/cpu-{}", prefix, normalized), "true".to_string());
    }

    // Architecture.
    labels.insert(
        format!("{}/cpu-hardware_multithreading", prefix),
        (features.cpu_threads > features.cpu_cores).to_string(),
    );

    // Kernel version.
    labels.insert(
        format!("{}/kernel-version.full", prefix),
        features.kernel_version.clone(),
    );
    if let Some(major) = features.kernel_version.split('.').next() {
        labels.insert(
            format!("{}/kernel-version.major", prefix),
            major.to_string(),
        );
    }

    // OS.
    labels.insert(
        format!("{}/system-os_release.ID", prefix),
        features.os_image.clone(),
    );

    // GPU presence.
    if features.nvidia_gpu_count > 0 {
        labels.insert("nvidia.com/gpu.present".to_string(), "true".to_string());
        labels.insert(
            "nvidia.com/gpu.count".to_string(),
            features.nvidia_gpu_count.to_string(),
        );
    }

    // Hugepages.
    for (size_kb, count) in &features.hugepages {
        if *count > 0 {
            labels.insert(
                format!("{}/hugepages-{}kB", prefix, size_kb),
                "true".to_string(),
            );
        }
    }

    labels
}

/// Convert features to extended resource capacity claims.
pub fn features_to_extended_resources(features: &NodeFeatures) -> HashMap<String, u64> {
    let mut resources = HashMap::new();

    if features.nvidia_gpu_count > 0 {
        resources.insert(
            "nvidia.com/gpu".to_string(),
            features.nvidia_gpu_count as u64,
        );
    }

    for (size_kb, count) in &features.hugepages {
        if *count > 0 {
            resources.insert(format!("hugepages-{}kB", size_kb), *count);
        }
    }

    resources
}

/// Write node labels to the Kubernetes API server.
pub async fn write_node_labels(
    node_name: &str,
    labels: &HashMap<String, String>,
    client: Option<&kube::Client>,
) -> Result<()> {
    let Some(client) = client else {
        info!(node = %node_name, labels = labels.len(), "Standalone: skip label PATCH");
        return Ok(());
    };

    use k8s_openapi::api::core::v1::Node;
    use kube::api::{Api, Patch, PatchParams};

    let label_map: serde_json::Map<String, serde_json::Value> = labels
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();

    let patch = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": node_name,
            "labels": label_map
        }
    });

    let nodes: Api<Node> = Api::all(client.clone());
    nodes
        .patch(
            node_name,
            &PatchParams::apply("kubelet-nfd").force(),
            &Patch::Apply(patch),
        )
        .await
        .map_err(|e| KubeletError::NodeStatus(format!("PATCH node labels: {}", e)))?;

    info!(node = %node_name, labels = labels.len(), "Node labels written via NFD");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::NodeFeatures;
    use super::*;

    fn sample_features() -> NodeFeatures {
        NodeFeatures {
            cpu_flags: vec!["avx2".to_string(), "aes".to_string(), "sse4_2".to_string()],
            cpu_cores: 4,
            cpu_threads: 8,
            cpu_model: "Intel Core i7".to_string(),
            memory_bytes: 16 * 1024 * 1024 * 1024,
            hugepages: [("2048".to_string(), 10)].into_iter().collect(),
            kernel_version: "6.8.0-100-generic".to_string(),
            os_image: "ubuntu".to_string(),
            architecture: "x86_64".to_string(),
            nvidia_gpu_count: 2,
            container_runtime_version: "containerd://1.7.0".to_string(),
            extended_resources: Default::default(),
            labels: Default::default(),
        }
    }

    #[test]
    fn test_features_to_labels_cpu_flags() {
        let features = sample_features();
        let labels = features_to_labels(&features);
        assert!(labels.contains_key("feature.node.kubernetes.io/cpu-avx2"));
        assert_eq!(labels["feature.node.kubernetes.io/cpu-avx2"], "true");
    }

    #[test]
    fn test_features_to_labels_kernel_version() {
        let features = sample_features();
        let labels = features_to_labels(&features);
        assert!(labels.contains_key("feature.node.kubernetes.io/kernel-version.full"));
        assert!(labels.contains_key("feature.node.kubernetes.io/kernel-version.major"));
        assert_eq!(
            labels["feature.node.kubernetes.io/kernel-version.major"],
            "6"
        );
    }

    #[test]
    fn test_features_to_labels_gpu() {
        let features = sample_features();
        let labels = features_to_labels(&features);
        assert_eq!(labels["nvidia.com/gpu.present"], "true");
        assert_eq!(labels["nvidia.com/gpu.count"], "2");
    }

    #[test]
    fn test_features_to_labels_hugepages() {
        let features = sample_features();
        let labels = features_to_labels(&features);
        assert!(labels.contains_key("feature.node.kubernetes.io/hugepages-2048kB"));
    }

    #[test]
    fn test_features_to_extended_resources() {
        let features = sample_features();
        let resources = features_to_extended_resources(&features);
        assert_eq!(resources["nvidia.com/gpu"], 2);
        assert_eq!(resources["hugepages-2048kB"], 10);
    }

    #[test]
    fn test_multithreading_label() {
        let features = sample_features(); // 4 cores, 8 threads -> HT = true
        let labels = features_to_labels(&features);
        assert_eq!(
            labels["feature.node.kubernetes.io/cpu-hardware_multithreading"],
            "true"
        );
    }
}
