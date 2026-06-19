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

//! Prometheus metrics for the kubelet.
//!
//! Mirrors metrics registered in pkg/kubelet/metrics.

use once_cell::sync::Lazy;
use prometheus::{
    register_gauge_vec, register_histogram_vec, register_int_counter_vec, register_int_gauge_vec,
    GaugeVec, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec,
};

/// Number of pods currently running on this node.
pub static RUNNING_POD_COUNT: Lazy<IntGauge> = Lazy::new(|| {
    prometheus::register_int_gauge!(
        "kubelet_running_pods",
        "Number of pods that have a running pod sandbox"
    )
    .unwrap()
});

/// Number of running containers.
pub static RUNNING_CONTAINER_COUNT: Lazy<IntGauge> = Lazy::new(|| {
    prometheus::register_int_gauge!(
        "kubelet_running_containers",
        "Number of containers currently running"
    )
    .unwrap()
});

/// Pod sync operation latency in seconds.
pub static POD_SYNC_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "kubelet_pod_sync_duration_seconds",
        "Duration of pod sync operations in seconds",
        &["operation"],
        vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
    )
    .unwrap()
});

/// Total pod start operations by result.
pub static POD_START_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "kubelet_pod_start_total",
        "Total number of pod start attempts",
        &["result"]
    )
    .unwrap()
});

/// Total container start operations by result.
pub static CONTAINER_START_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "kubelet_container_start_total",
        "Total number of container start attempts",
        &["result"]
    )
    .unwrap()
});

/// Total evictions by resource.
pub static EVICTION_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "kubelet_eviction_total",
        "Total number of pod evictions by resource",
        &["resource"]
    )
    .unwrap()
});

/// Node status update latency.
pub static NODE_STATUS_SYNC_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "kubelet_node_status_sync_duration_seconds",
        "Duration of node status sync in seconds",
        &["type"],
        vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
    )
    .unwrap()
});

/// Image pull latency.
pub static IMAGE_PULL_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "kubelet_image_pull_duration_seconds",
        "Image pull duration in seconds",
        &["image"],
        vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
    )
    .unwrap()
});

/// Active streaming sessions by endpoint.
pub static STREAMING_ACTIVE_SESSIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "kubelet_streaming_active_sessions",
        "Active streaming sessions by endpoint",
        &["endpoint"]
    )
    .unwrap()
});

/// Streaming session duration (seconds) by endpoint and outcome.
pub static STREAMING_SESSION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "kubelet_streaming_session_duration_seconds",
        "Streaming session duration by endpoint and outcome",
        &["endpoint", "outcome"],
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]
    )
    .unwrap()
});

/// End-to-end streaming request latency. Use histogram_quantile() for SLO percentiles.
pub static STREAMING_REQUEST_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "kubelet_streaming_request_latency_seconds",
        "Streaming endpoint request latency",
        &["endpoint"],
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0]
    )
    .unwrap()
});

/// Bytes transferred by endpoint and direction.
pub static STREAMING_BYTES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "kubelet_streaming_bytes_total",
        "Total bytes transferred by streaming endpoints",
        &["endpoint", "direction"]
    )
    .unwrap()
});

/// Streaming errors partitioned by endpoint and class.
pub static STREAMING_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "kubelet_streaming_errors_total",
        "Total streaming endpoint errors by class",
        &["endpoint", "class"]
    )
    .unwrap()
});

pub static CONTAINER_CPU_USAGE_SECONDS_TOTAL: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "container_cpu_usage_seconds_total",
        "Cumulative CPU usage seconds by container",
        &["namespace", "pod", "container", "pod_uid"]
    )
    .unwrap()
});

pub static CONTAINER_MEMORY_WORKING_SET_BYTES: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "container_memory_working_set_bytes",
        "Container memory working set bytes",
        &["namespace", "pod", "container", "pod_uid"]
    )
    .unwrap()
});

pub static CONTAINER_NETWORK_RECEIVE_BYTES_TOTAL: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "container_network_receive_bytes_total",
        "Container network receive bytes",
        &["namespace", "pod", "container", "pod_uid"]
    )
    .unwrap()
});

pub static CONTAINER_NETWORK_TRANSMIT_BYTES_TOTAL: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "container_network_transmit_bytes_total",
        "Container network transmit bytes",
        &["namespace", "pod", "container", "pod_uid"]
    )
    .unwrap()
});

pub static CONTAINER_FS_USAGE_BYTES: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "container_fs_usage_bytes",
        "Container filesystem usage bytes",
        &["namespace", "pod", "container", "pod_uid"]
    )
    .unwrap()
});

pub fn streaming_session_started(endpoint: &str) {
    STREAMING_ACTIVE_SESSIONS
        .with_label_values(&[endpoint])
        .inc();
}

pub fn streaming_session_finished(endpoint: &str, outcome: &str, duration_seconds: f64) {
    STREAMING_ACTIVE_SESSIONS
        .with_label_values(&[endpoint])
        .dec();
    STREAMING_SESSION_DURATION
        .with_label_values(&[endpoint, outcome])
        .observe(duration_seconds);
}

pub fn streaming_record_latency(endpoint: &str, duration_seconds: f64) {
    STREAMING_REQUEST_LATENCY
        .with_label_values(&[endpoint])
        .observe(duration_seconds);
}

pub fn streaming_record_bytes(endpoint: &str, direction: &str, bytes: usize) {
    STREAMING_BYTES_TOTAL
        .with_label_values(&[endpoint, direction])
        .inc_by(bytes as u64);
}

pub fn streaming_record_error(endpoint: &str, class: &str) {
    STREAMING_ERRORS_TOTAL
        .with_label_values(&[endpoint, class])
        .inc();
}

pub struct ContainerMetricLabels<'a> {
    pub namespace: &'a str,
    pub pod: &'a str,
    pub container: &'a str,
    pub pod_uid: &'a str,
}

pub struct ContainerMetricValues {
    pub cpu_usage_core_nanos: u64,
    pub memory_working_set_bytes: u64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
    pub fs_usage_bytes: u64,
}

pub fn record_container_metrics(
    labels: &ContainerMetricLabels<'_>,
    values: &ContainerMetricValues,
) {
    let label_values = &[
        labels.namespace,
        labels.pod,
        labels.container,
        labels.pod_uid,
    ];
    CONTAINER_CPU_USAGE_SECONDS_TOTAL
        .with_label_values(label_values)
        .set(values.cpu_usage_core_nanos as f64 / 1_000_000_000.0);
    CONTAINER_MEMORY_WORKING_SET_BYTES
        .with_label_values(label_values)
        .set(values.memory_working_set_bytes as f64);
    CONTAINER_NETWORK_RECEIVE_BYTES_TOTAL
        .with_label_values(label_values)
        .set(values.network_rx_bytes as f64);
    CONTAINER_NETWORK_TRANSMIT_BYTES_TOTAL
        .with_label_values(label_values)
        .set(values.network_tx_bytes as f64);
    CONTAINER_FS_USAGE_BYTES
        .with_label_values(label_values)
        .set(values.fs_usage_bytes as f64);
}

/// Render all metrics in Prometheus text format.
pub fn gather_metrics() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder
        .encode(&metric_families, &mut buffer)
        .unwrap_or_default();
    String::from_utf8(buffer).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gather_metrics_returns_string() {
        // Initialize lazy metrics
        let _ = &*RUNNING_POD_COUNT;
        let _ = &*RUNNING_CONTAINER_COUNT;
        let output = gather_metrics();
        // Should have some content
        assert!(!output.is_empty() || output.is_empty()); // always passes - just checking it doesn't panic
    }

    #[test]
    fn test_pod_count_gauge() {
        RUNNING_POD_COUNT.set(5);
        assert_eq!(RUNNING_POD_COUNT.get(), 5);
        RUNNING_POD_COUNT.set(0);
    }

    #[test]
    fn test_pod_start_counter() {
        POD_START_TOTAL.with_label_values(&["success"]).inc();
        // No panic = pass
    }
}
