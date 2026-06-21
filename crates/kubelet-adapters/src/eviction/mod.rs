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

//! Eviction manager adapter.
//!
//! Implements the kubelet eviction logic: monitors resource usage and evicts pods
//! when thresholds are exceeded.

pub mod manager;
pub mod pressure;

use kubelet_core::error::Result;
use std::collections::HashMap;
use tracing::{info, warn};

/// Eviction thresholds parsed from config strings like "100Mi", "10%".
#[derive(Debug, Clone)]
pub struct EvictionThreshold {
    pub resource: String,
    pub value: ThresholdValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ThresholdValue {
    /// Absolute bytes/quantity.
    Absolute(u64),
    /// Percentage of total capacity.
    Percentage(f64),
}

impl EvictionThreshold {
    /// Parse a threshold string like "100Mi" or "10%".
    pub fn parse(resource: &str, value: &str) -> Option<Self> {
        let trimmed = value.trim();
        if let Some(pct) = trimmed.strip_suffix('%') {
            pct.parse::<f64>().ok().map(|p| Self {
                resource: resource.to_string(),
                value: ThresholdValue::Percentage(p / 100.0),
            })
        } else {
            parse_quantity(trimmed).map(|b| Self {
                resource: resource.to_string(),
                value: ThresholdValue::Absolute(b),
            })
        }
    }

    /// Returns true if the current usage exceeds this threshold.
    pub fn is_exceeded(&self, current: u64, capacity: u64) -> bool {
        match self.value {
            ThresholdValue::Absolute(abs) => current < abs,
            ThresholdValue::Percentage(pct) => {
                let threshold_bytes = (capacity as f64 * pct) as u64;
                current < threshold_bytes
            }
        }
    }
}

/// Parse Kubernetes quantity strings: Ki, Mi, Gi, Ti, etc.
pub fn parse_quantity(s: &str) -> Option<u64> {
    let suffixes: &[(&str, u64)] = &[
        ("Ki", 1024),
        ("Mi", 1024 * 1024),
        ("Gi", 1024 * 1024 * 1024),
        ("Ti", 1024_u64.pow(4)),
        ("Pi", 1024_u64.pow(5)),
        ("K", 1000),
        ("M", 1000 * 1000),
        ("G", 1000_u64.pow(3)),
        ("T", 1000_u64.pow(4)),
    ];

    for (suffix, multiplier) in suffixes {
        if let Some(num_str) = s.strip_suffix(suffix)
            && let Ok(n) = num_str.parse::<f64>()
        {
            return Some((n * *multiplier as f64) as u64);
        }
    }

    s.parse::<u64>().ok()
}

/// Current node resource pressure state.
#[derive(Debug, Clone, Default)]
pub struct ResourcePressure {
    pub memory_pressure: bool,
    pub disk_pressure: bool,
    pub pid_pressure: bool,
}

/// Represents the available resources on a node.
#[derive(Debug, Clone)]
pub struct NodeResources {
    pub available_memory_bytes: u64,
    pub total_memory_bytes: u64,
    pub available_disk_bytes: u64,
    pub total_disk_bytes: u64,
    pub available_pids: u64,
    pub total_pids: u64,
}

/// Evaluates eviction thresholds against current resource state.
pub struct EvictionEvaluator {
    hard_thresholds: Vec<EvictionThreshold>,
    soft_thresholds: Vec<EvictionThreshold>,
}

impl EvictionEvaluator {
    pub fn new(hard: &HashMap<String, String>, soft: &HashMap<String, String>) -> Self {
        let hard_thresholds = hard
            .iter()
            .filter_map(|(k, v)| EvictionThreshold::parse(k, v))
            .collect();
        let soft_thresholds = soft
            .iter()
            .filter_map(|(k, v)| EvictionThreshold::parse(k, v))
            .collect();
        Self {
            hard_thresholds,
            soft_thresholds,
        }
    }

    /// Evaluate current resource usage and return pressure state.
    pub fn evaluate(&self, resources: &NodeResources) -> ResourcePressure {
        let mut pressure = ResourcePressure::default();

        for threshold in &self.hard_thresholds {
            let (current, capacity) = match threshold.resource.as_str() {
                "memory.available" => (
                    resources.available_memory_bytes,
                    resources.total_memory_bytes,
                ),
                "nodefs.available" => (resources.available_disk_bytes, resources.total_disk_bytes),
                "pid.available" => (resources.available_pids, resources.total_pids),
                _ => continue,
            };

            if threshold.is_exceeded(current, capacity) {
                match threshold.resource.as_str() {
                    "memory.available" => {
                        warn!("Memory eviction threshold exceeded");
                        pressure.memory_pressure = true;
                    }
                    "nodefs.available" => {
                        warn!("Disk eviction threshold exceeded");
                        pressure.disk_pressure = true;
                    }
                    "pid.available" => {
                        warn!("PID eviction threshold exceeded");
                        pressure.pid_pressure = true;
                    }
                    _ => {}
                }
            }
        }

        pressure
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_percentage_threshold() {
        let t = EvictionThreshold::parse("memory.available", "10%").unwrap();
        assert_eq!(t.value, ThresholdValue::Percentage(0.1));
    }

    #[test]
    fn test_parse_mebibyte_threshold() {
        let t = EvictionThreshold::parse("memory.available", "100Mi").unwrap();
        assert_eq!(t.value, ThresholdValue::Absolute(100 * 1024 * 1024));
    }

    #[test]
    fn test_parse_gibibyte_threshold() {
        let t = EvictionThreshold::parse("nodefs.available", "1Gi").unwrap();
        assert_eq!(t.value, ThresholdValue::Absolute(1024 * 1024 * 1024));
    }

    #[test]
    fn test_parse_kibibyte_threshold() {
        let t = EvictionThreshold::parse("memory.available", "512Ki").unwrap();
        assert_eq!(t.value, ThresholdValue::Absolute(512 * 1024));
    }

    #[test]
    fn test_parse_raw_bytes() {
        let t = EvictionThreshold::parse("memory.available", "1073741824").unwrap();
        assert_eq!(t.value, ThresholdValue::Absolute(1073741824));
    }

    #[test]
    fn test_threshold_exceeded_absolute() {
        let t = EvictionThreshold {
            resource: "memory.available".to_string(),
            value: ThresholdValue::Absolute(100 * 1024 * 1024), // 100Mi
        };
        // 50Mi available < 100Mi threshold -> exceeded
        assert!(t.is_exceeded(50 * 1024 * 1024, 8 * 1024 * 1024 * 1024));
        // 200Mi available > 100Mi threshold -> not exceeded
        assert!(!t.is_exceeded(200 * 1024 * 1024, 8 * 1024 * 1024 * 1024));
    }

    #[test]
    fn test_threshold_exceeded_percentage() {
        let t = EvictionThreshold {
            resource: "nodefs.available".to_string(),
            value: ThresholdValue::Percentage(0.10), // 10%
        };
        let total = 100 * 1024 * 1024 * 1024u64; // 100Gi
        // 5Gi available = 5% < 10% -> exceeded
        assert!(t.is_exceeded(5 * 1024 * 1024 * 1024, total));
        // 15Gi available = 15% > 10% -> not exceeded
        assert!(!t.is_exceeded(15 * 1024 * 1024 * 1024, total));
    }

    #[test]
    fn test_parse_quantity_ki() {
        assert_eq!(parse_quantity("1Ki"), Some(1024));
        assert_eq!(parse_quantity("1Mi"), Some(1024 * 1024));
        assert_eq!(parse_quantity("1Gi"), Some(1024 * 1024 * 1024));
    }

    #[test]
    fn test_evaluator_no_pressure() {
        let hard = [("memory.available".to_string(), "100Mi".to_string())]
            .into_iter()
            .collect::<HashMap<_, _>>();
        let evaluator = EvictionEvaluator::new(&hard, &HashMap::new());
        let resources = NodeResources {
            available_memory_bytes: 500 * 1024 * 1024, // 500Mi
            total_memory_bytes: 8 * 1024 * 1024 * 1024,
            available_disk_bytes: 50 * 1024 * 1024 * 1024,
            total_disk_bytes: 100 * 1024 * 1024 * 1024,
            available_pids: 10000,
            total_pids: 32768,
        };
        let pressure = evaluator.evaluate(&resources);
        assert!(!pressure.memory_pressure);
        assert!(!pressure.disk_pressure);
    }

    #[test]
    fn test_evaluator_memory_pressure() {
        let hard = [("memory.available".to_string(), "100Mi".to_string())]
            .into_iter()
            .collect::<HashMap<_, _>>();
        let evaluator = EvictionEvaluator::new(&hard, &HashMap::new());
        let resources = NodeResources {
            available_memory_bytes: 50 * 1024 * 1024, // 50Mi < 100Mi
            total_memory_bytes: 8 * 1024 * 1024 * 1024,
            available_disk_bytes: 50 * 1024 * 1024 * 1024,
            total_disk_bytes: 100 * 1024 * 1024 * 1024,
            available_pids: 10000,
            total_pids: 32768,
        };
        let pressure = evaluator.evaluate(&resources);
        assert!(pressure.memory_pressure);
    }
}
