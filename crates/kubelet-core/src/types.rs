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

//! Core type aliases and primitives used across the kubelet domain.

use serde::{Deserialize, Serialize};

/// Unique identifier for a pod (namespace/name).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PodUID(pub String);

impl PodUID {
    pub fn new(uid: impl Into<String>) -> Self {
        Self(uid.into())
    }
}

impl std::fmt::Display for PodUID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Qualified pod reference: namespace + name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PodRef {
    pub namespace: String,
    pub name: String,
}

impl PodRef {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for PodRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

/// Container name within a pod.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContainerName(pub String);

impl ContainerName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl std::fmt::Display for ContainerName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Resource quantity (mirrors k8s resource.Quantity).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceQuantity {
    pub value: i64,
    pub unit: ResourceUnit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ResourceUnit {
    Bytes,
    Millicores,
    Count,
}

impl ResourceQuantity {
    pub fn cpu_millicores(m: i64) -> Self {
        Self {
            value: m,
            unit: ResourceUnit::Millicores,
        }
    }

    pub fn memory_bytes(b: i64) -> Self {
        Self {
            value: b,
            unit: ResourceUnit::Bytes,
        }
    }
}
