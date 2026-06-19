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

//! Core error types for the kubelet domain.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum KubeletError {
    #[error("Pod not found: {0}")]
    PodNotFound(String),

    #[error("Container not found: {pod}/{container}")]
    ContainerNotFound { pod: String, container: String },

    #[error("Runtime error: {0}")]
    Runtime(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("Node status error: {0}")]
    NodeStatus(String),

    #[error("Eviction error: {0}")]
    Eviction(String),

    #[error("Probe error: {0}")]
    Probe(String),

    #[error("Image pull error: {0}")]
    ImagePull(String),

    #[error("Volume mount error: {0}")]
    VolumeMount(String),

    #[error("API error: {0}")]
    Api(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Resource allocation error: {0}")]
    Resource(String),

    #[error("Admission error: {0}")]
    Admission(String),

    #[error("Pod status error: {0}")]
    PodStatus(String),

    #[error("Lifecycle hook error: {0}")]
    Lifecycle(String),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("Security error: {0}")]
    Security(String),
}

pub type Result<T> = std::result::Result<T, KubeletError>;
