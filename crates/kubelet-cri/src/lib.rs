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

//! kubelet-cri: Real CRI gRPC adapter for the Rust kubelet.
//!
//! Provides `ContainerdClient` which implements both `ContainerRuntime` and
//! `ImageManager` ports via the Kubernetes CRI v1 gRPC API over a Unix socket.
//!
//! # Usage
//!
//! ```no_run
//! use kubelet_cri::client::ContainerdClient;
//! use kubelet_ports::driven::container_runtime::ContainerRuntime;
//!
//! # async fn run() -> anyhow::Result<()> {
//! let client = ContainerdClient::connect("unix:///run/containerd/containerd.sock").await?;
//! let sandboxes = client.list_pod_sandboxes().await?;
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod mock_server;
pub mod types;

pub use client::ContainerdClient;
