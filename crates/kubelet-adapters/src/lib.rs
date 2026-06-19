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

#![allow(unused_imports, unused_variables, dead_code, unused_mut)]
//! kubelet-adapters: Concrete implementations of kubelet ports.
//!
//! Each module provides one or more adapters that wire domain ports to real
//! infrastructure (CRI sockets, Kube API server, file system, etc.).

pub mod active_deadline;
pub mod admission;
pub mod cert_rotator;
pub mod cgroup;
pub mod checkpoint;
pub mod cni;
pub mod container_builder;
pub mod cpu_manager;
pub mod csi;
pub mod device_manager;
pub mod dra;
pub mod eviction;
pub mod file_config;
pub mod image_gc;
pub mod image_puller;
pub mod kube_client;
pub mod kube_reporter;
pub mod kube_watcher;
pub mod lease;
pub mod lifecycle;
pub mod log_manager;
pub mod memory_manager;
pub mod mock_runtime;
pub mod network;
pub mod nfd;
pub mod node_status;
pub mod oom_watcher;
pub mod plugin_registration;
pub mod probe_runner;
pub mod prober;
pub mod resource_manager;
pub mod resource_version;
pub mod runtime_class;
pub mod sandbox_builder;
pub mod security_profile;
pub mod shutdown;
pub mod stats;
pub mod tls;
pub mod topology_manager;
pub mod url_config;
pub mod volume;
pub mod volume_expander;
pub mod volume_fsgroup;
