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
//! kubelet-core: Domain layer for the Kubernetes kubelet.
//!
//! This crate contains the pure business logic of the kubelet with no
//! dependencies on external infrastructure (no CRI, no HTTP, no Kube API).
//! Following the Hexagonal Architecture pattern, all external interaction
//! is mediated through port traits defined in `kubelet-ports`.

pub mod config;
pub mod container;
pub mod error;
pub mod lease;
pub mod node;
pub mod pod;
pub mod qos;
pub mod types;
