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

//! kubelet-ports: Port definitions (interfaces) for the kubelet.
//!
//! In hexagonal architecture, ports define the boundary between the domain
//! and external infrastructure. This crate contains:
//! - **Driving ports**: interfaces called BY external actors to drive the kubelet
//! - **Driven ports**: interfaces the kubelet calls to access external infrastructure

pub mod driven;
pub mod driving;
