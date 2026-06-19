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

//! Pod source port - interface for receiving pod specs from various sources.
//!
//! Mirrors the kubelet's config.PodConfigNotificationMode and sources:
//! - API server watch
//! - Static pod manifests (file)
//! - Static pod URL (HTTP)

use async_trait::async_trait;
use kubelet_core::error::Result;
use kubelet_core::pod::PodUpdate;
use tokio::sync::mpsc;

/// A source of pod updates (API server, static files, HTTP endpoint).
#[async_trait]
pub trait PodSource: Send + Sync {
    /// Source identifier (e.g. "api", "file", "http").
    fn name(&self) -> &str;

    /// Start streaming pod updates into the given channel.
    async fn run(&self, tx: mpsc::Sender<PodUpdate>) -> Result<()>;
}

/// Combines multiple pod sources into a single merged stream.
pub struct MergedPodSource {
    sources: Vec<Box<dyn PodSource>>,
}

impl MergedPodSource {
    pub fn new(sources: Vec<Box<dyn PodSource>>) -> Self {
        Self { sources }
    }

    /// Start all sources and fan their output into a single channel.
    pub async fn run(self, tx: mpsc::Sender<PodUpdate>) -> Result<()> {
        let mut handles = Vec::new();
        for source in self.sources {
            let tx2 = tx.clone();
            handles.push(tokio::spawn(async move {
                if let Err(e) = source.run(tx2).await {
                    tracing::error!(source = source.name(), error = %e, "Pod source failed");
                }
            }));
        }
        // Wait for all sources to terminate (they shouldn't in normal operation)
        for handle in handles {
            let _ = handle.await;
        }
        Ok(())
    }
}
