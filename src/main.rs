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

//! kubelet - Kubernetes node agent reimplemented in Rust.
//!
//! This binary is a like-for-like functional replacement for the Go kubelet.
//! See README.md for architecture details.

// Use jemalloc as the global allocator to reduce memory fragmentation from
// glibc's per-thread arena model, which inflates RSS by 100MB+ at 30+ threads.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::Parser;
use kubelet_app::Kubelet;
use kubelet_app::cli::KubeletArgs;
use rustls::crypto::ring;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt::time::UtcTime};

fn setup_logging(verbosity: u8) {
    let filter = match verbosity {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(format!("kubelet={}", filter))),
        )
        .json()
        .with_timer(UtcTime::rfc_3339())
        .with_current_span(false)
        .with_span_list(false)
        .init();
}

fn main() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // Cap the blocking thread pool. The default (512) allows unbounded thread
        // creation from blocking ops (DNS, file I/O in gRPC), inflating RSS.
        .max_blocking_threads(32)
        .thread_name("kubelet-worker")
        .build()?
        .block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    ring::default_provider()
        .install_default()
        .expect("Failed to install rustls ring crypto provider");

    let args = KubeletArgs::parse();
    let cli_verbosity = args.v;

    // Parse config first (before logging) so we can use logging.verbosity from
    // the config file when no explicit -v flag was given on the CLI.
    let config = args.into_config()?;

    // CLI -v takes precedence; fall back to config file logging.verbosity.
    let verbosity = cli_verbosity.unwrap_or(config.log_level);
    setup_logging(verbosity);

    info!(
        version = env!("CARGO_PKG_VERSION"),
        "Starting kubelet (Rust)"
    );

    let kubelet = Kubelet::new(config).await;

    info!(node = kubelet.node_name(), "Kubelet initialized");

    let node_status = kubelet.initial_node_status();
    info!(
        node = %node_status.name,
        ready = node_status.is_ready(),
        cpus = node_status.capacity.cpu_cores,
        "Node status initialized"
    );

    info!("Kubelet starting runtime components");
    kubelet.run().await?;
    info!("Shutting down kubelet");

    Ok(())
}
