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

//! TLS server support for the kubelet HTTP API.
//!
//! The real kubelet serves HTTPS on port 10250. This module wraps the axum
//! router in a rustls/tokio-rustls TLS acceptor using the certificate managed
//! by the TLS rotation adapter.
//!
//! When no cert is configured, falls back to plain HTTP (dev/test mode).

use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, warn};

/// TLS configuration for the kubelet server.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_pem_path: std::path::PathBuf,
    pub key_pem_path: std::path::PathBuf,
    /// When set, mTLS is enabled: the server requests a client certificate and
    /// verifies it against this CA. The client cert CN is injected as a
    /// `ClientCertCN` request extension for use in auth middleware.
    pub client_ca_pem_path: Option<std::path::PathBuf>,
}

/// Start the kubelet HTTPS server with TLS, falling back to HTTP if no cert.
pub async fn serve_tls(
    addr: SocketAddr,
    router: Router,
    tls: Option<TlsConfig>,
) -> anyhow::Result<()> {
    match tls {
        None => {
            warn!(
                "No TLS config: kubelet serving plain HTTP on {} (not suitable for production)",
                addr
            );
            serve_plain(addr, router).await
        }
        Some(cfg) => {
            info!("Kubelet HTTPS server starting on {} with TLS", addr);
            serve_with_rustls(addr, router, cfg).await
        }
    }
}

async fn serve_plain(addr: SocketAddr, router: Router) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Kubelet server listening (HTTP) on {}", addr);
    axum::serve(listener, router).await?;
    Ok(())
}

async fn serve_with_rustls(addr: SocketAddr, router: Router, cfg: TlsConfig) -> anyhow::Result<()> {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    use tokio_rustls::rustls;
    use tokio_rustls::TlsAcceptor;

    // Load certificate chain.
    let cert_pem = std::fs::read(&cfg.cert_pem_path)
        .map_err(|e| anyhow::anyhow!("read cert '{}': {}", cfg.cert_pem_path.display(), e))?;
    let key_pem = std::fs::read(&cfg.key_pem_path)
        .map_err(|e| anyhow::anyhow!("read key '{}': {}", cfg.key_pem_path.display(), e))?;

    let certs = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("parse cert: {}", e))?;

    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .map_err(|e| anyhow::anyhow!("parse key: {}", e))?
        .ok_or_else(|| {
            anyhow::anyhow!("no private key found in '{}'", cfg.key_pem_path.display())
        })?;

    // Configure client auth if a CA was provided (mTLS).
    let tls_config = if let Some(ca_path) = &cfg.client_ca_pem_path {
        let ca_pem = std::fs::read(ca_path)
            .map_err(|e| anyhow::anyhow!("read client CA '{}': {}", ca_path.display(), e))?;
        let mut root_store = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut ca_pem.as_slice()) {
            let cert = cert.map_err(|e| anyhow::anyhow!("parse CA cert: {}", e))?;
            root_store
                .add(cert)
                .map_err(|e| anyhow::anyhow!("add CA cert: {}", e))?;
        }
        let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .allow_unauthenticated()
            .build()
            .map_err(|e| anyhow::anyhow!("build client verifier: {}", e))?;
        let mut cfg = rustls::ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, key)
            .map_err(|e| anyhow::anyhow!("TLS config: {}", e))?;
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        cfg
    } else {
        let mut cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| anyhow::anyhow!("TLS config: {}", e))?;
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        cfg
    };

    let acceptor = TlsAcceptor::from(Arc::new(tls_config));
    let listener = TcpListener::bind(addr).await?;
    info!("Kubelet server listening (HTTPS) on {}", addr);

    // Use axum's serve with a custom acceptor via tower-http hyper.
    // For axum 0.7: wrap with axum-server or custom accept loop.
    use hyper::server::conn::http1;
    use hyper_util::rt::TokioIo;

    let service = router.into_make_service();

    loop {
        let (stream, remote_addr) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let mut make_svc = service.clone();

        tokio::spawn(async move {
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    // Extract client cert CN (if presented) and store it so
                    // request handlers can use it for x509 authentication.
                    let raw_certs = tls_stream
                        .get_ref()
                        .1
                        .peer_certificates()
                        .map(|c| c.len())
                        .unwrap_or(0);
                    let peer_cn: Option<String> = tls_stream
                        .get_ref()
                        .1
                        .peer_certificates()
                        .and_then(|certs| certs.first())
                        .and_then(|cert| {
                            // Parse the DER certificate with x509-parser to extract the CN.
                            use x509_parser::prelude::*;
                            X509Certificate::from_der(cert.as_ref())
                                .ok()
                                .and_then(|(_, parsed)| {
                                    parsed
                                        .subject()
                                        .iter_common_name()
                                        .next()
                                        .and_then(|cn| cn.as_str().ok())
                                        .map(|s| s.to_string())
                                })
                        });
                    tracing::warn!(
                        from = %remote_addr,
                        peer_cert_count = raw_certs,
                        peer_cn = ?peer_cn,
                        "TLS connection accepted"
                    );

                    let io = TokioIo::new(tls_stream);
                    let svc = tower::Service::call(&mut make_svc, remote_addr)
                        .await
                        .expect("make_service failed");
                    if let Err(e) = http1::Builder::new()
                        .serve_connection(
                            io,
                            hyper::service::service_fn(move |mut req| {
                                // Inject the peer CN so handlers can read it.
                                if let Some(ref cn) = peer_cn {
                                    req.extensions_mut().insert(ClientCertCN(cn.clone()));
                                }
                                let mut svc = svc.clone();
                                async move { tower::Service::call(&mut svc, req).await }
                            }),
                        )
                        .with_upgrades()
                        .await
                    {
                        // Ignore benign connection resets.
                        if !e.to_string().contains("connection reset") {
                            tracing::warn!("HTTPS connection error: {}", e);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("TLS handshake failed from {:?}: {}", remote_addr, e);
                }
            }
        });
    }
}

/// Request extension carrying the CommonName from the client's TLS certificate.
/// Injected by the TLS accept loop when the client presents a certificate.
#[derive(Clone, Debug)]
pub struct ClientCertCN(pub String);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{build_router, ServerState};
    use kubelet_adapters::mock_runtime::MockRuntime;
    use kubelet_core::pod::manager::PodManager;
    use kubelet_ports::driven::container_runtime::ContainerRuntime;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    async fn start_tls_test_server() -> (u16, tokio::task::JoinHandle<()>, Vec<u8>) {
        let temp_dir = Arc::new(TempDir::new().unwrap());
        let cert_path = temp_dir.path().join("kubelet-serving.crt");
        let key_path = temp_dir.path().join("kubelet-serving.key");

        let cert = rcgen::generate_simple_self_signed(vec![
            "test-node".to_string(),
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .unwrap();
        let cert_pem = cert.cert.pem().into_bytes();
        std::fs::write(&cert_path, &cert_pem).unwrap();
        std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();

        let (tx, _rx) = mpsc::channel(10);
        let runtime: Arc<dyn ContainerRuntime> = Arc::new(MockRuntime::new());
        let state = ServerState {
            pod_manager: Arc::new(PodManager::new(tx)),
            runtime,
            node_name: "test-node".to_string(),
            anonymous_auth: true,
            always_allow: true,
            log_dir: "/tmp".to_string(),
            kube_client: None,
        };
        let router = build_router(state);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let temp_dir_guard = temp_dir.clone();
        let handle = tokio::spawn(async move {
            let _keep_temp_dir_alive = temp_dir_guard;
            serve_tls(
                (std::net::Ipv4Addr::LOCALHOST, port).into(),
                router,
                Some(TlsConfig {
                    cert_pem_path: cert_path,
                    key_pem_path: key_path,
                    client_ca_pem_path: None,
                }),
            )
            .await
            .unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        (port, handle, cert_pem)
    }

    #[test]
    fn test_tls_config_fields() {
        let cfg = TlsConfig {
            cert_pem_path: "/etc/kubelet/serving.crt".into(),
            key_pem_path: "/etc/kubelet/serving.key".into(),
            client_ca_pem_path: None,
        };
        assert_eq!(
            cfg.cert_pem_path.to_str().unwrap(),
            "/etc/kubelet/serving.crt"
        );
        assert_eq!(
            cfg.key_pem_path.to_str().unwrap(),
            "/etc/kubelet/serving.key"
        );
    }

    #[tokio::test]
    async fn test_tls_configz_returns_ok() {
        let (port, handle, cert_pem) = start_tls_test_server().await;

        let server_cert = reqwest::Certificate::from_pem(&cert_pem).unwrap();
        let client = reqwest::Client::builder()
            .add_root_certificate(server_cert)
            .build()
            .unwrap();

        let resp = client
            .get(format!("https://127.0.0.1:{}/configz", port))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["kubeletconfig"]["nodeName"], "test-node");

        handle.abort();
    }
}
