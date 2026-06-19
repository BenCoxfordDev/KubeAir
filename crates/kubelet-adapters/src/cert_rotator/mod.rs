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

//! Certificate rotation background task.
//!
//! Monitors the kubelet's serving certificate age and rotates it before expiry.
//! Mirrors pkg/kubelet/certificate/certificate_manager.go.
//!
//! Rotation flow:
//!   1. Background task checks cert age every minute.
//!   2. At 70% of lifetime elapsed -> request a new cert (CSR to API server).
//!   3. On success -> atomically swap the cert file.
//!   4. Signal the TLS server to reload (via a watch channel).

use chrono::{DateTime, Duration, Utc};
use kubelet_core::error::{KubeletError, Result};
use std::path::{Path, PathBuf};
use tokio::sync::watch;
use tracing::{error, info, warn};

// -- Rotation state ------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum RotationState {
    /// Certificate is valid and within safe lifetime.
    Valid,
    /// Certificate needs rotation (past 70% of lifetime).
    NeedsRotation,
    /// Certificate is missing or expired.
    MissingOrExpired,
}

/// Metadata about the current certificate.
#[derive(Debug, Clone)]
pub struct CertInfo {
    pub not_before: DateTime<Utc>,
    pub not_after: DateTime<Utc>,
    pub common_name: String,
}

impl CertInfo {
    pub fn age_fraction(&self) -> f64 {
        let total = (self.not_after - self.not_before).num_seconds() as f64;
        if total <= 0.0 {
            return 1.0;
        }
        let elapsed = (Utc::now() - self.not_before).num_seconds() as f64;
        (elapsed / total).clamp(0.0, 1.0)
    }

    pub fn rotation_state(&self) -> RotationState {
        if Utc::now() >= self.not_after {
            RotationState::MissingOrExpired
        } else if self.age_fraction() >= 0.70 {
            RotationState::NeedsRotation
        } else {
            RotationState::Valid
        }
    }

    pub fn time_remaining(&self) -> Duration {
        let r = self.not_after - Utc::now();
        if r < Duration::zero() {
            Duration::zero()
        } else {
            r
        }
    }
}

// -- Cert rotator --------------------------------------------------------------

pub struct CertRotator {
    node_name: String,
    cert_path: PathBuf,
    key_path: PathBuf,
    /// Signal channel: sends `()` when cert is rotated.
    reload_tx: watch::Sender<()>,
    pub reload_rx: watch::Receiver<()>,
}

impl CertRotator {
    pub fn new(
        node_name: impl Into<String>,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        let (tx, rx) = watch::channel(());
        Self {
            node_name: node_name.into(),
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            reload_tx: tx,
            reload_rx: rx,
        }
    }

    /// Background loop: check and rotate the cert every minute.
    pub async fn run(self) {
        info!(node = %self.node_name, "Certificate rotator started");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

        loop {
            interval.tick().await;

            match self.check_and_rotate().await {
                Ok(true) => {
                    info!(node = %self.node_name, "Certificate rotated successfully");
                    let _ = self.reload_tx.send(());
                }
                Ok(false) => {
                    // No rotation needed.
                }
                Err(e) => {
                    error!(node = %self.node_name, error = %e, "Certificate rotation failed");
                }
            }
        }
    }

    /// Check if rotation is needed and rotate if so.
    /// Returns Ok(true) if cert was rotated.
    async fn check_and_rotate(&self) -> Result<bool> {
        let info = self.load_cert_info()?;

        match info {
            None => {
                warn!("No serving certificate found; generating self-signed");
                self.generate_self_signed().await?;
                Ok(true)
            }
            Some(info) => match info.rotation_state() {
                RotationState::Valid => Ok(false),
                RotationState::NeedsRotation => {
                    info!(
                        age_pct = format!("{:.0}%", info.age_fraction() * 100.0),
                        remaining = %format!("{} hours", info.time_remaining().num_hours()),
                        "Rotating certificate"
                    );
                    self.request_new_cert().await?;
                    Ok(true)
                }
                RotationState::MissingOrExpired => {
                    warn!("Certificate expired; generating self-signed as fallback");
                    self.generate_self_signed().await?;
                    Ok(true)
                }
            },
        }
    }

    /// Load certificate metadata from the cert file.
    fn load_cert_info(&self) -> Result<Option<CertInfo>> {
        if !self.cert_path.exists() {
            return Ok(None);
        }

        // In a full implementation: parse the PEM file with x509-parser or rcgen.
        // Here: read the cert file and use file metadata as a proxy.
        let metadata = std::fs::metadata(&self.cert_path)
            .map_err(|e| KubeletError::Tls(format!("read cert metadata: {}", e)))?;

        let modified = metadata
            .modified()
            .map_err(|e| KubeletError::Tls(format!("cert mtime: {}", e)))?;

        let issued_at = DateTime::from(modified);
        // Assume 1-year cert (365 days) if we can't parse the actual expiry.
        let not_after = issued_at + Duration::days(365);

        Ok(Some(CertInfo {
            not_before: issued_at,
            not_after,
            common_name: format!("system:node:{}", self.node_name),
        }))
    }

    /// Request a new certificate from the API server via CertificateSigningRequest.
    async fn request_new_cert(&self) -> Result<()> {
        // Full implementation:
        //   1. Generate RSA/ECDSA key pair.
        //   2. Create CSR with CN=system:node:<node-name>, O=system:nodes.
        //   3. POST CertificateSigningRequest to the API server.
        //   4. Wait for approval (with timeout).
        //   5. Fetch signed certificate.
        //   6. Atomically write cert+key files (write to .tmp, then rename).
        //
        // For now: generate a new self-signed cert as a fallback.
        warn!("CSR-based rotation not yet connected to API server; using self-signed fallback");
        self.generate_self_signed().await
    }

    /// Generate a self-signed certificate (for standalone / bootstrap use).
    async fn generate_self_signed(&self) -> Result<()> {
        use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, SanType};

        let mut params = CertificateParams::default();
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(
            DnType::CommonName,
            format!("system:node:{}", self.node_name),
        );
        params
            .distinguished_name
            .push(DnType::OrganizationName, "system:nodes");
        params.subject_alt_names = vec![
            SanType::DnsName(
                rcgen::Ia5String::try_from(self.node_name.as_str())
                    .unwrap_or_else(|_| rcgen::Ia5String::try_from("localhost").unwrap()),
            ),
            SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        ];

        let key_pair =
            KeyPair::generate().map_err(|e| KubeletError::Tls(format!("generate key: {}", e)))?;
        let cert = params
            .self_signed(&key_pair)
            .map_err(|e| KubeletError::Tls(format!("self-sign cert: {}", e)))?;

        // Atomic write: write to .tmp then rename.
        let cert_tmp = self.cert_path.with_extension("crt.tmp");
        let key_tmp = self.key_path.with_extension("key.tmp");

        if let Some(parent) = self.cert_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| KubeletError::Tls(format!("create cert dir: {}", e)))?;
        }

        std::fs::write(&cert_tmp, cert.pem())
            .map_err(|e| KubeletError::Tls(format!("write cert: {}", e)))?;
        std::fs::write(&key_tmp, key_pair.serialize_pem())
            .map_err(|e| KubeletError::Tls(format!("write key: {}", e)))?;

        std::fs::rename(&cert_tmp, &self.cert_path)
            .map_err(|e| KubeletError::Tls(format!("rename cert: {}", e)))?;
        std::fs::rename(&key_tmp, &self.key_path)
            .map_err(|e| KubeletError::Tls(format!("rename key: {}", e)))?;

        info!(cert = %self.cert_path.display(), "Self-signed certificate written");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_cert_info_rotation_state_valid() {
        let info = CertInfo {
            not_before: Utc::now() - Duration::days(10),
            not_after: Utc::now() + Duration::days(355),
            common_name: "system:node:node1".to_string(),
        };
        assert_eq!(info.rotation_state(), RotationState::Valid);
    }

    #[test]
    fn test_cert_info_rotation_state_needs_rotation() {
        let info = CertInfo {
            not_before: Utc::now() - Duration::days(280),
            not_after: Utc::now() + Duration::days(85),
            common_name: "system:node:node1".to_string(),
        };
        assert_eq!(info.rotation_state(), RotationState::NeedsRotation);
    }

    #[test]
    fn test_cert_info_rotation_state_expired() {
        let info = CertInfo {
            not_before: Utc::now() - Duration::days(366),
            not_after: Utc::now() - Duration::days(1),
            common_name: "system:node:node1".to_string(),
        };
        assert_eq!(info.rotation_state(), RotationState::MissingOrExpired);
    }

    #[test]
    fn test_cert_info_age_fraction() {
        let info = CertInfo {
            not_before: Utc::now() - Duration::days(30),
            not_after: Utc::now() + Duration::days(60),
            common_name: "node1".to_string(),
        };
        let age = info.age_fraction();
        assert!((0.32..0.35).contains(&age));
    }

    #[tokio::test]
    async fn test_generate_self_signed_creates_files() {
        let dir = TempDir::new().unwrap();
        let rotator = CertRotator::new(
            "node1",
            dir.path().join("kubelet.crt"),
            dir.path().join("kubelet.key"),
        );
        rotator.generate_self_signed().await.unwrap();
        assert!(dir.path().join("kubelet.crt").exists());
        assert!(dir.path().join("kubelet.key").exists());
        let pem = std::fs::read_to_string(dir.path().join("kubelet.crt")).unwrap();
        assert!(pem.contains("BEGIN CERTIFICATE"));
    }

    #[test]
    fn test_load_cert_info_missing_returns_none() {
        let dir = TempDir::new().unwrap();
        let rotator = CertRotator::new(
            "node1",
            dir.path().join("nonexistent.crt"),
            dir.path().join("nonexistent.key"),
        );
        let info = rotator.load_cert_info().unwrap();
        assert!(info.is_none());
    }
}
