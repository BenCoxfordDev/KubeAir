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

//! TLS certificate rotation for the kubelet serving certificate.
//!
//! The kubelet serves HTTPS on port 10250. Its serving certificate must be
//! periodically rotated before expiry.
//!
//! Mirrors pkg/kubelet/certificate/ in the Go kubelet.

use chrono::{DateTime, Duration, Utc};
use kubelet_core::error::{KubeletError, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

// -- Certificate types ---------------------------------------------------------

/// Metadata about a certificate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateMeta {
    /// PEM-encoded certificate.
    pub pem: String,
    /// When the certificate was issued.
    pub not_before: DateTime<Utc>,
    /// When the certificate expires.
    pub not_after: DateTime<Utc>,
    /// Subject Common Name.
    pub common_name: String,
    /// Subject Alternative Names.
    pub sans: Vec<String>,
    /// Certificate serial number (hex).
    pub serial: String,
}

impl CertificateMeta {
    /// Fraction of the certificate lifetime already elapsed (0.0-1.0).
    pub fn age_fraction(&self) -> f64 {
        let total = (self.not_after - self.not_before).num_seconds() as f64;
        if total <= 0.0 {
            return 1.0;
        }
        let elapsed = (Utc::now() - self.not_before).num_seconds() as f64;
        (elapsed / total).clamp(0.0, 1.0)
    }

    /// Returns true if the certificate is expired.
    pub fn is_expired(&self) -> bool {
        Utc::now() >= self.not_after
    }

    /// Returns true if the certificate should be rotated.
    /// Kubernetes rotates at 80% of the certificate lifetime.
    pub fn should_rotate(&self) -> bool {
        self.age_fraction() >= 0.80 || self.is_expired()
    }

    /// Time remaining until expiry.
    pub fn time_remaining(&self) -> Duration {
        let remaining = self.not_after - Utc::now();
        if remaining < Duration::zero() {
            Duration::zero()
        } else {
            remaining
        }
    }
}

// -- Certificate store ---------------------------------------------------------

/// Stores current and pending certificates for the kubelet.
pub struct CertificateStore {
    cert_dir: PathBuf,
    current: Option<CertificateMeta>,
}

impl CertificateStore {
    pub fn new(cert_dir: impl Into<PathBuf>) -> Self {
        Self {
            cert_dir: cert_dir.into(),
            current: None,
        }
    }

    /// Load the current certificate from disk (if it exists).
    pub fn load(&mut self) -> Result<bool> {
        let cert_path = self.cert_dir.join("kubelet.crt");
        let meta_path = self.cert_dir.join("kubelet-cert-meta.json");
        if meta_path.exists() {
            let content = std::fs::read_to_string(&meta_path)?;
            let meta: CertificateMeta =
                serde_json::from_str(&content).map_err(KubeletError::Serialization)?;
            self.current = Some(meta);
            return Ok(true);
        }
        Ok(false)
    }

    /// Save the current certificate metadata to disk.
    pub fn save(&self, meta: &CertificateMeta) -> Result<()> {
        std::fs::create_dir_all(&self.cert_dir)?;
        let meta_path = self.cert_dir.join("kubelet-cert-meta.json");
        let content = serde_json::to_string_pretty(meta).map_err(KubeletError::Serialization)?;
        std::fs::write(&meta_path, content)?;
        Ok(())
    }

    /// Install a newly rotated certificate.
    pub fn install(&mut self, meta: CertificateMeta) -> Result<()> {
        self.save(&meta)?;
        info!(
            cn = %meta.common_name,
            expires = %meta.not_after,
            "Certificate installed"
        );
        self.current = Some(meta);
        Ok(())
    }

    pub fn current(&self) -> Option<&CertificateMeta> {
        self.current.as_ref()
    }

    pub fn needs_rotation(&self) -> bool {
        self.current
            .as_ref()
            .map(|c| c.should_rotate())
            .unwrap_or(true)
    }
}

// -- Certificate rotation manager ----------------------------------------------

/// Manages certificate rotation lifecycle.
pub struct CertificateRotationManager {
    store: CertificateStore,
    node_name: String,
}

impl CertificateRotationManager {
    pub fn new(node_name: impl Into<String>, cert_dir: impl Into<PathBuf>) -> Self {
        Self {
            store: CertificateStore::new(cert_dir),
            node_name: node_name.into(),
        }
    }

    /// Check if rotation is needed and return the reason.
    pub fn rotation_needed(&self) -> Option<String> {
        match self.store.current() {
            None => Some("No certificate installed".to_string()),
            Some(cert) if cert.is_expired() => {
                Some(format!("Certificate expired at {}", cert.not_after))
            }
            Some(cert) if cert.should_rotate() => Some(format!(
                "Certificate at {:.0}% of lifetime (rotate at 80%)",
                cert.age_fraction() * 100.0
            )),
            _ => None,
        }
    }

    /// Generate a self-signed certificate (for bootstrapping / tests).
    /// In production: submit a CertificateSigningRequest to the API server.
    pub fn generate_self_signed(&self, valid_days: i64) -> Result<CertificateMeta> {
        let now = Utc::now();
        // In a real implementation: generate RSA/ECDSA key + self-signed cert
        // using rcgen or openssl crate
        Ok(CertificateMeta {
            pem: format!(
                "-----BEGIN CERTIFICATE-----\n[generated for {}]\n-----END CERTIFICATE-----\n",
                self.node_name
            ),
            not_before: now,
            not_after: now + Duration::days(valid_days),
            common_name: format!("system:node:{}", self.node_name),
            sans: vec![self.node_name.clone(), "127.0.0.1".to_string()],
            serial: format!("{:x}", rand_serial()),
        })
    }

    pub fn store(&self) -> &CertificateStore {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut CertificateStore {
        &mut self.store
    }
}

fn rand_serial() -> u64 {
    // Simple pseudo-random serial for testing; real impl uses crypto RNG
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_cert(not_before: DateTime<Utc>, not_after: DateTime<Utc>) -> CertificateMeta {
        CertificateMeta {
            pem: "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----\n".to_string(),
            not_before,
            not_after,
            common_name: "system:node:node1".to_string(),
            sans: vec!["node1".to_string(), "127.0.0.1".to_string()],
            serial: "deadbeef".to_string(),
        }
    }

    #[test]
    fn test_cert_not_expired_fresh() {
        let cert = make_cert(
            Utc::now() - Duration::hours(1),
            Utc::now() + Duration::days(89),
        );
        assert!(!cert.is_expired());
    }

    #[test]
    fn test_cert_expired() {
        let cert = make_cert(
            Utc::now() - Duration::days(100),
            Utc::now() - Duration::hours(1),
        );
        assert!(cert.is_expired());
    }

    #[test]
    fn test_should_rotate_at_80_percent() {
        // 80 days into a 90-day cert = 88.9% -> should rotate
        let cert = make_cert(
            Utc::now() - Duration::days(80),
            Utc::now() + Duration::days(10),
        );
        assert!(cert.should_rotate());
    }

    #[test]
    fn test_should_not_rotate_at_50_percent() {
        // 45 days into a 90-day cert = 50% -> no rotation yet
        let cert = make_cert(
            Utc::now() - Duration::days(45),
            Utc::now() + Duration::days(45),
        );
        assert!(!cert.should_rotate());
    }

    #[test]
    fn test_age_fraction() {
        // 30 days into a 90-day cert = 33%
        let cert = make_cert(
            Utc::now() - Duration::days(30),
            Utc::now() + Duration::days(60),
        );
        let age = cert.age_fraction();
        assert!(age > 0.30 && age < 0.36);
    }

    #[test]
    fn test_time_remaining() {
        let cert = make_cert(Utc::now(), Utc::now() + Duration::days(30));
        let remaining = cert.time_remaining();
        assert!(remaining > Duration::days(29));
        assert!(remaining <= Duration::days(30));
    }

    #[test]
    fn test_time_remaining_expired_is_zero() {
        let cert = make_cert(
            Utc::now() - Duration::days(2),
            Utc::now() - Duration::hours(1),
        );
        assert_eq!(cert.time_remaining(), Duration::zero());
    }

    #[test]
    fn test_cert_store_needs_rotation_when_empty() {
        let dir = TempDir::new().unwrap();
        let store = CertificateStore::new(dir.path());
        assert!(store.needs_rotation(), "Store with no cert needs rotation");
    }

    #[test]
    fn test_cert_store_save_and_load() {
        let dir = TempDir::new().unwrap();
        let mut store = CertificateStore::new(dir.path());
        let cert = make_cert(Utc::now(), Utc::now() + Duration::days(90));
        store.install(cert.clone()).unwrap();

        let mut store2 = CertificateStore::new(dir.path());
        assert!(store2.load().unwrap());
        let loaded = store2.current().unwrap();
        assert_eq!(loaded.common_name, "system:node:node1");
        assert_eq!(loaded.serial, "deadbeef");
    }

    #[test]
    fn test_rotation_manager_detects_no_cert() {
        let dir = TempDir::new().unwrap();
        let mgr = CertificateRotationManager::new("node1", dir.path());
        assert!(mgr.rotation_needed().is_some());
    }

    #[test]
    fn test_rotation_manager_no_rotation_needed_fresh_cert() {
        let dir = TempDir::new().unwrap();
        let mut mgr = CertificateRotationManager::new("node1", dir.path());
        let fresh = make_cert(Utc::now(), Utc::now() + Duration::days(90));
        mgr.store_mut().install(fresh).unwrap();
        assert!(mgr.rotation_needed().is_none());
    }

    #[test]
    fn test_generate_self_signed() {
        let dir = TempDir::new().unwrap();
        let mgr = CertificateRotationManager::new("node1", dir.path());
        let cert = mgr.generate_self_signed(365).unwrap();
        assert!(cert.pem.contains("BEGIN CERTIFICATE"));
        assert!(cert.common_name.contains("node1"));
        assert!(!cert.is_expired());
    }
}
