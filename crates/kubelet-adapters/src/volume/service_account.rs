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

//! Service Account token volume plugin.
//!
//! Generates and rotates bound service account tokens using the TokenRequest API.
//! Mirrors pkg/volume/token/ in the Go kubelet.
//!
//! The kubelet:
//!  1. Calls TokenRequest API to get a time-limited token.
//!  2. Writes the token to /var/run/secrets/kubernetes.io/serviceaccount/token.
//!  3. Re-fetches when lifetime drops below 80% (or after 1 hour min).
//!
//! In standalone mode (no API server), falls back to generating a self-signed
//! JWT placeholder so pods can still start.

use chrono::{DateTime, Duration, Utc};
use kubelet_core::error::{KubeletError, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

// -- Token ---------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceAccountToken {
    pub token: String,
    pub expiry: DateTime<Utc>,
    pub audience: String,
}

impl ServiceAccountToken {
    /// Check if this token needs rotation (80% of lifetime elapsed).
    pub fn needs_rotation(&self, issued_at: DateTime<Utc>) -> bool {
        let total = (self.expiry - issued_at).num_seconds();
        if total <= 0 {
            return true;
        }
        let elapsed = (Utc::now() - issued_at).num_seconds();
        elapsed as f64 / total as f64 > 0.80
    }

    pub fn is_expired(&self) -> bool {
        Utc::now() >= self.expiry
    }

    pub fn time_remaining(&self) -> Duration {
        let remaining = self.expiry - Utc::now();
        if remaining < Duration::zero() {
            Duration::zero()
        } else {
            remaining
        }
    }
}

// -- Standard SA files ---------------------------------------------------------

/// The standard files mounted in every pod at /var/run/secrets/kubernetes.io/serviceaccount/.
pub struct ServiceAccountFiles {
    pub token: Option<ServiceAccountToken>,
    pub ca_bundle: Vec<u8>,
    pub namespace: String,
}

// -- Token volume manager ------------------------------------------------------

pub struct ServiceAccountTokenManager {
    base_dir: PathBuf,
    /// Default token lifetime. Matches k8s default (1 hour).
    default_expiration_seconds: u64,
}

impl ServiceAccountTokenManager {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            default_expiration_seconds: 3600,
        }
    }

    /// Mount the ServiceAccount volume for a pod.
    ///
    /// Writes token, ca.crt, and namespace to `target_path`.
    /// This is called by the projected volume plugin and also the legacy
    /// kubernetes.io/service-account-token volume plugin.
    pub fn mount(
        &self,
        pod_uid: &str,
        service_account_name: &str,
        namespace: &str,
        target_path: &Path,
        token: Option<&ServiceAccountToken>,
        ca_bundle: &[u8],
    ) -> Result<()> {
        std::fs::create_dir_all(target_path)
            .map_err(|e| KubeletError::Storage(format!("create sa dir: {}", e)))?;

        // Write token.
        let token_str = token
            .map(|t| t.token.clone())
            .unwrap_or_else(|| self.generate_placeholder_token(service_account_name, namespace));

        let token_path = target_path.join("token");
        std::fs::write(&token_path, token_str.as_bytes())
            .map_err(|e| KubeletError::Storage(format!("write sa token: {}", e)))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&token_path, std::fs::Permissions::from_mode(0o600));
        }

        // Write ca.crt.
        let ca_path = target_path.join("ca.crt");
        std::fs::write(&ca_path, ca_bundle)
            .map_err(|e| KubeletError::Storage(format!("write sa ca.crt: {}", e)))?;

        // Write namespace.
        let ns_path = target_path.join("namespace");
        std::fs::write(&ns_path, namespace.as_bytes())
            .map_err(|e| KubeletError::Storage(format!("write sa namespace: {}", e)))?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(target_path, std::fs::Permissions::from_mode(0o755));
        }

        info!(
            pod = %pod_uid, sa = %service_account_name,
            target = %target_path.display(),
            "ServiceAccount volume mounted"
        );
        Ok(())
    }

    pub fn unmount(&self, target_path: &Path) -> Result<()> {
        if target_path.exists() {
            std::fs::remove_dir_all(target_path)
                .map_err(|e| KubeletError::Storage(format!("remove sa dir: {}", e)))?;
        }
        Ok(())
    }

    /// Request a new token from the API server (TokenRequest API).
    /// In standalone mode, returns a placeholder.
    pub async fn request_token(
        &self,
        service_account_name: &str,
        namespace: &str,
        audience: &str,
        expiration_seconds: u64,
    ) -> Result<ServiceAccountToken> {
        // Real implementation: POST /api/v1/namespaces/{ns}/serviceaccounts/{sa}/token
        // with TokenRequestSpec { audiences: [audience], expirationSeconds }
        // and parse TokenRequestStatus.token + expirationTimestamp.
        warn!(
            "ServiceAccountTokenManager: TokenRequest API not connected; using placeholder token"
        );
        Ok(ServiceAccountToken {
            token: self.generate_placeholder_token(service_account_name, namespace),
            expiry: Utc::now() + Duration::seconds(expiration_seconds as i64),
            audience: audience.to_string(),
        })
    }

    /// Generate a minimal JWT placeholder for standalone operation.
    fn generate_placeholder_token(&self, sa: &str, namespace: &str) -> String {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"none","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(format!(
            r#"{{"sub":"system:serviceaccount:{}:{}","iss":"kubernetes/serviceaccount","exp":{}}}"#,
            namespace,
            sa,
            (Utc::now() + Duration::hours(1)).timestamp()
        ));
        format!("{}.{}.", header, payload) // unsigned JWT
    }

    pub fn staging_path(&self, pod_uid: &str) -> PathBuf {
        self.base_dir
            .join("pods")
            .join(pod_uid)
            .join("volumes")
            .join("kubernetes.io~service-account-token")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_token_not_expired_when_fresh() {
        let token = ServiceAccountToken {
            token: "test".to_string(),
            expiry: Utc::now() + Duration::hours(1),
            audience: "https://kubernetes.default.svc".to_string(),
        };
        assert!(!token.is_expired());
    }

    #[test]
    fn test_token_expired_when_past() {
        let token = ServiceAccountToken {
            token: "test".to_string(),
            expiry: Utc::now() - Duration::seconds(1),
            audience: "https://kubernetes.default.svc".to_string(),
        };
        assert!(token.is_expired());
    }

    #[test]
    fn test_token_needs_rotation_at_80_percent() {
        let issued = Utc::now() - Duration::minutes(49); // 49/60 min elapsed = 82%
        let token = ServiceAccountToken {
            token: "t".to_string(),
            expiry: issued + Duration::hours(1),
            audience: "k8s".to_string(),
        };
        assert!(token.needs_rotation(issued));
    }

    #[test]
    fn test_token_no_rotation_at_50_percent() {
        let issued = Utc::now() - Duration::minutes(30);
        let token = ServiceAccountToken {
            token: "t".to_string(),
            expiry: issued + Duration::hours(1),
            audience: "k8s".to_string(),
        };
        assert!(!token.needs_rotation(issued));
    }

    #[test]
    fn test_mount_writes_all_files() {
        let dir = TempDir::new().unwrap();
        let mgr = ServiceAccountTokenManager::new(dir.path());
        let target = dir.path().join("sa");
        let token = ServiceAccountToken {
            token: "mytoken".to_string(),
            expiry: Utc::now() + Duration::hours(1),
            audience: "k8s".to_string(),
        };
        mgr.mount(
            "uid-1",
            "default",
            "default",
            &target,
            Some(&token),
            b"CA_BUNDLE",
        )
        .unwrap();
        assert!(target.join("token").exists());
        assert!(target.join("ca.crt").exists());
        assert!(target.join("namespace").exists());
        assert_eq!(std::fs::read(target.join("namespace")).unwrap(), b"default");
        assert_eq!(std::fs::read(target.join("token")).unwrap(), b"mytoken");
    }

    #[test]
    fn test_placeholder_token_format() {
        let mgr = ServiceAccountTokenManager::new("/tmp");
        let tok = mgr.generate_placeholder_token("default", "default");
        // JWT format: header.payload.signature (unsigned has empty sig)
        assert_eq!(tok.matches('.').count(), 2);
        assert!(tok.ends_with('.'));
    }

    #[tokio::test]
    async fn test_request_token_returns_placeholder() {
        let mgr = ServiceAccountTokenManager::new("/tmp");
        let tok = mgr
            .request_token("default", "default", "k8s", 3600)
            .await
            .unwrap();
        assert!(!tok.token.is_empty());
        assert!(!tok.is_expired());
    }
}
