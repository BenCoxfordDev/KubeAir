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

//! TLS bootstrap client for `kubeadm join`.
//!
//! Mirrors `pkg/kubelet/certificate/bootstrap` in the Go kubelet.
//!
//! `kubeadm join` writes a *bootstrap* kubeconfig (`/etc/kubernetes/bootstrap-kubelet.conf`)
//! containing only short-lived bootstrap-token credentials, and expects the kubelet
//! itself to exchange that token for a real client certificate before it can act as
//! `system:node:<name>` against the API server. The Go kubelet does this by:
//!
//!   1. Connecting to the API server using the bootstrap kubeconfig's credentials.
//!   2. Generating a private key and an x509 CertificateSigningRequest for
//!      `CN=system:node:<name>, O=system:nodes`.
//!   3. Submitting a `certificates.k8s.io/v1` `CertificateSigningRequest` object with
//!      `signerName=kubernetes.io/kube-apiserver-client-kubelet`.
//!   4. Waiting for it to be approved (kubeadm's default RBAC auto-approves node client
//!      certs requested by members of `system:bootstrappers`) and signed.
//!   5. Writing the issued certificate + key to disk and a fully-authenticated
//!      kubeconfig to the real `--kubeconfig` path.
//!
//! Without this exchange, `--kubeconfig` never appears on disk after `kubeadm join`,
//! so the kubelet has no way to talk to the API server (node never registers, pods are
//! never watched).

use k8s_openapi::ByteString;
use k8s_openapi::api::certificates::v1::{
    CertificateSigningRequest, CertificateSigningRequestSpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, PostParams};
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{Client, Config};
use kubelet_core::error::{KubeletError, Result};
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
use std::path::Path;
use std::time::Duration;
use tracing::{info, warn};

/// Signer that issues client certificates kubelets use to authenticate to
/// kube-apiserver. Requests for this signer are auto-approved by the
/// kube-controller-manager's `csrapproving` controller when the requester is a
/// member of `system:bootstrappers` (the group kubeadm's bootstrap tokens map to).
const SIGNER_NAME: &str = "kubernetes.io/kube-apiserver-client-kubelet";

/// Total time to wait for the CSR to be approved and signed.
const CSR_APPROVAL_TIMEOUT: Duration = Duration::from_secs(60);
/// Delay between polls of the CSR's status while waiting for approval.
const CSR_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Ensure a usable kubelet kubeconfig exists at `kubeconfig_path`.
///
/// If `kubeconfig_path` already exists, this is a no-op. Otherwise, if
/// `bootstrap_kubeconfig_path` points to a readable bootstrap kubeconfig, this
/// performs the TLS bootstrap CSR exchange and writes the resulting client
/// certificate/key under `cert_dir` and a kubeconfig referencing them to
/// `kubeconfig_path`.
///
/// Failures are logged and swallowed (returning `Ok(())`) rather than aborting
/// startup: the caller falls back to standalone-like behavior, matching how the
/// rest of the kubelet already tolerates a missing/unreadable kubeconfig.
pub async fn ensure_kubeconfig(
    kubeconfig_path: &Path,
    bootstrap_kubeconfig_path: Option<&Path>,
    node_name: &str,
    cert_dir: &Path,
) {
    if kubeconfig_path.exists() {
        return;
    }

    let Some(bootstrap_path) = bootstrap_kubeconfig_path else {
        return;
    };

    if !bootstrap_path.exists() {
        warn!(
            path = %bootstrap_path.display(),
            "Bootstrap kubeconfig configured but not found on disk; skipping TLS bootstrap"
        );
        return;
    }

    info!(
        bootstrap = %bootstrap_path.display(),
        target = %kubeconfig_path.display(),
        "kubeconfig missing; starting TLS bootstrap via bootstrap-kubeconfig"
    );

    match run_bootstrap(kubeconfig_path, bootstrap_path, node_name, cert_dir).await {
        Ok(()) => info!(
            path = %kubeconfig_path.display(),
            "TLS bootstrap complete; kubeconfig written"
        ),
        Err(e) => warn!(error = %e, "TLS bootstrap failed; will retry on next restart"),
    }
}

async fn run_bootstrap(
    kubeconfig_path: &Path,
    bootstrap_path: &Path,
    node_name: &str,
    cert_dir: &Path,
) -> Result<()> {
    let bootstrap_kubeconfig = Kubeconfig::read_from(bootstrap_path)
        .map_err(|e| KubeletError::Auth(format!("read bootstrap kubeconfig: {}", e)))?;

    let client_config =
        Config::from_custom_kubeconfig(bootstrap_kubeconfig.clone(), &KubeConfigOptions::default())
            .await
            .map_err(|e| KubeletError::Auth(format!("build bootstrap client config: {}", e)))?;

    let client = Client::try_from(client_config)
        .map_err(|e| KubeletError::Auth(format!("create bootstrap client: {}", e)))?;

    let key_pair = KeyPair::generate()
        .map_err(|e| KubeletError::Tls(format!("generate bootstrap key pair: {}", e)))?;

    let mut params = CertificateParams::default();
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, format!("system:node:{}", node_name));
    params
        .distinguished_name
        .push(DnType::OrganizationName, "system:nodes");

    let csr = params
        .serialize_request(&key_pair)
        .map_err(|e| KubeletError::Tls(format!("serialize CSR: {}", e)))?;
    let csr_pem = csr
        .pem()
        .map_err(|e| KubeletError::Tls(format!("encode CSR as PEM: {}", e)))?;

    let csr_name = format!(
        "kube-air-{}-{}",
        sanitize_for_name(node_name),
        uuid::Uuid::new_v4()
    );

    let csr_api: Api<CertificateSigningRequest> = Api::all(client.clone());
    let csr_object = CertificateSigningRequest {
        metadata: ObjectMeta {
            name: Some(csr_name.clone()),
            ..Default::default()
        },
        spec: CertificateSigningRequestSpec {
            request: ByteString(csr_pem.into_bytes()),
            signer_name: SIGNER_NAME.to_string(),
            usages: Some(vec![
                "digital signature".to_string(),
                "key encipherment".to_string(),
                "client auth".to_string(),
            ]),
            ..Default::default()
        },
        status: None,
    };

    csr_api
        .create(&PostParams::default(), &csr_object)
        .await
        .map_err(|e| KubeletError::Auth(format!("submit CertificateSigningRequest: {}", e)))?;

    let issued_cert_pem = poll_for_issued_certificate(&csr_api, &csr_name).await?;

    // Write client cert + key to a single PEM file, matching the Go kubelet's
    // `kubelet-client-current.pem` convention under the cert directory.
    std::fs::create_dir_all(cert_dir)?;
    let client_cert_path = cert_dir.join("kubelet-client-current.pem");
    let combined_pem = format!("{}\n{}", issued_cert_pem, key_pair.serialize_pem());
    let tmp_path = client_cert_path.with_extension("pem.tmp");
    std::fs::write(&tmp_path, &combined_pem)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp_path, &client_cert_path)?;

    let kubeconfig = build_kubeconfig(&bootstrap_kubeconfig, node_name, &client_cert_path)?;
    write_kubeconfig(kubeconfig_path, &kubeconfig)?;

    Ok(())
}

/// Poll the CSR's status until a certificate is issued, it is denied/fails, or
/// `CSR_APPROVAL_TIMEOUT` elapses.
async fn poll_for_issued_certificate(
    csr_api: &Api<CertificateSigningRequest>,
    csr_name: &str,
) -> Result<String> {
    let deadline = tokio::time::Instant::now() + CSR_APPROVAL_TIMEOUT;

    loop {
        match csr_api.get(csr_name).await {
            Ok(csr) => {
                if let Some(status) = &csr.status {
                    if let Some(cert) = &status.certificate {
                        let pem = String::from_utf8(cert.0.clone()).map_err(|e| {
                            KubeletError::Tls(format!(
                                "issued certificate is not valid UTF-8: {}",
                                e
                            ))
                        })?;
                        return Ok(pem);
                    }
                    if let Some(conditions) = &status.conditions {
                        for cond in conditions {
                            if cond.type_ == "Denied" && cond.status == "True" {
                                return Err(KubeletError::Auth(format!(
                                    "CertificateSigningRequest {} was denied: {}",
                                    csr_name,
                                    cond.message.clone().unwrap_or_default()
                                )));
                            }
                            if cond.type_ == "Failed" && cond.status == "True" {
                                return Err(KubeletError::Auth(format!(
                                    "CertificateSigningRequest {} signing failed: {}",
                                    csr_name,
                                    cond.message.clone().unwrap_or_default()
                                )));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(csr = %csr_name, error = %e, "Failed to poll CertificateSigningRequest status");
            }
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(KubeletError::Timeout(format!(
                "timed out after {}s waiting for CertificateSigningRequest {} to be approved",
                CSR_APPROVAL_TIMEOUT.as_secs(),
                csr_name
            )));
        }

        tokio::time::sleep(CSR_POLL_INTERVAL).await;
    }
}

/// Build the final kubelet kubeconfig, reusing the cluster (server + CA) from the
/// bootstrap kubeconfig but replacing the user credentials with the issued client
/// certificate/key on disk.
fn build_kubeconfig(
    bootstrap_kubeconfig: &Kubeconfig,
    node_name: &str,
    client_cert_path: &Path,
) -> Result<Kubeconfig> {
    let cluster = bootstrap_kubeconfig
        .clusters
        .first()
        .cloned()
        .ok_or_else(|| KubeletError::Auth("bootstrap kubeconfig has no clusters".to_string()))?;

    let cluster_name = cluster.name.clone();
    let user_name = format!("system:node:{}", node_name);
    let context_name = "default-context".to_string();

    Ok(Kubeconfig {
        clusters: vec![cluster],
        auth_infos: vec![kube::config::NamedAuthInfo {
            name: user_name.clone(),
            auth_info: Some(kube::config::AuthInfo {
                client_certificate: Some(client_cert_path.display().to_string()),
                client_key: Some(client_cert_path.display().to_string()),
                ..Default::default()
            }),
        }],
        contexts: vec![kube::config::NamedContext {
            name: context_name.clone(),
            context: Some(kube::config::Context {
                cluster: cluster_name,
                user: user_name,
                ..Default::default()
            }),
        }],
        current_context: Some(context_name),
        ..Default::default()
    })
}

fn write_kubeconfig(path: &Path, kubeconfig: &Kubeconfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let yaml = serde_yaml::to_string(kubeconfig)
        .map_err(|e| KubeletError::Auth(format!("serialize kubeconfig: {}", e)))?;
    let tmp_path = path.with_extension("conf.tmp");
    std::fs::write(&tmp_path, yaml)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

fn sanitize_for_name(node_name: &str) -> String {
    node_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>()
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_sanitize_for_name() {
        assert_eq!(
            sanitize_for_name("Node_1.example.com"),
            "node-1.example.com"
        );
    }

    #[tokio::test]
    async fn test_ensure_kubeconfig_noop_when_already_exists() {
        let dir = TempDir::new().unwrap();
        let kubeconfig_path = dir.path().join("kubelet.conf");
        std::fs::write(&kubeconfig_path, "existing").unwrap();

        ensure_kubeconfig(&kubeconfig_path, None, "node1", dir.path()).await;

        assert_eq!(
            std::fs::read_to_string(&kubeconfig_path).unwrap(),
            "existing"
        );
    }

    #[tokio::test]
    async fn test_ensure_kubeconfig_noop_when_no_bootstrap_configured() {
        let dir = TempDir::new().unwrap();
        let kubeconfig_path = dir.path().join("kubelet.conf");

        ensure_kubeconfig(&kubeconfig_path, None, "node1", dir.path()).await;

        assert!(!kubeconfig_path.exists());
    }

    #[tokio::test]
    async fn test_ensure_kubeconfig_noop_when_bootstrap_file_missing() {
        let dir = TempDir::new().unwrap();
        let kubeconfig_path = dir.path().join("kubelet.conf");
        let bootstrap_path = dir.path().join("bootstrap-kubelet.conf");

        ensure_kubeconfig(
            &kubeconfig_path,
            Some(bootstrap_path.as_path()),
            "node1",
            dir.path(),
        )
        .await;

        assert!(!kubeconfig_path.exists());
    }
}
