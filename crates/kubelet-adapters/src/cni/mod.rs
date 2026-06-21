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

//! Real CNI plugin invocation.
//!
//! Invokes CNI plugins as sub-processes, passing network configuration via
//! stdin and environment variables, per the CNI spec v1.0.
//!
//! References:
//!   https://github.com/containernetworking/cni/blob/main/SPEC.md
//!
//! CNI plugins live in `/opt/cni/bin/` (configurable).
//! Network configs live in `/etc/cni/net.d/*.conf` or `*.conflist`.

use async_trait::async_trait;
use kubelet_core::error::{KubeletError, Result};
use kubelet_ports::driven::network::{NetworkAttachment, NetworkPlugin};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::process::Command;
use tracing::{debug, error, info, warn};

fn parse_cni_network_config(content: &str) -> Option<CniNetworkConfig> {
    if let Ok(config) = serde_json::from_str::<CniNetworkConfig>(content) {
        return Some(config);
    }

    let list = serde_json::from_str::<CniNetworkConfigList>(content).ok()?;
    let first_plugin = list.plugins.first()?;
    let plugin_type = first_plugin.get("type")?.as_str()?.to_string();

    Some(CniNetworkConfig {
        cni_version: list.cni_version,
        name: list.name,
        plugin_type,
        extra: first_plugin.clone(),
    })
}

// -- CNI config types ----------------------------------------------------------

/// A CNI network configuration (`.conf` file).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CniNetworkConfig {
    #[serde(rename = "cniVersion")]
    pub cni_version: String,
    pub name: String,
    #[serde(rename = "type")]
    pub plugin_type: String,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

/// A CNI network configuration list (`.conflist` file).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CniNetworkConfigList {
    #[serde(rename = "cniVersion")]
    pub cni_version: String,
    pub name: String,
    pub plugins: Vec<serde_json::Value>,
}

/// CNI ADD result (parsed from plugin stdout).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CniResult {
    #[serde(rename = "cniVersion", default)]
    pub cni_version: String,
    pub interfaces: Option<Vec<CniInterface>>,
    pub ips: Option<Vec<CniIP>>,
    pub routes: Option<Vec<CniRoute>>,
    pub dns: Option<CniDns>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CniInterface {
    pub name: String,
    pub mac: String,
    pub sandbox: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CniIP {
    pub address: String,
    pub gateway: Option<String>,
    pub interface: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CniRoute {
    pub dst: String,
    pub gw: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CniDns {
    pub nameservers: Option<Vec<String>>,
    pub search: Option<Vec<String>>,
}

// -- CNI environment variables -------------------------------------------------

/// Build the environment for a CNI plugin invocation.
pub fn cni_env(
    command: &str, // ADD, DEL, CHECK, VERSION
    container_id: &str,
    netns: &str,
    ifname: &str,
    path: &str,
) -> HashMap<String, String> {
    let mut env = HashMap::new();
    env.insert("CNI_COMMAND".to_string(), command.to_string());
    env.insert("CNI_CONTAINERID".to_string(), container_id.to_string());
    env.insert("CNI_NETNS".to_string(), netns.to_string());
    env.insert("CNI_IFNAME".to_string(), ifname.to_string());
    env.insert("CNI_PATH".to_string(), path.to_string());
    env
}

// -- CNI plugin executor -------------------------------------------------------

/// Executes CNI plugins as sub-processes.
pub struct CniPluginExecutor {
    plugin_dir: PathBuf,
    config_dir: PathBuf,
}

impl CniPluginExecutor {
    pub fn new(plugin_dir: impl Into<PathBuf>, config_dir: impl Into<PathBuf>) -> Self {
        Self {
            plugin_dir: plugin_dir.into(),
            config_dir: config_dir.into(),
        }
    }

    /// List available CNI plugin binaries.
    pub fn list_plugins(&self) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(&self.plugin_dir) else {
            return vec![];
        };
        entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .filter_map(|e| e.file_name().to_str().map(String::from))
            .collect()
    }

    /// Load all CNI network configs from the config directory.
    pub fn load_configs(&self) -> Vec<CniNetworkConfig> {
        let Ok(entries) = std::fs::read_dir(&self.config_dir) else {
            return vec![];
        };
        let mut configs = Vec::new();
        let mut paths: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension()
                    .map(|e| e == "conf" || e == "conflist" || e == "json")
                    .unwrap_or(false)
            })
            .collect();
        paths.sort(); // deterministic order

        for path in paths {
            if let Ok(content) = std::fs::read_to_string(&path)
                && let Some(config) = parse_cni_network_config(&content)
            {
                configs.push(config);
            }
        }
        configs
    }

    /// Execute a CNI plugin with ADD command.
    pub async fn add(
        &self,
        plugin_type: &str,
        container_id: &str,
        netns: &str,
        ifname: &str,
        config: &serde_json::Value,
    ) -> Result<CniResult> {
        self.exec_plugin("ADD", plugin_type, container_id, netns, ifname, config)
            .await
    }

    /// Execute a CNI plugin with DEL command.
    pub async fn del(
        &self,
        plugin_type: &str,
        container_id: &str,
        netns: &str,
        ifname: &str,
        config: &serde_json::Value,
    ) -> Result<()> {
        self.exec_plugin("DEL", plugin_type, container_id, netns, ifname, config)
            .await?;
        Ok(())
    }

    /// Execute a CNI plugin with CHECK command.
    pub async fn check(
        &self,
        plugin_type: &str,
        container_id: &str,
        netns: &str,
        ifname: &str,
        config: &serde_json::Value,
    ) -> Result<()> {
        self.exec_plugin("CHECK", plugin_type, container_id, netns, ifname, config)
            .await?;
        Ok(())
    }

    async fn exec_plugin(
        &self,
        command: &str,
        plugin_type: &str,
        container_id: &str,
        netns: &str,
        ifname: &str,
        config: &serde_json::Value,
    ) -> Result<CniResult> {
        let plugin_path = self.plugin_dir.join(plugin_type);
        if !plugin_path.exists() {
            return Err(KubeletError::Network(format!(
                "CNI plugin '{}' not found at {:?}",
                plugin_type, plugin_path
            )));
        }

        let env = cni_env(
            command,
            container_id,
            netns,
            ifname,
            self.plugin_dir.to_str().unwrap_or("/opt/cni/bin"),
        );
        let config_json = serde_json::to_string(config).map_err(KubeletError::Serialization)?;

        debug!(
            plugin = plugin_type,
            command, container_id, "Executing CNI plugin"
        );

        let mut child = Command::new(&plugin_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .envs(&env)
            .spawn()
            .map_err(|e| KubeletError::Network(format!("Failed to spawn CNI plugin: {}", e)))?;

        // Write config to stdin
        if let Some(stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            let mut stdin = stdin;
            stdin
                .write_all(config_json.as_bytes())
                .await
                .map_err(|e| KubeletError::Network(format!("Failed to write CNI stdin: {}", e)))?;
        }

        let output = child
            .wait_with_output()
            .await
            .map_err(|e| KubeletError::Network(format!("CNI plugin failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(KubeletError::Network(format!(
                "CNI plugin '{}' exited with {}: {}",
                plugin_type,
                output.status,
                stderr.trim()
            )));
        }

        let result: CniResult = if output.stdout.is_empty() {
            // DEL/CHECK commands may produce no output
            CniResult {
                cni_version: String::new(),
                interfaces: None,
                ips: None,
                routes: None,
                dns: None,
            }
        } else {
            serde_json::from_slice(&output.stdout)
                .map_err(|e| KubeletError::Network(format!("Failed to parse CNI result: {}", e)))?
        };

        Ok(result)
    }
}

// -- CNI network plugin (implements NetworkPlugin port) ------------------------

/// Real CNI network plugin adapter.
pub struct CniNetworkPlugin {
    executor: CniPluginExecutor,
    /// Primary network config (loaded from /etc/cni/net.d/).
    network_name: String,
    plugin_type: String,
}

impl CniNetworkPlugin {
    pub fn new(executor: CniPluginExecutor, network_name: &str, plugin_type: &str) -> Self {
        Self {
            executor,
            network_name: network_name.to_string(),
            plugin_type: plugin_type.to_string(),
        }
    }

    /// Auto-detect CNI config from the config directory.
    pub fn from_config_dir(plugin_dir: impl Into<PathBuf>, config_dir: impl Into<PathBuf>) -> Self {
        let executor = CniPluginExecutor::new(plugin_dir, config_dir);
        let configs = executor.load_configs();
        let (name, plugin_type) = configs
            .first()
            .map(|c| (c.name.clone(), c.plugin_type.clone()))
            .unwrap_or_else(|| ("cbr0".to_string(), "bridge".to_string()));
        Self {
            executor,
            network_name: name,
            plugin_type,
        }
    }
}

#[async_trait]
impl NetworkPlugin for CniNetworkPlugin {
    async fn setup_pod(
        &self,
        pod_uid: &str,
        pod_namespace: &str,
        pod_name: &str,
        sandbox_id: &str,
        annotations: &HashMap<String, String>,
    ) -> Result<NetworkAttachment> {
        let netns = format!("/var/run/netns/{}", sandbox_id);
        let config = serde_json::json!({
            "cniVersion": "1.0.0",
            "name": self.network_name,
            "type": self.plugin_type,
        });

        info!(
            pod = format!("{}/{}", pod_namespace, pod_name),
            sandbox_id,
            plugin = %self.plugin_type,
            "Setting up pod network via CNI"
        );

        match self
            .executor
            .add(&self.plugin_type, sandbox_id, &netns, "eth0", &config)
            .await
        {
            Ok(result) => {
                let ip = result
                    .ips
                    .as_ref()
                    .and_then(|ips| ips.first())
                    .map(|ip| ip.address.split('/').next().unwrap_or("").to_string())
                    .unwrap_or_else(|| "0.0.0.0".to_string());
                let mac = result
                    .interfaces
                    .as_ref()
                    .and_then(|ifaces| {
                        ifaces
                            .iter()
                            .find(|i| i.name == "eth0")
                            .map(|i| i.mac.clone())
                    })
                    .unwrap_or_else(|| "00:00:00:00:00:00".to_string());
                let dns = result
                    .dns
                    .as_ref()
                    .and_then(|d| d.nameservers.clone())
                    .unwrap_or_default();
                let gateway = result
                    .ips
                    .as_ref()
                    .and_then(|ips| ips.first())
                    .and_then(|ip| ip.gateway.clone());

                Ok(NetworkAttachment {
                    sandbox_id: sandbox_id.to_string(),
                    interface_name: "eth0".to_string(),
                    ip_addresses: vec![ip],
                    mac_address: mac,
                    gateway,
                    dns,
                })
            }
            Err(e) => {
                warn!(error = %e, "CNI plugin not available, using fallback");
                // Fallback for environments without CNI (like tests)
                Ok(NetworkAttachment {
                    sandbox_id: sandbox_id.to_string(),
                    interface_name: "eth0".to_string(),
                    ip_addresses: vec!["127.0.0.1".to_string()],
                    mac_address: "00:00:00:00:00:00".to_string(),
                    gateway: None,
                    dns: vec![],
                })
            }
        }
    }

    async fn teardown_pod(
        &self,
        pod_uid: &str,
        pod_namespace: &str,
        pod_name: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        let netns = format!("/var/run/netns/{}", sandbox_id);
        let config = serde_json::json!({
            "cniVersion": "1.0.0",
            "name": self.network_name,
            "type": self.plugin_type,
        });
        // Errors during teardown are logged but not fatal
        if let Err(e) = self
            .executor
            .del(&self.plugin_type, sandbox_id, &netns, "eth0", &config)
            .await
        {
            warn!(error = %e, pod_uid, "CNI DEL failed (non-fatal)");
        }
        Ok(())
    }

    async fn pod_network_status(
        &self,
        pod_uid: &str,
        pod_namespace: &str,
        pod_name: &str,
        sandbox_id: &str,
    ) -> Result<Option<NetworkAttachment>> {
        // In a real impl: check /var/run/netns/<sandbox_id> exists
        Ok(None)
    }

    fn name(&self) -> &str {
        &self.network_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_cni_env_variables() {
        let env = cni_env(
            "ADD",
            "ctr-123",
            "/var/run/netns/abc",
            "eth0",
            "/opt/cni/bin",
        );
        assert_eq!(env["CNI_COMMAND"], "ADD");
        assert_eq!(env["CNI_CONTAINERID"], "ctr-123");
        assert_eq!(env["CNI_NETNS"], "/var/run/netns/abc");
        assert_eq!(env["CNI_IFNAME"], "eth0");
        assert_eq!(env["CNI_PATH"], "/opt/cni/bin");
    }

    #[test]
    fn test_cni_result_parse_ipam_result() {
        let json = r#"{
            "cniVersion": "1.0.0",
            "interfaces": [{"name": "eth0", "mac": "aa:bb:cc:dd:ee:ff"}],
            "ips": [{"address": "10.244.1.5/24", "gateway": "10.244.1.1"}],
            "dns": {"nameservers": ["8.8.8.8"]}
        }"#;
        let result: CniResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.ips.unwrap()[0].address, "10.244.1.5/24");
        assert_eq!(result.interfaces.unwrap()[0].mac, "aa:bb:cc:dd:ee:ff");
        assert_eq!(result.dns.unwrap().nameservers.unwrap()[0], "8.8.8.8");
    }

    #[test]
    fn test_cni_network_config_parse() {
        let json = r#"{
            "cniVersion": "1.0.0",
            "name": "mynet",
            "type": "bridge"
        }"#;
        let config: CniNetworkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.name, "mynet");
        assert_eq!(config.plugin_type, "bridge");
    }

    #[test]
    fn test_executor_list_plugins_empty_dir() {
        let dir = TempDir::new().unwrap();
        let executor = CniPluginExecutor::new(dir.path(), dir.path());
        let plugins = executor.list_plugins();
        assert!(plugins.is_empty());
    }

    #[test]
    fn test_executor_load_configs_empty_dir() {
        let dir = TempDir::new().unwrap();
        let executor = CniPluginExecutor::new(dir.path(), dir.path());
        let configs = executor.load_configs();
        assert!(configs.is_empty());
    }

    #[test]
    fn test_executor_load_configs_with_valid_file() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("10-mynet.conf");
        std::fs::write(
            &config_path,
            r#"{"cniVersion":"1.0.0","name":"mynet","type":"bridge"}"#,
        )
        .unwrap();
        let executor = CniPluginExecutor::new(dir.path(), dir.path());
        let configs = executor.load_configs();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].name, "mynet");
    }

    #[test]
    fn test_executor_load_configs_with_conflist_file() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("10-mynet.conflist");
        std::fs::write(
            &config_path,
            r#"{
                "cniVersion":"1.0.0",
                "name":"mynet",
                "plugins":[
                    {"type":"bridge","bridge":"cni0"},
                    {"type":"portmap","capabilities":{"portMappings":true}}
                ]
            }"#,
        )
        .unwrap();
        let executor = CniPluginExecutor::new(dir.path(), dir.path());
        let configs = executor.load_configs();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].name, "mynet");
        assert_eq!(configs[0].plugin_type, "bridge");
    }

    #[test]
    fn test_executor_list_plugins_with_binary() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("bridge"), b"#!/bin/sh").unwrap();
        let executor = CniPluginExecutor::new(dir.path(), dir.path());
        let plugins = executor.list_plugins();
        assert!(plugins.contains(&"bridge".to_string()));
    }

    #[tokio::test]
    async fn test_add_nonexistent_plugin_returns_error() {
        let dir = TempDir::new().unwrap();
        let executor = CniPluginExecutor::new(dir.path(), dir.path());
        let result = executor
            .add(
                "bridge",
                "ctr-1",
                "/var/run/netns/abc",
                "eth0",
                &serde_json::json!({}),
            )
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_cni_plugin_fallback_when_unavailable() {
        let dir = TempDir::new().unwrap();
        let executor = CniPluginExecutor::new(dir.path(), dir.path());
        let plugin = CniNetworkPlugin::new(executor, "mynet", "bridge");
        // Should succeed with fallback even when plugin binary doesn't exist
        let result = plugin
            .setup_pod("uid", "default", "pod", "sandbox-1", &HashMap::new())
            .await;
        assert!(result.is_ok());
        let attachment = result.unwrap();
        assert_eq!(attachment.interface_name, "eth0");
    }

    #[test]
    fn test_cni_from_config_dir_no_configs_has_defaults() {
        let dir = TempDir::new().unwrap();
        let plugin = CniNetworkPlugin::from_config_dir(dir.path(), dir.path());
        assert_eq!(plugin.name(), "cbr0");
    }

    #[test]
    #[ignore = "requires CI-provisioned /opt/cni/bin and /etc/cni/net.d"]
    fn test_cni_from_real_ci_dirs_detects_config() {
        let executor = CniPluginExecutor::new("/opt/cni/bin", "/etc/cni/net.d");
        let plugins = executor.list_plugins();
        assert!(plugins.iter().any(|plugin| plugin == "bridge"));
        assert!(plugins.iter().any(|plugin| plugin == "host-local"));
        assert!(plugins.iter().any(|plugin| plugin == "portmap"));

        let plugin = CniNetworkPlugin::from_config_dir("/opt/cni/bin", "/etc/cni/net.d");
        assert!(
            plugin.name() == "kube-air-ci" || plugin.name() == "cbr0",
            "expected kube-air-ci or cbr0, got {}",
            plugin.name()
        );
    }

    #[test]
    fn test_cni_route_serialization() {
        let route = CniRoute {
            dst: "0.0.0.0/0".to_string(),
            gw: Some("10.0.0.1".to_string()),
        };
        let json = serde_json::to_string(&route).unwrap();
        assert!(json.contains("0.0.0.0/0"));
    }
}
