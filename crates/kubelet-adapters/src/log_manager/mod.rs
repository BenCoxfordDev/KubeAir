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

//! Container log manager -- JSON log driver + rotation.
//!
//! Each container's logs are written as JSON lines to:
//!   /var/log/pods/{namespace}_{name}_{uid}/{container_name}/{N}.log
//!
//! where N starts at 0 and increments on rotation.
//!
//! Rotation triggers when a log file exceeds `containerLogMaxSize`.
//! Only `containerLogMaxFiles` files are kept per container.
//!
//! JSON log format (Docker-compatible, used by `kubectl logs`):
//!   {"log":"...\n","stream":"stdout","time":"2026-05-15T04:31:00.000Z"}
//!
//! Mirrors pkg/kubelet/logs/ and pkg/util/tail/.

use chrono::Utc;
use kubelet_core::error::{KubeletError, Result};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

// -- JSON log entry ------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry {
    /// The log line content.
    pub log: String,
    /// "stdout" or "stderr".
    pub stream: String,
    /// RFC3339Nano timestamp.
    pub time: String,
}

impl LogEntry {
    pub fn stdout(message: &str) -> Self {
        Self {
            log: if message.ends_with('\n') {
                message.to_string()
            } else {
                format!("{}\n", message)
            },
            stream: "stdout".to_string(),
            time: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        }
    }

    pub fn stderr(message: &str) -> Self {
        Self {
            log: if message.ends_with('\n') {
                message.to_string()
            } else {
                format!("{}\n", message)
            },
            stream: "stderr".to_string(),
            time: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        }
    }

    pub fn to_json_line(&self) -> String {
        serde_json::to_string(self).unwrap_or_default() + "\n"
    }

    /// Parse a log line from either JSON format ({"log":...}) or CRI log format
    /// (TIMESTAMP STREAM FLAGS MESSAGE).
    ///
    /// CRI log format: `<timestamp> <stream> <flags> <message>`
    /// where flags is `F` (full line) or `P` (partial line).
    pub fn parse_line(line: &str) -> Option<Self> {
        // Try JSON first (our internal format)
        if let Ok(entry) = serde_json::from_str::<LogEntry>(line) {
            return Some(entry);
        }

        // Try CRI log format: TIMESTAMP STREAM FLAGS MESSAGE
        // Example: 2026-05-29T10:41:44.977387624+01:00 stdout F mount type...
        let mut parts = line.splitn(4, ' ');
        let timestamp = parts.next()?;
        let stream = parts.next()?;
        let _flags = parts.next()?; // F=full, P=partial
        let message = parts.next().unwrap_or("");

        // Validate stream field
        if stream != "stdout" && stream != "stderr" {
            return None;
        }

        let log = if message.ends_with('\n') {
            message.to_string()
        } else {
            format!("{}\n", message)
        };

        Some(LogEntry {
            log,
            stream: stream.to_string(),
            time: timestamp.to_string(),
        })
    }
}

// -- Log file manager ----------------------------------------------------------

/// Manages log files for a single container.
pub struct ContainerLogManager {
    /// Log directory for this container.
    log_dir: PathBuf,
    /// Maximum size per log file in bytes.
    max_size_bytes: u64,
    /// Maximum number of log files to retain.
    max_files: u32,
    /// Current log file index (0, 1, 2, ...).
    current_index: u32,
    /// Current file handle.
    current_file: Option<std::fs::File>,
    /// Current file size in bytes.
    current_size: u64,
}

impl ContainerLogManager {
    /// Create a log manager for a container.
    ///
    /// `log_dir` = /var/log/pods/{ns}_{name}_{uid}/{container_name}/
    pub fn new(log_dir: PathBuf, max_size_bytes: u64, max_files: u32) -> Result<Self> {
        std::fs::create_dir_all(&log_dir)
            .map_err(|e| KubeletError::Storage(format!("create log dir: {}", e)))?;

        // Find the highest existing log index.
        let current_index = Self::find_current_index(&log_dir);
        let current_path = log_dir.join(format!("{}.log", current_index));
        let current_size = std::fs::metadata(&current_path)
            .map(|m| m.len())
            .unwrap_or(0);

        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current_path)
            .map_err(|e| KubeletError::Storage(format!("open log file: {}", e)))?;

        Ok(Self {
            log_dir,
            max_size_bytes,
            max_files,
            current_index,
            current_file: Some(file),
            current_size,
        })
    }

    /// Write a log line to the current file, rotating if needed.
    pub fn write(&mut self, entry: &LogEntry) -> Result<()> {
        let line = entry.to_json_line();
        let line_bytes = line.as_bytes();

        // Rotate if needed.
        if self.current_size + line_bytes.len() as u64 > self.max_size_bytes {
            self.rotate()?;
        }

        if let Some(file) = &mut self.current_file {
            file.write_all(line_bytes)
                .map_err(|e| KubeletError::Storage(format!("write log: {}", e)))?;
            self.current_size += line_bytes.len() as u64;
        }
        Ok(())
    }

    /// Force a log rotation.
    pub fn rotate(&mut self) -> Result<()> {
        // Flush and close current file.
        drop(self.current_file.take());

        // Advance to next index.
        self.current_index += 1;
        self.current_size = 0;

        // Clean up old files beyond max_files.
        if self.current_index >= self.max_files {
            let to_delete = self.current_index.saturating_sub(self.max_files);
            let old_path = self.log_dir.join(format!("{}.log", to_delete));
            if old_path.exists() {
                let _ = std::fs::remove_file(&old_path);
                debug!(path = %old_path.display(), "Rotated old log file removed");
            }
        }

        let new_path = self.log_dir.join(format!("{}.log", self.current_index));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&new_path)
            .map_err(|e| KubeletError::Storage(format!("open rotated log: {}", e)))?;

        info!(path = %new_path.display(), "Log rotated");
        self.current_file = Some(file);
        Ok(())
    }

    /// Return the path of the most recent (current) log file.
    pub fn current_log_path(&self) -> PathBuf {
        self.log_dir.join(format!("{}.log", self.current_index))
    }

    /// Return all log file paths sorted oldest first.
    pub fn log_files(&self) -> Vec<PathBuf> {
        let start = self.current_index.saturating_sub(self.max_files - 1);
        (start..=self.current_index)
            .map(|i| self.log_dir.join(format!("{}.log", i)))
            .filter(|p| p.exists())
            .collect()
    }

    fn find_current_index(log_dir: &Path) -> u32 {
        std::fs::read_dir(log_dir)
            .ok()
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                name.strip_suffix(".log")?.parse::<u32>().ok()
            })
            .max()
            .unwrap_or(0)
    }
}

// -- Node-level log manager ----------------------------------------------------

/// Creates and manages log directories for all pods/containers.
pub struct LogManager {
    log_root: PathBuf,
    max_size_bytes: u64,
    max_files: u32,
}

impl LogManager {
    pub fn new(log_root: impl Into<PathBuf>, max_size_bytes: u64, max_files: u32) -> Self {
        Self {
            log_root: log_root.into(),
            max_size_bytes,
            max_files,
        }
    }

    /// Get the log directory for a container.
    /// /var/log/pods/{namespace}_{pod_name}_{uid}/{container_name}/
    pub fn container_log_dir(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        container_name: &str,
    ) -> PathBuf {
        self.log_root
            .join(format!("{}_{}_{}", namespace, pod_name, pod_uid))
            .join(container_name)
    }

    /// Create a `ContainerLogManager` for a specific container.
    pub fn for_container(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        container_name: &str,
    ) -> Result<ContainerLogManager> {
        let dir = self.container_log_dir(namespace, pod_name, pod_uid, container_name);
        ContainerLogManager::new(dir, self.max_size_bytes, self.max_files)
    }

    /// Remove all logs for a pod (called on pod GC).
    pub fn remove_pod_logs(&self, namespace: &str, pod_name: &str, pod_uid: &str) -> Result<()> {
        let pod_dir = self
            .log_root
            .join(format!("{}_{}_{}", namespace, pod_name, pod_uid));
        if pod_dir.exists() {
            std::fs::remove_dir_all(&pod_dir)
                .map_err(|e| KubeletError::Storage(format!("remove pod logs: {}", e)))?;
            info!(pod = %pod_name, "Pod logs removed");
        }
        Ok(())
    }

    /// Return current log size in bytes for a container.
    pub fn log_size_bytes(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        container_name: &str,
    ) -> u64 {
        let dir = self.container_log_dir(namespace, pod_name, pod_uid, container_name);
        walkdir_size(&dir)
    }
}

fn walkdir_size(dir: &Path) -> u64 {
    if !dir.exists() {
        return 0;
    }
    std::fs::read_dir(dir)
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

/// Parse a size string like "10Mi" into bytes.
pub fn parse_log_max_size(s: &str) -> u64 {
    if let Some(n) = s.strip_suffix("Ki") {
        return n.parse::<u64>().unwrap_or(10) * 1024;
    }
    if let Some(n) = s.strip_suffix("Mi") {
        return n.parse::<u64>().unwrap_or(10) * 1024 * 1024;
    }
    if let Some(n) = s.strip_suffix("Gi") {
        return n.parse::<u64>().unwrap_or(1) * 1024 * 1024 * 1024;
    }
    s.parse().unwrap_or(10 * 1024 * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_log_entry_stdout() {
        let e = LogEntry::stdout("hello world");
        assert_eq!(e.stream, "stdout");
        assert!(e.log.ends_with('\n'));
    }

    #[test]
    fn test_log_entry_json() {
        let e = LogEntry::stdout("test message");
        let json = e.to_json_line();
        let parsed: serde_json::Value = serde_json::from_str(json.trim()).unwrap();
        assert_eq!(parsed["stream"], "stdout");
        assert!(parsed["log"].as_str().unwrap().contains("test message"));
    }

    #[test]
    fn test_container_log_manager_write() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path().join("ns_pod_uid/app");
        let mut mgr = ContainerLogManager::new(log_dir.clone(), 1024 * 1024, 5).unwrap();
        let entry = LogEntry::stdout("Hello, logs!");
        mgr.write(&entry).unwrap();
        let current = mgr.current_log_path();
        assert!(current.exists());
        let content = std::fs::read_to_string(&current).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
        assert!(parsed["log"].as_str().unwrap().contains("Hello, logs!"));
    }

    #[test]
    fn test_container_log_manager_rotation() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path().join("logs");
        // Set tiny max size to force rotation.
        let mut mgr = ContainerLogManager::new(log_dir.clone(), 10, 3).unwrap();
        for i in 0..5 {
            mgr.write(&LogEntry::stdout(&format!("line {}", i)))
                .unwrap();
        }
        // Should have rotated at least once.
        assert!(mgr.current_index >= 1);
    }

    #[test]
    fn test_parse_log_max_size() {
        assert_eq!(parse_log_max_size("10Mi"), 10 * 1024 * 1024);
        assert_eq!(parse_log_max_size("100Ki"), 100 * 1024);
        assert_eq!(parse_log_max_size("1Gi"), 1024 * 1024 * 1024);
        assert_eq!(parse_log_max_size("5242880"), 5242880);
    }

    #[test]
    fn test_log_manager_directory_structure() {
        let dir = TempDir::new().unwrap();
        let mgr = LogManager::new(dir.path(), 10 * 1024 * 1024, 5);
        let log_dir = mgr.container_log_dir("default", "my-pod", "uid-1", "app");
        assert!(log_dir
            .to_str()
            .unwrap()
            .contains("default_my-pod_uid-1/app"));
    }

    #[test]
    fn test_log_manager_for_container() {
        let dir = TempDir::new().unwrap();
        let mgr = LogManager::new(dir.path(), 1024 * 1024, 5);
        let mut ctr_mgr = mgr
            .for_container("default", "nginx", "uid-2", "nginx")
            .unwrap();
        ctr_mgr
            .write(&LogEntry::stderr("an error occurred"))
            .unwrap();
        let size = mgr.log_size_bytes("default", "nginx", "uid-2", "nginx");
        assert!(size > 0);
    }

    #[test]
    fn test_log_manager_remove_pod_logs() {
        let dir = TempDir::new().unwrap();
        let mgr = LogManager::new(dir.path(), 1024 * 1024, 5);
        let mut ctr_mgr = mgr
            .for_container("default", "pod1", "uid-3", "app")
            .unwrap();
        ctr_mgr.write(&LogEntry::stdout("hello")).unwrap();
        let pod_dir = dir.path().join("default_pod1_uid-3");
        assert!(pod_dir.exists());
        mgr.remove_pod_logs("default", "pod1", "uid-3").unwrap();
        assert!(!pod_dir.exists());
    }
}
