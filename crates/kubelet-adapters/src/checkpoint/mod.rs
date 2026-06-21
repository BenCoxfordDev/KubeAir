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

//! Checkpoint manager -- persists kubelet state across restarts.
//!
//! Mirrors pkg/kubelet/checkpointmanager.
//!
//! Checkpoints are written atomically: write to a temp file, then rename.
//! Each checkpoint is identified by a key (e.g. pod UID) and stores
//! a JSON-serialized payload.

use kubelet_core::error::{KubeletError, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

/// A single checkpoint entry on disk.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub key: String,
    pub path: PathBuf,
}

/// The checkpoint manager -- reads and writes state to a directory.
pub struct CheckpointManager {
    dir: PathBuf,
}

impl CheckpointManager {
    /// Create a manager rooted at `dir`. Creates the directory if needed.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    /// Write a checkpoint. Uses an atomic write (temp + rename).
    pub fn write<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let path = self.path_for(key);
        let tmp = path.with_extension("tmp");
        let json = serde_json::to_string_pretty(value).map_err(KubeletError::Serialization)?;
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, &path)?;
        debug!(key, path = %path.display(), "Checkpoint written");
        Ok(())
    }

    /// Read a checkpoint. Returns `None` if not found.
    pub fn read<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let path = self.path_for(key);
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(&path)?;
        let value = serde_json::from_str(&content).map_err(KubeletError::Serialization)?;
        Ok(Some(value))
    }

    /// Delete a checkpoint.
    pub fn delete(&self, key: &str) -> Result<()> {
        let path = self.path_for(key);
        if path.exists() {
            std::fs::remove_file(&path)?;
            debug!(key, "Checkpoint deleted");
        }
        Ok(())
    }

    /// List all checkpoint keys in the directory.
    pub fn list(&self) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map(|e| e == "json").unwrap_or(false)
                && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
            {
                keys.push(stem.to_string());
            }
        }
        Ok(keys)
    }

    /// Check if a checkpoint exists.
    pub fn exists(&self, key: &str) -> bool {
        self.path_for(key).exists()
    }

    fn path_for(&self, key: &str) -> PathBuf {
        // Sanitize key to be filesystem-safe
        let safe_key = key.replace(['/', ':'], "_");
        self.dir.join(format!("{}.json", safe_key))
    }
}

/// Typed wrapper for storing pod-specific checkpoints.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PodCheckpoint {
    pub uid: String,
    pub name: String,
    pub namespace: String,
    pub sandbox_id: Option<String>,
    pub container_ids: HashMap<String, String>, // container_name -> runtime_id
    pub restart_counts: HashMap<String, u32>,
}

impl PodCheckpoint {
    pub fn new(uid: &str, name: &str, namespace: &str) -> Self {
        Self {
            uid: uid.to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
            sandbox_id: None,
            container_ids: HashMap::new(),
            restart_counts: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestPayload {
        value: String,
        count: u32,
    }

    fn mgr(dir: &Path) -> CheckpointManager {
        CheckpointManager::new(dir).unwrap()
    }

    #[test]
    fn test_write_and_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let m = mgr(dir.path());
        let payload = TestPayload {
            value: "hello".to_string(),
            count: 42,
        };
        m.write("test-key", &payload).unwrap();
        let read: TestPayload = m.read("test-key").unwrap().unwrap();
        assert_eq!(read, payload);
    }

    #[test]
    fn test_read_nonexistent_returns_none() {
        let dir = TempDir::new().unwrap();
        let m = mgr(dir.path());
        let result: Option<TestPayload> = m.read("missing").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_exists() {
        let dir = TempDir::new().unwrap();
        let m = mgr(dir.path());
        assert!(!m.exists("k1"));
        m.write(
            "k1",
            &TestPayload {
                value: "x".to_string(),
                count: 0,
            },
        )
        .unwrap();
        assert!(m.exists("k1"));
    }

    #[test]
    fn test_delete() {
        let dir = TempDir::new().unwrap();
        let m = mgr(dir.path());
        m.write(
            "del-key",
            &TestPayload {
                value: "y".to_string(),
                count: 1,
            },
        )
        .unwrap();
        m.delete("del-key").unwrap();
        assert!(!m.exists("del-key"));
    }

    #[test]
    fn test_list_returns_all_keys() {
        let dir = TempDir::new().unwrap();
        let m = mgr(dir.path());
        for i in 0..5 {
            m.write(
                &format!("key-{}", i),
                &TestPayload {
                    value: "v".to_string(),
                    count: i,
                },
            )
            .unwrap();
        }
        let mut keys = m.list().unwrap();
        keys.sort();
        assert_eq!(keys.len(), 5);
        assert_eq!(keys[0], "key-0");
    }

    #[test]
    fn test_overwrite_updates_value() {
        let dir = TempDir::new().unwrap();
        let m = mgr(dir.path());
        m.write(
            "k",
            &TestPayload {
                value: "first".to_string(),
                count: 1,
            },
        )
        .unwrap();
        m.write(
            "k",
            &TestPayload {
                value: "second".to_string(),
                count: 2,
            },
        )
        .unwrap();
        let result: TestPayload = m.read("k").unwrap().unwrap();
        assert_eq!(result.value, "second");
        assert_eq!(result.count, 2);
    }

    #[test]
    fn test_key_with_slash_is_sanitized() {
        let dir = TempDir::new().unwrap();
        let m = mgr(dir.path());
        let payload = TestPayload {
            value: "ns/name".to_string(),
            count: 0,
        };
        m.write("default/my-pod", &payload).unwrap();
        assert!(m.exists("default/my-pod"));
        let read: TestPayload = m.read("default/my-pod").unwrap().unwrap();
        assert_eq!(read.value, "ns/name");
    }

    #[test]
    fn test_pod_checkpoint_roundtrip() {
        let dir = TempDir::new().unwrap();
        let m = mgr(dir.path());
        let mut cp = PodCheckpoint::new("uid-abc", "my-pod", "default");
        cp.sandbox_id = Some("sandbox-123".to_string());
        cp.container_ids
            .insert("nginx".to_string(), "ctr-456".to_string());
        cp.restart_counts.insert("nginx".to_string(), 3);

        m.write("uid-abc", &cp).unwrap();
        let read: PodCheckpoint = m.read("uid-abc").unwrap().unwrap();
        assert_eq!(read.sandbox_id, Some("sandbox-123".to_string()));
        assert_eq!(read.container_ids["nginx"], "ctr-456");
        assert_eq!(read.restart_counts["nginx"], 3);
    }

    #[test]
    fn test_list_empty_dir() {
        let dir = TempDir::new().unwrap();
        let m = mgr(dir.path());
        assert!(m.list().unwrap().is_empty());
    }

    #[test]
    fn test_creates_dir_if_missing() {
        let dir = TempDir::new().unwrap();
        let subdir = dir.path().join("a").join("b").join("c");
        let m = CheckpointManager::new(&subdir).unwrap();
        m.write(
            "k",
            &TestPayload {
                value: "v".to_string(),
                count: 0,
            },
        )
        .unwrap();
        assert!(m.exists("k"));
    }
}
