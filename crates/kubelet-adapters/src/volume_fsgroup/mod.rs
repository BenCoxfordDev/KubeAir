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

//! Volume FSGroup + ownership change.
//!
//! Before a container starts, the kubelet must recursively chown mounted volumes
//! to match the pod's fsGroup GID so the container process can read/write them.
//!
//! Mirrors pkg/volume/util/fs.go + pkg/kubelet/volume_host.go.
//!
//! FSGroupPolicy controls how chown is applied:
//!   ReadWriteOnceWithFSType -- only for block volumes with specific fsType.
//!   File                   -- always apply fsGroup (default for most volumes).
//!   None                   -- never apply.

use kubelet_core::error::{KubeletError, Result};
use std::path::Path;
use tracing::{debug, warn};

#[derive(Debug, Clone, PartialEq)]
pub enum FsGroupPolicy {
    None,
    File,
    ReadWriteOnceWithFsType,
}

/// Apply fsGroup ownership to a volume mount path.
///
/// Sets the GID of all files/dirs to `fs_group` and sets the setgid bit
/// on directories so new files inherit the GID.
pub fn apply_fs_group(path: &Path, fs_group: u32, policy: &FsGroupPolicy) -> Result<()> {
    if *policy == FsGroupPolicy::None {
        return Ok(());
    }

    if !path.exists() {
        return Ok(());
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        apply_fs_group_recursive(path, fs_group)?;
    }

    debug!(path = %path.display(), gid = fs_group, "FSGroup applied");
    Ok(())
}

#[cfg(unix)]
fn apply_fs_group_recursive(path: &Path, gid: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    // chown the entry itself.
    chown_gid(path, gid)?;

    // If it's a directory, set setgid and recurse.
    let metadata = std::fs::metadata(path)
        .map_err(|e| KubeletError::Storage(format!("stat {}: {}", path.display(), e)))?;

    if metadata.is_dir() {
        // Set setgid bit so new files inherit GID.
        let mode = metadata.permissions().mode();
        let new_mode = mode | 0o2000; // setgid
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(new_mode))
            .map_err(|e| KubeletError::Storage(format!("chmod setgid: {}", e)))?;

        // Recurse into children.
        let Ok(entries) = std::fs::read_dir(path) else {
            return Ok(());
        };
        for entry in entries.flatten() {
            let child_path = entry.path();
            // Don't follow symlinks (matches Go kubelet behaviour).
            if entry.file_type().map(|t| t.is_symlink()).unwrap_or(false) {
                chown_gid(&child_path, gid)?;
                continue;
            }
            apply_fs_group_recursive(&child_path, gid)?;
        }
    }

    Ok(())
}

#[cfg(unix)]
fn chown_gid(path: &Path, gid: u32) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| KubeletError::Storage("invalid path for chown".to_string()))?;

    // Use lchown to avoid following symlinks.
    let ret = unsafe { libc::lchown(c_path.as_ptr(), u32::MAX, gid) }; // -1 = don't change uid
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        return Err(KubeletError::Storage(format!(
            "lchown {} to gid {}: {}",
            path.display(),
            gid,
            err
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn apply_fs_group_recursive(_path: &Path, _gid: u32) -> Result<()> {
    Ok(()) // no-op on non-Unix
}

/// Check if an existing volume already has the correct fsGroup ownership.
/// Returns true if all files/dirs are already owned by `fs_group`.
#[cfg(unix)]
pub fn volume_has_correct_ownership(path: &Path, fs_group: u32) -> bool {
    use std::os::unix::fs::MetadataExt;
    if !path.exists() {
        return false;
    }

    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if metadata.gid() != fs_group {
        return false;
    }

    if metadata.is_dir() {
        let Ok(entries) = std::fs::read_dir(path) else {
            return true;
        };
        for entry in entries.flatten() {
            if !volume_has_correct_ownership(&entry.path(), fs_group) {
                return false;
            }
        }
    }
    true
}

#[cfg(not(unix))]
pub fn volume_has_correct_ownership(_path: &Path, _fs_group: u32) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_apply_fs_group_none_policy_noop() {
        let dir = TempDir::new().unwrap();
        let result = apply_fs_group(dir.path(), 2000, &FsGroupPolicy::None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_fs_group_file_policy_on_dir() {
        let dir = TempDir::new().unwrap();
        // Write a file to the dir.
        std::fs::write(dir.path().join("file.txt"), b"hello").unwrap();
        // Use the current process GID so lchown succeeds without root privileges.
        #[cfg(unix)]
        let gid = unsafe { libc::getegid() };
        #[cfg(not(unix))]
        let gid = 0u32;
        let result = apply_fs_group(dir.path(), gid, &FsGroupPolicy::File);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_fs_group_missing_path_is_ok() {
        let result = apply_fs_group(Path::new("/nonexistent/path"), 1000, &FsGroupPolicy::File);
        assert!(result.is_ok());
    }
}
