//! Thread-local directory registry.
//!
//! Maps opaque string ids ([`crate::FsAddress::id`]) to a ready-to-use
//! `dialog_storage` [`FileSystem`] provider rooted at the registered
//! directory. Consumers register a directory before invoking any capability
//! that targets the corresponding [`FsAddress`](crate::FsAddress); the provider
//! looks the directory up at execution time and delegates the capability to it.
//!
//! Thread-local because the typical WASM host is single-threaded and the
//! directory handles it manages are themselves `!Send` (`web_sys` JS
//! references). On native the constraint is harmless in practice — each test
//! registers its own unique id under its own tempdir.

use crate::FsError;
use dialog_storage::provider::FileSystem;
use std::cell::RefCell;
use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

thread_local! {
    static REGISTRY: RefCell<HashMap<String, FileSystem>> = RefCell::new(HashMap::new());
}

/// Register a local directory under the given id.
///
/// Existing entries for the same id are replaced. The directory is *not*
/// created — it is created on first write by the underlying provider, which
/// `mkdir -p`s parents.
#[cfg(not(target_arch = "wasm32"))]
pub fn register_directory(id: impl Into<String>, path: PathBuf) -> Result<(), FsError> {
    let handle = dialog_storage::provider::FileSystemHandle::try_from(path)
        .map_err(|e| FsError::UnregisteredHandle(e.to_string()))?;
    REGISTRY.with(|r| r.borrow_mut().insert(id.into(), FileSystem::from(handle)));
    Ok(())
}

/// Register a File System Access API directory handle under the given id.
///
/// The host calls this before any invocation targeting the corresponding
/// [`FsAddress`](crate::FsAddress) is dispatched. Typically the handle comes
/// from `showDirectoryPicker()` (or `navigator.storage.getDirectory()` in
/// tests).
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub fn register_directory(id: impl Into<String>, handle: web_sys::FileSystemDirectoryHandle) {
    let id = id.into();
    let provider = dialog_storage::provider::WebRoot::new(&id, handle).provider();
    REGISTRY.with(|r| r.borrow_mut().insert(id, provider));
}

/// Drop the entry for `id`. Returns whether an entry was present.
pub fn unregister_directory(id: &str) -> bool {
    REGISTRY.with(|r| r.borrow_mut().remove(id).is_some())
}

/// Look up the [`FileSystem`] provider previously registered under `id`.
pub(crate) fn lookup(id: &str) -> Result<FileSystem, FsError> {
    REGISTRY.with(|r| {
        r.borrow()
            .get(id)
            .cloned()
            .ok_or_else(|| FsError::UnregisteredHandle(id.to_string()))
    })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[dialog_common::test]
    async fn it_registers_and_looks_up_a_directory() {
        let tmp = tempdir().unwrap();
        let id = format!("registry-test-{}", tmp.path().display());
        register_directory(&id, tmp.path().to_path_buf()).unwrap();
        assert!(lookup(&id).is_ok());
        assert!(unregister_directory(&id));
        assert!(matches!(lookup(&id), Err(FsError::UnregisteredHandle(_))));
    }
}
