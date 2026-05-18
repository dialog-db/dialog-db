//! Thread-local handle registry.
//!
//! Maps opaque string ids ([`crate::FsAddress::id`]) to platform-specific
//! directory handles. Consumers register a handle before invoking any
//! capability that targets the corresponding [`FsAddress`]; providers
//! look up the handle at execution time.
//!
//! Thread-local because the typical WASM host is single-threaded and the
//! handles it manages are themselves `!Send` (`web_sys` JS references).
//! On native the constraint is harmless in practice — each test
//! registers its own unique id under its own tempdir.

use crate::FsError;
use crate::handle::Handle;
use std::cell::RefCell;
use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

thread_local! {
    static REGISTRY: RefCell<HashMap<String, Handle>> = RefCell::new(HashMap::new());
}

/// Register a local directory under the given id.
///
/// Existing entries for the same id are replaced. The directory is *not*
/// created — callers should ensure the path exists (or will be created on
/// first write via [`crate::handle::FsHandle::write`], which `mkdir -p`s
/// the parent).
#[cfg(not(target_arch = "wasm32"))]
pub fn register_directory(id: impl Into<String>, path: PathBuf) {
    let handle = crate::handle::native::NativeHandle::new(path);
    REGISTRY.with(|r| r.borrow_mut().insert(id.into(), handle));
}

/// Register a File System Access API directory handle under the given id.
///
/// The host calls this before any invocation targeting the corresponding
/// [`FsAddress`] is dispatched.
#[cfg(target_arch = "wasm32")]
pub fn register_directory(
    id: impl Into<String>,
    handle: web_sys::FileSystemDirectoryHandle,
) {
    let id = id.into();
    let entry = crate::handle::web::WebHandle::new(id.clone(), handle);
    REGISTRY.with(|r| r.borrow_mut().insert(id, entry));
}

/// Drop the entry for `id`. Returns whether an entry was present.
pub fn unregister_directory(id: &str) -> bool {
    REGISTRY.with(|r| r.borrow_mut().remove(id).is_some())
}

/// Look up a previously-registered handle.
pub(crate) fn lookup(id: &str) -> Result<Handle, FsError> {
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

    #[test]
    fn it_registers_and_looks_up_a_handle() {
        let tmp = tempdir().unwrap();
        let id = format!("registry-test-{}", tmp.path().display());
        register_directory(&id, tmp.path().to_path_buf());
        let handle = lookup(&id).unwrap();
        assert_eq!(handle.path(), tmp.path());
        assert!(unregister_directory(&id));
        assert!(matches!(
            lookup(&id),
            Err(FsError::UnregisteredHandle(_))
        ));
    }
}
