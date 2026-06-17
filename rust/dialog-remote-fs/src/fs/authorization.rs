//! FS authorization material.
//!
//! FS-remote has no over-the-wire authorization. The host's authorization is a
//! directory [`Grant`](dialog_effects::credential::Grant) (a path on native, a
//! `FileSystemDirectoryHandle` on the web), loaded from the credential store at
//! authorize time and resolved into a ready-to-use
//! [`FileSystem`](dialog_storage::provider::FileSystem). The authorization
//! carries that resolved provider; the [`provider`](crate::fs::provider) just
//! delegates the capability to it.

use dialog_storage::provider::FileSystem;

/// FS authorization material — the resolved [`FileSystem`] provider rooted at
/// the granted directory.
///
/// Not serializable (a web `FileSystem` wraps a live JS handle), which is fine:
/// [`Site::Authorization`](dialog_capability::Site) only requires
/// `ConditionalSend + 'static`. The serializable part — the directory id — lives
/// in [`FsAddress`](crate::FsAddress).
#[derive(Debug, Clone)]
pub struct FsAuthorization {
    filesystem: FileSystem,
}

impl FsAuthorization {
    /// Wrap a resolved provider as authorization material.
    pub fn new(filesystem: FileSystem) -> Self {
        Self { filesystem }
    }

    /// The resolved provider for the granted directory.
    pub fn filesystem(&self) -> &FileSystem {
        &self.filesystem
    }
}
