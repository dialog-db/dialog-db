//! FS authorization material.
//!
//! FS-remote has no over-the-wire authorization. At authorize time the
//! directory is opened and verified to be the space for the invocation's
//! subject, producing a ready-to-use
//! [`FileSystem`](dialog_storage::provider::FileSystem). The authorization
//! carries that resolved provider; the [`provider`](crate::fs::provider) just
//! delegates the capability to it.

use dialog_storage::provider::FileSystem;

/// FS authorization material — the resolved [`FileSystem`] provider rooted at
/// the verified directory.
///
/// Not serializable (a web `FileSystem` wraps a live JS handle), which is fine:
/// [`Site::Authorization`](dialog_capability::Site) only requires
/// `ConditionalSend + 'static`. The serializable part — the address — lives in
/// [`FsAddress`](crate::FsAddress).
#[derive(Debug, Clone)]
pub struct FsAuthorization {
    filesystem: FileSystem,
}

impl FsAuthorization {
    /// Wrap a resolved provider as authorization material.
    pub fn new(filesystem: FileSystem) -> Self {
        Self { filesystem }
    }

    /// The resolved provider for the verified directory.
    pub fn filesystem(&self) -> &FileSystem {
        &self.filesystem
    }
}
