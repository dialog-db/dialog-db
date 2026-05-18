//! Filesystem-handle abstraction for the FS-remote backend.
//!
//! Internal contract for the small surface a provider needs to perform a
//! capability: navigate by path segment, read/write/rename/remove/list,
//! and create directories on demand. Two concrete implementations live
//! behind a `cfg(target_arch)` switch:
//!
//! - [`NativeHandle`](native::NativeHandle) — backed by `tokio::fs` and
//!   `PathBuf`. Used on non-wasm targets and exercised by the round-trip
//!   / byte-compat tests against `dialog-storage::NativeSpace`.
//! - [`WebHandle`](web::WebHandle) — backed by
//!   `web_sys::FileSystemDirectoryHandle` from the File System Access
//!   API, on `target_arch = "wasm32"`.
//!
//! The trait is `pub(crate)` on purpose: it's an internal implementation
//! detail, not part of the crate's public contract. Consumers register a
//! platform-specific directory via [`crate::registry`] and the providers
//! pick up the right [`Handle`] type automatically.

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
#[cfg(target_arch = "wasm32")]
pub(crate) mod web;

use crate::FsError;
use async_trait::async_trait;

/// I/O operations a provider needs to perform against the registered
/// directory.
///
/// Method semantics deliberately mirror
/// `dialog_storage::storage::provider::fs::FileSystemHandle` so the
/// on-disk layout is byte-identical to what `dialog-storage`'s native
/// FS provider produces.
///
/// `remove` and `list` aren't called by the archive providers; they're
/// here in advance for the memory provider work (Retract uses `remove`;
/// directory enumeration uses `list`).
#[allow(dead_code)]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub(crate) trait FsHandle: Clone {
    /// Resolve a child segment under this handle's directory. Implementations
    /// must reject segments that would escape the directory (e.g. `..`,
    /// absolute paths).
    async fn resolve(&self, segment: &str) -> Result<Self, FsError>;

    /// Read the file at this handle, returning `None` if the file does not
    /// exist.
    async fn read_optional(&self) -> Result<Option<Vec<u8>>, FsError>;

    /// Write the file at this handle, creating parent directories as
    /// needed.
    async fn write(&self, contents: &[u8]) -> Result<(), FsError>;

    /// Atomically rename this handle's target to `to`.
    async fn rename(&self, to: &Self) -> Result<(), FsError>;

    /// Remove the file at this handle. Returns `Ok(())` if already absent.
    async fn remove(&self) -> Result<(), FsError>;

    /// List file names directly under this handle's directory. Returns an
    /// empty vec if the directory does not exist.
    async fn list(&self) -> Result<Vec<String>, FsError>;

    /// Check whether this handle's target exists.
    async fn exists(&self) -> bool;

    /// Ensure this handle's directory (and parents) exists.
    async fn ensure_dir(&self) -> Result<(), FsError>;
}

/// The handle type selected for this build target.
///
/// On native (`not(target_arch = "wasm32")`), this is
/// [`native::NativeHandle`] (backed by `tokio::fs`/`PathBuf`). On
/// WebAssembly with browser hosts, this is [`web::WebHandle`] (backed by
/// `web_sys::FileSystemDirectoryHandle`).
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type Handle = native::NativeHandle;
#[cfg(target_arch = "wasm32")]
pub(crate) type Handle = web::WebHandle;
