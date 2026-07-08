//! Native (non-WASM) type aliases and defaults.

use crate::provider::{FileSystem, Space, Storage};

/// Space backed by filesystem providers (blocks, memory, credentials,
/// certificates, and blobs all on the same `FileSystem`).
pub type NativeSpace = Space<FileSystem, FileSystem, FileSystem, FileSystem, FileSystem>;

impl Default for Storage<NativeSpace> {
    fn default() -> Self {
        Self::new()
    }
}
