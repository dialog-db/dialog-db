//! Native (non-WASM) type aliases and defaults.

use crate::provider::{FileSystem, Space, Storage};

/// Space backed by filesystem providers.
pub type NativeSpace = Space<FileSystem, FileSystem, FileSystem, FileSystem>;

impl Default for Storage<NativeSpace> {
    fn default() -> Self {
        Self::new()
    }
}
