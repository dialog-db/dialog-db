//! Native (non-WASM) type aliases and defaults.

use super::super::FileSystem;
use super::super::space::Space;
use super::Storage;

/// Space backed by filesystem providers.
pub type NativeSpace = Space<FileSystem, FileSystem, FileSystem, FileSystem>;

impl Default for Storage<NativeSpace> {
    fn default() -> Self {
        Self::new()
    }
}
