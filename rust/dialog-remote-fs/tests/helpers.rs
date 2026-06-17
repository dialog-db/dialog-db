//! Shared setup for the FS-remote integration tests.
//!
//! Builds an [`FsNetwork`] rooted at a fresh tempdir and the matching
//! `dialog_storage::FileSystem` over the same directory, so a test can drive
//! capabilities through the FS-remote site and cross-check the on-disk vault
//! with a native consumer. Native-only: the integration tests it serves are
//! gated to native (a tempdir-backed vault).

#![cfg(not(target_arch = "wasm32"))]
#![allow(dead_code)]

use dialog_effects::storage::{Directory, Location};
use dialog_remote_fs::FsAddress;
use dialog_remote_fs::helpers::FsNetwork;
use dialog_storage::provider::FileSystem;
use dialog_storage::resource::Resource;
use dialog_storage::unique_name;
use tempfile::TempDir;

/// A tempdir, the FS-remote network rooted at it, and an address naming it.
/// The `TempDir` keeps the directory alive for the test's lifetime.
pub struct Setup {
    pub _tmp: TempDir,
    pub network: FsNetwork,
    pub address: FsAddress,
}

/// Open a fresh FS-remote test environment over a tempdir.
pub async fn setup() -> Setup {
    let tmp = tempfile::tempdir().unwrap();
    let id = unique_name("fs-remote");
    let filesystem = open_at(tmp.path()).await;
    Setup {
        _tmp: tmp,
        network: FsNetwork::from(filesystem),
        address: FsAddress::new(id),
    }
}

/// Open a `dialog_storage::FileSystem` rooted at the given directory.
pub async fn open_at(path: &std::path::Path) -> FileSystem {
    let location = Location::new(Directory::At(path.to_string_lossy().into_owned()), "");
    FileSystem::open(&location).await.unwrap()
}
