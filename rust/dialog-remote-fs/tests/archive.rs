//! Native round-trip and byte-compat tests for the archive providers.
//!
//! The byte-compat tests are the load-bearing assertion of this crate: a
//! directory written through the FS-remote site must be a valid
//! `dialog_storage::FileSystem` vault and vice versa. Since the FS-remote now
//! delegates straight to that provider, "byte-compat" is structural — but the
//! tests guard against a future divergence in the resolution layer.

#![cfg(not(target_arch = "wasm32"))]

mod helpers;

use anyhow::Result;
use base58::ToBase58;
use dialog_common::Blake3Hash;
use dialog_effects::archive::ArchiveError;
use dialog_effects::archive::prelude::*;
use dialog_effects::storage::{Directory, Location};
use dialog_remote_fs::register_directory;
use dialog_storage::provider::FileSystem;
use dialog_storage::resource::Resource;
use dialog_storage::{unique_did, unique_name};
use helpers::execute;
use tempfile::TempDir;

/// Set up a tempdir registered under a unique handle id. The returned
/// `TempDir` keeps the directory alive for the test's lifetime.
fn setup() -> (TempDir, String) {
    let tmp = tempfile::tempdir().unwrap();
    let id = unique_name("fs-remote-archive");
    register_directory(id.clone(), tmp.path().to_path_buf()).unwrap();
    (tmp, id)
}

#[dialog_common::test]
async fn it_returns_none_for_missing_blob() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let digest = Blake3Hash::hash(b"never written");

    let result = execute(&id, did.archive().catalog("index").get(digest)).await?;
    assert_eq!(result, None);
    Ok(())
}

#[dialog_common::test]
async fn it_writes_and_reads_back_a_blob() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let content = b"hello fs-remote".to_vec();
    let digest = Blake3Hash::hash(&content);

    execute(
        &id,
        did.clone().archive().catalog("index").put(content.clone()),
    )
    .await?;

    let result = execute(&id, did.archive().catalog("index").get(digest)).await?;
    assert_eq!(result, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_writes_byte_compatibly_with_native_space() -> Result<()> {
    // Write via FS-remote, then read back via dialog-storage's FileSystem
    // pointed at the same directory.
    let (tmp, id) = setup();
    let did = unique_did().await;
    let content = b"compat: fs-remote -> FileSystem".to_vec();
    let digest = Blake3Hash::hash(&content);

    execute(
        &id,
        did.clone().archive().catalog("index").put(content.clone()),
    )
    .await?;

    // The expected layout on disk:
    let expected_path = tmp
        .path()
        .join("archive")
        .join("index")
        .join(digest.as_bytes().to_base58());
    assert!(
        expected_path.is_file(),
        "fs-remote should have written {expected_path:?}",
    );

    // Open the same directory as a FileSystem and read the blob through it.
    let location = Location::new(Directory::At(tmp.path().to_string_lossy().into_owned()), "");
    let native = FileSystem::open(&location).await?;
    let loaded = did
        .archive()
        .catalog("index")
        .get(digest)
        .perform(&native)
        .await?;
    assert_eq!(loaded, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_reads_byte_compatibly_from_native_space() -> Result<()> {
    // Reverse direction: a native consumer writes via FileSystem, and
    // FS-remote reads the same vault.
    let (tmp, id) = setup();
    let did = unique_did().await;
    let content = b"compat: FileSystem -> fs-remote".to_vec();
    let digest = Blake3Hash::hash(&content);

    let location = Location::new(Directory::At(tmp.path().to_string_lossy().into_owned()), "");
    let native = FileSystem::open(&location).await?;
    did.clone()
        .archive()
        .catalog("index")
        .put(content.clone())
        .perform(&native)
        .await?;

    let result = execute(&id, did.archive().catalog("index").get(digest)).await?;
    assert_eq!(result, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_is_idempotent_for_repeated_puts() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let content = b"idempotent".to_vec();
    let digest = Blake3Hash::hash(&content);

    execute(
        &id,
        did.clone().archive().catalog("index").put(content.clone()),
    )
    .await?;
    execute(
        &id,
        did.clone().archive().catalog("index").put(content.clone()),
    )
    .await?;

    let result = execute(&id, did.archive().catalog("index").get(digest)).await?;
    assert_eq!(result, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_errors_on_unregistered_handle() -> Result<()> {
    let did = unique_did().await;
    let digest = Blake3Hash::hash(b"anything");

    let result = execute(
        "not-a-registered-handle",
        did.archive().catalog("index").get(digest),
    )
    .await;
    assert!(matches!(result, Err(ArchiveError::Storage(_))));
    Ok(())
}
