//! Native round-trip and byte-compat tests for the archive providers.
//!
//! The byte-compat tests are the load-bearing assertion of this crate: a
//! directory written through the FS-remote site must be a valid
//! `dialog_storage::FileSystem` vault and vice versa.

#![cfg(not(target_arch = "wasm32"))]

mod helpers;

use anyhow::Result;
use base58::ToBase58;
use dialog_common::Blake3Hash;
use dialog_effects::archive::prelude::*;
use dialog_storage::unique_did;
use helpers::{open_at, setup};

#[dialog_common::test]
async fn it_returns_none_for_missing_blob() -> Result<()> {
    let env = setup().await;
    let did = unique_did().await;
    let digest = Blake3Hash::hash(b"never written");

    let result = did
        .archive()
        .catalog("index")
        .get(digest)
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    assert_eq!(result, None);
    Ok(())
}

#[dialog_common::test]
async fn it_writes_and_reads_back_a_blob() -> Result<()> {
    let env = setup().await;
    let did = unique_did().await;
    let content = b"hello fs-remote".to_vec();
    let digest = Blake3Hash::hash(&content);

    did.clone()
        .archive()
        .catalog("index")
        .put(content.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let result = did
        .archive()
        .catalog("index")
        .get(digest)
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    assert_eq!(result, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_writes_byte_compatibly_with_native_space() -> Result<()> {
    // Write via FS-remote, then read back via dialog-storage's FileSystem
    // pointed at the same directory.
    let env = setup().await;
    let did = unique_did().await;
    let content = b"compat: fs-remote -> FileSystem".to_vec();
    let digest = Blake3Hash::hash(&content);

    did.clone()
        .archive()
        .catalog("index")
        .put(content.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    // The expected layout on disk:
    let expected_path = env
        ._tmp
        .path()
        .join("archive")
        .join("index")
        .join(digest.as_bytes().to_base58());
    assert!(
        expected_path.is_file(),
        "fs-remote should have written {expected_path:?}",
    );

    // Open the same directory as a FileSystem and read the blob through it.
    let native = open_at(env._tmp.path()).await;
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
    let env = setup().await;
    let did = unique_did().await;
    let content = b"compat: FileSystem -> fs-remote".to_vec();
    let digest = Blake3Hash::hash(&content);

    let native = open_at(env._tmp.path()).await;
    did.clone()
        .archive()
        .catalog("index")
        .put(content.clone())
        .perform(&native)
        .await?;

    let result = did
        .archive()
        .catalog("index")
        .get(digest)
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    assert_eq!(result, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_is_idempotent_for_repeated_puts() -> Result<()> {
    let env = setup().await;
    let did = unique_did().await;
    let content = b"idempotent".to_vec();
    let digest = Blake3Hash::hash(&content);

    did.clone()
        .archive()
        .catalog("index")
        .put(content.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    did.clone()
        .archive()
        .catalog("index")
        .put(content.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let result = did
        .archive()
        .catalog("index")
        .get(digest)
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    assert_eq!(result, Some(content));
    Ok(())
}
