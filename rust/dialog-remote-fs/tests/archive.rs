//! Native round-trip, byte-compat, and authorization tests for the archive
//! providers.
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
use helpers::{file_url, open_at, setup};

#[dialog_common::test]
async fn it_returns_none_for_missing_blob() -> Result<()> {
    let env = setup().await;
    let digest = Blake3Hash::hash(b"never written");

    let result = env
        .subject
        .clone()
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
    let content = b"hello fs-remote".to_vec();
    let digest = Blake3Hash::hash(&content);

    env.subject
        .clone()
        .archive()
        .catalog("index")
        .put(content.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let result = env
        .subject
        .clone()
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
    let content = b"compat: fs-remote -> FileSystem".to_vec();
    let digest = Blake3Hash::hash(&content);

    env.subject
        .clone()
        .archive()
        .catalog("index")
        .put(content.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let expected_path = env
        .tmp
        .path()
        .join("archive")
        .join("index")
        .join(digest.as_bytes().to_base58());
    assert!(
        expected_path.is_file(),
        "fs-remote should have written {expected_path:?}",
    );

    let native = open_at(env.tmp.path()).await;
    let loaded = env
        .subject
        .clone()
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
    // Reverse direction: a native consumer writes via FileSystem, FS-remote reads.
    let env = setup().await;
    let content = b"compat: FileSystem -> fs-remote".to_vec();
    let digest = Blake3Hash::hash(&content);

    let native = open_at(env.tmp.path()).await;
    env.subject
        .clone()
        .archive()
        .catalog("index")
        .put(content.clone())
        .perform(&native)
        .await?;

    let result = env
        .subject
        .clone()
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
    let content = b"idempotent".to_vec();
    let digest = Blake3Hash::hash(&content);

    env.subject
        .clone()
        .archive()
        .catalog("index")
        .put(content.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    env.subject
        .clone()
        .archive()
        .catalog("index")
        .put(content.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let result = env
        .subject
        .clone()
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
async fn it_denies_when_subject_is_not_the_directory() -> Result<()> {
    // A directory authorizes only its own subject. An invocation scoped to a
    // different subject must be denied — the directory is not that space.
    let env = setup().await;
    let stranger = unique_did().await;
    let digest = Blake3Hash::hash(b"anything");

    let result = stranger
        .archive()
        .catalog("index")
        .get(digest)
        .fork(&env.address)
        .perform(&env.network)
        .await;
    assert!(result.is_err(), "mismatched subject must be denied");
    Ok(())
}

#[dialog_common::test]
async fn it_denies_when_directory_is_not_a_space() -> Result<()> {
    // A bare directory with no credential/key/self can't authorize anyone.
    let env = setup().await;
    let empty = tempfile::tempdir()?;
    let address = dialog_remote_fs::FsAddress::new(file_url(empty.path()));
    let digest = Blake3Hash::hash(b"anything");

    let result = env
        .subject
        .clone()
        .archive()
        .catalog("index")
        .get(digest)
        .fork(&address)
        .perform(&env.network)
        .await;
    assert!(result.is_err(), "directory without a credential must deny");
    Ok(())
}
