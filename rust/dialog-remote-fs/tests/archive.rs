//! Native round-trip and byte-compat tests for the archive providers.
//!
//! The byte-compat tests are the load-bearing assertion of this crate:
//! a directory written by `dialog-remote-fs` must be a valid
//! `dialog-storage::FileSystem` vault and vice versa. Without that,
//! consumers can't share a directory across the two providers — which
//! is the whole point of the FS-remote.

#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use base58::ToBase58;
use dialog_capability::Subject;
use dialog_common::Blake3Hash;
use dialog_effects::archive::prelude::*;
use dialog_effects::archive::{Archive, ArchiveError, Catalog, Get, Put};
use dialog_effects::storage::{Directory, Location};
use dialog_remote_fs::{Fs, FsAuthorization, IntoRequest, registry};
use dialog_storage::provider::FileSystem;
use dialog_storage::resource::Resource;
use dialog_storage::{unique_did, unique_name};
use tempfile::TempDir;

/// Build a `Capability<Get>` for the given catalog + digest under a subject.
fn build_get(subject: Subject, catalog: &str, digest: Blake3Hash) -> dialog_capability::Capability<Get> {
    subject
        .attenuate(Archive)
        .attenuate(Catalog::new(catalog))
        .invoke(Get::new(digest))
}

/// Build a `Capability<Put>` for the given catalog + (digest, content).
fn build_put(
    subject: Subject,
    catalog: &str,
    digest: Blake3Hash,
    content: Vec<u8>,
) -> dialog_capability::Capability<Put> {
    subject
        .attenuate(Archive)
        .attenuate(Catalog::new(catalog))
        .invoke(Put::new(digest, content))
}

/// Set up a tempdir registered under a unique handle id. The returned
/// `TempDir` keeps the directory alive for the test's lifetime.
fn setup() -> (TempDir, String) {
    let tmp = tempfile::tempdir().unwrap();
    let id = unique_name("fs-remote-archive");
    registry::register_directory(id.clone(), tmp.path().to_path_buf());
    (tmp, id)
}

/// Execute a capability through the full FS-remote ForkInvocation pipeline
/// (auth redeem → permit → invocation → I/O). Bypasses the operator-level
/// `Network` dispatch because these tests want to exercise the Fs site
/// directly, not the composite.
async fn execute_get(
    handle_id: &str,
    capability: dialog_capability::Capability<Get>,
) -> Result<Option<Vec<u8>>, ArchiveError> {
    let request = capability.to_request();
    let auth = FsAuthorization::new(request);
    let permit = auth.redeem(&dialog_remote_fs::FsAddress::new(handle_id));
    permit.invoke(capability).perform(&Fs).await
}

async fn execute_put(
    handle_id: &str,
    capability: dialog_capability::Capability<Put>,
) -> Result<(), ArchiveError> {
    let request = capability.to_request();
    let auth = FsAuthorization::new(request);
    let permit = auth.redeem(&dialog_remote_fs::FsAddress::new(handle_id));
    permit.invoke(capability).perform(&Fs).await
}

#[dialog_common::test]
async fn it_returns_none_for_missing_blob() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let digest = Blake3Hash::hash(b"never written");

    let result = execute_get(&id, build_get(did.into(), "index", digest)).await?;
    assert_eq!(result, None);
    Ok(())
}

#[dialog_common::test]
async fn it_writes_and_reads_back_a_blob() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let content = b"hello fs-remote".to_vec();
    let digest = Blake3Hash::hash(&content);

    execute_put(
        &id,
        build_put(did.clone().into(), "index", digest.clone(), content.clone()),
    )
    .await?;

    let result = execute_get(&id, build_get(did.into(), "index", digest)).await?;
    assert_eq!(result, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_writes_byte_compatibly_with_native_space() -> Result<()> {
    // Write via FS-remote, then read back via dialog-storage's
    // NativeSpace pointed at the same directory. This is the
    // FS-remote -> native-dialog-storage direction.
    let (tmp, id) = setup();
    let did = unique_did().await;
    let content = b"compat: fs-remote -> NativeSpace".to_vec();
    let digest = Blake3Hash::hash(&content);

    execute_put(
        &id,
        build_put(did.clone().into(), "index", digest.clone(), content.clone()),
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

    // Open the same directory as a NativeSpace and read the blob through
    // its archive provider.
    let location = Location::new(Directory::At(tmp.path().to_string_lossy().into_owned()), "");
    let native_space = FileSystem::open(&location).await?;
    let loaded = did
        .archive()
        .catalog("index")
        .get(digest)
        .perform(&native_space)
        .await?;
    assert_eq!(loaded, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_reads_byte_compatibly_from_native_space() -> Result<()> {
    // Reverse direction: a native consumer writes via NativeSpace, and
    // FS-remote reads the same vault.
    let (tmp, id) = setup();
    let did = unique_did().await;
    let content = b"compat: NativeSpace -> fs-remote".to_vec();
    let digest = Blake3Hash::hash(&content);

    let location = Location::new(Directory::At(tmp.path().to_string_lossy().into_owned()), "");
    let native_space = FileSystem::open(&location).await?;
    did.clone()
        .archive()
        .catalog("index")
        .put(digest.clone(), content.clone())
        .perform(&native_space)
        .await?;

    let result = execute_get(&id, build_get(did.into(), "index", digest)).await?;
    assert_eq!(result, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_digest_mismatch() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let content = b"declared content".to_vec();
    let wrong_digest = Blake3Hash::hash(b"actually-different");

    let result = execute_put(
        &id,
        build_put(did.into(), "index", wrong_digest, content),
    )
    .await;
    assert!(matches!(result, Err(ArchiveError::DigestMismatch { .. })));
    Ok(())
}

#[dialog_common::test]
async fn it_is_idempotent_for_repeated_puts() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let content = b"idempotent".to_vec();
    let digest = Blake3Hash::hash(&content);

    execute_put(
        &id,
        build_put(did.clone().into(), "index", digest.clone(), content.clone()),
    )
    .await?;
    execute_put(
        &id,
        build_put(did.clone().into(), "index", digest.clone(), content.clone()),
    )
    .await?;

    let result = execute_get(&id, build_get(did.into(), "index", digest)).await?;
    assert_eq!(result, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_errors_on_unregistered_handle() -> Result<()> {
    let did = unique_did().await;
    let digest = Blake3Hash::hash(b"anything");

    let result = execute_get(
        "not-a-registered-handle",
        build_get(did.into(), "index", digest),
    )
    .await;
    assert!(matches!(result, Err(ArchiveError::Io(_))));
    Ok(())
}
