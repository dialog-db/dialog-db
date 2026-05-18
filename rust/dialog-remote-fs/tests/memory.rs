//! Native round-trip, CAS, and byte-compat tests for memory providers.

#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use dialog_capability::Subject;
use dialog_common::Blake3Hash;
use dialog_effects::memory::prelude::*;
use dialog_effects::memory::{
    Cell, Edition, Memory, MemoryError, Publish, Resolve, Retract, Space, Version,
};
use dialog_effects::storage::{Directory, Location};
use dialog_remote_fs::{Fs, FsAddress, FsAuthorization, IntoRequest, registry};
use dialog_storage::provider::FileSystem;
use dialog_storage::resource::Resource;
use dialog_storage::{unique_did, unique_name};
use tempfile::TempDir;

fn build_resolve(
    subject: Subject,
    space: &str,
    cell: &str,
) -> dialog_capability::Capability<Resolve> {
    subject
        .attenuate(Memory)
        .attenuate(Space::new(space))
        .attenuate(Cell::new(cell))
        .invoke(Resolve)
}

fn build_publish(
    subject: Subject,
    space: &str,
    cell: &str,
    content: Vec<u8>,
    when: Option<Version>,
) -> dialog_capability::Capability<Publish> {
    subject
        .attenuate(Memory)
        .attenuate(Space::new(space))
        .attenuate(Cell::new(cell))
        .invoke(Publish::new(content, when))
}

fn build_retract(
    subject: Subject,
    space: &str,
    cell: &str,
    when: Version,
) -> dialog_capability::Capability<Retract> {
    subject
        .attenuate(Memory)
        .attenuate(Space::new(space))
        .attenuate(Cell::new(cell))
        .invoke(Retract::new(when))
}

fn setup() -> (TempDir, String) {
    let tmp = tempfile::tempdir().unwrap();
    let id = unique_name("fs-remote-memory");
    registry::register_directory(id.clone(), tmp.path().to_path_buf());
    (tmp, id)
}

async fn execute_resolve(
    handle_id: &str,
    capability: dialog_capability::Capability<Resolve>,
) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
    let request = capability.to_request();
    let auth = FsAuthorization::new(request);
    let permit = auth.redeem(&FsAddress::new(handle_id));
    permit.invoke(capability).perform(&Fs).await
}

async fn execute_publish(
    handle_id: &str,
    capability: dialog_capability::Capability<Publish>,
) -> Result<Version, MemoryError> {
    let request = capability.to_request();
    let auth = FsAuthorization::new(request);
    let permit = auth.redeem(&FsAddress::new(handle_id));
    permit.invoke(capability).perform(&Fs).await
}

async fn execute_retract(
    handle_id: &str,
    capability: dialog_capability::Capability<Retract>,
) -> Result<(), MemoryError> {
    let request = capability.to_request();
    let auth = FsAuthorization::new(request);
    let permit = auth.redeem(&FsAddress::new(handle_id));
    permit.invoke(capability).perform(&Fs).await
}

#[dialog_common::test]
async fn it_resolves_none_for_missing_cell() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;

    let result = execute_resolve(&id, build_resolve(did.into(), "local", "head")).await?;
    assert!(result.is_none());
    Ok(())
}

#[dialog_common::test]
async fn it_publishes_initial_content() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let content = b"first revision".to_vec();
    let expected_version = Version::from(Blake3Hash::hash(&content).as_bytes());

    let version = execute_publish(
        &id,
        build_publish(did.clone().into(), "local", "head", content.clone(), None),
    )
    .await?;
    assert_eq!(version, expected_version);

    let resolved = execute_resolve(&id, build_resolve(did.into(), "local", "head")).await?;
    let edition = resolved.expect("cell should resolve to the published edition");
    assert_eq!(edition.content, content);
    assert_eq!(edition.version, expected_version);
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_initial_publish_when_cell_exists() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let first = b"first revision".to_vec();
    let second = b"second revision".to_vec();

    execute_publish(
        &id,
        build_publish(did.clone().into(), "local", "head", first.clone(), None),
    )
    .await?;

    // Second publish with `when: None` (IfNoneMatch) must fail because
    // the cell already exists.
    let result = execute_publish(
        &id,
        build_publish(did.into(), "local", "head", second, None),
    )
    .await;
    assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));
    Ok(())
}

#[dialog_common::test]
async fn it_updates_with_correct_ifmatch_version() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let first = b"first".to_vec();
    let second = b"second".to_vec();

    let v1 = execute_publish(
        &id,
        build_publish(did.clone().into(), "local", "head", first, None),
    )
    .await?;

    let v2 = execute_publish(
        &id,
        build_publish(did.clone().into(), "local", "head", second.clone(), Some(v1)),
    )
    .await?;

    let resolved = execute_resolve(&id, build_resolve(did.into(), "local", "head")).await?;
    let edition = resolved.expect("cell should be present");
    assert_eq!(edition.content, second);
    assert_eq!(edition.version, v2);
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_update_with_wrong_ifmatch() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let first = b"first".to_vec();
    let bogus_version = Version::from(Blake3Hash::hash(b"never-published").as_bytes());

    execute_publish(
        &id,
        build_publish(did.clone().into(), "local", "head", first, None),
    )
    .await?;

    let result = execute_publish(
        &id,
        build_publish(
            did.into(),
            "local",
            "head",
            b"second".to_vec(),
            Some(bogus_version),
        ),
    )
    .await;
    assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_update_when_cell_missing() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let bogus_version = Version::from(Blake3Hash::hash(b"anything").as_bytes());

    let result = execute_publish(
        &id,
        build_publish(
            did.into(),
            "local",
            "head",
            b"content".to_vec(),
            Some(bogus_version),
        ),
    )
    .await;
    assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));
    Ok(())
}

#[dialog_common::test]
async fn it_is_idempotent_when_republishing_same_content() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let content = b"same".to_vec();

    let v1 = execute_publish(
        &id,
        build_publish(did.clone().into(), "local", "head", content.clone(), None),
    )
    .await?;

    // Republishing the same content with an `IfNoneMatch` precondition
    // should normally fail (cell exists) but the same-content shortcut
    // returns the existing version. This matches `dialog-storage`'s
    // native FS provider behaviour — it's what makes retries safe.
    let v2 = execute_publish(
        &id,
        build_publish(did.into(), "local", "head", content, None),
    )
    .await?;
    assert_eq!(v1, v2);
    Ok(())
}

#[dialog_common::test]
async fn it_retracts_with_correct_version() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let content = b"to be retracted".to_vec();

    let version = execute_publish(
        &id,
        build_publish(did.clone().into(), "local", "head", content, None),
    )
    .await?;

    execute_retract(&id, build_retract(did.clone().into(), "local", "head", version)).await?;

    let resolved = execute_resolve(&id, build_resolve(did.into(), "local", "head")).await?;
    assert!(resolved.is_none());
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_retract_with_wrong_version() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let content = b"locked content".to_vec();
    let bogus_version = Version::from(Blake3Hash::hash(b"never-published").as_bytes());

    execute_publish(
        &id,
        build_publish(did.clone().into(), "local", "head", content, None),
    )
    .await?;

    let result =
        execute_retract(&id, build_retract(did.into(), "local", "head", bogus_version)).await;
    assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));
    Ok(())
}

#[dialog_common::test]
async fn it_retract_on_missing_cell_is_noop() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let bogus_version = Version::from(Blake3Hash::hash(b"anything").as_bytes());

    // Retracting a non-existent cell is a no-op (returns Ok) regardless
    // of the version expected. Matches dialog-storage native FS provider.
    execute_retract(&id, build_retract(did.into(), "local", "head", bogus_version)).await?;
    Ok(())
}

#[dialog_common::test]
async fn it_writes_byte_compatibly_with_native_space() -> Result<()> {
    let (tmp, id) = setup();
    let did = unique_did().await;
    let content = b"memory: fs-remote -> NativeSpace".to_vec();

    execute_publish(
        &id,
        build_publish(did.clone().into(), "local", "head", content.clone(), None),
    )
    .await?;

    // Confirm the file landed at the expected path.
    let expected_path = tmp.path().join("memory").join("local").join("head");
    assert!(
        expected_path.is_file(),
        "expected memory cell at {expected_path:?}",
    );

    // Same directory, opened as a NativeSpace.
    let location = Location::new(Directory::At(tmp.path().to_string_lossy().into_owned()), "");
    let native_space = FileSystem::open(&location).await?;
    let resolved = did
        .memory()
        .space("local")
        .cell("head")
        .resolve()
        .perform(&native_space)
        .await?;
    let edition = resolved.expect("NativeSpace should see the cell fs-remote wrote");
    assert_eq!(edition.content, content);
    Ok(())
}

#[dialog_common::test]
async fn it_reads_byte_compatibly_from_native_space() -> Result<()> {
    let (tmp, id) = setup();
    let did = unique_did().await;
    let content = b"memory: NativeSpace -> fs-remote".to_vec();
    let expected_version = Version::from(Blake3Hash::hash(&content).as_bytes());

    let location = Location::new(Directory::At(tmp.path().to_string_lossy().into_owned()), "");
    let native_space = FileSystem::open(&location).await?;
    did.clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(content.clone(), None)
        .perform(&native_space)
        .await?;

    let resolved = execute_resolve(&id, build_resolve(did.into(), "local", "head")).await?;
    let edition = resolved.expect("fs-remote should see the cell NativeSpace wrote");
    assert_eq!(edition.content, content);
    assert_eq!(edition.version, expected_version);
    Ok(())
}
