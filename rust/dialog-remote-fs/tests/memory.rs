//! Native round-trip, CAS, and byte-compat tests for memory providers.

#![cfg(not(target_arch = "wasm32"))]

mod helpers;

use anyhow::Result;
use dialog_common::Blake3Hash;
use dialog_effects::memory::prelude::*;
use dialog_effects::memory::{MemoryError, Version};
use dialog_effects::storage::{Directory, Location};
use dialog_remote_fs::register_directory;
use dialog_storage::provider::FileSystem;
use dialog_storage::resource::Resource;
use dialog_storage::{unique_did, unique_name};
use helpers::execute;
use tempfile::TempDir;

fn setup() -> (TempDir, String) {
    let tmp = tempfile::tempdir().unwrap();
    let id = unique_name("fs-remote-memory");
    register_directory(id.clone(), tmp.path().to_path_buf()).unwrap();
    (tmp, id)
}

#[dialog_common::test]
async fn it_resolves_none_for_missing_cell() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;

    let result = execute(&id, did.memory().space("local").cell("head").resolve()).await?;
    assert!(result.is_none());
    Ok(())
}

#[dialog_common::test]
async fn it_publishes_initial_content() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let content = b"first revision".to_vec();
    let expected_version = Version::from(Blake3Hash::hash(&content).as_bytes());

    let version = execute(
        &id,
        did.clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(content.clone(), None),
    )
    .await?;
    assert_eq!(version, expected_version);

    let resolved = execute(&id, did.memory().space("local").cell("head").resolve()).await?;
    let edition = resolved.expect("cell should resolve to the published edition");
    assert_eq!(edition.content, content);
    assert_eq!(edition.version, expected_version);
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_initial_publish_when_cell_exists() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;

    execute(
        &id,
        did.clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(b"first revision".to_vec(), None),
    )
    .await?;

    // Second publish with `when: None` (IfNoneMatch) must fail because the
    // cell already exists.
    let result = execute(
        &id,
        did.memory()
            .space("local")
            .cell("head")
            .publish(b"second revision".to_vec(), None),
    )
    .await;
    assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));
    Ok(())
}

#[dialog_common::test]
async fn it_updates_with_correct_ifmatch_version() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let second = b"second".to_vec();

    let v1 = execute(
        &id,
        did.clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(b"first".to_vec(), None),
    )
    .await?;

    let v2 = execute(
        &id,
        did.clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(second.clone(), Some(v1)),
    )
    .await?;

    let resolved = execute(&id, did.memory().space("local").cell("head").resolve()).await?;
    let edition = resolved.expect("cell should be present");
    assert_eq!(edition.content, second);
    assert_eq!(edition.version, v2);
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_update_with_wrong_ifmatch() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let bogus_version = Version::from(Blake3Hash::hash(b"never-published").as_bytes());

    execute(
        &id,
        did.clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(b"first".to_vec(), None),
    )
    .await?;

    let result = execute(
        &id,
        did.memory()
            .space("local")
            .cell("head")
            .publish(b"second".to_vec(), Some(bogus_version)),
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

    let result = execute(
        &id,
        did.memory()
            .space("local")
            .cell("head")
            .publish(b"content".to_vec(), Some(bogus_version)),
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

    let v1 = execute(
        &id,
        did.clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(content.clone(), None),
    )
    .await?;

    // Republishing the same content with an `IfNoneMatch` precondition would
    // normally fail (cell exists), but the same-content shortcut returns the
    // existing version. Matches `dialog_storage`'s FileSystem provider — it's
    // what makes retries safe.
    let v2 = execute(
        &id,
        did.memory()
            .space("local")
            .cell("head")
            .publish(content, None),
    )
    .await?;
    assert_eq!(v1, v2);
    Ok(())
}

#[dialog_common::test]
async fn it_retracts_with_correct_version() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;

    let version = execute(
        &id,
        did.clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(b"to be retracted".to_vec(), None),
    )
    .await?;

    execute(
        &id,
        did.clone()
            .memory()
            .space("local")
            .cell("head")
            .retract(version),
    )
    .await?;

    let resolved = execute(&id, did.memory().space("local").cell("head").resolve()).await?;
    assert!(resolved.is_none());
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_retract_with_wrong_version() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let bogus_version = Version::from(Blake3Hash::hash(b"never-published").as_bytes());

    execute(
        &id,
        did.clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(b"locked content".to_vec(), None),
    )
    .await?;

    let result = execute(
        &id,
        did.memory()
            .space("local")
            .cell("head")
            .retract(bogus_version),
    )
    .await;
    assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));
    Ok(())
}

#[dialog_common::test]
async fn it_retract_on_missing_cell_is_noop() -> Result<()> {
    let (_tmp, id) = setup();
    let did = unique_did().await;
    let bogus_version = Version::from(Blake3Hash::hash(b"anything").as_bytes());

    // Retracting a non-existent cell is a no-op (returns Ok) regardless of the
    // expected version. Matches the dialog_storage FileSystem provider.
    execute(
        &id,
        did.memory()
            .space("local")
            .cell("head")
            .retract(bogus_version),
    )
    .await?;
    Ok(())
}

#[dialog_common::test]
async fn it_writes_byte_compatibly_with_native_space() -> Result<()> {
    let (tmp, id) = setup();
    let did = unique_did().await;
    let content = b"memory: fs-remote -> FileSystem".to_vec();

    execute(
        &id,
        did.clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(content.clone(), None),
    )
    .await?;

    // Confirm the file landed at the expected path.
    let expected_path = tmp.path().join("memory").join("local").join("head");
    assert!(
        expected_path.is_file(),
        "expected memory cell at {expected_path:?}",
    );

    // Same directory, opened as a FileSystem.
    let location = Location::new(Directory::At(tmp.path().to_string_lossy().into_owned()), "");
    let native = FileSystem::open(&location).await?;
    let resolved = did
        .memory()
        .space("local")
        .cell("head")
        .resolve()
        .perform(&native)
        .await?;
    let edition = resolved.expect("FileSystem should see the cell fs-remote wrote");
    assert_eq!(edition.content, content);
    Ok(())
}

#[dialog_common::test]
async fn it_writes_a_nested_cell_path() -> Result<()> {
    // Dialog repository stores branch heads at `branch/{name}` under the
    // space's `memory/` — so the cell name itself contains a `/`. Make sure
    // the path splits on `/` and the provider creates the nested directories
    // rather than rejecting the slash as a containment violation.
    let (tmp, id) = setup();
    let did = unique_did().await;
    let content = b"branch head".to_vec();
    let expected_version = Version::from(Blake3Hash::hash(&content).as_bytes());

    let version = execute(
        &id,
        did.clone()
            .memory()
            .space("local")
            .cell("branch/main")
            .publish(content.clone(), None),
    )
    .await?;
    assert_eq!(version, expected_version);

    // The on-disk file should land at memory/local/branch/main.
    let expected_path = tmp
        .path()
        .join("memory")
        .join("local")
        .join("branch")
        .join("main");
    assert!(
        expected_path.is_file(),
        "expected nested cell at {expected_path:?}",
    );

    let resolved = execute(
        &id,
        did.memory().space("local").cell("branch/main").resolve(),
    )
    .await?;
    let edition = resolved.expect("nested cell should resolve");
    assert_eq!(edition.content, content);
    assert_eq!(edition.version, expected_version);
    Ok(())
}

#[dialog_common::test]
async fn it_reads_byte_compatibly_from_native_space() -> Result<()> {
    let (tmp, id) = setup();
    let did = unique_did().await;
    let content = b"memory: FileSystem -> fs-remote".to_vec();
    let expected_version = Version::from(Blake3Hash::hash(&content).as_bytes());

    let location = Location::new(Directory::At(tmp.path().to_string_lossy().into_owned()), "");
    let native = FileSystem::open(&location).await?;
    did.clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(content.clone(), None)
        .perform(&native)
        .await?;

    let resolved = execute(&id, did.memory().space("local").cell("head").resolve()).await?;
    let edition = resolved.expect("fs-remote should see the cell FileSystem wrote");
    assert_eq!(edition.content, content);
    assert_eq!(edition.version, expected_version);
    Ok(())
}
