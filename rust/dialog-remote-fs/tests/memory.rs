//! Native round-trip, CAS, and byte-compat tests for memory providers.

#![cfg(not(target_arch = "wasm32"))]

mod helpers;

use anyhow::Result;
use dialog_common::Blake3Hash;
use dialog_effects::memory::prelude::*;
use dialog_effects::memory::{MemoryError, Version};
use helpers::{open_at, setup};

#[dialog_common::test]
async fn it_resolves_none_for_missing_cell() -> Result<()> {
    let env = setup().await;

    let result = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .resolve()
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    assert!(result.is_none());
    Ok(())
}

#[dialog_common::test]
async fn it_publishes_initial_content() -> Result<()> {
    let env = setup().await;
    let content = b"first revision".to_vec();
    let expected_version = Version::from(Blake3Hash::hash(&content).as_bytes());

    let version = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(content.clone(), None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    assert_eq!(version, expected_version);

    let resolved = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .resolve()
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    let edition = resolved.expect("cell should resolve to the published edition");
    assert_eq!(edition.content, content);
    assert_eq!(edition.version, expected_version);
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_initial_publish_when_cell_exists() -> Result<()> {
    let env = setup().await;

    env.subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(b"first revision".to_vec(), None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let result = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(b"second revision".to_vec(), None)
        .fork(&env.address)
        .perform(&env.network)
        .await;
    assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));
    Ok(())
}

#[dialog_common::test]
async fn it_updates_with_correct_ifmatch_version() -> Result<()> {
    let env = setup().await;
    let second = b"second".to_vec();

    let v1 = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(b"first".to_vec(), None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let v2 = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(second.clone(), Some(v1))
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let resolved = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .resolve()
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    let edition = resolved.expect("cell should be present");
    assert_eq!(edition.content, second);
    assert_eq!(edition.version, v2);
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_update_with_wrong_ifmatch() -> Result<()> {
    let env = setup().await;
    let bogus_version = Version::from(Blake3Hash::hash(b"never-published").as_bytes());

    env.subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(b"first".to_vec(), None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let result = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(b"second".to_vec(), Some(bogus_version))
        .fork(&env.address)
        .perform(&env.network)
        .await;
    assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));
    Ok(())
}

#[dialog_common::test]
async fn it_is_idempotent_when_republishing_same_content() -> Result<()> {
    let env = setup().await;
    let content = b"same".to_vec();

    let v1 = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(content.clone(), None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let v2 = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(content, None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    assert_eq!(v1, v2);
    Ok(())
}

#[dialog_common::test]
async fn it_retracts_with_correct_version() -> Result<()> {
    let env = setup().await;

    let version = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(b"to be retracted".to_vec(), None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    env.subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .retract(version)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let resolved = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .resolve()
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    assert!(resolved.is_none());
    Ok(())
}

#[dialog_common::test]
async fn it_writes_byte_compatibly_with_native_space() -> Result<()> {
    let env = setup().await;
    let content = b"memory: fs-remote -> FileSystem".to_vec();

    env.subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(content.clone(), None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let expected_path = env.tmp.path().join("memory").join("local").join("head");
    assert!(
        expected_path.is_file(),
        "expected memory cell at {expected_path:?}",
    );

    let native = open_at(env.tmp.path()).await;
    let resolved = env
        .subject
        .clone()
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
    // space's `memory/`, so the cell name itself contains a `/`.
    let env = setup().await;
    let content = b"branch head".to_vec();

    env.subject
        .clone()
        .memory()
        .space("local")
        .cell("branch/main")
        .publish(content.clone(), None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let expected_path = env
        .tmp
        .path()
        .join("memory")
        .join("local")
        .join("branch")
        .join("main");
    assert!(
        expected_path.is_file(),
        "expected nested cell at {expected_path:?}",
    );

    let resolved = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("branch/main")
        .resolve()
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    let edition = resolved.expect("nested cell should resolve");
    assert_eq!(edition.content, content);
    Ok(())
}

#[dialog_common::test]
async fn it_reads_byte_compatibly_from_native_space() -> Result<()> {
    let env = setup().await;
    let content = b"memory: FileSystem -> fs-remote".to_vec();
    let expected_version = Version::from(Blake3Hash::hash(&content).as_bytes());

    let native = open_at(env.tmp.path()).await;
    env.subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(content.clone(), None)
        .perform(&native)
        .await?;

    let resolved = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .resolve()
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    let edition = resolved.expect("fs-remote should see the cell FileSystem wrote");
    assert_eq!(edition.content, content);
    assert_eq!(edition.version, expected_version);
    Ok(())
}
