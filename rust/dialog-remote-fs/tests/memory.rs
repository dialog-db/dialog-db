//! Cross-target round-trip, CAS, and byte-compat tests for memory providers.
//!
//! These drive the [`Fs`](dialog_remote_fs) provider directly via
//! [`perform`](dialog_remote_fs::helpers::perform); the env-bound
//! `authorize`/`prove` path is covered by the Operator-driven tests in
//! `e2e.rs`. They run on native (a tempdir) and in the browser (an OPFS
//! subdirectory) alike.

mod helpers;

use anyhow::Result;
use dialog_common::Blake3Hash;
use dialog_effects::memory::prelude::*;
use dialog_effects::memory::{MemoryError, Version};
use helpers::{perform, setup};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[dialog_common::test]
async fn it_resolves_none_for_missing_cell() -> Result<()> {
    let env = setup().await;

    let result = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .resolve()
            .fork(&env.address),
    )
    .await??;
    assert!(result.is_none());
    Ok(())
}

#[dialog_common::test]
async fn it_publishes_initial_content() -> Result<()> {
    let env = setup().await;
    let content = b"first revision".to_vec();
    let expected_version = Version::from(Blake3Hash::hash(&content).as_bytes());

    let version = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(content.clone(), None)
            .fork(&env.address),
    )
    .await??;
    assert_eq!(version, expected_version);

    let resolved = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .resolve()
            .fork(&env.address),
    )
    .await??;
    let edition = resolved.expect("cell should resolve to the published edition");
    assert_eq!(edition.content, content);
    assert_eq!(edition.version, expected_version);
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_initial_publish_when_cell_exists() -> Result<()> {
    let env = setup().await;

    perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(b"first revision".to_vec(), None)
            .fork(&env.address),
    )
    .await??;

    let result = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(b"second revision".to_vec(), None)
            .fork(&env.address),
    )
    .await?;
    assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));
    Ok(())
}

#[dialog_common::test]
async fn it_updates_with_correct_ifmatch_version() -> Result<()> {
    let env = setup().await;
    let second = b"second".to_vec();

    let v1 = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(b"first".to_vec(), None)
            .fork(&env.address),
    )
    .await??;

    let v2 = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(second.clone(), Some(v1))
            .fork(&env.address),
    )
    .await??;

    let resolved = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .resolve()
            .fork(&env.address),
    )
    .await??;
    let edition = resolved.expect("cell should be present");
    assert_eq!(edition.content, second);
    assert_eq!(edition.version, v2);
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_update_with_wrong_ifmatch() -> Result<()> {
    let env = setup().await;
    let bogus_version = Version::from(Blake3Hash::hash(b"never-published").as_bytes());

    perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(b"first".to_vec(), None)
            .fork(&env.address),
    )
    .await??;

    let result = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(b"second".to_vec(), Some(bogus_version))
            .fork(&env.address),
    )
    .await?;
    assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));
    Ok(())
}

#[dialog_common::test]
async fn it_is_idempotent_when_republishing_same_content() -> Result<()> {
    let env = setup().await;
    let content = b"same".to_vec();

    let v1 = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(content.clone(), None)
            .fork(&env.address),
    )
    .await??;

    let v2 = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(content, None)
            .fork(&env.address),
    )
    .await??;
    assert_eq!(v1, v2);
    Ok(())
}

#[dialog_common::test]
async fn it_retracts_with_correct_version() -> Result<()> {
    let env = setup().await;

    let version = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(b"to be retracted".to_vec(), None)
            .fork(&env.address),
    )
    .await??;

    perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .retract(version)
            .fork(&env.address),
    )
    .await??;

    let resolved = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .resolve()
            .fork(&env.address),
    )
    .await??;
    assert!(resolved.is_none());
    Ok(())
}

#[dialog_common::test]
async fn it_writes_byte_compatibly_with_a_direct_filesystem() -> Result<()> {
    let env = setup().await;
    let content = b"memory: fs-remote -> FileSystem".to_vec();

    perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(content.clone(), None)
            .fork(&env.address),
    )
    .await??;

    let resolved = env
        .subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .resolve()
        .perform(&env.filesystem)
        .await?;
    let edition = resolved.expect("a direct FileSystem should see the cell fs-remote wrote");
    assert_eq!(edition.content, content);
    Ok(())
}

#[dialog_common::test]
async fn it_writes_a_nested_cell_path() -> Result<()> {
    // Dialog repository stores branch heads at `branch/{name}` under the
    // space's `memory/`, so the cell name itself contains a `/`.
    let env = setup().await;
    let content = b"branch head".to_vec();

    perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("branch/main")
            .publish(content.clone(), None)
            .fork(&env.address),
    )
    .await??;

    let resolved = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("branch/main")
            .resolve()
            .fork(&env.address),
    )
    .await??;
    let edition = resolved.expect("nested cell should resolve");
    assert_eq!(edition.content, content);
    Ok(())
}

#[dialog_common::test]
async fn it_reads_byte_compatibly_from_a_direct_filesystem() -> Result<()> {
    let env = setup().await;
    let content = b"memory: FileSystem -> fs-remote".to_vec();
    let expected_version = Version::from(Blake3Hash::hash(&content).as_bytes());

    env.subject
        .clone()
        .memory()
        .space("local")
        .cell("head")
        .publish(content.clone(), None)
        .perform(&env.filesystem)
        .await?;

    let resolved = perform(
        env.subject
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .resolve()
            .fork(&env.address),
    )
    .await??;
    let edition = resolved.expect("fs-remote should see the cell a direct FileSystem wrote");
    assert_eq!(edition.content, content);
    assert_eq!(edition.version, expected_version);
    Ok(())
}
