//! Cross-target byte-compat tests for the archive providers.
//!
//! The load-bearing assertion of this crate: a directory written through the
//! FS-remote provider must be a valid `dialog_storage::FileSystem` vault and
//! vice versa. These drive the [`Fs`](dialog_remote_fs) provider directly via
//! [`perform`](dialog_remote_fs::helpers::perform) -- the env-bound
//! `authorize`/`prove` path (read-vs-write gating, subject verification) is
//! covered by the Operator-driven tests in `e2e.rs`. They run on native (a
//! tempdir) and in the browser (an OPFS subdirectory) alike.

mod helpers;

use anyhow::Result;
use dialog_common::Blake3Hash;
use dialog_effects::archive::prelude::*;
use helpers::{perform, setup};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[dialog_common::test]
async fn it_returns_none_for_missing_blob() -> Result<()> {
    let env = setup().await;
    let digest = Blake3Hash::hash(b"never written");

    let result = perform(
        env.subject
            .clone()
            .archive()
            .catalog("index")
            .get(digest)
            .fork(&env.address),
    )
    .await??;
    assert_eq!(result, None);
    Ok(())
}

#[dialog_common::test]
async fn it_writes_and_reads_back_a_blob() -> Result<()> {
    let env = setup().await;
    let content = b"hello fs-remote".to_vec();
    let digest = Blake3Hash::hash(&content);

    perform(
        env.subject
            .clone()
            .archive()
            .catalog("index")
            .put(content.clone())
            .fork(&env.address),
    )
    .await??;

    let result = perform(
        env.subject
            .clone()
            .archive()
            .catalog("index")
            .get(digest)
            .fork(&env.address),
    )
    .await??;
    assert_eq!(result, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_writes_byte_compatibly_with_a_direct_filesystem() -> Result<()> {
    // Write via FS-remote, then read back via a dialog-storage FileSystem rooted
    // at the same directory: the vault fs-remote writes is a valid FileSystem
    // vault.
    let env = setup().await;
    let content = b"compat: fs-remote -> FileSystem".to_vec();
    let digest = Blake3Hash::hash(&content);

    perform(
        env.subject
            .clone()
            .archive()
            .catalog("index")
            .put(content.clone())
            .fork(&env.address),
    )
    .await??;

    let loaded = env
        .subject
        .clone()
        .archive()
        .catalog("index")
        .get(digest)
        .perform(&env.filesystem)
        .await?;
    assert_eq!(loaded, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_reads_byte_compatibly_from_a_direct_filesystem() -> Result<()> {
    // Reverse direction: a direct FileSystem writes, FS-remote reads.
    let env = setup().await;
    let content = b"compat: FileSystem -> fs-remote".to_vec();
    let digest = Blake3Hash::hash(&content);

    env.subject
        .clone()
        .archive()
        .catalog("index")
        .put(content.clone())
        .perform(&env.filesystem)
        .await?;

    let result = perform(
        env.subject
            .clone()
            .archive()
            .catalog("index")
            .get(digest)
            .fork(&env.address),
    )
    .await??;
    assert_eq!(result, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_is_idempotent_for_repeated_puts() -> Result<()> {
    let env = setup().await;
    let content = b"idempotent".to_vec();
    let digest = Blake3Hash::hash(&content);

    perform(
        env.subject
            .clone()
            .archive()
            .catalog("index")
            .put(content.clone())
            .fork(&env.address),
    )
    .await??;
    perform(
        env.subject
            .clone()
            .archive()
            .catalog("index")
            .put(content.clone())
            .fork(&env.address),
    )
    .await??;

    let result = perform(
        env.subject
            .clone()
            .archive()
            .catalog("index")
            .get(digest)
            .fork(&env.address),
    )
    .await??;
    assert_eq!(result, Some(content));
    Ok(())
}
