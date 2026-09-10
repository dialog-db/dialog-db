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

/// Concurrent gets of one digest from one vault join a single in-flight
/// request, and a join never crosses vaults: two vaults can disagree
/// about holding a block, so a concurrent get from a vault that lacks it
/// must answer `None`, not the other vault's bytes. One test, because
/// overlap is forced through the process-global simulated link; the
/// assertions read the digest-scoped get ledger, so concurrent tests in
/// the same process (fetching their own digests) cannot skew them.
/// Native-only: shaping does not exist on wasm.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_joins_concurrent_gets_within_a_vault_only() -> Result<()> {
    use dialog_remote_fs::simulation::{self, NetworkShape};

    let holder = setup().await;
    let empty = setup().await;
    let shared = format!("joined once {}", dialog_storage::unique_name("block"));
    let shared = shared.into_bytes();
    let shared_digest = Blake3Hash::hash(&shared);
    let held = format!("held by one vault {}", dialog_storage::unique_name("block"));
    let held = held.into_bytes();
    let held_digest = Blake3Hash::hash(&held);
    for content in [shared.clone(), held.clone()] {
        perform(
            holder
                .subject
                .clone()
                .archive()
                .catalog("index")
                .put(content)
                .fork(&holder.address),
        )
        .await??;
    }

    simulation::configure(Some(NetworkShape {
        latency: std::time::Duration::from_millis(5),
        auth_latency: std::time::Duration::ZERO,
        bandwidth: None,
    }));

    // Same vault, same digest, concurrently: one wire request, shared.
    let get = || {
        perform(
            holder
                .subject
                .clone()
                .archive()
                .catalog("index")
                .get(shared_digest.clone())
                .fork(&holder.address),
        )
    };
    let (first, second) = futures_util::future::join(get(), get()).await;
    assert_eq!(first??, Some(shared.clone()));
    assert_eq!(second??, Some(shared));
    let record = simulation::get_ledger().record(&shared_digest.to_string());
    assert_eq!(
        (record.requests, record.empty),
        (1, 0),
        "concurrent identical gets share one wire request"
    );

    // Different vaults, same digest, concurrently: no join, and the vault
    // that lacks the block answers None.
    let (present, missing) = futures_util::future::join(
        perform(
            holder
                .subject
                .clone()
                .archive()
                .catalog("index")
                .get(held_digest.clone())
                .fork(&holder.address),
        ),
        perform(
            empty
                .subject
                .clone()
                .archive()
                .catalog("index")
                .get(held_digest.clone())
                .fork(&empty.address),
        ),
    )
    .await;
    simulation::configure(None);

    assert_eq!(present??, Some(held));
    assert_eq!(missing??, None, "a vault that lacks the block answers None");
    let record = simulation::get_ledger().record(&held_digest.to_string());
    assert_eq!(
        (record.requests, record.empty),
        (2, 1),
        "gets against different vaults never share a request"
    );
    Ok(())
}
