//! A tree sealed in layers and kept in an archive catalog.
//!
//! The layered envelopes go into the archive as ordinary content-addressed
//! blocks. Each claim runs on the volatile provider and on the filesystem
//! one (OPFS on the web), since the filesystem files blocks by path and the
//! volatile one by raw key.

use anyhow::Result;
use dialog_capability::{Provider, Subject};
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_effects::archive::prelude::{ArchiveScope, CatalogScope};
use dialog_effects::archive::{Get, Import};
use dialog_effects::storage::{Directory, Location};
use dialog_keyring::EpochId;
use dialog_keyring::KeyringError;
use dialog_keyring::layered::{
    Access, LayeredArchive, LayeredBlocks, LayeredRoot, Level, LevelSecret, Writer,
};
use dialog_search_tree::{Delta, PersistentTree};
use dialog_storage::provider::{FileSystem, Volatile};
use dialog_storage::resource::Resource as _;
use dialog_varsig::did;
use futures_util::StreamExt;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

type Archive<'a, Env> = LayeredArchive<'a, [u8; 4], Vec<u8>, Env>;
type Tree = PersistentTree<[u8; 4], Vec<u8>>;

/// Enough entries for a root over a couple of dozen leaves.
const ENTRIES: u32 = 4096;

/// A recognisable run of bytes inside every value, so a test can check the
/// archive never holds it.
const CANARY: &[u8] = b"canary-plaintext-marker";

/// The archive effects a test performs.
trait Store: Provider<Get> + Provider<Import> + ConditionalSync + 'static {}
impl<T> Store for T where T: Provider<Get> + Provider<Import> + ConditionalSync + 'static {}

fn catalog() -> CatalogScope {
    ArchiveScope::new(Subject::from(did!("key:zLayeredArchiveTest"))).catalog("index")
}

/// A name no other test run uses, so concurrent runs never share a
/// directory.
fn unique_name(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let now = dialog_common::time::now()
        .duration_since(dialog_common::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}-{now}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// A filesystem archive in a fresh temp directory; OPFS on the web.
async fn filesystem(name: &str) -> Result<FileSystem> {
    Ok(FileSystem::open(&Location::new(Directory::Temp, unique_name(name))).await?)
}

fn generation(tag: u8) -> EpochId {
    EpochId::from([tag; 32])
}

fn secret(tag: u8) -> LevelSecret {
    LevelSecret::new(generation(tag), [tag.wrapping_mul(31); 32])
}

fn range_one() -> LevelSecret {
    secret(1)
}

fn content_one() -> LevelSecret {
    secret(2)
}

fn writer() -> Writer {
    Writer::new(range_one(), content_one())
}

/// A member holding the first generations.
fn member() -> Access {
    Access::content(
        Level::new().with(range_one()),
        Level::new().with(content_one()),
    )
}

fn value(index: u32) -> Vec<u8> {
    let mut value = CANARY.to_vec();
    value.extend_from_slice(&index.to_be_bytes());
    value.resize(48, index as u8);
    value
}

/// A tree of `entries` built in one edit through `env`, persisted into a
/// fresh delta that is returned with it, not yet sealed.
async fn stage<P>(env: &P) -> Result<(Blake3Hash, Delta<Blake3Hash, dialog_common::Buffer>)>
where
    P: Provider<dialog_search_tree::LoadBlock> + ConditionalSync,
{
    let mut delta = Delta::zero();
    let mut transient = Tree::empty().edit();
    for index in 0..ENTRIES {
        transient = transient
            .insert(index.to_be_bytes(), value(index), env)
            .await?;
    }
    let tree = transient.persist(&mut delta)?;
    Ok((tree.root().clone(), delta))
}

/// Build the tree and write it into `archive`.
async fn build<Env: Store>(archive: &Archive<'_, Env>) -> Result<LayeredRoot> {
    let (root, mut delta) = stage(archive).await?;
    Ok(archive.write(&writer(), &mut delta, &root).await?)
}

/// Every entry of the tree at `root`, read by a member who did not write
/// it, reaching each node from the root.
async fn read_back<Env: Store>(
    archive: &Archive<'_, Env>,
    root: &LayeredRoot,
) -> Result<Vec<([u8; 4], Vec<u8>)>> {
    let reader = archive.as_party(member());
    let tree = Tree::from_hash(reader.open_root(root).await?);
    let mut stream = Box::pin(tree.stream(&reader));
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        let entry = entry?;
        entries.push((entry.key, entry.value));
    }
    Ok(entries)
}

/// A member writes a tree into the archive and another member reads every
/// entry back from it.
async fn reads_back<Env: Store>(env: &Env) -> Result<()> {
    let archive = Archive::new(env, catalog(), member());
    let root = build(&archive).await?;

    let read = read_back(&archive, &root).await?;
    assert_eq!(read.len(), ENTRIES as usize);
    for (index, (key, value_read)) in read.into_iter().enumerate() {
        assert_eq!(key, (index as u32).to_be_bytes());
        assert_eq!(value_read, value(index as u32));
    }
    Ok(())
}

#[dialog_common::test]
async fn a_member_reads_a_tree_back_from_the_archive() -> Result<()> {
    reads_back(&Volatile::new()).await
}

#[dialog_common::test]
async fn a_member_reads_a_tree_back_from_the_archive_on_the_filesystem() -> Result<()> {
    reads_back(&filesystem("layered-read").await?).await
}

/// Every block the tree put in the archive is an envelope: none holds a
/// value in the clear, and nothing is filed under the root's plaintext
/// identity.
async fn holds_no_plaintext<Env: Store>(env: &Env) -> Result<()> {
    let archive = Archive::new(env, catalog(), member());
    let root = build(&archive).await?;

    let reached = archive.walk(&root).await?;
    assert!(reached.len() > 1, "expected a multi-node tree");
    for address in &reached {
        let bytes = catalog()
            .get(address.clone())
            .perform(env)
            .await?
            .expect("a reached envelope is stored");
        assert!(
            !bytes.windows(CANARY.len()).any(|window| window == CANARY),
            "a value survived into the archive in the clear"
        );
    }

    let identity = archive.as_party(member()).open_root(&root).await?;
    assert!(
        catalog().get(identity).perform(env).await?.is_none(),
        "the archive is addressable by plaintext identity"
    );
    Ok(())
}

#[dialog_common::test]
async fn the_archive_never_holds_plaintext() -> Result<()> {
    holds_no_plaintext(&Volatile::new()).await
}

#[dialog_common::test]
async fn the_archive_never_holds_plaintext_on_the_filesystem() -> Result<()> {
    holds_no_plaintext(&filesystem("layered-plaintext").await?).await
}

/// A party with structure access walks the whole tree in the archive but
/// opens no node; one with range access opens no node either.
async fn levels_hold<Env: Store>(env: &Env) -> Result<()> {
    let archive = Archive::new(env, catalog(), member());
    let root = build(&archive).await?;

    let replicator = archive.as_party(Access::structure());
    assert!(replicator.walk(&root).await?.len() > 1);
    let refused = replicator.open_root(&root).await;
    assert!(
        matches!(&refused, Err(KeyringError::MissingGeneration(generation)) if generation == range_one().generation()),
        "a replicator opened the root: {refused:?}"
    );

    let router = archive.as_party(Access::ranges(Level::new().with(range_one())));
    let refused = router.open_root(&root).await;
    assert!(
        matches!(&refused, Err(KeyringError::MissingGeneration(generation)) if generation == content_one().generation()),
        "a range holder opened the root: {refused:?}"
    );
    Ok(())
}

#[dialog_common::test]
async fn levels_hold_in_the_archive() -> Result<()> {
    levels_hold(&Volatile::new()).await
}

#[dialog_common::test]
async fn levels_hold_in_the_archive_on_the_filesystem() -> Result<()> {
    levels_hold(&filesystem("layered-levels").await?).await
}

/// A write whose staged blocks miss a node is refused before anything
/// reaches the archive, and keeps what was staged.
async fn refused_write_imports_nothing<Env: Store>(env: &Env) -> Result<()> {
    // The envelopes the full write would produce, sealed in memory: sealing
    // is deterministic, so these are the addresses a write would import.
    let memory = LayeredBlocks::<[u8; 4], Vec<u8>>::new(member());
    let (root, mut delta) = stage(&memory).await?;
    let expected = memory.write(&writer(), &mut delta, &root)?;
    let would_import: Vec<Blake3Hash> = memory
        .stored()
        .into_iter()
        .map(|(address, _)| address)
        .collect();

    let archive = Archive::new(env, catalog(), member());
    let (staged_root, mut delta) = stage(&archive).await?;
    assert_eq!(staged_root, root);
    let missing = delta
        .flush()
        .map(|(identity, _)| identity)
        .find(|identity| identity != &root)
        .expect("a node besides the root");
    // `flush` emptied the delta; stage again and drop only that node.
    let (_, mut delta) = stage(&archive).await?;
    delta.remove(&missing);
    let staged = delta.len();

    let refused = archive.write(&writer(), &mut delta, &root).await;
    assert!(
        matches!(&refused, Err(KeyringError::UnknownNode(node)) if node == &missing),
        "a write missing a node was not refused: {refused:?}"
    );

    assert_eq!(delta.len(), staged, "a refused write consumed the delta");
    for address in &would_import {
        assert!(
            catalog().get(address.clone()).perform(env).await?.is_none(),
            "a refused write imported an envelope"
        );
    }
    assert!(
        catalog()
            .get(expected.address)
            .perform(env)
            .await?
            .is_none()
    );
    Ok(())
}

#[dialog_common::test]
async fn a_refused_write_imports_nothing() -> Result<()> {
    refused_write_imports_nothing(&Volatile::new()).await
}

#[dialog_common::test]
async fn a_refused_write_imports_nothing_on_the_filesystem() -> Result<()> {
    refused_write_imports_nothing(&filesystem("layered-refused").await?).await
}

/// A member edits a tree reading through the archive: the nodes it does not
/// touch are linked where they already live, so only the changed path is
/// imported again.
async fn edits_reseal_their_path<Env: Store>(env: &Env) -> Result<()> {
    let archive = Archive::new(env, catalog(), member());
    let root = build(&archive).await?;
    let before = archive.walk(&root).await?;

    let editor = archive.as_party(member());
    let tree = Tree::from_hash(editor.open_root(&root).await?);
    let mut delta = Delta::zero();
    let edited = tree
        .edit()
        .insert(ENTRIES.to_be_bytes(), value(ENTRIES), &editor)
        .await?
        .persist(&mut delta)?;
    let next = editor.write(&writer(), &mut delta, edited.root()).await?;

    let after = archive.walk(&next).await?;
    let shared = after
        .iter()
        .filter(|address| before.contains(address))
        .count();
    assert!(
        shared > 0 && after.len() - shared < before.len() / 2,
        "expected most envelopes shared: {shared} of {} shared, {} before",
        after.len(),
        before.len()
    );
    assert_eq!(
        read_back(&archive, &next).await?.len(),
        ENTRIES as usize + 1
    );
    Ok(())
}

#[dialog_common::test]
async fn an_edit_through_the_archive_reseals_only_its_path() -> Result<()> {
    edits_reseal_their_path(&Volatile::new()).await
}

#[dialog_common::test]
async fn an_edit_through_the_archive_reseals_only_its_path_on_the_filesystem() -> Result<()> {
    edits_reseal_their_path(&filesystem("layered-edit").await?).await
}

/// The same tree sealed into a volatile archive and a filesystem one lands
/// at the same addresses with the same bytes, and the same as in memory.
#[dialog_common::test]
async fn archives_converge_across_providers() -> Result<()> {
    let volatile = Volatile::new();
    let disk = filesystem("layered-converge").await?;
    let one = Archive::new(&volatile, catalog(), member());
    let two = Archive::new(&disk, catalog(), member());
    let memory = LayeredBlocks::<[u8; 4], Vec<u8>>::new(member());

    let root_one = build(&one).await?;
    let root_two = build(&two).await?;
    let (root, mut delta) = stage(&memory).await?;
    let root_memory = memory.write(&writer(), &mut delta, &root)?;
    assert_eq!(root_one, root_two);
    assert_eq!(root_one, root_memory);

    let addresses = one.walk(&root_one).await?;
    assert_eq!(addresses, two.walk(&root_two).await?);
    for address in addresses {
        let a = catalog().get(address.clone()).perform(&volatile).await?;
        let b = catalog().get(address.clone()).perform(&disk).await?;
        assert_eq!(a, b);
        assert_eq!(a, memory.raw(&address));
    }
    Ok(())
}
