//! A tree sealed in layers, read at each level of access.
//!
//! Each test pins a claim the layered design rests on: what a replicator, a
//! range holder and a member can and cannot do with the same envelopes, that
//! replicas still converge, that corruption is caught without a key, that an
//! edit reseals only the path it changed, and that rotating the content
//! generation locks out a party that lost it.

use dialog_common::{Blake3Hash, Buffer};
use dialog_keyring::EpochId;
use dialog_keyring::KeyringError;
use dialog_keyring::layered::{
    Access, Envelope, LayeredBlocks, LayeredRoot, Level, LevelSecret, Writer,
};
use dialog_search_tree::{Delta, NodeBody, PersistentNode, PersistentTree};
use futures_util::StreamExt;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

type Store = LayeredBlocks<[u8; 4], Vec<u8>>;
type Tree = PersistentTree<[u8; 4], Vec<u8>>;

/// How many entries to write. Enough for a root over a couple of dozen
/// leaves, so routing has choices to make and an edit has siblings to share.
const ENTRIES: u32 = 4096;

/// A recognisable run of bytes inside every value, so a test can check the
/// store never holds it.
const CANARY: &[u8] = b"canary-plaintext-marker";

fn generation(tag: u8) -> EpochId {
    EpochId::from([tag; 32])
}

fn secret(tag: u8) -> LevelSecret {
    LevelSecret::new(generation(tag), [tag.wrapping_mul(31); 32])
}

/// The first range and content generations.
fn range_one() -> LevelSecret {
    secret(1)
}
fn content_one() -> LevelSecret {
    secret(2)
}
/// A later content generation, minted when someone lost content access.
fn content_two() -> LevelSecret {
    secret(3)
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

/// Build a tree of `entries` in one edit through `store`, seal it under
/// `writer`, and return where it starts.
async fn build(store: &Store, writer: &Writer, entries: std::ops::Range<u32>) -> LayeredRoot {
    let mut delta = Delta::zero();
    let mut transient = Tree::empty().edit();
    for index in entries {
        transient = transient
            .insert(index.to_be_bytes(), value(index), store)
            .await
            .expect("insert");
    }
    let tree = transient.persist(&mut delta).expect("persist");
    store.write(writer, &mut delta, tree.root()).expect("write")
}

/// Open the tree at `root` as a member reading through `store`.
fn open(store: &Store, root: &LayeredRoot) -> Tree {
    Tree::from_hash(store.open_root(root).expect("open root"))
}

/// Every entry of `tree`, in order.
async fn entries(tree: &Tree, store: &Store) -> Vec<([u8; 4], Vec<u8>)> {
    let mut stream = Box::pin(tree.stream(store));
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        let entry = entry.expect("entry");
        entries.push((entry.key, entry.value));
    }
    entries
}

#[dialog_common::test]
async fn a_member_reads_the_tree_back() {
    let store = Store::new(member());
    let root = build(&store, &writer(), 0..ENTRIES).await;

    // A member who did not write it, reaching every node from the root.
    let reader = store.as_party(member());
    let tree = open(&reader, &root);

    for index in (0..ENTRIES).step_by(97) {
        assert_eq!(
            tree.get(&index.to_be_bytes(), &reader).await.expect("get"),
            Some(value(index)),
            "entry {index}"
        );
    }
    let read = entries(&tree, &reader).await;
    assert_eq!(read.len(), ENTRIES as usize);
    for (index, (key, value_read)) in read.into_iter().enumerate() {
        assert_eq!(key, (index as u32).to_be_bytes());
        assert_eq!(value_read, value(index as u32));
    }
}

#[dialog_common::test]
async fn the_store_never_holds_plaintext() {
    let store = Store::new(member());
    build(&store, &writer(), 0..ENTRIES).await;

    let stored = store.stored();
    assert!(stored.len() > 1, "expected a multi-node tree");
    for (_, bytes) in stored {
        assert!(
            !bytes.windows(CANARY.len()).any(|window| window == CANARY),
            "a value survived into the store in the clear"
        );
    }
}

#[dialog_common::test]
async fn a_replicator_moves_a_tree_it_cannot_read() {
    let source = Store::new(member());
    let root = build(&source, &writer(), 0..ENTRIES).await;

    let replicator = source.as_party(Access::structure());

    // It reaches and checks every envelope...
    let reached = replicator.walk(&root).expect("walk");
    assert_eq!(reached.len(), source.len());

    // ...and copies them somewhere else,
    let destination = Store::new(Access::structure());
    let copied = destination
        .replicate_from(&replicator, &root)
        .expect("replicate");
    assert_eq!(copied, source.len());

    // but opens neither a separator nor a node.
    let envelope = replicator.fetch(&root.address).expect("fetch");
    assert!(matches!(
        envelope.separators(&Access::structure(), &root.structure),
        Err(KeyringError::MissingGeneration(_))
    ));
    assert!(matches!(
        replicator.open_root(&root),
        Err(KeyringError::MissingGeneration(_))
    ));

    // A member reads the tree from where the replicator put it.
    let reader = destination.as_party(member());
    let tree = open(&reader, &root);
    assert_eq!(entries(&tree, &reader).await.len(), ENTRIES as usize);
}

#[dialog_common::test]
async fn a_range_holder_routes_without_reading() {
    let store = Store::new(member());
    let root = build(&store, &writer(), 0..ENTRIES).await;

    let router = store.as_party(Access::ranges(Level::new().with(range_one())));
    let reference = store.as_party(member());

    for index in (0..ENTRIES).step_by(31) {
        let key = index.to_be_bytes();
        assert_eq!(
            router.route(&root, &key).expect("route"),
            route_by_content(&reference, &root, &key),
            "key {index}"
        );
    }

    // Routing opened no content, and the range holder cannot.
    assert!(matches!(
        router.open_root(&root),
        Err(KeyringError::MissingGeneration(_))
    ));
}

/// Where `key` routes, worked out by a member from each index's plaintext
/// separators rather than from the range regions: an independent check that
/// the range regions say what the nodes say.
fn route_by_content(store: &Store, root: &LayeredRoot, key: &[u8]) -> Blake3Hash {
    let (mut address, mut structure) = (root.address.clone(), root.structure);
    loop {
        let envelope = store.fetch(&address).expect("fetch");
        let plain = Buffer::from(envelope.content(&member(), &structure).expect("content"));
        let node = PersistentNode::<[u8; 4], Vec<u8>>::try_from(plain).expect("node");
        let NodeBody::Index(index) = node.body() else {
            return address;
        };
        let separators: Vec<Vec<u8>> = (0..index.len())
            .map(|at| index.separator(at).expect("separator"))
            .collect();
        let at = separators
            .iter()
            .rposition(|separator| separator.as_slice() <= key)
            .unwrap_or(0);
        let (child, child_key) = envelope
            .children(&structure)
            .expect("children")
            .into_iter()
            .nth(at)
            .expect("child");
        address = child;
        structure = child_key;
    }
}

#[dialog_common::test]
async fn content_access_requires_range_access() {
    let store = Store::new(member());
    let root = build(&store, &writer(), 0..64).await;

    // The content secret alone derives nothing: the content key is derived
    // from the range key.
    let content_only = store.as_party(Access::content(
        Level::new(),
        Level::new().with(content_one()),
    ));
    assert!(matches!(
        content_only.open_root(&root),
        Err(KeyringError::MissingGeneration(generation)) if generation == range_one().generation().clone()
    ));
}

#[dialog_common::test]
async fn two_replicas_seal_byte_identical_stores() {
    let here = Store::new(member());
    let there = Store::new(member());

    let mine = build(&here, &writer(), 0..ENTRIES).await;
    let theirs = build(&there, &writer(), 0..ENTRIES).await;

    assert_eq!(mine, theirs);
    let mut ours = here.stored();
    let mut yours = there.stored();
    ours.sort();
    yours.sort();
    assert_eq!(ours, yours);
}

#[dialog_common::test]
async fn corruption_is_caught_without_a_key() {
    let store = Store::new(member());
    let root = build(&store, &writer(), 0..ENTRIES).await;

    // Flip one byte of some envelope below the root.
    let replicator = store.as_party(Access::structure());
    let victim = replicator.walk(&root).expect("walk")[1].clone();
    let mut bytes = store.raw(&victim).expect("stored");
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    store.put_raw(victim.clone(), bytes);

    assert!(matches!(
        replicator.walk(&root),
        Err(KeyringError::Corrupt(address)) if address == victim
    ));
}

#[dialog_common::test]
async fn an_edit_reseals_only_the_path_it_changed() {
    let store = Store::new(member());
    let root = build(&store, &writer(), 0..ENTRIES).await;
    let before: Vec<Blake3Hash> = store.walk(&root).expect("walk");

    // Edit as a member reading through the store, so the nodes it does not
    // touch are linked where they already live.
    let editor = store.as_party(member());
    let tree = open(&editor, &root);
    let mut delta = Delta::zero();
    let edited = tree
        .edit()
        .insert(ENTRIES.to_be_bytes(), value(ENTRIES), &editor)
        .await
        .expect("insert")
        .persist(&mut delta)
        .expect("persist");
    let next = editor
        .write(&writer(), &mut delta, edited.root())
        .expect("write");

    let after: Vec<Blake3Hash> = store.walk(&next).expect("walk");
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

    println!(
        "one insert into {} envelopes resealed {} and shared {shared}",
        before.len(),
        after.len() - shared
    );

    let reader = store.as_party(member());
    let tree = open(&reader, &next);
    assert_eq!(entries(&tree, &reader).await.len(), ENTRIES as usize + 1);
}

#[dialog_common::test]
async fn rotating_content_locks_out_a_party_that_lost_it() {
    let store = Store::new(member());
    let root = build(&store, &writer(), 0..ENTRIES).await;

    // Someone loses content access, so the content generation rotates and
    // the next edit seals under it.
    let rotated = Writer::new(range_one(), content_two());
    let both = Access::content(
        Level::new().with(range_one()),
        Level::new().with(content_one()).with(content_two()),
    );
    let editor = store.as_party(both.clone());
    let tree = open(&editor, &root);
    let mut delta = Delta::zero();
    let edited = tree
        .edit()
        .insert(ENTRIES.to_be_bytes(), value(ENTRIES), &editor)
        .await
        .expect("insert")
        .persist(&mut delta)
        .expect("persist");
    let next = editor
        .write(&rotated, &mut delta, edited.root())
        .expect("write");

    // A member holding both generations reads the whole edited tree, old
    // nodes and new.
    let reader = store.as_party(both);
    let tree = open(&reader, &next);
    assert_eq!(entries(&tree, &reader).await.len(), ENTRIES as usize + 1);

    // The party that lost access still reads what it could before...
    let removed = store.as_party(member());
    let old = open(&removed, &root);
    assert_eq!(entries(&old, &removed).await.len(), ENTRIES as usize);

    // ...and nothing written since.
    assert!(matches!(
        removed.open_root(&next),
        Err(KeyringError::MissingGeneration(generation)) if generation == content_two().generation().clone()
    ));
}

#[dialog_common::test]
async fn relabelling_a_generation_fails_to_open() {
    let store = Store::new(member());
    let root = build(&store, &writer(), 0..64).await;

    // Rewrite the root's header to name a content generation the reader
    // also holds. The regions authenticate the header, so the node does not
    // open under the other generation's key.
    let mut bytes = store.raw(&root.address).expect("stored");
    bytes[33..65].copy_from_slice(content_two().generation().as_bytes());
    let relabelled = LayeredRoot {
        address: Blake3Hash::hash(&bytes),
        structure: root.structure,
    };
    store.put_raw(relabelled.address.clone(), bytes);

    let reader = store.as_party(Access::content(
        Level::new().with(range_one()),
        Level::new().with(content_one()).with(content_two()),
    ));
    assert!(matches!(
        reader.open_root(&relabelled),
        Err(KeyringError::Failed)
    ));
}

#[dialog_common::test]
async fn an_envelope_round_trips_and_refuses_truncation() {
    let store = Store::new(member());
    let root = build(&store, &writer(), 0..64).await;
    let bytes = store.raw(&root.address).expect("stored");

    let envelope = Envelope::from_bytes(&bytes).expect("decode");
    assert_eq!(envelope.to_bytes(), bytes);
    assert_eq!(envelope.address(), root.address);

    // Cut through the header, the structure nonce or a region length: the
    // bytes do not say what they hold, so they do not decode.
    for cut in [0, 1, 64, 76, 80] {
        assert!(
            matches!(
                Envelope::from_bytes(&bytes[..cut]),
                Err(KeyringError::Malformed)
            ),
            "a cut at {cut} should not decode"
        );
    }

    // Cut through the content region, which runs to the end: the bytes
    // decode, but the content no longer opens, and they no longer match the
    // address anyone would fetch them by.
    let short = &bytes[..bytes.len() - 1];
    let truncated = Envelope::from_bytes(short).expect("decode");
    assert!(matches!(
        truncated.content(&member(), &root.structure),
        Err(KeyringError::Failed)
    ));
    assert_ne!(Blake3Hash::hash(short), root.address);
}
