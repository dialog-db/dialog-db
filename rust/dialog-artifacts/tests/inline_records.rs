//! Revision records written in the inline form stay readable beside the
//! spilled form.
//!
//! Every release through 0.4.0-rc.3 wrote a revision record's bytes into
//! its two index keys whenever they fit `inline_n`, which they always did.
//! A record now spills by rule (`RevisionRecord::entries`), and nothing
//! rewrites the records a replica already holds, so every reader of
//! records must accept both forms in one tree: the history reader, the
//! ancestry walk (`log`), lineage verification (`novelty`) and the context
//! derivation (`context_of`). The fixture holds nodes a pre-change writer
//! produced, byte for byte.

use std::str::FromStr as _;

use base58::ToBase58 as _;
use dialog_artifacts::history::{
    Context, History as _, REVISION_ATTRIBUTE, REVISION_RECORD_FORMAT, RevisionRecord, TreeHistory,
    Version, context_of, log, novelty,
};
use dialog_artifacts::tree::{ArtifactTree, ArtifactTreeExt as _};
use dialog_artifacts::{
    ArchiveDelta, AttributeKey, AttributeKeyPart, Entity, Key, KeyViewConstruct, KeyViewMut as _,
    Relation, State,
};
use dialog_common::Blake3Hash;
use dialog_search_tree::MemoryBlocks;
use ed25519_dalek::{Signer as _, SigningKey};
use futures_util::StreamExt as _;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

const FIXTURE: &str = include_str!("fixtures/inline-revision-records.txt");

/// How many records the fixture's chain holds.
const INLINE: usize = 3;

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).expect("hex"))
        .collect()
}

/// The fixture's nodes in a fresh store, and its root.
fn load() -> (MemoryBlocks, Blake3Hash) {
    let store = MemoryBlocks::new();
    let mut root = None;
    for line in FIXTURE.lines() {
        if let Some(hex) = line.strip_prefix("tree root=") {
            root = Some(Blake3Hash::try_from(unhex(hex)).expect("hash"));
        } else if let Some(hex) = line.strip_prefix("node ") {
            store.store(unhex(hex).into());
        }
    }
    (store, root.expect("a root line"))
}

/// The chain the fixture was generated from, extended to `length`
/// records: the first `INLINE` are the fixture's own, byte for byte
/// (Ed25519 signatures are deterministic), and the rest continue it.
fn chain(length: usize) -> anyhow::Result<Vec<RevisionRecord>> {
    let key = SigningKey::from_bytes(&[7u8; 32]);
    let issuer = {
        let mut bytes = vec![0xed, 0x01];
        bytes.extend_from_slice(key.verifying_key().as_bytes());
        format!("did:key:z{}", bytes.to_base58())
    };
    let branch = Entity::from_str("fixture:branch")?;
    let mut records = Vec::new();
    let mut parents: Vec<Version> = Vec::new();
    let mut skips: Vec<Version> = Vec::new();
    for _ in 0..length {
        let mut record = RevisionRecord {
            format: REVISION_RECORD_FORMAT,
            branch: branch.clone(),
            issuer: issuer.clone(),
            authority: "did:web:fixture.example".to_string(),
            parents: parents.clone(),
            skips: skips.clone(),
            signature: Vec::new(),
        };
        record.signature = key.sign(&record.payload()?).to_bytes().to_vec();
        let version = record.version();
        skips.insert(0, version);
        parents = vec![version];
        records.push(record);
    }
    Ok(records)
}

/// The attribute-ordered keys of every revision record in `tree`, in tree
/// order, with whether each spilled.
async fn record_keys(
    tree: &ArtifactTree,
    store: &MemoryBlocks,
) -> anyhow::Result<Vec<(Key, bool)>> {
    let the = Relation::from_str(REVISION_ATTRIBUTE)?;
    let start = <AttributeKey<Key> as KeyViewConstruct>::min()
        .set_attribute(AttributeKeyPart::from(&the))
        .into_key();
    let end = <AttributeKey<Key> as KeyViewConstruct>::max()
        .set_attribute(AttributeKeyPart::from(&the))
        .into_key();
    let stream = tree.stream_range(start..=end, store);
    futures_util::pin_mut!(stream);
    let mut keys = Vec::new();
    while let Some(entry) = stream.next().await {
        let entry = entry?;
        assert!(matches!(entry.value, State::Added(_)));
        let spilled = entry.key.value_spill_hash().is_some();
        keys.push((entry.key, spilled));
    }
    Ok(keys)
}

/// Every reader of records agrees with `records` being the chain that
/// `history` holds, from its last record back to genesis.
async fn assert_reads(
    history: &TreeHistory<MemoryBlocks>,
    store: &MemoryBlocks,
    records: &[RevisionRecord],
) -> anyhow::Result<()> {
    let head = records.last().expect("a chain").version();
    for record in records {
        assert_eq!(
            history.revision_record(&record.version()).await?.as_ref(),
            Some(record),
            "the history reader finds and verifies {}",
            record.version()
        );
    }

    let walked = log(&head, history, usize::MAX).await?;
    let newest_first: Vec<&RevisionRecord> = records.iter().rev().collect();
    assert_eq!(
        walked.iter().map(|(_, record)| record).collect::<Vec<_>>(),
        newest_first,
        "the ancestry walk lists the whole chain newest first"
    );

    let empty = TreeHistory::new(ArtifactTree::empty(), store.clone());
    let novel = novelty(&head, history, &empty).await?;
    assert_eq!(
        novel.iter().map(|(_, record)| record).collect::<Vec<_>>(),
        records.iter().collect::<Vec<_>>(),
        "lineage verification vouches for every record, ancestors first"
    );

    let context: Context = context_of(&head, history).await?;
    assert_eq!(context.len(), 1, "one origin wrote the chain");
    let watermark = context.get(&head.origin).expect("the chain's origin");
    assert_eq!(watermark.edition, head.edition);
    assert_eq!(watermark.count, records.len() as u64);
    Ok(())
}

/// A tree of records a pre-change writer stored inline reads back through
/// every reader, unchanged.
#[dialog_common::test]
async fn it_reads_revision_records_a_previous_release_wrote_inline() -> anyhow::Result<()> {
    let (store, root) = load();
    let records = chain(INLINE)?;
    let tree = ArtifactTree::from_hash(root);

    let keys = record_keys(&tree, &store).await?;
    assert_eq!(keys.len(), INLINE, "one attribute-ordered entry per record");
    assert!(
        keys.iter().all(|(_, spilled)| !spilled),
        "the fixture's records are inline"
    );

    let history = TreeHistory::new(tree, store.clone());
    assert_reads(&history, &store, &records).await
}

/// A tree holding records of both forms, the inline ones a previous release
/// wrote and the spilled ones this release appends after them, keeps its
/// entries in order and reads back through every reader as one chain.
#[dialog_common::test]
async fn it_reads_a_tree_mixing_inline_and_spilled_revision_records() -> anyhow::Result<()> {
    let (store, root) = load();
    const SPILLED: usize = 2;
    let records = chain(INLINE + SPILLED)?;

    // This release's writer continues the chain on the fixture's tree.
    let mut tree = ArtifactTree::from_hash(root.clone());
    let mut delta = ArchiveDelta::zero();
    let manifest = tree.manifest(&store).await?;
    for record in &records[INLINE..] {
        let entries = record.entries(&manifest)?;
        assert!(entries.spill.is_some(), "a record spills by rule");
        tree.record(&store, &mut delta, entries).await?;
    }
    delta.flush_into(&store);

    let keys = record_keys(&tree, &store).await?;
    assert_eq!(keys.len(), INLINE + SPILLED);
    assert_eq!(
        keys.iter().filter(|(_, spilled)| *spilled).count(),
        SPILLED,
        "the appended records spilled and the fixture's stayed inline"
    );
    assert!(
        keys.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "the entries of both forms are in strictly increasing key order"
    );

    let history = TreeHistory::new(tree, store.clone());
    assert_reads(&history, &store, &records).await?;

    // Lineage verification from a replica that holds the inline prefix:
    // only the spilled records are novel.
    let known = TreeHistory::new(ArtifactTree::from_hash(root), store.clone());
    let head = records.last().expect("a chain").version();
    let novel = novelty(&head, &history, &known).await?;
    assert_eq!(
        novel.iter().map(|(_, record)| record).collect::<Vec<_>>(),
        records[INLINE..].iter().collect::<Vec<_>>()
    );
    Ok(())
}
