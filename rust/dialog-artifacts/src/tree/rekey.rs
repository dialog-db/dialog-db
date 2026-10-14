//! Rewriting a tree under another value encoding.
//!
//! A tree's manifest decides how its keys lay a number out: in one band
//! for every number ([`NUMBER_BAND`](crate::NUMBER_BAND)), or in a band
//! per number type, as the release before the band wrote. The rewrite
//! walks every entry of a tree in key order, re-keys each entry whose
//! value slot holds a number under the target encoding, and inserts it
//! into an empty tree under the target manifest. Leaf values are copied
//! as they are: a datum names its cause and a history record carries its
//! claim, and neither depends on how the key lays its value out.
//!
//! The result is canonical, so two replicas rewriting the same tree
//! produce the same root. That is what lets a merge treat a tree an
//! earlier release wrote and its rewrite as one tree, and a migration
//! two replicas run on their own converge.

use std::mem;

use dialog_search_tree::Manifest;
use futures_util::TryStreamExt as _;

use crate::artifacts::decode_value;
use crate::key::varkey::{ValuePayload, build_key, parse_key};
use crate::merge::full_scope;
use crate::tree::{ArtifactNodeCache, ArtifactTree, ArtifactTreeExt as _};
use crate::{
    ArchiveDelta, ArchiveReader, DialogArtifactsError, Key, ValueDataType, encode_key_value_owned,
};

/// How many entries one edit batch of the rewrite inserts.
const BATCH: usize = 4096;

/// `key`, as the same key reads under `value_encoding`: a number in its
/// value slot is decoded from the band the key carries and encoded in the
/// band the target keys it in; every other value slot is as it was.
pub fn rekey(key: &[u8], value_encoding: u32) -> Result<Key, DialogArtifactsError> {
    let mut parts = parse_key(key).ok_or_else(|| {
        DialogArtifactsError::InvalidKey("a stored key does not parse".to_string())
    })?;
    let numeric = matches!(
        parts.value_type,
        ValueDataType::UnsignedInt
            | ValueDataType::SignedInt
            | ValueDataType::Float
            | ValueDataType::Number
    );
    if numeric && let ValuePayload::Inline(payload) = &parts.value {
        let (value, rest) = decode_value(parts.value_type, payload).ok_or_else(|| {
            DialogArtifactsError::InvalidKey("a stored key's number does not decode".to_string())
        })?;
        if !rest.is_empty() {
            return Err(DialogArtifactsError::InvalidKey(
                "a stored key's number has trailing bytes".to_string(),
            ));
        }
        parts.value_type = value.data_type().band(value_encoding);
        parts.value = ValuePayload::Inline(encode_key_value_owned(&value, value_encoding));
    }
    Ok(Key::from(build_key(&parts)))
}

/// `source`, rewritten under `manifest`: every entry re-keyed by
/// [`rekey`] for the manifest's `value_encoding`, inserted into an empty
/// tree created under `manifest`. The new tree's nodes land in `delta`;
/// the caller persists them. `cache` is the node cache the result reads
/// through.
pub async fn rekey_tree<S>(
    source: &ArtifactTree,
    store: &S,
    delta: &mut ArchiveDelta,
    manifest: Manifest,
    cache: ArtifactNodeCache,
) -> Result<ArtifactTree, DialogArtifactsError>
where
    S: ArchiveReader + Clone,
{
    let value_encoding = manifest.value_encoding;
    let mut target = ArtifactTree::empty_with_manifest(manifest, cache);
    let [range] = full_scope();
    let stream = source.stream_range_handles(range, store);
    tokio::pin!(stream);
    let mut batch = Vec::with_capacity(BATCH);
    while let Some(entry) = stream.try_next().await? {
        batch.push((rekey(entry.key.as_ref(), value_encoding)?, entry.value));
        if batch.len() == BATCH {
            let entries = mem::replace(&mut batch, Vec::with_capacity(BATCH));
            target.record(store, delta, entries.into()).await?;
        }
    }
    if !batch.is_empty() {
        target.record(store, delta, batch.into()).await?;
    }
    Ok(target)
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use dialog_search_tree::{Manifest, MemoryBlocks, NodeCache, PersistentTree};
    use futures_util::{TryStreamExt as _, stream};

    use super::rekey_tree;
    use crate::tree::{ArtifactTree, ArtifactTreeExt as _, SpillCache};
    use crate::{
        ArchiveDelta, Artifact, ArtifactSelector, Entity, Instruction, NUMBER_BAND, Pick, Relation,
        Value,
    };

    fn size() -> Relation {
        "item/size".parse().expect("parses")
    }

    /// One item per value, with a text label beside its size, so the tree
    /// holds keys the rewrite leaves alone next to keys it moves.
    fn items(values: &[Value]) -> anyhow::Result<Vec<Instruction>> {
        let mut instructions = Vec::new();
        for value in values {
            let of = Entity::new()?;
            instructions.push(Instruction::Assert(
                Artifact {
                    the: size(),
                    of: of.clone(),
                    is: value.clone(),
                    cause: None,
                },
                Pick::All,
            ));
            instructions.push(Instruction::Assert(
                Artifact {
                    the: "item/label".parse()?,
                    of,
                    is: Value::String(format!("{value:?}")),
                    cause: None,
                },
                Pick::All,
            ));
        }
        Ok(instructions)
    }

    async fn tree(
        store: &MemoryBlocks,
        instructions: Vec<Instruction>,
    ) -> anyhow::Result<ArtifactTree> {
        let mut tree: ArtifactTree =
            PersistentTree::empty_with_manifest(Manifest::default(), NodeCache::new());
        let mut delta = ArchiveDelta::zero();
        tree.apply(store, &mut delta, stream::iter(instructions))
            .await?;
        delta.flush_into(store);
        Ok(tree)
    }

    async fn sizes(tree: &ArtifactTree, store: &MemoryBlocks) -> anyhow::Result<Vec<Value>> {
        let rows: Vec<Artifact> = tree
            .clone()
            .scan_owned(
                store.clone(),
                SpillCache::with_budget(0),
                ArtifactSelector::new().the(size()),
            )
            .try_collect()
            .await?;
        let mut values: Vec<Value> = rows.into_iter().map(|row| row.is).collect();
        values.sort_by(|a, b| number(a).total_cmp(&number(b)));
        Ok(values)
    }

    fn number(value: &Value) -> f64 {
        match value {
            Value::UnsignedInt(value) => *value as f64,
            Value::SignedInt(value) => *value as f64,
            Value::Float(value) => *value,
            _ => f64::NAN,
        }
    }

    async fn rekeyed(
        tree: &ArtifactTree,
        store: &MemoryBlocks,
        value_encoding: u32,
    ) -> anyhow::Result<ArtifactTree> {
        let manifest = Manifest {
            value_encoding,
            ..tree.manifest(store).await?
        };
        let mut delta = ArchiveDelta::zero();
        let rewritten = rekey_tree(tree, store, &mut delta, manifest, NodeCache::new()).await?;
        delta.flush_into(store);
        Ok(rewritten)
    }

    /// A tree rewritten under the per-type bands and back under the one
    /// band is the tree it started from, root for root: the rewrite loses
    /// nothing and its result is canonical. Under the per-type bands the
    /// same numbers read back, each in the band the rewrite put it in.
    #[dialog_common::test]
    async fn it_rewrites_a_tree_losslessly_and_deterministically() -> anyhow::Result<()> {
        let store = MemoryBlocks::new();
        let values = vec![
            Value::UnsignedInt(3),
            Value::SignedInt(-2),
            Value::Float(2.5),
            Value::Float(5.0),
            Value::UnsignedInt(u64::MAX as u128 + 1),
        ];
        let original = tree(&store, items(&values)?).await?;
        assert_eq!(
            original.manifest(&store).await?.value_encoding,
            NUMBER_BAND,
            "a new tree keys numbers in one band"
        );

        let legacy = rekeyed(&original, &store, 0).await?;
        assert_eq!(legacy.manifest(&store).await?.value_encoding, 0);
        assert_ne!(legacy.root(), original.root());
        let read = sizes(&legacy, &store).await?;
        assert_eq!(read.len(), values.len());
        for (read, written) in read.iter().zip(sizes(&original, &store).await?.iter()) {
            assert!(read.same_number(written), "{read:?} is {written:?}");
        }

        let restored = rekeyed(&legacy, &store, NUMBER_BAND).await?;
        assert_eq!(restored.root(), original.root(), "the round trip is exact");
        let again = rekeyed(&legacy, &store, NUMBER_BAND).await?;
        assert_eq!(
            again.root(),
            restored.root(),
            "the rewrite is deterministic"
        );
        Ok(())
    }
}
