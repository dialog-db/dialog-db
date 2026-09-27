//! Streaming merge + tombstone helpers for query-time source composition.
//!
//! Everything here works on `Stream<Item = Result<ArtifactView, _>>` —
//! [`ArtifactStream`]s — and is agnostic to where the streams came
//! from (a branch's tree scan, a [`Changes`] overlay, anything else
//! that implements `Provider<Select>`). Rows travel as borrowed-access
//! [`ArtifactView`]s; nothing in this layer materializes an owned
//! `Artifact`, and merge order comes from each row's
//! [`sort_key`](ArtifactView::sort_key) — derived straight from a scanned
//! row's stored key bytes, with no per-row value decode or re-encode.
//!
//! Every helper takes the format [`Manifest`] of the trees the streams are
//! read from. A scanned row's key already carries that format; an owned
//! row (an overlay fact) and a retracted fact are keyed under it, so they
//! order and match exactly as the tree's own rows do.
//!
//! - [`merge_grouped`] is the k-way merge that backs query-time
//!   union of multiple sources. It preserves the "as-if merged into a
//!   single physical tree" order via [`sort_key`](dialog_artifacts::sort_key)
//!   and dedupes identical `(the, of, is, cause)` artifacts within
//!   each `(the, of)` run.
//! - [`tombstones_from`] + [`filter_tombstones`] lift retract
//!   instructions out of a [`Changes`] overlay and apply them to a
//!   source stream as a filter — the mechanism that lets a
//!   [`Transaction::retract`](crate::repository::branch::Transaction::retract)
//!   suppress facts in the underlying branch view.

use std::collections::HashSet;
use std::sync::Arc;

use dialog_artifacts::{Artifact, ArtifactStream, ArtifactView, Cause, Changes, SortKey, sort_key};
use dialog_search_tree::Manifest;
use futures_util::{StreamExt, stream};

/// Merge sorted artifact streams into one stream whose order matches
/// what a single physical prolly tree containing every input would
/// produce, deduplicating identical claims that appear in more than one
/// source.
///
/// Each input is assumed sorted by [`sort_key`] under `manifest` — true
/// of scans of trees written under `manifest` by construction (the
/// prolly tree stores entries in that order), and of in-memory overlays
/// read with that manifest ([`Changes::select`], `Ephemeral::select`),
/// which sort their materialized rows by it. Implemented as a streaming k-way
/// merge over per-stream head slots, each holding the front row and its
/// sort key.
///
/// # Order: "as-if merged into one tree"
///
/// The k-way merge picks the minimum head by [`sort_key`], not by
/// [`group_key`]. That distinction matters within a `(the, of)` group
/// with cardinality > 1: two items from different streams sharing the
/// same `(the, of)` but different values would otherwise come out in
/// arbitrary (stream-index) order. Concretely, two sources each
/// holding `(alice, name, "Bob")` and `(alice, name, "Alice")` would
/// yield `["Bob", "Alice"]` if the merge tiebroke on stream index,
/// but a single physical tree yields `["Alice", "Bob"]` (sorted by
/// `value_reference`).
///
/// `sort_key` works as the comparator here *for any selector* because
/// it is the one total order consistent with all three tree index
/// layouts — see the [`SortKey`](dialog_artifacts::SortKey) docs for
/// the full why. Every stream reaching this merge was produced by the
/// same selector, so they're all already in `sort_key` order; the
/// merge just interleaves them.
///
/// # Dedup: "same claim from two sources is still one claim"
///
/// When the same `(the, of, is, cause)` claim appears in multiple
/// inputs, only the first occurrence within a `(the, of)` run is
/// yielded. The dedup region is the `(the, of)` run, tracked by the sort
/// key's leading components; the fingerprint is the full [`SortKey`] plus
/// the row's cause. The sort key's value tail identifies the value exactly
/// (an inline tail is the value's lossless order-preserving encoding; a
/// spilled tail carries the whole-value content hash), so two rows share a
/// fingerprint iff they are the same `(the, of, is, cause)` claim — no
/// value decode needed.
///
/// # Keys
///
/// With [`MergeKeys::Stored`] a scanned row's key is read off its stored key
/// bytes, which is exact when every scanned input was written under
/// `manifest`. Inputs written under different manifests carry keys that do
/// not compare, so [`MergeKeys::Fields`] derives every row's key from its
/// fields under `manifest` instead: slower (each row's value is
/// materialized), but the dedup fingerprint then identifies a claim across
/// formats. Each input stays grouped by `(the, of)` whatever its format, so
/// runs still merge whole; only the order of values within a run may differ
/// from a single tree's.
pub(crate) fn merge_grouped<'a>(
    streams: Vec<ArtifactStream<'a>>,
    manifest: Manifest,
    keys: MergeKeys,
) -> ArtifactStream<'a> {
    if streams.is_empty() {
        return Box::pin(stream::empty());
    }
    if streams.len() == 1 {
        // A single-stream merge can still surface duplicates if the
        // caller passes an already-unioned stream, but for branch /
        // overlay scans every key is unique within a single stream so
        // the dedup pass would be pure overhead. Pass through unchanged.
        return streams.into_iter().next().expect("len == 1");
    }

    let mut streams = streams;

    Box::pin(async_stream::try_stream! {
        // One head slot per input: the stream's current front row paired
        // with its sort key, computed ONCE per row as the slot fills. The
        // minimum-head scan below compares slots by reference and the
        // winner is moved out — no per-round key clone, no re-derivation
        // (the pre-view code re-encoded each head's value once per
        // competing stream per yielded item; the peekable-based merge
        // after it still cloned the winning key on every beat).
        let mut heads: Vec<Option<(SortKey, ArtifactView)>> = Vec::with_capacity(streams.len());
        for stream in &mut streams {
            heads.push(match stream.next().await {
                None => None,
                Some(row) => {
                    let view = row?;
                    let key = keys.of(&view, &manifest)?;
                    Some((key, view))
                }
            });
        }

        // Fingerprints already yielded within the current (the, of) run.
        // Cleared whenever the run advances to a new group.
        let mut current_group: Option<(Vec<u8>, Vec<u8>)> = None;
        let mut seen: HashSet<(Vec<u8>, Option<Cause>)> = HashSet::new();

        loop {
            let mut min_idx: Option<usize> = None;
            for (i, slot) in heads.iter().enumerate() {
                if let Some((key, _)) = slot {
                    let beats = match min_idx {
                        Some(min) => {
                            let (min_key, _) = heads[min].as_ref().expect("min slot filled");
                            key < min_key
                        }
                        None => true,
                    };
                    if beats {
                        min_idx = Some(i);
                    }
                }
            }
            let Some(idx) = min_idx else { break };
            let (key, view) = heads[idx].take().expect("minimum chosen from filled slot");
            // Refill the winner's slot from its stream before yielding, so
            // the next round sees every live head.
            heads[idx] = match streams[idx].next().await {
                None => None,
                Some(row) => {
                    let next_view = row?;
                    let next_key = keys.of(&next_view, &manifest)?;
                    Some((next_key, next_view))
                }
            };

            let (the, of, tail) = key;
            let group = (the, of);
            if current_group.as_ref() != Some(&group) {
                current_group = Some(group);
                seen.clear();
            }
            // Within the (the, of) run the value tail + cause identify the
            // claim, so the fingerprint needs only those.
            if seen.insert((tail, view.cause().cloned())) {
                yield view;
            }
        }
    })
}

/// Where [`merge_grouped`] reads each row's [`SortKey`] from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeKeys {
    /// Off a scanned row's stored key bytes: every scanned input was
    /// written under the merge's manifest.
    Stored,
    /// From every row's fields under the merge's manifest: the scanned
    /// inputs were written under different manifests.
    Fields,
}

impl MergeKeys {
    fn of(
        self,
        view: &ArtifactView,
        manifest: &Manifest,
    ) -> Result<SortKey, dialog_artifacts::DialogArtifactsError> {
        match self {
            MergeKeys::Stored => view.sort_key(manifest),
            MergeKeys::Fields => Ok(sort_key(&view.to_owned()?, manifest)),
        }
    }
}

/// Extract a tombstone set from a [`Changes`] overlay — one
/// [`SortKey`] per retracted artifact.
///
/// Asserts and Replaces are ignored; only Retracts contribute. Used
/// at query time to filter matching source facts out of branch
/// streams before they reach the merge.
pub(crate) fn tombstones_from(changes: &Changes, manifest: &Manifest) -> HashSet<SortKey> {
    let mut tombstones = HashSet::new();
    for (entity, attribute, change) in changes.iter() {
        if let dialog_artifacts::Change::Retract(value) = change {
            let artifact = Artifact {
                the: attribute.clone(),
                of: entity.clone(),
                is: value.clone(),
                cause: None,
            };
            tombstones.insert(sort_key(&artifact, manifest));
        }
    }
    tombstones
}

/// Wrap an artifact stream in a filter that drops any item whose
/// [`sort_key`] under `manifest` is in `tombstones` (which must be keyed
/// under the same manifest). No-op when the set is empty.
pub(crate) fn filter_tombstones<'a>(
    inner: ArtifactStream<'a>,
    tombstones: Arc<HashSet<SortKey>>,
    manifest: Manifest,
) -> ArtifactStream<'a> {
    if tombstones.is_empty() {
        return inner;
    }
    Box::pin(stream::unfold(
        (inner, tombstones),
        move |(mut inner, tombstones)| async move {
            loop {
                match inner.next().await {
                    None => return None,
                    Some(Err(e)) => return Some((Err::<ArtifactView, _>(e), (inner, tombstones))),
                    Some(Ok(view)) => {
                        // A scanned row's sort key comes straight from its
                        // stored key bytes, written under the tree's
                        // manifest; the tombstones were keyed under that
                        // same manifest (see `ArtifactView::sort_key`).
                        match view.sort_key(&manifest) {
                            Err(e) => return Some((Err(e), (inner, tombstones))),
                            Ok(key) => {
                                if tombstones.contains(&key) {
                                    continue;
                                }
                            }
                        }
                        return Some((Ok(view), (inner, tombstones)));
                    }
                }
            }
        },
    ))
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_artifacts::tree::ArtifactTree;
    use dialog_artifacts::{DialogArtifactsError, Entity, Instruction, Update as _, Value};
    use dialog_search_tree::{Cache, Delta};
    use dialog_storage::MemoryStorageBackend;

    fn artifact(of: &str, the: &str, is: &str) -> Artifact {
        Artifact {
            the: the.parse().expect("attribute"),
            of: of.parse().expect("entity"),
            is: Value::String(is.into()),
            cause: None,
        }
    }

    fn stream_of(items: Vec<Artifact>) -> ArtifactStream<'static> {
        Box::pin(stream::iter(
            items
                .into_iter()
                .map(|artifact| Ok::<_, DialogArtifactsError>(artifact.into())),
        ))
    }

    async fn collect(s: ArtifactStream<'_>) -> anyhow::Result<Vec<Artifact>> {
        Ok(s.collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|row| row.and_then(|view| view.to_owned()))
            .collect::<Result<_, _>>()?)
    }

    #[dialog_common::test]
    async fn it_yields_empty_stream_when_no_inputs() -> anyhow::Result<()> {
        let merged = merge_grouped(vec![], Manifest::default(), MergeKeys::Stored);
        let items = collect(merged).await?;
        assert!(items.is_empty());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_passes_single_stream_through_without_dedup() -> anyhow::Result<()> {
        // A single input is returned as-is — even if it has duplicates,
        // since branch / overlay scans are duplicate-free by
        // construction.
        let a = artifact("id:a", "test/name", "Alice");
        let merged = merge_grouped(
            vec![stream_of(vec![a.clone(), a.clone()])],
            Manifest::default(),
            MergeKeys::Stored,
        );
        let items = collect(merged).await?;
        assert_eq!(items.len(), 2);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_dedupes_identical_artifacts_across_streams() -> anyhow::Result<()> {
        // Same artifact from two streams collapses to one in the
        // merged output.
        let a = artifact("id:a", "test/name", "Alice");
        let merged = merge_grouped(
            vec![stream_of(vec![a.clone()]), stream_of(vec![a.clone()])],
            Manifest::default(),
            MergeKeys::Stored,
        );
        let items = collect(merged).await?;
        assert_eq!(items.len(), 1);
        Ok(())
    }

    #[dialog_common::test]
    fn it_extracts_tombstones_from_retracts_only() -> anyhow::Result<()> {
        let mut changes = Changes::new();
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        changes.associate(
            "test/name".parse()?,
            alice.clone(),
            Value::String("Alice".into()),
        );
        changes.dissociate(
            "test/name".parse()?,
            bob.clone(),
            Value::String("Bob".into()),
        );

        let tombstones = tombstones_from(&changes, &Manifest::default());
        assert_eq!(tombstones.len(), 1, "only the retract contributes");
        // The lone tombstone matches the retracted artifact.
        let retracted = artifact("id:bob", "test/name", "Bob");
        assert!(tombstones.contains(&sort_key(&retracted, &Manifest::default())));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_filters_matching_artifacts_via_tombstones() -> anyhow::Result<()> {
        let keep = artifact("id:a", "test/name", "Keep");
        let drop = artifact("id:b", "test/name", "Drop");
        let mut tombstones = HashSet::new();
        tombstones.insert(sort_key(&drop, &Manifest::default()));

        let filtered = filter_tombstones(
            stream_of(vec![keep.clone(), drop]),
            Arc::new(tombstones),
            Manifest::default(),
        );
        let items = collect(filtered).await?;
        assert_eq!(items, vec![keep]);
        Ok(())
    }

    /// A tree holding `facts`, written under `manifest`, and its store.
    async fn tree_under(
        manifest: Manifest,
        facts: Vec<Artifact>,
    ) -> anyhow::Result<(ArtifactTree, MemoryStorageBackend<[u8; 32], Vec<u8>>)> {
        use dialog_artifacts::tree::ArtifactTreeExt as _;
        use dialog_storage::StorageBackend as _;

        let mut store = MemoryStorageBackend::default();
        let mut delta = Delta::zero();
        let mut tree = ArtifactTree::empty_with_manifest(manifest, Cache::new());
        tree.apply(
            &mut store,
            &mut delta,
            stream::iter(facts.into_iter().map(Instruction::Assert)),
        )
        .await?;
        for (digest, buffer) in delta.flush() {
            store.set(*digest.as_bytes(), buffer.into_vec()).await?;
        }
        Ok((tree, store))
    }

    /// Lines written under different manifests carry different stored keys
    /// for the same fact (here one spills its value, the other inlines it),
    /// so their rows are keyed from fields: the shared fact comes out once,
    /// and a retract keyed under a line's own manifest hides it from that
    /// line.
    #[dialog_common::test]
    async fn it_merges_and_filters_lines_written_under_different_manifests() -> anyhow::Result<()> {
        use dialog_artifacts::ArtifactSelector;
        use dialog_artifacts::tree::{ArtifactTreeExt as _, spill_cache};

        let long = "x".repeat(100);
        let shared = artifact("id:a", "test/bio", &long);
        let other = artifact("id:b", "test/bio", "short");
        let small = Manifest {
            inline_n: 32,
            ..Manifest::default()
        };
        let (spilling, spilling_store) = tree_under(small, vec![shared.clone()]).await?;
        let (inlining, inlining_store) =
            tree_under(Manifest::default(), vec![shared.clone(), other.clone()]).await?;
        let selector = || ArtifactSelector::new().the("test/bio".parse().expect("attribute"));
        let scans = || -> Vec<ArtifactStream<'static>> {
            vec![
                Box::pin(
                    spilling
                        .clone()
                        .scan(spilling_store.clone(), spill_cache(), selector()),
                ),
                Box::pin(
                    inlining
                        .clone()
                        .scan(inlining_store.clone(), spill_cache(), selector()),
                ),
            ]
        };

        let merged = collect(merge_grouped(scans(), small, MergeKeys::Fields)).await?;
        assert_eq!(
            merged,
            vec![shared.clone(), other.clone()],
            "the shared fact comes out once"
        );

        let stored = collect(merge_grouped(scans(), small, MergeKeys::Stored)).await?;
        assert_eq!(
            stored.len(),
            3,
            "stored keys do not match across the formats"
        );

        let mut changes = Changes::new();
        changes.dissociate(shared.the.clone(), shared.of.clone(), shared.is.clone());
        let hidden = filter_tombstones(
            Box::pin(
                spilling
                    .clone()
                    .scan(spilling_store.clone(), spill_cache(), selector()),
            ),
            Arc::new(tombstones_from(&changes, &small)),
            small,
        );
        assert!(
            collect(hidden).await?.is_empty(),
            "a retract keyed under the line's format hides it"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_passes_stream_through_when_tombstones_are_empty() -> anyhow::Result<()> {
        let a = artifact("id:a", "test/name", "Alice");
        let b = artifact("id:b", "test/name", "Bob");
        let filtered = filter_tombstones(
            stream_of(vec![a.clone(), b.clone()]),
            Arc::new(HashSet::new()),
            Manifest::default(),
        );
        let items = collect(filtered).await?;
        assert_eq!(items, vec![a, b]);
        Ok(())
    }
}
