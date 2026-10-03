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
//! - [`tombstones_from`] lifts retract instructions out of a
//!   [`Changes`] overlay, and [`Hidden`] + [`filter_hidden`] apply them,
//!   with a transaction's staged retracts and the cells its replaces
//!   claimed, to a source stream as a filter — the mechanism that lets a
//!   [`Transaction::retract`](crate::repository::branch::Transaction::retract)
//!   suppress facts in the underlying branch view.

use std::collections::HashSet;
use std::sync::Arc;

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStream, ArtifactView, Cause, Changes, SortKey, sort_key,
};
use dialog_search_tree::Manifest;
use futures_util::{StreamExt, stream};

use crate::Cells;

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

/// What a query's upper layers hide in the streams beneath them: exact
/// facts (tombstones, by sort key) and whole `(attribute, entity)`
/// cells (claimed by a replace). Holds each layer's sets as shared,
/// rather than one merged copy, so building it never copies a set.
#[derive(Clone, Debug, Default)]
pub(crate) struct Hidden {
    facts: Vec<Arc<HashSet<SortKey>>>,
    cells: Vec<Arc<Cells>>,
}

impl Hidden {
    /// Also hide the facts in `tombstones`.
    pub(crate) fn facts(mut self, tombstones: Arc<HashSet<SortKey>>) -> Self {
        if !tombstones.is_empty() {
            self.facts.push(tombstones);
        }
        self
    }

    /// Also hide every fact in `cells`.
    pub(crate) fn cells(mut self, cells: Arc<Cells>) -> Self {
        if !cells.is_empty() {
            self.cells.push(cells);
        }
        self
    }

    /// Whether nothing is hidden.
    pub(crate) fn is_empty(&self) -> bool {
        self.facts.is_empty() && self.cells.is_empty()
    }

    /// What of this can hide a row `selector` matches: cells of another
    /// attribute cannot, so a scan that no replace touched is not
    /// filtered for cells at all.
    pub(crate) fn within(&self, selector: &ArtifactSelector<Constrained>) -> Self {
        let Some(attribute) = selector.attribute() else {
            return self.clone();
        };
        let the = attribute.as_str().as_bytes();
        Self {
            facts: self.facts.clone(),
            cells: self
                .cells
                .iter()
                .filter(|cells| cells.contains_key(the))
                .cloned()
                .collect(),
        }
    }

    /// Whether `view` is hidden. A claimed cell is checked on the row's
    /// attribute and entity bytes as stored; the sort key is derived
    /// only when an exact fact could match it.
    fn hides(
        &self,
        view: &ArtifactView,
        manifest: &Manifest,
    ) -> Result<bool, dialog_artifacts::DialogArtifactsError> {
        if !self.cells.is_empty() {
            let the = view.the_bytes()?;
            let mut claimed = self
                .cells
                .iter()
                .filter_map(|cells| cells.get(the.as_ref()))
                .peekable();
            if claimed.peek().is_some() {
                let of = view.of_bytes()?;
                if claimed.any(|entities| entities.contains(of.as_ref())) {
                    return Ok(true);
                }
            }
        }
        if self.facts.is_empty() {
            return Ok(false);
        }
        let key = view.sort_key(manifest)?;
        Ok(self.facts.iter().any(|facts| facts.contains(&key)))
    }
}

/// Wrap an artifact stream in a filter that drops any item `hidden`
/// hides, its sort key taken under `manifest` (which the hidden facts
/// must be keyed under too; cells key the same under every format).
/// No-op when nothing is hidden.
pub(crate) fn filter_hidden<'a>(
    inner: ArtifactStream<'a>,
    hidden: Hidden,
    manifest: Manifest,
) -> ArtifactStream<'a> {
    if hidden.is_empty() {
        return inner;
    }
    Box::pin(stream::unfold(
        (inner, hidden, manifest),
        |(mut inner, hidden, manifest)| async move {
            loop {
                match inner.next().await {
                    None => return None,
                    Some(Err(e)) => {
                        return Some((Err::<ArtifactView, _>(e), (inner, hidden, manifest)));
                    }
                    Some(Ok(view)) => {
                        // A scanned row's sort key comes straight from its
                        // stored key bytes, written under the tree's
                        // manifest; the tombstones were keyed under that
                        // same manifest (see `ArtifactView::sort_key`).
                        match hidden.hides(&view, &manifest) {
                            Err(e) => return Some((Err(e), (inner, hidden, manifest))),
                            Ok(true) => continue,
                            Ok(false) => {}
                        }
                        return Some((Ok(view), (inner, hidden, manifest)));
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
    use dialog_artifacts::ArchiveDelta;
    use dialog_artifacts::tree::ArtifactTree;
    use dialog_artifacts::{DialogArtifactsError, Entity, Instruction, Update as _, Value};
    use dialog_search_tree::{MemoryBlocks, NodeCache};

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

        let filtered = filter_hidden(
            stream_of(vec![keep.clone(), drop]),
            Hidden::default().facts(Arc::new(tombstones)),
            Manifest::default(),
        );
        let items = collect(filtered).await?;
        assert_eq!(items, vec![keep]);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_filters_every_value_of_a_claimed_cell() -> anyhow::Result<()> {
        let keep = artifact("id:a", "test/name", "Keep");
        let first = artifact("id:b", "test/name", "One");
        let second = artifact("id:b", "test/name", "Two");
        let other = artifact("id:b", "test/age", "Kept");
        let mut cells = Cells::new();
        cells
            .entry(b"test/name".to_vec())
            .or_default()
            .insert(b"id:b".to_vec());

        let filtered = filter_hidden(
            stream_of(vec![keep.clone(), first, second, other.clone()]),
            Hidden::default().cells(Arc::new(cells)),
            Manifest::default(),
        );
        let items = collect(filtered).await?;
        assert_eq!(items, vec![keep, other]);
        Ok(())
    }

    /// A tree holding `facts`, written under `manifest`, and its store.
    async fn tree_under(
        manifest: Manifest,
        facts: Vec<Artifact>,
    ) -> anyhow::Result<(ArtifactTree, MemoryBlocks)> {
        use dialog_artifacts::tree::ArtifactTreeExt as _;

        let store = MemoryBlocks::new();
        let mut delta = ArchiveDelta::zero();
        let mut tree = ArtifactTree::empty_with_manifest(manifest, NodeCache::new());
        tree.apply(
            &store,
            &mut delta,
            stream::iter(facts.into_iter().map(Instruction::Assert)),
        )
        .await?;
        delta.flush_into(&store);
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
        let (spilling, spilling_store) = tree_under(small.clone(), vec![shared.clone()]).await?;
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

        let merged = collect(merge_grouped(scans(), small.clone(), MergeKeys::Fields)).await?;
        assert_eq!(
            merged,
            vec![shared.clone(), other.clone()],
            "the shared fact comes out once"
        );

        let stored = collect(merge_grouped(scans(), small.clone(), MergeKeys::Stored)).await?;
        assert_eq!(
            stored.len(),
            3,
            "stored keys do not match across the formats"
        );

        let mut changes = Changes::new();
        changes.dissociate(shared.the.clone(), shared.of.clone(), shared.is.clone());
        let hidden = filter_hidden(
            Box::pin(
                spilling
                    .clone()
                    .scan(spilling_store.clone(), spill_cache(), selector()),
            ),
            Hidden::default().facts(Arc::new(tombstones_from(&changes, &small))),
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
        let filtered = filter_hidden(
            stream_of(vec![a.clone(), b.clone()]),
            Hidden::default().facts(Arc::new(HashSet::new())),
            Manifest::default(),
        );
        let items = collect(filtered).await?;
        assert_eq!(items, vec![a, b]);
        Ok(())
    }
}
