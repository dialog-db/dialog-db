//! Streaming merge + tombstone helpers for query-time source composition.
//!
//! Everything here works on `Stream<Item = Result<Artifact, _>>` —
//! [`ArtifactStream`]s — and is agnostic to where the streams came
//! from (a branch's tree scan, a [`Changes`] overlay, anything else
//! that implements `Provider<Select>`).
//!
//! - [`merge_grouped`] is the k-way merge that backs query-time
//!   union of multiple sources. It preserves the "as-if merged into a
//!   single physical tree" order via [`sort_key`](dialog_artifacts::sort_key)
//!   and dedupes identical `(the, of, is, cause)` artifacts within
//!   each `(the, of)` run.
//! - [`tombstones_from`] + [`filter_tombstones`] lift the shadowing set
//!   ([`Tombstones`]) out of a [`Changes`] overlay and apply it to a
//!   source stream as a filter — the mechanism that lets a
//!   [`Transaction::retract`](crate::repository::branch::Transaction::retract)
//!   suppress a fact in the underlying branch view, and a pending
//!   cardinality-one `Replace` shadow the priors it supersedes so
//!   mid-transaction reads see their own writes.

use std::collections::{HashMap, HashSet};

use dialog_artifacts::{Artifact, ArtifactStream, Cause, Changes, SortKey, sort_key};
use futures_util::{StreamExt, stream};

/// The canonical group key for artifacts traveling through a query stream.
///
/// Consumers — notably the cardinality-one sliding window in
/// [`AttributeQueryOnly::evaluate`](dialog_query::attribute::query::AttributeQuery) —
/// assume that artifacts sharing the same `(the, of)` pair arrive
/// consecutively. Anything that unions facts from multiple sources must
/// preserve that invariant; this helper produces the comparable key used
/// when grouping.
pub(crate) fn group_key(artifact: &Artifact) -> (Vec<u8>, Vec<u8>) {
    (
        artifact.the.key_bytes().to_vec(),
        artifact.of.key_bytes().to_vec(),
    )
}

/// Merge sorted artifact streams into one stream whose order matches
/// what a single physical prolly tree containing every input would
/// produce, deduplicating identical claims that appear in more than one
/// source.
///
/// Each input is assumed sorted by [`sort_key`] — true of branch
/// scans by construction (the prolly tree stores entries in that
/// order) and true of `Provider<Select> for Changes` by construction
/// (it sorts its materialized vec). Implemented as a streaming k-way
/// merge with peekable inputs.
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
/// When the same `(the, of, is, cause)` artifact appears in multiple
/// inputs, only the first occurrence within a `(the, of)` run is
/// yielded. The dedup region is the `(the, of)` group, tracked via
/// [`group_key`]; the fingerprint is `Cause::from(&artifact)` which
/// hashes all four fields so position-independent duplicates collapse.
pub(crate) fn merge_grouped<'a>(streams: Vec<ArtifactStream<'a>>) -> ArtifactStream<'a> {
    use std::pin::Pin;

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

    let mut peekable: Vec<_> = streams.into_iter().map(StreamExt::peekable).collect();

    Box::pin(async_stream::try_stream! {
        // Fingerprints already yielded within the current (the, of) run.
        // Cleared whenever the run advances to a new group_key.
        let mut current_key: Option<(Vec<u8>, Vec<u8>)> = None;
        let mut seen: HashSet<Cause> = HashSet::new();

        loop {
            let mut min_idx: Option<usize> = None;
            let mut min_sort: Option<SortKey> = None;
            for (i, s) in peekable.iter_mut().enumerate() {
                match Pin::new(s).peek().await {
                    None => continue,
                    Some(Err(_)) => {
                        min_idx = Some(i);
                        break;
                    }
                    Some(Ok(head)) => {
                        let sk = sort_key(head);
                        if min_sort.as_ref().is_none_or(|cur| &sk < cur) {
                            min_sort = Some(sk);
                            min_idx = Some(i);
                        }
                    }
                }
            }
            let Some(idx) = min_idx else { break };
            let item = peekable[idx]
                .next()
                .await
                .expect("peek returned Some, so next must too")?;

            let key = group_key(&item);
            if current_key.as_ref() != Some(&key) {
                current_key = Some(key);
                seen.clear();
            }
            // `Cause::from(&Artifact)` hashes (the, of, is, cause) — two
            // artifacts with identical fields produce identical
            // fingerprints.
            if seen.insert(Cause::from(&item)) {
                yield item;
            }
        }
    })
}

/// The set of branch facts an overlay's pending changes shadow at
/// query time, so a mid-transaction read agrees with post-commit state.
///
/// Two kinds of shadow, lifted from a [`Changes`] overlay by
/// [`tombstones_from`] and applied to a source stream by
/// [`filter_tombstones`]:
///
/// - **Retracts** remove one exact artifact — a `tx.retract(x)`
///   suppresses `x` in the branch view.
/// - **Replaces** shadow a whole `(the, of)` group *except* the
///   replacement value itself. A `Cardinality::One` typed assert emits
///   `Change::Replace`, which on commit supersedes **all**
///   different-valued priors at that `(the, of)`
///   (`tree.rs`'s supersession scan). Mid-transaction reads must match:
///   the committed prior is a different-value sibling and would
///   otherwise stream alongside the pending replacement, letting
///   [`choose`](dialog_query::attribute::query) pick the stale value by
///   content order. Shadowing the priors leaves exactly the replacement
///   value (surfaced by `Provider<Select> for Changes`) — read-your-writes.
///
/// Asserts (`Cardinality::Many`) contribute nothing: sibling
/// accumulation is their intended semantics.
#[derive(Default, Clone)]
pub(crate) struct Tombstones {
    /// Exact-artifact removals lifted from `Retract` changes — one
    /// [`SortKey`] each.
    retracts: HashSet<SortKey>,
    /// `(the, of)` groups carrying a pending `Replace`, each mapped to
    /// the [`SortKey`]s of its replacement value(s). A branch fact in
    /// one of these groups is shadowed unless its own `SortKey` is in
    /// the set — i.e. only different-value priors are suppressed; a
    /// branch fact already holding the replacement value survives (a
    /// same-value `Replace` is a no-op on commit).
    replaced_groups: HashMap<(Vec<u8>, Vec<u8>), HashSet<SortKey>>,
}

impl Tombstones {
    /// No pending change shadows anything — the filter is a pass-through.
    fn is_empty(&self) -> bool {
        self.retracts.is_empty() && self.replaced_groups.is_empty()
    }

    /// Whether an overlay change suppresses `artifact` when it appears
    /// in a branch source stream.
    fn shadows(&self, artifact: &Artifact) -> bool {
        let key = sort_key(artifact);
        if self.retracts.contains(&key) {
            return true;
        }
        // A pending `Replace` shadows every prior in its `(the, of)`
        // group except one carrying the replacement value itself.
        if let Some(replacement_keys) = self.replaced_groups.get(&(key.0.clone(), key.1.clone())) {
            return !replacement_keys.contains(&key);
        }
        false
    }
}

/// Lift the shadowing set out of a [`Changes`] overlay: an exact
/// [`SortKey`] per `Retract`, and a `(the, of)` group shadow per
/// `Replace`. Asserts contribute nothing. See [`Tombstones`] for the
/// read-your-writes rationale.
pub(crate) fn tombstones_from(changes: &Changes) -> Tombstones {
    let mut tombstones = Tombstones::default();
    for (entity, attribute, change) in changes.iter() {
        let (value, is_replace) = match change {
            dialog_artifacts::Change::Retract(value) => (value, false),
            dialog_artifacts::Change::Replace(value) => (value, true),
            // Asserts (cardinality-many) accumulate as siblings — no shadow.
            dialog_artifacts::Change::Assert(_) => continue,
        };
        let artifact = Artifact {
            the: attribute.clone(),
            of: entity.clone(),
            is: value.clone(),
            cause: None,
        };
        let key = sort_key(&artifact);
        if is_replace {
            tombstones
                .replaced_groups
                .entry(group_key(&artifact))
                .or_default()
                .insert(key);
        } else {
            tombstones.retracts.insert(key);
        }
    }
    tombstones
}

/// Wrap an artifact stream in a filter that drops any item shadowed by
/// `tombstones`. No-op when nothing is shadowed.
pub(crate) fn filter_tombstones<'a>(
    inner: ArtifactStream<'a>,
    tombstones: Tombstones,
) -> ArtifactStream<'a> {
    if tombstones.is_empty() {
        return inner;
    }
    Box::pin(stream::unfold(
        (inner, tombstones),
        |(mut inner, tombstones)| async move {
            loop {
                match inner.next().await {
                    None => return None,
                    Some(Err(e)) => return Some((Err::<Artifact, _>(e), (inner, tombstones))),
                    Some(Ok(artifact)) => {
                        if tombstones.shadows(&artifact) {
                            continue;
                        }
                        return Some((Ok(artifact), (inner, tombstones)));
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
    use dialog_artifacts::{DialogArtifactsError, Entity, Update as _, Value};

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
            items.into_iter().map(Ok::<_, DialogArtifactsError>),
        ))
    }

    async fn collect(s: ArtifactStream<'_>) -> anyhow::Result<Vec<Artifact>> {
        Ok(s.collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?)
    }

    #[dialog_common::test]
    async fn it_yields_empty_stream_when_no_inputs() -> anyhow::Result<()> {
        let merged = merge_grouped(vec![]);
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
        let merged = merge_grouped(vec![stream_of(vec![a.clone(), a.clone()])]);
        let items = collect(merged).await?;
        assert_eq!(items.len(), 2);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_dedupes_identical_artifacts_across_streams() -> anyhow::Result<()> {
        // Same artifact from two streams collapses to one in the
        // merged output.
        let a = artifact("id:a", "test/name", "Alice");
        let merged = merge_grouped(vec![stream_of(vec![a.clone()]), stream_of(vec![a.clone()])]);
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

        let tombstones = tombstones_from(&changes);
        assert_eq!(tombstones.retracts.len(), 1, "only the retract contributes");
        assert!(
            tombstones.replaced_groups.is_empty(),
            "an assert is not a replace"
        );
        // The lone tombstone matches the retracted artifact.
        let retracted = artifact("id:bob", "test/name", "Bob");
        assert!(tombstones.retracts.contains(&sort_key(&retracted)));
        Ok(())
    }

    #[dialog_common::test]
    fn it_lifts_a_group_shadow_from_a_replace() -> anyhow::Result<()> {
        let mut changes = Changes::new();
        let alice: Entity = "id:alice".parse()?;
        // `associate_unique` is the cardinality-one write — a `Replace`.
        changes.associate_unique(
            "test/name".parse()?,
            alice.clone(),
            Value::String("Alicia".into()),
        );

        let tombstones = tombstones_from(&changes);
        assert!(tombstones.retracts.is_empty(), "a replace is not a retract");
        // The group is shadowed, and only the replacement value survives it.
        let replacement = artifact("id:alice", "test/name", "Alicia");
        let superseded = artifact("id:alice", "test/name", "Alice");
        assert!(
            !tombstones.shadows(&replacement),
            "the replacement value itself is not shadowed"
        );
        assert!(
            tombstones.shadows(&superseded),
            "a different-value prior at the same (the, of) is shadowed"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_filters_matching_artifacts_via_tombstones() -> anyhow::Result<()> {
        let keep = artifact("id:a", "test/name", "Keep");
        let drop = artifact("id:b", "test/name", "Drop");
        let mut tombstones = Tombstones::default();
        tombstones.retracts.insert(sort_key(&drop));

        let filtered = filter_tombstones(stream_of(vec![keep.clone(), drop]), tombstones);
        let items = collect(filtered).await?;
        assert_eq!(items, vec![keep]);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_shadows_superseded_priors_but_keeps_the_replacement() -> anyhow::Result<()> {
        // A pending `Replace("Alicia")` at (test/name, id:a) shadows the
        // committed prior "Alice" streaming from the branch, but leaves a
        // same-value "Alicia" branch fact untouched — and an unrelated
        // (the, of) is never touched.
        let mut changes = Changes::new();
        let alice: Entity = "id:a".parse()?;
        changes.associate_unique("test/name".parse()?, alice, Value::String("Alicia".into()));
        let tombstones = tombstones_from(&changes);

        let superseded = artifact("id:a", "test/name", "Alice");
        let same_value = artifact("id:a", "test/name", "Alicia");
        let other_entity = artifact("id:b", "test/name", "Alice");
        let filtered = filter_tombstones(
            stream_of(vec![superseded, same_value.clone(), other_entity.clone()]),
            tombstones,
        );
        let items = collect(filtered).await?;
        assert_eq!(items, vec![same_value, other_entity]);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_passes_stream_through_when_tombstones_are_empty() -> anyhow::Result<()> {
        let a = artifact("id:a", "test/name", "Alice");
        let b = artifact("id:b", "test/name", "Bob");
        let filtered =
            filter_tombstones(stream_of(vec![a.clone(), b.clone()]), Tombstones::default());
        let items = collect(filtered).await?;
        assert_eq!(items, vec![a, b]);
        Ok(())
    }
}
