//! Observed-remove screening for merge differentials.
//!
//! A pull merges by integrating the *upstream's* changes since the sync
//! base onto the *local* tree. Raw tree integration alone cannot express
//! deletion semantics — the active indexes carry no tombstones — so the
//! incoming differential is screened before integration (see
//! `notes/version-control.md`):
//!
//! - **R1** — an incoming live claim the receiver has *observed* (its
//!   producing revision is in the local head's ancestry) is never
//!   re-applied: if the local cache still holds it, there is nothing to
//!   do; if it no longer does, some record in the local log covered it,
//!   and applying it would resurrect a deletion. Unobserved claims are
//!   news and pass through (a same-key contest between two unobserved
//!   byte-variants of the same fact falls to the tree's deterministic
//!   hash race — both assert the same value).
//! - **R2** — incoming removes pass through untouched; the tree's
//!   byte-guarded remove (only delete what matches exactly) is already
//!   the correct observed-remove rule.
//! - **R3** — incoming history records that *cover* claims (a
//!   retraction's `cause`, a replace's `supersedes`) are applied to the
//!   local live set: for each covered version still live locally, a
//!   guarded remove is emitted. This is how a deletion reaches a replica
//!   whose sync base never covered the fact (e.g. an empty-base pull),
//!   with no tombstone anywhere.
//!
//! # Two passes, in this order
//!
//! Coverage must land before data: an incoming re-assert and the
//! retraction it supersedes can arrive in one delta, and if the data
//! change were integrated first it would contest a slot that R3 is
//! about to clear — letting an arbitrary race decide what causality
//! already decided. The merge therefore integrates the **history
//! region first** ([`screen_history`]: record appends + R3 removes
//! against the pre-merge snapshot), then the **data regions**
//! ([`screen_data`]: R1). Region scoping rides the key tags: history
//! keys sort under [`HISTORY_KEY_TAG`], data under the
//! entity/attribute/value (and blob) tags.
//!
//! Every rule is O(1) per changed key, and the screen reads only the
//! receiver's own snapshot and context — nothing about the sender's
//! state beyond the differential itself.

use core::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use dialog_common::Blake3Hash;
use dialog_search_tree::{
    Change, ContentAddressedStorage, DialogSearchTreeError, Differential, Entry,
};
use dialog_storage::{DialogStorageError, StorageBackend};

use crate::history::{Context, REVISION_ATTRIBUTE, RevisionRecord};
use crate::tree::ArtifactTree;
use crate::{
    Attribute, AttributeKey, AttributeKeyPart, BLOB_KEY_TAG, COVERAGE_KEY_TAG, Datum,
    ENTITY_KEY_TAG, Entity, EntityKey, EntityKeyPart, FromKey as _, HISTORY_KEY_TAG, Key,
    KeyViewConstruct, KeyViewMut as _, State, VALUE_KEY_TAG, ValueKey,
};

/// The full key span of one region tag.
fn tag_span(tag: u8) -> RangeInclusive<Key> {
    // Variable-length keys: every key under `tag` begins with that byte, so
    // the region runs from the bare tag to the tag followed by a run of
    // `0xFF`. `KEY_SPAN_FILLER` bytes of it exceeds any real key's length,
    // and a longer key with the same prefix still sorts below it.
    let lo = vec![tag];
    let mut hi = vec![tag];
    hi.extend(std::iter::repeat_n(u8::MAX, KEY_SPAN_FILLER));
    Key::from(lo)..=Key::from(hi)
}

/// Filler length for an open-ended key span bound. A key never reaches this
/// many trailing `0xFF` bytes, so a bound built this way is above every key
/// sharing its prefix.
const KEY_SPAN_FILLER: usize = 64;

/// The bottom of the key space: the empty key sorts below every other.
fn bottom_key() -> Key {
    Key::from(Vec::new())
}

/// The top of the key space.
fn top_key() -> Key {
    Key::from(vec![u8::MAX; KEY_SPAN_FILLER])
}

/// The history-side key ranges for the first merge pass: the history
/// region itself plus the coverage region that mirrors its covering
/// records (compact, value-free entries whose only purpose is to make
/// "every deletion or replacement since the sync base" enumerable as a
/// scoped diff, without streaming the value-bearing assert records).
/// Coverage entries screen like any append-only records; the R3 slot
/// scans fire from the history records, so the mirror never doubles the
/// coverage work.
pub fn history_scope() -> [RangeInclusive<Key>; 2] {
    [tag_span(HISTORY_KEY_TAG), tag_span(COVERAGE_KEY_TAG)]
}

/// The entire key space as one range, for computing an unscoped tree
/// difference through the scoped API.
pub fn full_scope() -> [RangeInclusive<Key>; 1] {
    [bottom_key()..=top_key()]
}

/// The coverage region's key range: the compact mirror of covering
/// records, enumerated by graft repair.
pub fn coverage_scope() -> [RangeInclusive<Key>; 1] {
    [tag_span(COVERAGE_KEY_TAG)]
}

/// Convert conservative divergence bounds (as reported by
/// [`TreeDifference::divergent_bounds`](dialog_search_tree::TreeDifference::divergent_bounds))
/// into inclusive spans: an absent lower bound starts at the bottom of
/// the key space, and an exclusive lower bound becomes the successor
/// key.
pub fn spans_from_bounds(bounds: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> Vec<RangeInclusive<Key>> {
    bounds
        .into_iter()
        .filter_map(|(lower, upper)| {
            // `lower` is the frontier node's separator: an INCLUSIVE lower
            // bound (the smallest key the node can hold). `upper` is the next
            // node's separator, which sorts strictly above this node's maximum
            // key, so the span ends just below it. The last node has no
            // successor and runs to the top of the key space.
            let start = Key::from(lower);
            let end = match upper {
                Some(next) => predecessor_of(&Key::from(next))?,
                None => top_key(),
            };
            (start <= end).then_some(start..=end)
        })
        .collect()
}

/// The data regions' key ranges (EAV/AEV/VAE and the blob index), for
/// scoping the second merge pass.
pub fn data_scope() -> [RangeInclusive<Key>; 2] {
    let lo = vec![ENTITY_KEY_TAG];
    let mut hi = vec![VALUE_KEY_TAG];
    hi.extend(std::iter::repeat_n(u8::MAX, KEY_SPAN_FILLER));
    [Key::from(lo)..=Key::from(hi), tag_span(BLOB_KEY_TAG)]
}

/// The entity-ordered key span of the `(entity, attribute)` slot a
/// history record speaks about — the range R3 scans for covered claims.
///
/// Coverage matches claims by *version*, not by the record's own value:
/// a replace record supersedes claims of **other** values, and data keys
/// embed the value hash, so the covered claims live at different keys
/// than the record's. The whole slot must be scanned.
pub fn coverage_range(key: &Key) -> Result<RangeInclusive<Key>, DialogSearchTreeError> {
    let decode = |e: crate::DialogArtifactsError| {
        DialogSearchTreeError::Node(format!("history record: {e}"))
    };
    // The record's entity and attribute live in its key, not its payload.
    let parts = crate::key::varkey::parse_key(key.as_ref()).ok_or_else(|| {
        DialogSearchTreeError::Node("history key did not parse".to_string())
    })?;
    let of = Entity::from_str(
        std::str::from_utf8(&parts.entity)
            .map_err(|e| DialogSearchTreeError::Node(format!("entity is not UTF-8: {e}")))?,
    )
    .map_err(decode)?;
    let the = Attribute::from_str(
        std::str::from_utf8(&parts.attribute)
            .map_err(|e| DialogSearchTreeError::Node(format!("attribute is not UTF-8: {e}")))?,
    )
    .map_err(decode)?;
    let start = <EntityKey<Key> as KeyViewConstruct>::min()
        .set_entity(EntityKeyPart::from(&of))
        .set_attribute(AttributeKeyPart::from(&the))
        .into_key();
    let end = <EntityKey<Key> as KeyViewConstruct>::max()
        .set_entity(EntityKeyPart::from(&of))
        .set_attribute(AttributeKeyPart::from(&the))
        .into_key();
    Ok(Key::from(start)..=Key::from(end))
}

/// Screen the **history-region** slice of an incoming merge
/// differential: every record appends (history keys are unique and
/// immutable — never contested), and each *covering* record (a
/// retraction, or a replace with a non-empty supersedes set) emits
/// guarded removes for the covered claims still live in the receiver's
/// snapshot (R3). Run — and integrate — before the data pass.
pub fn screen_history<'a, Backend, C>(
    changes: C,
    local: ArtifactTree,
    storage: ContentAddressedStorage<Backend>,
) -> impl Differential<Key, State<Datum>> + 'a
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + Clone
        + dialog_common::ConditionalSync
        + 'a,
    C: Differential<Key, State<Datum>> + 'a,
{
    async_stream::try_stream! {
        futures_util::pin_mut!(changes);
        while let Some(change) = futures_util::StreamExt::next(&mut changes).await {
            match change? {
                Change::Add(entry) => {
                    // A record covers claims iff its supersedes set is
                    // non-empty (a retraction's cause and a replace's
                    // superseded priors both land there — see
                    // `Record::into_entry`). A genesis retraction covers
                    // nothing and needs no scan.
                    // The slot a record covers is named by its KEY (entity
                    // and attribute live there now), so carry the key.
                    let covering = match &entry.value {
                        State::Added(datum)
                            if entry.key.as_ref().first() == Some(&HISTORY_KEY_TAG)
                                && !datum.supersedes.is_empty() =>
                        {
                            Some((entry.key.clone(), datum.supersedes.clone()))
                        }
                        _ => None,
                    };
                    yield Change::Add(entry);

                    // R3: the record covers claims — retire any still
                    // live locally in the record's (entity, attribute)
                    // slot. Covered claims are matched by version: a
                    // replace supersedes claims of *other* values, which
                    // live at other keys (keys embed the value hash), so
                    // the scan walks the whole slot rather than probing
                    // the record's own keys. The emitted removes are
                    // guarded (byte-exact), so a claim the record never
                    // observed (e.g. a later re-assert standing at the
                    // same key) is untouched.
                    if let Some((record_key, superseded)) = covering {
                        let candidates =
                            local.stream_range(coverage_range(&record_key)?, &storage);
                        futures_util::pin_mut!(candidates);
                        while let Some(candidate) =
                            futures_util::StreamExt::next(&mut candidates).await
                        {
                            let candidate = candidate?;
                            let covered = match &candidate.value {
                                State::Added(datum) => datum
                                    .version
                                    .is_some_and(|version| superseded.contains(&version)),
                                _ => false,
                            };
                            if covered {
                                let entity_key = EntityKey(Key::from(candidate.key));
                                let attribute_key = AttributeKey::from_key(&entity_key);
                                let value_key = ValueKey::from_key(&entity_key);
                                for key in [
                                    entity_key.into_key(),
                                    attribute_key.into_key(),
                                    value_key.into_key(),
                                ] {
                                    yield Change::Remove(Entry {
                                        key: Key::from(key),
                                        value: candidate.value.clone(),
                                    });
                                }
                            }
                        }
                    }
                }
                // A history key vanishing from the upstream would mean
                // history was rewritten; the guarded remove makes it a
                // no-op unless our copy matches theirs byte for byte.
                remove @ Change::Remove(_) => yield remove,
            }
        }
    }
}

/// Screen the **data-region** slice of an incoming merge differential
/// by the receiver's causal context (R1); incoming guarded removes pass
/// through (R2). Run — and integrate — after [`screen_history`]'s pass,
/// so coverage has already retired what causality decided.
pub fn screen_data<'a, C>(
    changes: C,
    context: Context,
) -> impl Differential<Key, State<Datum>> + 'a
where
    C: Differential<Key, State<Datum>> + 'a,
{
    async_stream::try_stream! {
        futures_util::pin_mut!(changes);
        while let Some(change) = futures_util::StreamExt::next(&mut changes).await {
            match change? {
                Change::Add(entry) => match &entry.value {
                    // Legacy tombstones from pre-observed-remove trees
                    // never propagate: deletion travels as history now.
                    State::Removed => continue,
                    State::Added(datum) => {
                        // R1: a claim whose revision is already in the
                        // local ancestry is never news — either it is
                        // still live locally (nothing to do) or some
                        // local record covered it (re-applying it would
                        // resurrect a deletion). Claims without version
                        // tags (unversioned writes) cannot be reasoned
                        // about and pass through.
                        let observed = datum
                            .version
                            .map(|version| context.observes(&version))
                            .unwrap_or(false);
                        if observed {
                            continue;
                        }
                        yield Change::Add(entry);
                    }
                },
                // R2: guarded removes pass through; integrate applies
                // them only when the local bytes match exactly.
                remove @ Change::Remove(_) => yield remove,
            }
        }
    }
}

/// Wrap a data-region differential, folding the version of every
/// revision record riding it into `observed`.
///
/// The revision records in an upstream delta are exactly the
/// upstream-ancestry revisions the receiver may lack: a tree's records
/// are a subset of its head's ancestry, and records at or below the
/// sync base arrived with the pulls that established it. So `local
/// context + observed` is the context of a head that adopts or merges
/// this upstream — derived while the differential streams anyway, at
/// zero extra reads, in place of the O(ancestry) `context_of` walk.
///
/// Versions are derived from record *contents* (`RevisionRecord::version`,
/// the same derivation the read-side check binds records with), not
/// trusted from the datum's version tag. A record that fails to decode
/// fails the merge — the same strictness the durable history reader
/// applies.
pub fn observe_revisions<'a, C>(
    changes: C,
    observed: Arc<Mutex<Context>>,
) -> impl Differential<Key, State<Datum>> + 'a
where
    C: Differential<Key, State<Datum>> + 'a,
{
    async_stream::try_stream! {
        futures_util::pin_mut!(changes);
        while let Some(change) = futures_util::StreamExt::next(&mut changes).await {
            let change = change?;
            // The attribute and the value both live in the key now, so the
            // revision record is recognised and decoded from the key rather
            // than from the payload.
            if let Change::Add(entry) = &change
                && let State::Added(_) = &entry.value
                && let Some(parts) = crate::key::varkey::parse_key_ref(entry.key.as_ref())
                && parts.attribute.as_ref() == REVISION_ATTRIBUTE.as_bytes()
            {
                let bytes = match &parts.value {
                    crate::key::varkey::ValueRef::Inline(inline) => inline.to_vec(),
                    crate::key::varkey::ValueRef::Reference(_) => continue,
                };
                let record = RevisionRecord::try_from_bytes(&bytes).map_err(|error| {
                    DialogSearchTreeError::Node(format!("revision record: {error}"))
                })?;
                observed
                    .lock()
                    .expect("the revision observer mutex is never poisoned")
                    .record(record.version());
            }
            yield change;
        }
    }
}

/// Which source a key span of a three-way merge is taken from.
///
/// The partition below classifies the whole key space against the two
/// sides' divergence spans (the key ranges where each side's tree
/// differs from the shared base). Spans only one side changed are
/// adopted from that side wholesale; spans both sides changed need the
/// screened merge; spans neither side changed are identical in all
/// three trees, so either side serves (the partition says `Theirs` so
/// unchanged space fuses with adopted upstream spans).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpanSource {
    /// Only our side diverged from the base here: take our subtree.
    Ours,
    /// Only the upstream diverged, or neither did: take its subtree.
    Theirs,
    /// Both sides diverged: the span needs the screened merge.
    Contested,
}

/// The immediate successor of a key, or `None` at the top of the key
/// space.
fn key_successor(key: &Key) -> Option<Key> {
    // On variable-length keys the successor is just the key with a `0x00`
    // appended: nothing sorts strictly between the two, and unlike the
    // fixed-width form there is no carry to overflow.
    let mut next = key.as_ref().to_vec();
    next.push(u8::MIN);
    Some(Key::from(next))
}

/// Sort and coalesce a list of inclusive spans: overlapping or adjacent
/// spans fuse into one.
fn normalize_spans(spans: &[RangeInclusive<Key>]) -> Vec<RangeInclusive<Key>> {
    let mut spans: Vec<_> = spans.to_vec();
    spans.sort_by(|a, b| a.start().cmp(b.start()));
    let mut normalized: Vec<RangeInclusive<Key>> = Vec::with_capacity(spans.len());
    for span in spans {
        match normalized.last_mut() {
            Some(last)
                if *span.start()
                    <= key_successor(last.end()).unwrap_or_else(|| last.end().clone()) =>
            {
                if span.end() > last.end() {
                    *last = last.start().clone()..=span.end().clone();
                }
            }
            _ => normalized.push(span),
        }
    }
    normalized
}

/// Partition the entire key space by the two sides' divergence spans,
/// producing an ordered, gap-free, non-overlapping run of
/// `(span, source)` pieces with adjacent same-source pieces coalesced.
///
/// The inputs are conservative divergence spans (a span that contains
/// no real change is harmless: it only shifts work between the adopt
/// and screen paths, never the outcome), typically derived from the
/// divergent node bounds of two tree differentials.
pub fn partition_spans(
    ours: &[RangeInclusive<Key>],
    theirs: &[RangeInclusive<Key>],
) -> Vec<(RangeInclusive<Key>, SpanSource)> {
    let ours = normalize_spans(ours);
    let theirs = normalize_spans(theirs);

    let mut pieces: Vec<(RangeInclusive<Key>, SpanSource)> = Vec::new();
    let mut cursor = bottom_key();
    let mut ours_at = 0;
    let mut theirs_at = 0;

    loop {
        // Advance past spans that end before the cursor.
        while ours_at < ours.len() && ours[ours_at].end() < &cursor {
            ours_at += 1;
        }
        while theirs_at < theirs.len() && theirs[theirs_at].end() < &cursor {
            theirs_at += 1;
        }

        let in_ours = ours_at < ours.len() && ours[ours_at].start() <= &cursor;
        let in_theirs = theirs_at < theirs.len() && theirs[theirs_at].start() <= &cursor;
        let source = match (in_ours, in_theirs) {
            (true, true) => SpanSource::Contested,
            (true, false) => SpanSource::Ours,
            _ => SpanSource::Theirs,
        };

        // The current classification holds until the nearest boundary:
        // the end of a span the cursor is inside, or the start of the
        // next span ahead.
        let mut until = top_key();
        if in_ours {
            until = until.min(ours[ours_at].end().clone());
        } else if ours_at < ours.len() {
            if let Some(before) = predecessor_of(ours[ours_at].start()) {
                until = until.min(before);
            }
        }
        if in_theirs {
            until = until.min(theirs[theirs_at].end().clone());
        } else if theirs_at < theirs.len() {
            if let Some(before) = predecessor_of(theirs[theirs_at].start()) {
                until = until.min(before);
            }
        }

        let done = until >= top_key();
        match pieces.last_mut() {
            Some((span, last_source)) if *last_source == source => {
                *span = span.start().clone()..=until.clone();
            }
            _ => pieces.push((cursor.clone()..=until.clone(), source)),
        }

        if done {
            break;
        }
        match key_successor(&until) {
            Some(next) => cursor = next,
            None => break,
        }
    }

    pieces
}

/// The immediate predecessor of a key. Only called for span starts that
/// lie strictly ahead of the cursor, which is at least the minimum key,
/// so the input is never the minimum.
fn predecessor_of(key: &Key) -> Option<Key> {
    // The greatest key strictly below `key`. A key ending in `0x00` loses that
    // byte (that key is itself the successor of the shorter one); otherwise
    // decrement the last byte and pad with `0xFF`, which is above every key
    // sharing the decremented prefix. The empty key has no predecessor.
    let bytes = key.as_ref();
    let (&last, head) = bytes.split_last()?;
    let mut previous = head.to_vec();
    if last != u8::MIN {
        previous.push(last - 1);
        previous.extend(std::iter::repeat_n(u8::MAX, KEY_SPAN_FILLER));
    }
    Some(Key::from(previous))
}

#[cfg(test)]
mod span_tests {
    use super::*;

    fn key(at: u8) -> Key {
        Key::from(vec![at])
    }

    fn top() -> Key {
        top_key()
    }

    fn bottom() -> Key {
        bottom_key()
    }

    #[test]
    fn it_defaults_the_whole_space_to_theirs() {
        let pieces = partition_spans(&[], &[]);
        assert_eq!(pieces, vec![(bottom()..=top(), SpanSource::Theirs)]);
    }

    #[test]
    fn it_carves_our_spans_out_of_their_space() {
        let pieces = partition_spans(&[key(2)..=key(3)], &[]);
        assert_eq!(
            pieces,
            vec![
                (bottom()..=predecessor_of(&key(2)), SpanSource::Theirs),
                (key(2)..=key(3), SpanSource::Ours),
                (key_successor(&key(3)).unwrap()..=top(), SpanSource::Theirs),
            ]
        );
    }

    #[test]
    fn it_marks_overlap_contested_and_splits_the_rest() {
        let pieces = partition_spans(&[key(2)..=key(5)], &[key(4)..=key(8)]);
        assert_eq!(
            pieces,
            vec![
                (bottom()..=predecessor_of(&key(2)), SpanSource::Theirs),
                (key(2)..=predecessor_of(&key(4)), SpanSource::Ours),
                (key(4)..=key(5), SpanSource::Contested),
                (key_successor(&key(5)).unwrap()..=top(), SpanSource::Theirs),
            ]
        );
    }

    #[test]
    fn it_coalesces_their_spans_with_unchanged_space() {
        // A theirs-divergence span fuses with the surrounding unchanged
        // space into one piece.
        let pieces = partition_spans(&[key(6)..=key(6)], &[key(1)..=key(2)]);
        assert_eq!(
            pieces,
            vec![
                (bottom()..=predecessor_of(&key(6)), SpanSource::Theirs),
                (key(6)..=key(6), SpanSource::Ours),
                (key_successor(&key(6)).unwrap()..=top(), SpanSource::Theirs),
            ]
        );
    }

    #[test]
    fn it_normalizes_unsorted_and_overlapping_inputs() {
        let pieces = partition_spans(&[key(5)..=key(6), key(2)..=key(4), key(4)..=key(5)], &[]);
        assert_eq!(
            pieces,
            vec![
                (bottom()..=predecessor_of(&key(2)), SpanSource::Theirs),
                (key(2)..=key(6), SpanSource::Ours),
                (key_successor(&key(6)).unwrap()..=top(), SpanSource::Theirs),
            ]
        );
    }

    #[test]
    fn it_partitions_gap_free_and_in_order() {
        let pieces = partition_spans(
            &[key(1)..=key(3), key(10)..=key(20)],
            &[key(2)..=key(12), key(30)..=key(40)],
        );
        // Gap-free coverage in ascending order, alternating sources.
        let mut cursor = bottom();
        for (span, _) in &pieces {
            assert_eq!(*span.start(), cursor, "no gaps between pieces");
            cursor = key_successor(span.end()).unwrap_or(top());
        }
        assert_eq!(*pieces.last().unwrap().0.end(), top());
        // No two adjacent pieces share a source.
        for window in pieces.windows(2) {
            assert_ne!(window[0].1, window[1].1, "adjacent pieces coalesce");
        }
    }
}
