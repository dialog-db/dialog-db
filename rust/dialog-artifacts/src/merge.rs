//! Observed-remove screening for merge differentials.
//!
//! A pull merges by integrating the *upstream's* changes since the sync
//! base onto the *local* tree. Raw tree integration alone cannot express
//! deletion semantics — the active indexes carry no tombstones — so the
//! incoming differential is screened before integration (see
//! `notes/observed-remove-merge.md`):
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

use dialog_common::Blake3Hash;
use dialog_search_tree::{
    Change, ContentAddressedStorage, Differential, DialogSearchTreeError, Entry,
};
use dialog_storage::{DialogStorageError, StorageBackend};

use crate::history::Context;
use crate::key::KEY_LENGTH;
use crate::tree::ArtifactTree;
use crate::{
    Artifact, Attribute, AttributeKey, BLOB_KEY_TAG, Datum, ENTITY_KEY_TAG, Entity, EntityKey,
    FromKey as _, HISTORY_KEY_TAG, KeyBytes, State, VALUE_KEY_TAG, Value, ValueDataType, ValueKey,
};

/// The full key span of one region tag.
fn tag_span(tag: u8) -> RangeInclusive<KeyBytes> {
    let mut lo = [u8::MIN; KEY_LENGTH];
    let mut hi = [u8::MAX; KEY_LENGTH];
    lo[0] = tag;
    hi[0] = tag;
    lo..=hi
}

/// The history region's key range, for scoping the first merge pass.
pub fn history_scope() -> [RangeInclusive<KeyBytes>; 1] {
    [tag_span(HISTORY_KEY_TAG)]
}

/// The data regions' key ranges (EAV/AEV/VAE and the blob index), for
/// scoping the second merge pass.
pub fn data_scope() -> [RangeInclusive<KeyBytes>; 2] {
    let mut lo = [u8::MIN; KEY_LENGTH];
    let mut hi = [u8::MAX; KEY_LENGTH];
    lo[0] = ENTITY_KEY_TAG;
    hi[0] = VALUE_KEY_TAG;
    [lo..=hi, tag_span(BLOB_KEY_TAG)]
}

/// Rebuild the three data-region keys for the fact a history record
/// speaks about, from the record's stored datum.
fn data_keys(datum: &Datum) -> Result<[KeyBytes; 3], DialogSearchTreeError> {
    let decode = |e: crate::DialogArtifactsError| {
        DialogSearchTreeError::Node(format!("history record: {e}"))
    };
    let artifact = Artifact {
        the: Attribute::from_str(&datum.attribute).map_err(decode)?,
        of: Entity::from_str(&datum.entity).map_err(decode)?,
        is: Value::try_from((ValueDataType::from(datum.value_type), datum.value.clone()))
            .map_err(decode)?,
        cause: None,
    };
    let entity_key = EntityKey::from(&artifact);
    let value_key = ValueKey::from_key(&entity_key);
    let attribute_key = AttributeKey::from_key(&entity_key);
    Ok([
        KeyBytes::from(entity_key.into_key()),
        KeyBytes::from(attribute_key.into_key()),
        KeyBytes::from(value_key.into_key()),
    ])
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
) -> impl Differential<KeyBytes, State<Datum>> + 'a
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + Clone
        + dialog_common::ConditionalSync
        + 'a,
    C: Differential<KeyBytes, State<Datum>> + 'a,
{
    async_stream::try_stream! {
        futures_util::pin_mut!(changes);
        while let Some(change) = futures_util::StreamExt::next(&mut changes).await {
            match change? {
                Change::Add(entry) => {
                    let covering = match &entry.value {
                        State::Added(datum)
                            if datum.retraction || !datum.supersedes.is_empty() =>
                        {
                            Some(datum.clone())
                        }
                        _ => None,
                    };
                    yield Change::Add(entry);

                    // R3: the record covers claims — retire any still
                    // live locally. The emitted removes are guarded
                    // (byte-exact), so a claim the record never observed
                    // (e.g. a later re-assert standing at the same key)
                    // is untouched.
                    if let Some(record) = covering {
                        for data_key in data_keys(&record)? {
                            let standing = local.get(&data_key, &storage).await?;
                            if let Some(State::Added(datum)) = standing
                                && let Some(version) = datum.version
                                && record.supersedes.contains(&version)
                            {
                                yield Change::Remove(Entry {
                                    key: data_key,
                                    value: State::Added(datum),
                                });
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
) -> impl Differential<KeyBytes, State<Datum>> + 'a
where
    C: Differential<KeyBytes, State<Datum>> + 'a,
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
