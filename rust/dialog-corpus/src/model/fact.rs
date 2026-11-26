//! Fact data structure representing semantic triples.
//!
//! This module defines the core [`Fact`] type which represents a semantic triple
//! (subject-predicate-object) in the Dialog database. Facts are the fundamental
//! units of data storage and retrieval.

use std::{
    borrow::Cow,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    str::{FromStr, from_utf8, from_utf8_unchecked},
};

use dialog_common::ConditionalSend;
use dialog_search_tree::KeyHandle;
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    ATTRIBUTE_KEY_TAG, AttributeKey, Datum, DialogCorpusError, ENTITY_KEY_TAG, EntityKey, Key,
    KeyView, VALUE_KEY_TAG, ValueKey, decode_value,
    history::{Edition, Version},
    key::varkey::{self, KeyLayout, KeyRef, ValueRef},
    make_reference,
};

use super::{Cause, Entity, Relation, Value};
use crate::{Uri, ValueDataType};

/// A [`Fact`] embodies a datum - a semantic triple - that may be stored in or
/// retrieved from a [`Corpus`].
#[derive(Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Fact {
    /// The [`Relation`] of the [`Fact`]; the predicate of the triple
    pub the: Relation,
    /// The [`Entity`] of the [`Fact`]; the subject of the triple
    #[serde(
        serialize_with = "dialog_capability::identity::to_utf8",
        deserialize_with = "dialog_capability::identity::from_utf8"
    )]
    pub of: Entity,
    /// The [`Value`] of the [`Fact`]; the object of the triple
    // TODO: This is in support of Facts<->CSV but we probably want
    // different (de)serialization for Facts<->JSON (assuming we ever
    // want that.
    #[serde(
        serialize_with = "crate::model::value::to_utf8",
        deserialize_with = "crate::model::value::from_utf8"
    )]
    pub is: Value,
    /// The [`Cause`] of the [`Fact`], which is a reference to an ancester
    /// version with a different [`Value`].
    pub cause: Option<Cause>,
}

impl Debug for Fact {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Fact")
            .field("the", &self.the.to_string())
            .field("of", &self.of.to_string())
            .field("is", &self.is)
            .field("cause", &self.cause.as_ref().map(|cause| cause.to_string()))
            .finish()
    }
}

impl Display for Fact {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let attribute = self.the.to_string();
        let entity = self.of.to_string();
        let value = self.is.to_utf8();

        write!(f, "Fact: the '{attribute}' of '{entity}' is '{value}'")
    }
}

impl Fact {
    /// Reconstructs a fact from an index `key` and its stored [`Datum`] payload,
    /// for a key whose value is stored INLINE.
    ///
    /// The entity, attribute, value type, and value all come from the key
    /// (stored losslessly and order-preservingly by [`EntityKey::from`] and
    /// friends). This is a convenience over
    /// [`Fact::from_key_datum_with_value`] with no spilled bytes; if the key
    /// is spilled (`key.value_is_spilled()`) it errors, because the raw value
    /// bytes live in a separate archive block the caller must fetch. Use
    /// [`Fact::from_key_datum_with_value`] for the general case.
    pub fn from_key_datum(key: &Key, datum: &Datum) -> Result<Self, DialogCorpusError> {
        Self::from_key_datum_with_value(key, datum, None)
    }

    /// Reconstructs a fact from an index `key`, its stored [`Datum`] payload,
    /// and, for a *spilled* key, the raw value bytes fetched from the archive
    /// block store.
    ///
    /// The entity, attribute, and value type come from the key. The value is
    /// decoded from the key's inline order-preserving payload when it fits
    /// inline (in which case `spilled` is ignored), or reconstructed from
    /// `spilled` (the block fetched by the key's 32-byte reference) when the key
    /// is spilled. Pass `spilled = None` for inline keys; pass `Some(bytes)` for
    /// spilled keys (an inline key with `Some` bytes just ignores them, and a
    /// spilled key with `None` errors).
    pub fn from_key_datum_with_value(
        key: &Key,
        datum: &Datum,
        spilled: Option<Vec<u8>>,
    ) -> Result<Self, DialogCorpusError> {
        // Parse the key ONCE into borrowed components. Every ordering
        // (EAV/AEV/VAE) decodes to the same logical
        // entity/attribute/value_type/payload, so a single `parse_key_ref` walk
        // yields all fields the reconstruction needs, borrowing them from the
        // key bytes (no per-field allocation) except an escaped entity/
        // attribute. This replaces the previous per-field `KeyView` accessors,
        // each of which re-ran `split_components` (a fresh alloc + full key
        // walk) — a scan reconstructing N facts paid ~6 such walks per fact,
        // which dominated the scan cost on the variable-length M3 key format.
        let parts = varkey::parse_key_ref(key.as_ref()).ok_or_else(|| {
            DialogCorpusError::InvalidKey("key did not parse into components".to_string())
        })?;
        reconstruct(&parts, datum, spilled)
    }

    /// Reconstructs a [`Fact`] from an already-parsed [`KeyRef`], a datum,
    /// and (for a spilled value) the fetched block bytes. The scan path parses
    /// each key once into a [`KeyRef`] for matching and spill resolution, then
    /// hands that same parse here so reconstruction adds no further key walk.
    pub fn from_key_ref_datum_value(
        parts: &KeyRef<'_>,
        datum: &Datum,
        spilled: Option<Vec<u8>>,
    ) -> Result<Self, DialogCorpusError> {
        reconstruct(parts, datum, spilled)
    }

    /// Reconstructs a fact for display when the raw value bytes are not
    /// available: the entity, attribute, and cause come from the key and
    /// payload, and a spilled value is stood in for by a `<spilled value>`
    /// placeholder string. For a sync render path (the diagnose TUI) that has no
    /// store to fetch the spilled block. An inline key reconstructs its real
    /// value as usual.
    pub fn from_key_datum_placeholder(key: &Key, datum: &Datum) -> Result<Self, DialogCorpusError> {
        // Reconstruct entity/attribute from the key under its ordering; whether
        // the value spilled is read from that same view. If it did not spill,
        // fall through to the normal inline reconstruction.
        let (of, the, spilled) = match key.tag() {
            ENTITY_KEY_TAG => {
                let view = EntityKey(key);
                let (of, the) = entity_attribute(view.clone())?;
                (of, the, view.value_is_spilled())
            }
            ATTRIBUTE_KEY_TAG => {
                let view = AttributeKey(key);
                let (of, the) = entity_attribute(view.clone())?;
                (of, the, view.value_is_spilled())
            }
            VALUE_KEY_TAG => {
                let view = ValueKey(key);
                let (of, the) = entity_attribute(view.clone())?;
                (of, the, view.value_is_spilled())
            }
            tag => {
                return Err(DialogCorpusError::InvalidKey(format!(
                    "unknown index key tag {tag}"
                )));
            }
        };
        if !spilled {
            return Self::from_key_datum(key, datum);
        }
        Ok(Fact {
            the,
            of,
            is: Value::String("<spilled value>".to_string()),
            cause: datum.cause.clone(),
        })
    }
}

/// A fact row that has NOT been materialized into an owned [`Fact`].
///
/// Scans yield these instead of owned [`Fact`]s because full
/// materialization is the dominant per-row cost of a scan — an entity URI
/// parse, an attribute alloc, a value decode with its allocs, and a cause
/// clone, per row — and many consumers never need most of it (a count, a
/// filter on one field, a re-encode). A scanned row holds the index key, its
/// stored [`Datum`] payload, and (for a spilled value) the fetched block
/// bytes, exactly as the scan produced them; the key already carries the
/// entity, attribute, and value losslessly, so accessors borrow straight from
/// the key bytes on demand:
///
/// - [`parts`](Self::parts) parses the key once into borrowed components —
///   the right call when reading several fields of the same row.
/// - [`the_bytes`](Self::the_bytes) / [`of_bytes`](Self::of_bytes) read one
///   field's raw bytes.
/// - [`value`](Self::value) decodes just the value.
/// - [`cause`](Self::cause) reads the payload's cause without any key walk.
/// - [`sort_key`](Self::sort_key) derives the query layer's merge order
///   straight from the stored key bytes, with no value decode or re-encode.
/// - [`to_owned`](Self::to_owned) materializes the full [`Fact`] for
///   consumers that genuinely need ownership.
///
/// A view can also back onto an owned [`Fact`] (via `From<Fact>`):
/// that is how sources with no stored key — the in-memory `Changes` overlay a
/// query unions with branch scans — travel the same streams. The borrowed
/// accessors all work on both backings; only [`key`](Self::key),
/// [`datum`](Self::datum), [`spilled`](Self::spilled), and
/// [`parts`](Self::parts) are scanned-only.
#[derive(Clone, Debug)]
pub struct FactView {
    backing: Backing,
}

#[derive(Clone, Debug)]
enum Backing {
    /// A row as the tree scan holds it: key + payload + fetched spill block.
    /// The key is a [`KeyHandle`]: on a warm leaf it borrows the node's
    /// memoized decoded-keys arena (cloning clones an `Arc`, and the arena
    /// stays alive while any view over it does), so carrying a view costs
    /// no per-row key copy.
    Scanned {
        key: KeyHandle,
        /// Where the key's components sit, read once when the row was
        /// scanned, so its fields are read without walking the key again.
        layout: KeyLayout,
        datum: Datum,
        spilled: Option<Vec<u8>>,
    },
    /// A row synthesized without a stored key (e.g. an uncommitted overlay
    /// fact), carried as the owned [`Fact`] it was made from. Boxed so
    /// the common scanned row doesn't pay the owned form's footprint. A
    /// pending write carries the standing the commit will give it (see
    /// [`FactView::pending`]); any other owned row has none.
    Owned {
        fact: Box<Fact>,
        standing: Option<(Edition, [u8; 32])>,
    },
}

impl From<Fact> for FactView {
    fn from(fact: Fact) -> Self {
        Self {
            backing: Backing::Owned {
                fact: Box::new(fact),
                standing: None,
            },
        }
    }
}

impl FactView {
    /// For tests only: a scanned row of `the` on `of` whose key carries
    /// `value_type` and the value bytes `payload` as they are, with
    /// `cause`. A test builds a row a tree may hold but no [`Fact`] can
    /// stand for with it, such as one whose value does not decode.
    #[cfg(any(test, feature = "helpers"))]
    pub fn scanned(
        the: &Relation,
        of: &Entity,
        value_type: ValueDataType,
        payload: Vec<u8>,
        cause: Option<Cause>,
    ) -> Self {
        let key = varkey::build_key(&varkey::KeyParts {
            tag: ENTITY_KEY_TAG,
            entity: of.to_string().into_bytes(),
            attribute: the.as_str().as_bytes().to_vec(),
            value_type,
            value: varkey::ValuePayload::Inline(payload),
            version: None,
        });
        let layout = varkey::parse_key_layout(&key).expect("a built key parses");
        Self::new(
            KeyHandle::Owned(key),
            layout,
            Datum {
                cause,
                blob: None,
                version: None,
                collapsed: vec![],
                supersedes: vec![],
                retraction: false,
            },
            None,
        )
    }

    /// Assembles a view from a scanned entry's key handle, the layout the
    /// scan read from it, its payload, and (for a spilled value) the
    /// fetched block bytes.
    pub(crate) fn new(
        key: KeyHandle,
        layout: KeyLayout,
        datum: Datum,
        spilled: Option<Vec<u8>>,
    ) -> Self {
        Self {
            backing: Backing::Scanned {
                key,
                layout,
                datum,
                spilled,
            },
        }
    }

    /// A view over a write the transaction holding it will commit: it
    /// stands at `edition`, the edition that commit mints, so a read over
    /// the transaction elects it as the commit will, over every row the
    /// line holds. Every write of one transaction lands in one commit
    /// and shares its version, so they stand equal here too, with the
    /// version's hash, not known yet, read as zero; what the commit
    /// orders among them by cause and value, a read over the
    /// transaction orders the same way.
    pub fn pending(fact: Fact, edition: Edition) -> Self {
        Self {
            backing: Backing::Owned {
                fact: Box::new(fact),
                standing: Some((edition, [0; 32])),
            },
        }
    }

    /// The raw bytes of the index key this row was scanned at, or `None`
    /// for a row backed by an owned [`Fact`] (no stored key exists).
    pub fn key(&self) -> Option<&[u8]> {
        match &self.backing {
            Backing::Scanned { key, .. } => Some(key.as_ref()),
            Backing::Owned { .. } => None,
        }
    }

    /// The stored payload of this row (the parts of the fact the key does not
    /// carry), or `None` for a row backed by an owned [`Fact`].
    pub fn datum(&self) -> Option<&Datum> {
        match &self.backing {
            Backing::Scanned { datum, .. } => Some(datum),
            Backing::Owned { .. } => None,
        }
    }

    /// Every claim version this row stands for (the revision that wrote
    /// it plus any collapsed claims of the same fact, see
    /// [`Datum::versions`]). Empty for a row no revision tagged: data
    /// applied to a corpus with no version, or an owned row.
    pub fn versions(&self) -> impl Iterator<Item = &Version> {
        self.datum().into_iter().flat_map(Datum::versions)
    }

    /// The standing this row brings to the cardinality-one election: its
    /// deepest claim version, as the pair the election compares (the
    /// edition, then the hash of the whole version), or the standing a
    /// pending write will have. `None` for a row no revision tagged.
    pub fn standing(&self) -> Option<(Edition, [u8; 32])> {
        if let Backing::Owned { standing, .. } = &self.backing {
            return *standing;
        }
        self.versions()
            .map(|version| (version.edition, make_reference(version.key_bytes())))
            .max()
    }

    /// The [`Cause`] of this fact, if any, without touching the key.
    pub fn cause(&self) -> Option<&Cause> {
        match &self.backing {
            Backing::Scanned { datum, .. } => datum.cause.as_ref(),
            Backing::Owned { fact, .. } => fact.cause.as_ref(),
        }
    }

    /// The raw bytes of this row's spilled value block, when the value
    /// spilled (`None` for an inline value or an owned-backed row).
    pub fn spilled(&self) -> Option<&[u8]> {
        match &self.backing {
            Backing::Scanned { spilled, .. } => spilled.as_deref(),
            Backing::Owned { .. } => None,
        }
    }

    /// The key's borrowed components: entity, attribute, value type, and
    /// value payload, borrowing from the key bytes (owning only an escaped
    /// entity/attribute). The scan read the key's layout once, so this
    /// slices the key without walking it again. Errors for a row
    /// backed by an owned [`Fact`], which has no stored key — the
    /// field accessors below work on both backings.
    pub fn parts(&self) -> Result<KeyRef<'_>, DialogCorpusError> {
        match &self.backing {
            Backing::Scanned { key, layout, .. } => Ok(layout.view(key.as_ref())),
            Backing::Owned { .. } => Err(DialogCorpusError::InvalidKey(
                "owned-backed view has no index key to parse".to_string(),
            )),
        }
    }

    /// The raw attribute bytes of this row (`namespace/predicate`, no alloc
    /// unless the stored key carried an escape).
    pub fn the_bytes(&self) -> Result<Cow<'_, [u8]>, DialogCorpusError> {
        match &self.backing {
            Backing::Scanned { .. } => Ok(self.parts()?.attribute),
            Backing::Owned { fact, .. } => Ok(Cow::Borrowed(fact.the.as_str().as_bytes())),
        }
    }

    /// The raw entity bytes of this row (the full URI, no alloc unless the
    /// stored key carried an escape).
    pub fn of_bytes(&self) -> Result<Cow<'_, [u8]>, DialogCorpusError> {
        match &self.backing {
            Backing::Scanned { .. } => Ok(self.parts()?.entity),
            Backing::Owned { fact, .. } => Ok(Cow::Borrowed(fact.of.as_str().as_bytes())),
        }
    }

    /// Decodes just this row's [`Value`], from the key's inline payload or
    /// from the spilled block fetched at scan time.
    pub fn value(&self) -> Result<Value, DialogCorpusError> {
        match &self.backing {
            Backing::Scanned { spilled, .. } => decode_value_parts(&self.parts()?, spilled.clone()),
            Backing::Owned { fact, .. } => Ok(fact.is.clone()),
        }
    }

    /// This row's value read in place: borrowed from the key's inline
    /// payload or from the spilled block, with nothing materialized. The
    /// borrowing counterpart of [`Self::value`].
    pub fn value_view(&self) -> Result<super::ValueView<'_>, DialogCorpusError> {
        match &self.backing {
            Backing::Scanned { spilled, .. } => {
                let parts = self.parts()?;
                match parts.value {
                    ValueRef::Inline(payload) => {
                        super::decode_value_view(parts.value_type, payload)
                    }
                    ValueRef::Spilled { .. } => {
                        let bytes = spilled.as_deref().ok_or_else(|| {
                            DialogCorpusError::InvalidValue(
                                "spilled value key has no fetched block bytes".to_string(),
                            )
                        })?;
                        super::spilled_value_view(parts.value_type, bytes)
                    }
                }
            }
            Backing::Owned { fact, .. } => Ok(super::ValueView::from(&fact.is)),
        }
    }

    /// The stored value's type, as the key records it: read without
    /// walking the key. `None` for a row with no stored key.
    pub fn stored_type(&self) -> Option<ValueDataType> {
        match &self.backing {
            Backing::Scanned { layout, .. } => Some(layout.value_type()),
            Backing::Owned { .. } => None,
        }
    }

    /// This row's entity, as the URI the key stores, borrowed.
    pub fn of_str(&self) -> Result<Cow<'_, str>, DialogCorpusError> {
        match &self.backing {
            Backing::Scanned { .. } => super::view::cow_str(self.parts()?.entity),
            Backing::Owned { fact, .. } => Ok(Cow::Borrowed(fact.of.as_str())),
        }
    }

    /// This row's attribute, as the name the key stores, borrowed.
    pub fn the_str(&self) -> Result<Cow<'_, str>, DialogCorpusError> {
        match &self.backing {
            Backing::Scanned { .. } => super::view::cow_str(self.parts()?.attribute),
            Backing::Owned { fact, .. } => Ok(Cow::Borrowed(fact.the.as_str())),
        }
    }

    /// Checks that this row materializes, without materializing it: what
    /// [`Self::to_owned`] would refuse ([`CorruptEntry`] for stored bytes
    /// that fail validation), this refuses, so a reader may defer the
    /// materialization and still drop a corrupt row where main drops it.
    ///
    /// [`CorruptEntry`]: DialogCorpusError::CorruptEntry
    pub fn check(&self) -> Result<(), DialogCorpusError> {
        if let Backing::Owned { .. } = &self.backing {
            return Ok(());
        }
        let of = self.of_str()?;
        Uri::check_stored(&of)
            .map_err(|error| DialogCorpusError::CorruptEntry(format!("{error}")))?;
        super::view::check_relation(&self.the_str()?)?;
        self.value_view().map_err(as_corrupt_entry)?;
        Ok(())
    }

    /// This row's entity, materialized alone.
    pub fn entity(&self) -> Result<Entity, DialogCorpusError> {
        match &self.backing {
            Backing::Scanned { .. } => Ok(Entity::from_stored(&self.of_str()?)?),
            Backing::Owned { fact, .. } => Ok(fact.of.clone()),
        }
    }

    /// This row's attribute, materialized alone.
    pub fn relation(&self) -> Result<Relation, DialogCorpusError> {
        match &self.backing {
            Backing::Scanned { .. } => {
                Relation::from_str(&self.the_str()?).map_err(as_corrupt_entry)
            }
            Backing::Owned { fact, .. } => Ok(fact.the.clone()),
        }
    }

    /// The [`SortKey`](crate::SortKey) of this row — the query layer's
    /// cross-index merge order (see the `SortKey` docs).
    ///
    /// For a scanned row every component comes straight from the stored key
    /// bytes: the attribute and entity columns, and the value tail exactly as
    /// the key carries it (type byte, value slot, spilled hash) — no value
    /// decode, no re-encode. That reproduces
    /// [`sort_key`](crate::sort_key) under the manifest the row was WRITTEN
    /// with, which is the tree's own order by construction, and `manifest` is
    /// not consulted. An owned-backed row has no stored key and derives the
    /// same key from its fields under `manifest`, which must be the format of
    /// the tree the row is ordered or matched against.
    pub fn sort_key(
        &self,
        manifest: &dialog_search_tree::Manifest,
    ) -> Result<crate::SortKey, DialogCorpusError> {
        match &self.backing {
            Backing::Scanned { .. } => {
                let parts = self.parts()?;
                let slot = parts.value.slot_bytes();
                let mut tail = Vec::with_capacity(1 + slot.len() + 32);
                tail.push(u8::from(parts.value_type));
                tail.extend_from_slice(slot);
                if let ValueRef::Spilled { hash, .. } = &parts.value {
                    tail.extend_from_slice(hash);
                }
                Ok((
                    parts.attribute.into_owned(),
                    parts.entity.into_owned(),
                    tail,
                ))
            }
            Backing::Owned { fact, .. } => Ok(crate::sort_key(fact, manifest)),
        }
    }

    /// Materializes the full owned [`Fact`]: entity, attribute, value,
    /// and cause. This is the whole per-row cost scans stopped paying by
    /// default — reach for it only when ownership is genuinely needed.
    pub fn to_owned(&self) -> Result<Fact, DialogCorpusError> {
        match &self.backing {
            Backing::Scanned { datum, spilled, .. } => {
                reconstruct(&self.parts()?, datum, spilled.clone())
            }
            Backing::Owned { fact, .. } => Ok(fact.as_ref().clone()),
        }
    }

    /// The cardinality-one election: of this row and a `challenger`
    /// standing at the same `(attribute, entity)`, the row a
    /// `Cardinality::One` reader observes. The policy belongs to the value
    /// layer — the query engine folds competing rows through this method
    /// and encodes no rule of its own — so refining how concurrent edits
    /// resolve happens here without touching the engine.
    ///
    /// Two live rows at one `(attribute, entity)` are concurrent claims: a
    /// writer that had observed the other row and replaced it retracted
    /// it, so the rows that remain were written without sight of each
    /// other (or by a writer that chose to keep both). The election is
    /// therefore which concurrent revision the reader follows, and it
    /// decides that by the revision, not by the fact: a row's standing is
    /// its deepest claim version, compared by edition (causal depth: the
    /// revision that had seen more when it wrote wins) and then by the
    /// hash of the version (a coin no writer can load without grinding
    /// its own origin's editions). Because every fact one revision wrote
    /// carries the same version, every fact of that revision resolves the
    /// same way against every fact of a concurrent one: a batch wins or
    /// loses whole, so a reader never sees half of one writer's batch and
    /// half of another's. Depth also makes the outcome monotonic along a
    /// lineage: once a revision beats a concurrent one, so does every
    /// revision built on it.
    ///
    /// A versioned row beats one no revision tagged. Rows the version
    /// cannot separate (both unversioned, or both written by the same
    /// revision) fall to the older rule: the higher cause wins, a caused
    /// row beats an uncaused one, and equal (including absent) causes fall
    /// to the hash of the value ([`Value::tiebreak`](crate::Value::tiebreak)),
    /// the greater winning, so the tie favours no value over another. This
    /// is the order
    /// [`Cardinality::newer`](crate::Cardinality::newer) elects by,
    /// in the tree, in a commit's settlement and in a concept read, so
    /// every read of a cell under `last` returns the same claim. Every
    /// tier is deterministic and commutative: folding any set of rows in
    /// any order elects the same row, which is what lets every replica
    /// agree on the observed value without coordination. Versions and
    /// causes read straight off the rows; only a genuine tie pays for the
    /// materialization the value needs.
    pub fn elect(self, challenger: FactView) -> Result<FactView, DialogCorpusError> {
        match (self.standing(), challenger.standing()) {
            (Some(a), Some(b)) if a > b => return Ok(self),
            (Some(a), Some(b)) if a < b => return Ok(challenger),
            (Some(_), None) => return Ok(self),
            (None, Some(_)) => return Ok(challenger),
            _ => {}
        }
        Ok(match (self.cause(), challenger.cause()) {
            (Some(a), Some(b)) if a > b => self,
            (Some(a), Some(b)) if a < b => challenger,
            (Some(_), None) => self,
            (None, Some(_)) => challenger,
            _ => {
                // Causes are equal: the value's hash is the deterministic
                // tiebreaker, as it is for every other election of the
                // cell. The value needs materialization, which is where a
                // corrupt stored row (`CorruptEntry`) surfaces — such a row
                // must be invisible to readers, so it LOSES to any
                // materializable rival. Two corrupt rows elect either (the
                // winner fails materialization downstream and is skipped
                // there); any other error propagates.
                use DialogCorpusError::CorruptEntry;
                match (self.to_owned(), challenger.to_owned()) {
                    (Ok(a), Ok(b)) => {
                        if super::update::value_beats(&b.is, &a.is) {
                            challenger
                        } else {
                            self
                        }
                    }
                    (Ok(_), Err(CorruptEntry(_))) => self,
                    (Err(CorruptEntry(_)), Ok(_)) => challenger,
                    (Err(CorruptEntry(_)), Err(CorruptEntry(_))) => self,
                    (Err(error), _) | (_, Err(error)) => return Err(error),
                }
            }
        })
    }
}

/// Where one checked component of a row sits: a span of the row's key
/// bytes, or (for a component that carried an escape, a spilled value, or
/// a row with no stored key) the bytes themselves.
#[derive(Debug, Clone)]
enum Piece {
    Span(u32, u32),
    Owned(Box<[u8]>),
}

impl Piece {
    /// Locates `bytes` in `key` when they borrow from it, or owns them.
    fn of(key: &[u8], bytes: Cow<'_, [u8]>) -> Piece {
        let base = key.as_ptr() as usize;
        match bytes {
            Cow::Borrowed(bytes)
                if !key.is_empty()
                    && bytes.as_ptr() as usize >= base
                    && bytes.as_ptr() as usize + bytes.len() <= base + key.len() =>
            {
                Piece::Span((bytes.as_ptr() as usize - base) as u32, bytes.len() as u32)
            }
            bytes => Piece::Owned(bytes.into_owned().into_boxed_slice()),
        }
    }

    /// Locates checked text.
    fn text(key: &[u8], text: Cow<'_, str>) -> Piece {
        Piece::of(
            key,
            match text {
                Cow::Borrowed(text) => Cow::Borrowed(text.as_bytes()),
                Cow::Owned(text) => Cow::Owned(text.into_bytes()),
            },
        )
    }

    fn bytes<'a>(&'a self, key: &'a [u8]) -> &'a [u8] {
        match self {
            Piece::Span(start, len) => &key[*start as usize..(*start + *len) as usize],
            Piece::Owned(bytes) => bytes,
        }
    }

    /// The piece as text. Only for a piece made by [`Piece::text`].
    fn read<'a>(&'a self, key: &'a [u8]) -> &'a str {
        // SAFETY: a text piece holds, or spans, bytes validated as UTF-8
        // when the row was checked, and a row's key bytes never change
        // while it is held.
        unsafe { from_utf8_unchecked(self.bytes(key)) }
    }
}

/// A checked row's value: text, entities, symbols, bytes and records by
/// where they sit, numbers and booleans read out.
#[derive(Debug, Clone)]
enum Stored {
    String(Piece),
    Entity(Piece),
    Symbol(Piece),
    Bytes(Piece),
    Record(Piece),
    UnsignedInt(u128),
    SignedInt(i128),
    Float(f64),
    Boolean(bool),
    Composite(super::Composite),
}

/// A row validated once, so everything that reads it afterwards reads its
/// components in place without walking or validating the key again: what a
/// query binds when it binds a scanned row in place. Validation is what
/// [`FactView::to_owned`] would do; a row that fails it is not a
/// `CheckedRow` ([`FactView::checked`]).
///
/// It keeps the key, the cause and where each component sits, and nothing
/// else of the scanned row: the payload's versions are read once, by the
/// scan, when it takes the row's standing.
#[derive(Debug, Clone)]
pub struct CheckedRow {
    key: Option<KeyHandle>,
    cause: Option<Cause>,
    entity: Piece,
    attribute: Piece,
    value: Stored,
}

impl FactView {
    /// Validates this row once, as [`Self::to_owned`] would, without
    /// materializing it: a row `to_owned` refuses is refused with the same
    /// error, and a row it reads reads the same.
    ///
    /// A value is read in place when its stored bytes are what `to_owned`
    /// would read: anything else (a value that does not decode, or an
    /// entity stored as a URI that is not its own canonical rendering) is
    /// left to `to_owned`, which decides, so the in-place and the eager
    /// scan of a row agree.
    pub fn checked(self) -> Result<CheckedRow, DialogCorpusError> {
        let checked = self.check_pieces()?;
        Ok(self.assemble(checked))
    }

    /// [`Self::checked`], passing over a row this replica cannot read yet
    /// ([`CorruptEntry`](DialogCorpusError::CorruptEntry)) through
    /// [`pass_over`](crate::pass_over): `None` for such a row, any other
    /// error refused as `checked` refuses it.
    pub fn checked_or_passed_over(self) -> Result<Option<CheckedRow>, DialogCorpusError> {
        match self.check_pieces() {
            Ok(checked) => Ok(Some(self.assemble(checked))),
            Err(DialogCorpusError::CorruptEntry(reason)) => {
                crate::pass_over(self.key(), reason);
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Validates this row for [`Self::checked`], borrowing it.
    fn check_pieces(&self) -> Result<Checked, DialogCorpusError> {
        let key = self.key().unwrap_or(&[]);
        // One walk of the key's components for all three.
        let (of, the, value) = match &self.backing {
            Backing::Scanned { spilled, .. } => {
                let parts = self.parts()?;
                let value = match parts.value {
                    ValueRef::Inline(payload) => {
                        super::decode_value_view(parts.value_type, payload)
                    }
                    ValueRef::Spilled { .. } => {
                        let bytes = spilled.as_deref().ok_or_else(|| {
                            DialogCorpusError::InvalidValue(
                                "spilled value key has no fetched block bytes".to_string(),
                            )
                        })?;
                        super::spilled_value_view(parts.value_type, bytes)
                    }
                };
                // An entity whose stored URI is its canonical rendering
                // reads as stored: `to_owned`'s parse renders it
                // unchanged. That parse also refuses whitespace and
                // controls, which a canonical rendering can still hold
                // in an opaque path ("user:a b"), so they are checked
                // apart.
                let value = match value {
                    Ok(super::ValueView::Entity(uri))
                        if uri
                            .chars()
                            .any(|c| c.is_ascii_whitespace() || c.is_control())
                            || Uri::check_stored(&uri).is_err() =>
                    {
                        None
                    }
                    Ok(view) => Some(view),
                    Err(_) => None,
                };
                let Some(value) = value else {
                    return self.checked_as_owned().map(Checked::Whole);
                };
                (
                    super::view::cow_str(parts.entity)?,
                    super::view::cow_str(parts.attribute)?,
                    value,
                )
            }
            Backing::Owned { .. } => (self.of_str()?, self.the_str()?, self.value_view()?),
        };
        Uri::check_stored(&of)
            .map_err(|error| DialogCorpusError::CorruptEntry(format!("{error}")))?;
        super::view::check_relation(&the)?;
        Ok(Checked::Pieces(
            Piece::text(key, of),
            Piece::text(key, the),
            Stored::of(key, value),
        ))
    }

    /// The checked row of `checked`, taking this row's key.
    fn assemble(self, checked: Checked) -> CheckedRow {
        let (entity, attribute, value) = match checked {
            Checked::Pieces(entity, attribute, value) => (entity, attribute, value),
            Checked::Whole(row) => return row,
        };
        let cause = self.cause().cloned();
        let key = match self.backing {
            Backing::Scanned { key, .. } => Some(key),
            Backing::Owned { .. } => None,
        };
        CheckedRow {
            key,
            cause,
            entity,
            attribute,
            value,
        }
    }

    fn checked_as_owned(&self) -> Result<CheckedRow, DialogCorpusError> {
        let fact = self.to_owned()?;
        let owned = |text: &str| Piece::Owned(text.as_bytes().into());
        Ok(CheckedRow {
            key: None,
            cause: fact.cause.clone(),
            entity: owned(fact.of.as_str()),
            attribute: owned(fact.the.as_str()),
            value: Stored::of(&[], super::ValueView::from(&fact.is)),
        })
    }
}

/// What [`FactView::check_pieces`] found: the row's checked components,
/// read in place, or the whole checked row, read as `to_owned` reads it.
enum Checked {
    Pieces(Piece, Piece, Stored),
    Whole(CheckedRow),
}

impl Stored {
    /// `value`, its text and bytes located in `key` when they borrow from
    /// it, owned otherwise.
    fn of(key: &[u8], value: super::ValueView<'_>) -> Stored {
        match value {
            super::ValueView::String(text) => Stored::String(Piece::text(key, text)),
            super::ValueView::Entity(text) => Stored::Entity(Piece::text(key, text)),
            super::ValueView::Symbol(text) => Stored::Symbol(Piece::text(key, text)),
            super::ValueView::Bytes(bytes) => Stored::Bytes(Piece::of(key, bytes)),
            super::ValueView::Record(bytes) => Stored::Record(Piece::of(key, bytes)),
            super::ValueView::UnsignedInt(n) => Stored::UnsignedInt(n),
            super::ValueView::SignedInt(n) => Stored::SignedInt(n),
            super::ValueView::Float(n) => Stored::Float(n),
            super::ValueView::Boolean(b) => Stored::Boolean(b),
            super::ValueView::Composite(composite) => Stored::Composite(composite),
        }
    }
}

impl CheckedRow {
    fn key(&self) -> &[u8] {
        self.key.as_ref().map_or(&[], AsRef::as_ref)
    }

    /// The entity's URI, in place.
    pub fn of_str(&self) -> &str {
        self.entity.read(self.key())
    }

    /// The attribute's name, in place.
    pub fn the_str(&self) -> &str {
        self.attribute.read(self.key())
    }

    /// The value, in place.
    pub fn value_view(&self) -> super::ValueView<'_> {
        let key = self.key();
        match &self.value {
            Stored::String(piece) => super::ValueView::String(Cow::Borrowed(piece.read(key))),
            Stored::Entity(piece) => super::ValueView::Entity(Cow::Borrowed(piece.read(key))),
            Stored::Symbol(piece) => super::ValueView::Symbol(Cow::Borrowed(piece.read(key))),
            Stored::Bytes(piece) => super::ValueView::Bytes(Cow::Borrowed(piece.bytes(key))),
            Stored::Record(piece) => super::ValueView::Record(Cow::Borrowed(piece.bytes(key))),
            Stored::UnsignedInt(n) => super::ValueView::UnsignedInt(*n),
            Stored::SignedInt(n) => super::ValueView::SignedInt(*n),
            Stored::Float(n) => super::ValueView::Float(*n),
            Stored::Boolean(b) => super::ValueView::Boolean(*b),
            Stored::Composite(composite) => super::ValueView::Composite(composite.clone()),
        }
    }

    /// The entity, materialized. Skips the URI check `checked` made.
    pub fn entity(&self) -> Entity {
        Entity::try_from(Uri::stored_unchecked(self.of_str()))
            .expect("a checked row's entity materializes")
    }

    /// The attribute, materialized.
    pub fn relation(&self) -> Relation {
        Relation::from_str(self.the_str()).expect("a checked row's attribute materializes")
    }

    /// The value, materialized.
    pub fn value(&self) -> Value {
        self.value_view()
            .to_value()
            .expect("a checked row's value materializes")
    }

    /// The cause, as stored.
    pub fn cause(&self) -> Option<&Cause> {
        self.cause.as_ref()
    }
}

/// Chainable materialization for streams of scanned rows: `.owned()` turns a
/// stream of [`FactView`]s into a stream of owned [`Fact`]s by calling
/// [`FactView::to_owned`] on every row.
///
/// This is the explicit opt-in to the full per-row materialization cost —
/// scans stopped paying it by default. Prefer reading fields off the views
/// where possible; reach for `.owned()` when rows genuinely leave the scan's
/// scope (collected into results handed to a caller, serialized outward, fed
/// to an API that requires [`Fact`]).
pub trait FactViewStream: Stream<Item = Result<FactView, DialogCorpusError>> + Sized {
    /// Materializes every row into an owned [`Fact`].
    ///
    /// A row whose stored bytes fail read-side validation
    /// ([`CorruptEntry`](DialogCorpusError::CorruptEntry) — e.g. an entity
    /// that is not a canonical URI) is SKIPPED with a warning rather than
    /// yielded as an error: a corrupt or foreign-written entry in a tree must
    /// not fail every query that ranges over it. All other errors pass
    /// through.
    fn owned(self) -> impl Stream<Item = Result<Fact, DialogCorpusError>> + ConditionalSend
    where
        Self: ConditionalSend,
    {
        self.filter_map(|row| async move {
            let view = match row {
                Ok(view) => view,
                Err(error) => return Some(Err(error)),
            };
            match view.to_owned() {
                Err(DialogCorpusError::CorruptEntry(reason)) => {
                    crate::pass_over(view.key(), reason);
                    None
                }
                other => Some(other),
            }
        })
    }
}

impl<S> FactViewStream for S where S: Stream<Item = Result<FactView, DialogCorpusError>> + Sized {}

/// Extracts the entity and attribute from a key view, decoding the raw UTF-8
/// key columns.
fn entity_attribute<K: KeyView>(key: K) -> Result<(Entity, Relation), DialogCorpusError> {
    // These bytes came out of the tree: every validation failure here —
    // non-UTF-8 columns, a non-canonical entity, an attribute breaking the
    // attribute invariants — marks a corrupt or foreign-written entry, so it
    // is classified `CorruptEntry` and scan paths may ignore the row.
    let of = Entity::from_stored(from_utf8(key.entity().raw()).map_err(|error| {
        DialogCorpusError::CorruptEntry(format!("entity key is not UTF-8: {error}"))
    })?)?;
    let the = Relation::from_str(from_utf8(key.attribute().raw()).map_err(|error| {
        DialogCorpusError::CorruptEntry(format!("attribute key is not UTF-8: {error}"))
    })?)
    .map_err(as_corrupt_entry)?;
    Ok((of, the))
}

/// Reclassifies a stored column's validation failure as
/// [`CorruptEntry`](DialogCorpusError::CorruptEntry). Used only where the
/// failing bytes are known to have come out of the tree; the underlying
/// validators keep their caller-error variants for ingest paths.
fn as_corrupt_entry(error: DialogCorpusError) -> DialogCorpusError {
    match error {
        already @ DialogCorpusError::CorruptEntry(_) => already,
        other => DialogCorpusError::CorruptEntry(format!("{other}")),
    }
}

/// Reconstructs a [`Fact`] from a single borrowed parse of the key's
/// components and its payload. The entity, attribute, and value type come from
/// the parsed key; the value is decoded inline from the key's payload or taken
/// from `spilled` (the archive block bytes) when it spilled.
///
/// Takes the already-parsed [`KeyRef`] so the whole reconstruction is a single
/// key walk that borrows the key bytes; see
/// [`Fact::from_key_datum_with_value`] for why.
fn reconstruct(
    parts: &KeyRef<'_>,
    datum: &Datum,
    spilled: Option<Vec<u8>>,
) -> Result<Fact, DialogCorpusError> {
    // These bytes came out of the tree — validation failures are classified
    // `CorruptEntry` (an ignorable row); see `entity_attribute`.
    let of = Entity::from_stored(from_utf8(&parts.entity).map_err(|error| {
        DialogCorpusError::CorruptEntry(format!("entity key is not UTF-8: {error}"))
    })?)?;
    let the = Relation::from_str(from_utf8(&parts.attribute).map_err(|error| {
        DialogCorpusError::CorruptEntry(format!("attribute key is not UTF-8: {error}"))
    })?)
    .map_err(as_corrupt_entry)?;

    let is = decode_value_parts(parts, spilled)?;

    Ok(Fact {
        the,
        of,
        is,
        cause: datum.cause.clone(),
    })
}

/// Decodes a fact's [`Value`] from its parsed key components: the inline
/// order-preserving payload for an inline value, or the fetched archive block
/// bytes (`spilled`) for a spilled one. The value-only slice of
/// [`reconstruct`], for callers (a value predicate re-check, a
/// [`FactView::value`] access) that need the value without paying for the
/// entity and attribute materialization.
pub(crate) fn decode_value_parts(
    parts: &KeyRef<'_>,
    spilled: Option<Vec<u8>>,
) -> Result<Value, DialogCorpusError> {
    // These bytes came out of the tree: a value that does not decode, or
    // that this dialog would not have written, is a claim no reader here
    // can read (`CorruptEntry`), passed over as if absent. A spilled
    // block not fetched is not such a claim, and stays an error.
    Ok(match parts.value {
        // The key carries the value's prefix and hash; the raw value bytes
        // live in a content-addressed archive block the caller fetched and
        // passed in.
        ValueRef::Spilled { .. } => {
            let bytes = spilled.ok_or_else(|| {
                DialogCorpusError::InvalidValue(
                    "spilled value key has no fetched block bytes".to_string(),
                )
            })?;
            match parts.value_type.storage() {
                ValueDataType::Entity => {
                    let uri = from_utf8(&bytes).map_err(|error| {
                        DialogCorpusError::CorruptEntry(format!(
                            "spilled entity is not UTF-8: {error}"
                        ))
                    })?;
                    Value::Entity(super::view::stored_entity(uri)?)
                }
                _ => Value::try_from((parts.value_type, bytes)).map_err(as_corrupt_entry)?,
            }
        }
        // Decode the inline order-preserving value from the key.
        ValueRef::Inline(inline_payload) => {
            let (value, rest) =
                decode_value(parts.value_type, inline_payload).ok_or_else(|| {
                    DialogCorpusError::CorruptEntry(
                        "inline value payload did not decode".to_string(),
                    )
                })?;
            if !rest.is_empty() {
                return Err(DialogCorpusError::CorruptEntry(
                    "inline value payload had trailing bytes".to_string(),
                ));
            }
            value
        }
    })
}

#[cfg(test)]
mod tests {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::history::Origin;
    use crate::key::varkey::{ValuePayload, build_key};
    use crate::{ValueDataType, encode_value_owned};

    fn row(entity: &[u8], value: &str, version: Option<Version>, cause: Option<Cause>) -> FactView {
        let parts = varkey::KeyParts {
            tag: ENTITY_KEY_TAG,
            entity: entity.to_vec(),
            attribute: b"user/name".to_vec(),
            value_type: ValueDataType::String,
            value: ValuePayload::Inline(encode_value_owned(&Value::String(value.into()))),
            version: None,
        };
        let key = build_key(&parts);
        let layout = varkey::parse_key_layout(&key).expect("a built key parses");
        FactView::new(
            KeyHandle::Owned(key),
            layout,
            Datum {
                cause,
                blob: None,
                version,
                collapsed: vec![],
                supersedes: vec![],
                retraction: false,
            },
            None,
        )
    }

    fn view(entity: &[u8]) -> FactView {
        row(entity, "v", None, None)
    }

    fn version(origin: u8, edition: u64) -> Version {
        Version::new(Origin([origin; 32]), Edition::new(edition))
    }

    fn elected(winner: Result<FactView, DialogCorpusError>) -> Value {
        winner.expect("elects").to_owned().expect("materializes").is
    }

    /// Between concurrent revisions the deeper edition wins, in either
    /// fold order, whatever the rows' causes say.
    #[dialog_common::test]
    fn it_elects_the_deeper_edition_in_either_order() {
        let shallow = row(
            b"user:alice",
            "Alice",
            Some(version(1, 2)),
            Some(Cause([9; 32])),
        );
        let deep = row(b"user:alice", "Alicia", Some(version(2, 3)), None);
        assert_eq!(
            elected(shallow.clone().elect(deep.clone())),
            Value::String("Alicia".into())
        );
        assert_eq!(elected(deep.elect(shallow)), Value::String("Alicia".into()));
    }

    /// Equal editions fall to the hash of the version, so the outcome is
    /// the same in either fold order and decided by the revision, not the
    /// fact: every fact of one revision beats every fact of the other,
    /// however the facts themselves hash.
    #[dialog_common::test]
    fn it_resolves_a_batch_of_equal_editions_by_the_version_hash() {
        let (a, b) = (version(1, 2), version(2, 2));
        let favoured = if make_reference(a.key_bytes()) > make_reference(b.key_bytes()) {
            a
        } else {
            b
        };
        for index in 0..16u8 {
            let entity = format!("user:{index}");
            let ours = row(entity.as_bytes(), "ours", Some(a), None);
            let theirs = row(entity.as_bytes(), "theirs", Some(b), None);
            for winner in [ours.clone().elect(theirs.clone()), theirs.elect(ours)] {
                let winner = winner.expect("elects");
                assert_eq!(
                    winner.versions().copied().collect::<Vec<_>>(),
                    vec![favoured]
                );
            }
        }
    }

    /// A row a revision tagged beats one committed without a version,
    /// whatever their causes.
    #[dialog_common::test]
    fn it_prefers_a_versioned_row_to_an_unversioned_one() {
        let tagged = row(b"user:alice", "Alice", Some(version(1, 1)), None);
        let untagged = row(b"user:alice", "Alicia", None, Some(Cause([9; 32])));
        assert_eq!(
            elected(tagged.clone().elect(untagged.clone())),
            Value::String("Alice".into())
        );
        assert_eq!(
            elected(untagged.elect(tagged)),
            Value::String("Alice".into())
        );
    }

    /// Two values one revision wrote for the same attribute cannot be told
    /// apart by version, so they fall to the cause rule.
    #[dialog_common::test]
    fn it_falls_to_the_cause_rule_within_one_revision() {
        let low = row(
            b"user:alice",
            "Alice",
            Some(version(1, 1)),
            Some(Cause([1; 32])),
        );
        let high = row(
            b"user:alice",
            "Alicia",
            Some(version(1, 1)),
            Some(Cause([2; 32])),
        );
        assert_eq!(
            elected(low.clone().elect(high.clone())),
            Value::String("Alicia".into())
        );
        assert_eq!(elected(high.elect(low)), Value::String("Alicia".into()));
    }

    /// A collapsed claim counts: a fact two revisions asserted stands at
    /// the deeper of them.
    #[dialog_common::test]
    fn it_stands_at_its_deepest_collapsed_claim() {
        let mut shared = row(b"user:alice", "Alice", Some(version(1, 1)), None);
        match &mut shared.backing {
            Backing::Scanned { datum, .. } => datum.absorb_versions([&version(2, 5)]),
            Backing::Owned { .. } => unreachable!("scanned row"),
        }
        let rival = row(b"user:alice", "Alicia", Some(version(3, 4)), None);
        assert_eq!(
            elected(shared.clone().elect(rival.clone())),
            Value::String("Alice".into())
        );
        assert_eq!(elected(rival.elect(shared)), Value::String("Alice".into()));
    }

    /// In a cause-tied election a corrupt stored row (an entity that fails
    /// read-side validation) loses to a materializable rival from either
    /// side, and two corrupt rows still elect without erroring (the winner
    /// is dropped at materialization instead).
    /// A row is moved by value through every scan stage, and the tree
    /// scan's generator keeps one in its state: it holds the scanned
    /// key, its layout, its payload and the spilled block, and nothing
    /// else. The layer a row was read from travels beside it, not in
    /// it (see [`Layered`](crate::Layered)).
    ///
    /// The row holds pointers (the key handle, the datum, the spilled
    /// block), so its size follows the pointer width: 256 bytes where a
    /// pointer is 8 bytes, 192 where it is 4 (wasm32). The same test
    /// pins both; the expected value is chosen by the target.
    #[dialog_common::test]
    fn a_row_is_256_bytes() {
        #[cfg(target_pointer_width = "64")]
        let expected = 256;
        #[cfg(target_pointer_width = "32")]
        let expected = 192;
        assert_eq!(size_of::<FactView>(), expected);
    }

    #[dialog_common::test]
    fn it_elects_the_valid_row_over_a_corrupt_one() {
        let valid = view(b"user:alice");
        let corrupt = view(b"not a uri at all");

        let winner = corrupt.clone().elect(valid.clone()).expect("elects");
        assert!(winner.to_owned().is_ok(), "valid beats corrupt challenger");
        let winner = valid.elect(corrupt.clone()).expect("elects");
        assert!(winner.to_owned().is_ok(), "valid beats corrupt incumbent");

        let winner = corrupt.clone().elect(corrupt).expect("still elects");
        assert!(matches!(
            winner.to_owned(),
            Err(DialogCorpusError::CorruptEntry(_))
        ));
    }
}

#[cfg(test)]
mod checked_tests;
