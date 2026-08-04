//! Artifact data structure representing semantic triples.
//!
//! This module defines the core [`Artifact`] type which represents a semantic triple
//! (subject-predicate-object) in the Dialog database. Artifacts are the fundamental
//! units of data storage and retrieval.

use std::{
    borrow::Cow,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    str::{FromStr, from_utf8},
};

use dialog_common::ConditionalSend;
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    ATTRIBUTE_KEY_TAG, AttributeKey, Datum, DialogArtifactsError, ENTITY_KEY_TAG, EntityKey, Key,
    KeyView, VALUE_KEY_TAG, ValueKey, decode_value,
    key::varkey::{self, KeyRef, ValueRef},
};

use super::{Attribute, Cause, Entity, Value};

/// A [`Artifact`] embodies a datum - a semantic triple - that may be stored in or
/// retrieved from a [`ArtifactStore`].
#[derive(Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Artifact {
    /// The [`Attribute`] of the [`Artifact`]; the predicate of the triple
    pub the: Attribute,
    /// The [`Entity`] of the [`Artifact`]; the subject of the triple
    #[serde(
        serialize_with = "crate::artifacts::entity::to_utf8",
        deserialize_with = "crate::artifacts::entity::from_utf8"
    )]
    pub of: Entity,
    /// The [`Value`] of the [`Artifact`]; the object of the triple
    // TODO: This is in support of Artifacts<->CSV but we probably want
    // different (de)serialization for Artifacts<->JSON (assuming we ever
    // want that.
    #[serde(
        serialize_with = "crate::artifacts::value::to_utf8",
        deserialize_with = "crate::artifacts::value::from_utf8"
    )]
    pub is: Value,
    /// The [`Cause`] of the [`Artifact`], which is a reference to an ancester
    /// version with a different [`Value`].
    pub cause: Option<Cause>,
}

impl Debug for Artifact {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Artifact")
            .field("the", &self.the.to_string())
            .field("of", &self.of.to_string())
            .field("is", &self.is)
            .field("cause", &self.cause.as_ref().map(|cause| cause.to_string()))
            .finish()
    }
}

impl Display for Artifact {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let attribute = self.the.to_string();
        let entity = self.of.to_string();
        let value = self.is.to_utf8();

        write!(f, "Artifact: the '{attribute}' of '{entity}' is '{value}'")
    }
}

impl Artifact {
    /// Reconstructs a fact from an index `key` and its stored [`Datum`] payload,
    /// for a key whose value is stored INLINE.
    ///
    /// The entity, attribute, value type, and value all come from the key
    /// (stored losslessly and order-preservingly by [`EntityKey::from`] and
    /// friends). This is a convenience over
    /// [`Artifact::from_key_datum_with_value`] with no spilled bytes; if the key
    /// is spilled (`key.value_is_spilled()`) it errors, because the raw value
    /// bytes live in a separate archive block the caller must fetch. Use
    /// [`Artifact::from_key_datum_with_value`] for the general case.
    pub fn from_key_datum(key: &Key, datum: &Datum) -> Result<Self, DialogArtifactsError> {
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
    ) -> Result<Self, DialogArtifactsError> {
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
            DialogArtifactsError::InvalidKey("key did not parse into components".to_string())
        })?;
        reconstruct(&parts, datum, spilled)
    }

    /// Reconstructs an [`Artifact`] from an already-parsed [`KeyRef`], a datum,
    /// and (for a spilled value) the fetched block bytes. The scan path parses
    /// each key once into a [`KeyRef`] for matching and spill resolution, then
    /// hands that same parse here so reconstruction adds no further key walk.
    pub fn from_key_ref_datum_value(
        parts: &KeyRef<'_>,
        datum: &Datum,
        spilled: Option<Vec<u8>>,
    ) -> Result<Self, DialogArtifactsError> {
        reconstruct(parts, datum, spilled)
    }

    /// Reconstructs a fact for display when the raw value bytes are not
    /// available: the entity, attribute, and cause come from the key and
    /// payload, and a spilled value is stood in for by a `<spilled value>`
    /// placeholder string. For a sync render path (the diagnose TUI) that has no
    /// store to fetch the spilled block. An inline key reconstructs its real
    /// value as usual.
    pub fn from_key_datum_placeholder(
        key: &Key,
        datum: &Datum,
    ) -> Result<Self, DialogArtifactsError> {
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
                return Err(DialogArtifactsError::InvalidKey(format!(
                    "unknown index key tag {tag}"
                )));
            }
        };
        if !spilled {
            return Self::from_key_datum(key, datum);
        }
        Ok(Artifact {
            the,
            of,
            is: Value::String("<spilled value>".to_string()),
            cause: datum.cause.clone(),
        })
    }
}

/// A fact row that has NOT been materialized into an owned [`Artifact`].
///
/// Scans yield these instead of owned [`Artifact`]s because full
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
/// - [`to_owned`](Self::to_owned) materializes the full [`Artifact`] for
///   consumers that genuinely need ownership.
///
/// A view can also back onto an owned [`Artifact`] (via `From<Artifact>`):
/// that is how sources with no stored key — the in-memory `Changes` overlay a
/// query unions with branch scans — travel the same streams. The borrowed
/// accessors all work on both backings; only [`key`](Self::key),
/// [`datum`](Self::datum), [`spilled`](Self::spilled), and
/// [`parts`](Self::parts) are scanned-only.
#[derive(Clone, Debug)]
pub struct ArtifactView {
    backing: Backing,
}

#[derive(Clone, Debug)]
enum Backing {
    /// A row as the tree scan holds it: key + payload + fetched spill block.
    Scanned {
        key: Key,
        datum: Datum,
        spilled: Option<Vec<u8>>,
    },
    /// A row synthesized without a stored key (e.g. an uncommitted overlay
    /// fact), carried as the owned [`Artifact`] it was made from. Boxed so
    /// the common scanned row doesn't pay the owned form's footprint.
    Owned(Box<Artifact>),
}

impl From<Artifact> for ArtifactView {
    fn from(artifact: Artifact) -> Self {
        Self {
            backing: Backing::Owned(Box::new(artifact)),
        }
    }
}

impl ArtifactView {
    /// Assembles a view from a scanned entry's key, its payload, and (for a
    /// spilled value) the fetched block bytes.
    pub(crate) fn new(key: Key, datum: Datum, spilled: Option<Vec<u8>>) -> Self {
        Self {
            backing: Backing::Scanned {
                key,
                datum,
                spilled,
            },
        }
    }

    /// The index key this row was scanned at, or `None` for a row backed by
    /// an owned [`Artifact`] (no stored key exists).
    pub fn key(&self) -> Option<&Key> {
        match &self.backing {
            Backing::Scanned { key, .. } => Some(key),
            Backing::Owned(_) => None,
        }
    }

    /// The stored payload of this row (the parts of the fact the key does not
    /// carry), or `None` for a row backed by an owned [`Artifact`].
    pub fn datum(&self) -> Option<&Datum> {
        match &self.backing {
            Backing::Scanned { datum, .. } => Some(datum),
            Backing::Owned(_) => None,
        }
    }

    /// The [`Cause`] of this fact, if any, without touching the key.
    pub fn cause(&self) -> Option<&Cause> {
        match &self.backing {
            Backing::Scanned { datum, .. } => datum.cause.as_ref(),
            Backing::Owned(artifact) => artifact.cause.as_ref(),
        }
    }

    /// The raw bytes of this row's spilled value block, when the value
    /// spilled (`None` for an inline value or an owned-backed row).
    pub fn spilled(&self) -> Option<&[u8]> {
        match &self.backing {
            Backing::Scanned { spilled, .. } => spilled.as_deref(),
            Backing::Owned(_) => None,
        }
    }

    /// Parses the key into borrowed components: entity, attribute, value
    /// type, and value payload, borrowing from the key bytes (owning only an
    /// escaped entity/attribute). One walk of the key; call this once and
    /// read every field a consumer needs off the result. Errors for a row
    /// backed by an owned [`Artifact`], which has no stored key — the
    /// field accessors below work on both backings.
    pub fn parts(&self) -> Result<KeyRef<'_>, DialogArtifactsError> {
        match &self.backing {
            Backing::Scanned { key, .. } => varkey::parse_key_ref(key.as_ref()).ok_or_else(|| {
                DialogArtifactsError::InvalidKey("key did not parse into components".to_string())
            }),
            Backing::Owned(_) => Err(DialogArtifactsError::InvalidKey(
                "owned-backed view has no index key to parse".to_string(),
            )),
        }
    }

    /// The raw attribute bytes of this row (`namespace/predicate`, no alloc
    /// unless the stored key carried an escape).
    pub fn the_bytes(&self) -> Result<Cow<'_, [u8]>, DialogArtifactsError> {
        match &self.backing {
            Backing::Scanned { .. } => Ok(self.parts()?.attribute),
            Backing::Owned(artifact) => Ok(Cow::Borrowed(artifact.the.as_str().as_bytes())),
        }
    }

    /// The raw entity bytes of this row (the full URI, no alloc unless the
    /// stored key carried an escape).
    pub fn of_bytes(&self) -> Result<Cow<'_, [u8]>, DialogArtifactsError> {
        match &self.backing {
            Backing::Scanned { .. } => Ok(self.parts()?.entity),
            Backing::Owned(artifact) => Ok(Cow::Borrowed(artifact.of.as_str().as_bytes())),
        }
    }

    /// Decodes just this row's [`Value`], from the key's inline payload or
    /// from the spilled block fetched at scan time.
    pub fn value(&self) -> Result<Value, DialogArtifactsError> {
        match &self.backing {
            Backing::Scanned { spilled, .. } => decode_value_parts(&self.parts()?, spilled.clone()),
            Backing::Owned(artifact) => Ok(artifact.is.clone()),
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
    /// with, which is the tree's own order by construction. An owned-backed
    /// row derives the same key from its fields under the default manifest
    /// ([`default_sort_key`](crate::default_sort_key)); the two agree
    /// wherever the tree's manifest is the default — see `default_sort_key`'s
    /// soundness note.
    pub fn sort_key(&self) -> Result<crate::SortKey, DialogArtifactsError> {
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
            Backing::Owned(artifact) => Ok(crate::default_sort_key(artifact)),
        }
    }

    /// Materializes the full owned [`Artifact`]: entity, attribute, value,
    /// and cause. This is the whole per-row cost scans stopped paying by
    /// default — reach for it only when ownership is genuinely needed.
    pub fn to_owned(&self) -> Result<Artifact, DialogArtifactsError> {
        match &self.backing {
            Backing::Scanned { datum, spilled, .. } => {
                reconstruct(&self.parts()?, datum, spilled.clone())
            }
            Backing::Owned(artifact) => Ok(artifact.as_ref().clone()),
        }
    }

    /// The cardinality-one election: of this row and a `challenger`
    /// standing at the same `(attribute, entity)`, the row a
    /// `Cardinality::One` reader observes. The policy belongs to the value
    /// layer — the query engine folds competing rows through this method
    /// and encodes no rule of its own — so refining how concurrent edits
    /// resolve (e.g. merging by value and cause rather than electing a
    /// winner) happens here without touching the engine.
    ///
    /// The current rule: the higher cause wins, a caused row beats an
    /// uncaused one, and equal (including absent) causes fall to the fact
    /// hash. Deterministic and commutative — folding any set of rows in
    /// any order elects the same row — which is what lets every replica
    /// agree on the observed value without coordination. Causes read
    /// straight off the rows; only a genuine tie pays for the
    /// materialization the fact hash needs.
    pub fn elect(self, challenger: ArtifactView) -> Result<ArtifactView, DialogArtifactsError> {
        Ok(match (self.cause(), challenger.cause()) {
            (Some(a), Some(b)) if a > b => self,
            (Some(a), Some(b)) if a < b => challenger,
            (Some(_), None) => self,
            (None, Some(_)) => challenger,
            _ => {
                // Causes are equal: the fact hash is the deterministic
                // tiebreaker.
                if Cause::from(&self.to_owned()?) >= Cause::from(&challenger.to_owned()?) {
                    self
                } else {
                    challenger
                }
            }
        })
    }
}

/// Chainable materialization for streams of scanned rows: `.owned()` turns a
/// stream of [`ArtifactView`]s into a stream of owned [`Artifact`]s by calling
/// [`ArtifactView::to_owned`] on every row.
///
/// This is the explicit opt-in to the full per-row materialization cost —
/// scans stopped paying it by default. Prefer reading fields off the views
/// where possible; reach for `.owned()` when rows genuinely leave the scan's
/// scope (collected into results handed to a caller, serialized outward, fed
/// to an API that requires [`Artifact`]).
pub trait ArtifactViewStream:
    Stream<Item = Result<ArtifactView, DialogArtifactsError>> + Sized
{
    /// Materializes every row into an owned [`Artifact`].
    fn owned(self) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + ConditionalSend
    where
        Self: ConditionalSend,
    {
        self.map(|row| row.and_then(|view| view.to_owned()))
    }
}

impl<S> ArtifactViewStream for S where
    S: Stream<Item = Result<ArtifactView, DialogArtifactsError>> + Sized
{
}

/// Extracts the entity and attribute from a key view, decoding the raw UTF-8
/// key columns.
fn entity_attribute<K: KeyView>(key: K) -> Result<(Entity, Attribute), DialogArtifactsError> {
    let of = Entity::from_str(from_utf8(key.entity().raw()).map_err(|error| {
        DialogArtifactsError::InvalidEntity(format!("entity key is not UTF-8: {error}"))
    })?)?;
    let the = Attribute::from_str(from_utf8(key.attribute().raw()).map_err(|error| {
        DialogArtifactsError::InvalidAttribute(format!("attribute key is not UTF-8: {error}"))
    })?)?;
    Ok((of, the))
}

/// Reconstructs an [`Artifact`] from a single borrowed parse of the key's
/// components and its payload. The entity, attribute, and value type come from
/// the parsed key; the value is decoded inline from the key's payload or taken
/// from `spilled` (the archive block bytes) when it spilled.
///
/// Takes the already-parsed [`KeyRef`] so the whole reconstruction is a single
/// key walk that borrows the key bytes; see
/// [`Artifact::from_key_datum_with_value`] for why.
fn reconstruct(
    parts: &KeyRef<'_>,
    datum: &Datum,
    spilled: Option<Vec<u8>>,
) -> Result<Artifact, DialogArtifactsError> {
    let of = Entity::from_str(from_utf8(&parts.entity).map_err(|error| {
        DialogArtifactsError::InvalidEntity(format!("entity key is not UTF-8: {error}"))
    })?)?;
    let the = Attribute::from_str(from_utf8(&parts.attribute).map_err(|error| {
        DialogArtifactsError::InvalidAttribute(format!("attribute key is not UTF-8: {error}"))
    })?)?;

    let is = decode_value_parts(parts, spilled)?;

    Ok(Artifact {
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
/// [`ArtifactView::value`] access) that need the value without paying for the
/// entity and attribute materialization.
pub(crate) fn decode_value_parts(
    parts: &KeyRef<'_>,
    spilled: Option<Vec<u8>>,
) -> Result<Value, DialogArtifactsError> {
    Ok(match parts.value {
        // The key carries the value's prefix and hash; the raw value bytes
        // live in a content-addressed archive block the caller fetched and
        // passed in.
        ValueRef::Spilled { .. } => {
            let bytes = spilled.ok_or_else(|| {
                DialogArtifactsError::InvalidValue(
                    "spilled value key has no fetched block bytes".to_string(),
                )
            })?;
            Value::try_from((parts.value_type, bytes))?
        }
        // Decode the inline order-preserving value from the key.
        ValueRef::Inline(inline_payload) => {
            let (value, rest) =
                decode_value(parts.value_type, inline_payload).ok_or_else(|| {
                    DialogArtifactsError::InvalidValue(
                        "inline value payload did not decode".to_string(),
                    )
                })?;
            if !rest.is_empty() {
                return Err(DialogArtifactsError::InvalidValue(
                    "inline value payload had trailing bytes".to_string(),
                ));
            }
            value
        }
    })
}
