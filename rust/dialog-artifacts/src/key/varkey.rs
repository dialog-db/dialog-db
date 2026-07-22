//! Variable-length, lossless, order-preserving artifact key construction and
//! parsing.
//!
//! Replaces the fixed 162-byte padded key. A key is a tag byte followed by its
//! components in the ordering's byte order, each encoded with the
//! order-preserving `ordkey` discipline so that byte order equals semantic
//! order and every component is self-delimiting:
//!
//! - **entity, attribute**: `0x00`-escaped, `0x00`-terminated byte strings.
//!   Lossless (no 32-byte truncation-plus-hash) and variable-length.
//! - **value type**: one byte.
//! - **value slot**: the value's order-preserving encoding (fixed-width for
//!   numerics, escaped/terminated for byte strings). A value whose encoding
//!   exceeds the inline threshold spills: its slot carries the encoding of
//!   its first `spill_prefix` raw bytes — sorting the key INTO its type band
//!   next to inline values — and the 32-byte whole-value hash is appended at
//!   the absolute end of the key. The spill signal is exactly that
//!   remainder: zero bytes after the final component means inline, 32 means
//!   spilled. The trailing hash keeps distinct large values distinct
//!   (cardinality-many) and addresses the archive block.
//!
//! Because every component is self-delimiting, the components concatenate into
//! a key and a reader splits them back out by scanning, with no length table
//! and no fixed offsets. This module owns only the byte layout per ordering;
//! the `Key` type and `KeyView` traits are refactored onto it separately.

use std::borrow::Cow;

use crate::{
    ATTRIBUTE_KEY_TAG, BLOB_KEY_TAG, ENTITY_KEY_TAG, VALUE_KEY_TAG, ValueDataType,
    decode_bytes_cow, encode_bytes,
};

/// The length of a spilled value's content-addressed reference.
pub const VALUE_REFERENCE_LENGTH: usize = 32;

/// The payload a key carries for its value.
///
/// An inline payload is the value's complete order-preserving encoding. A
/// spilled payload (a value whose encoding exceeds the inline threshold)
/// carries the order-preserving encoding of the value's first
/// `spill_prefix` RAW bytes in the value slot — encoded exactly like an
/// inline value, so spilled values sort INTO their type band next to inline
/// ones — plus the 32-byte whole-value hash, which the key builder appends
/// at the ABSOLUTE END of the key (after the ordering's last component).
///
/// The trailing hash is what keeps distinct large values distinct keys
/// (cardinality-many: two values sharing their first `spill_prefix` bytes
/// must not collapse), and it is the block address for loading the value
/// from the archive. There is no spill flag anywhere: a key is spilled
/// exactly when 32 bytes remain after its final component parses (the final
/// component's own terminator is the signal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValuePayload {
    /// The value's order-preserving bytes, inline in the key.
    Inline(Vec<u8>),
    /// A spilled value: the encoded key-prefix of the value (self-delimiting,
    /// exactly like an inline payload) and the whole-value hash appended at
    /// the key's end.
    Spilled {
        /// Order-preserving encoding of the value's first `spill_prefix` raw
        /// bytes, including its terminator.
        prefix: Vec<u8>,
        /// The 32-byte whole-value hash (the archive block address).
        hash: Vec<u8>,
    },
}

impl ValuePayload {
    /// The bytes occupying the value SLOT of the key: the full inline
    /// encoding, or the spilled value's encoded prefix. The trailing hash of
    /// a spilled payload is NOT part of the slot; it sits at the key's end.
    pub fn slot_bytes(&self) -> &[u8] {
        match self {
            ValuePayload::Inline(bytes) => bytes,
            ValuePayload::Spilled { prefix, .. } => prefix,
        }
    }

    /// Whether this payload spilled (the key carries a prefix + trailing
    /// whole-value hash instead of the complete encoding).
    pub fn is_reference(&self) -> bool {
        matches!(self, ValuePayload::Spilled { .. })
    }
}

/// The decoded components of an artifact key, borrowed where possible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyParts {
    /// The index ordering tag.
    pub tag: u8,
    /// The entity bytes (the full URI, losslessly).
    pub entity: Vec<u8>,
    /// The attribute bytes (`namespace/predicate`).
    pub attribute: Vec<u8>,
    /// The value type.
    pub value_type: ValueDataType,
    /// The value payload: inline order-preserving bytes or a spilled reference.
    pub value: ValuePayload,
}

impl KeyParts {
    /// The minimum components for an ordering: empty entity/attribute, the
    /// minimum value type, and the minimum inline value of that type.
    ///
    /// The minimum value type ([`ValueDataType::min`], `Bytes`) is a
    /// terminated-string type, whose minimum value is the empty byte string.
    /// That encodes to a lone terminator, NOT zero bytes: an inline payload
    /// must be self-delimiting so the key round-trips (and so a `set_*` on this
    /// sentinel re-parses instead of falling back to the max parts). An empty
    /// `Vec` here would leave the value tail without a terminator and corrupt
    /// the parse of the fields that follow in the VAE ordering.
    pub fn min(tag: u8) -> Self {
        let mut value = Vec::new();
        encode_bytes(&[], &mut value);
        Self {
            tag,
            entity: Vec::new(),
            attribute: Vec::new(),
            value_type: ValueDataType::min(),
            value: ValuePayload::Inline(value),
        }
    }

    /// The maximum components for an ordering.
    ///
    /// The entity and attribute are variable-length, so there is no exact
    /// maximum; a bounded [`MAX_FILLER_BYTE`] filler is used, which dominates
    /// every UTF-8 byte and so every real entity URI and attribute name.
    /// Attributes are capped at [`ATTRIBUTE_LENGTH`](crate::ATTRIBUTE_LENGTH)
    /// (64) bytes, so the attribute filler is exact for them; entities are
    /// unbounded, so an entity upper bound is only exact once its true value
    /// or prefix is set.
    // TODO(m3): a variable-length ordering has no representable inclusive
    // maximum for an unbounded trailing field (a value- or attribute-only scan
    // whose trailing entity is unconstrained). This filler dominates all
    // realistic entities but a pathologically long entity could exceed it.
    // Revisit once selector ranges can express an exclusive (prefix-successor)
    // upper bound instead of `RangeInclusive<Key>`.
    pub fn max(tag: u8) -> Self {
        Self {
            tag,
            entity: vec![MAX_FILLER_BYTE; MAX_FILLER],
            attribute: vec![MAX_FILLER_BYTE; MAX_FILLER],
            value_type: ValueDataType::max(),
            // A parseable payload dominating every real value of the maximum
            // type: `MAX_FILLER_BYTE` exceeds every UTF-8 byte, and real
            // symbol names are UTF-8. (Like the entity/attribute fillers this
            // is a bounded synthetic maximum; `set_value_*` replaces it with a
            // real payload — this is only the unset bound.)
            value: ValuePayload::Inline({
                let mut payload = Vec::new();
                encode_bytes(&[MAX_FILLER_BYTE; MAX_FILLER], &mut payload);
                payload
            }),
        }
    }
}

/// Filler length for the maximum of a variable-length field. Comfortably
/// exceeds the 64-byte attribute cap and any realistic entity URI.
const MAX_FILLER: usize = 256;

/// Filler byte for the maximum of a variable-length field.
///
/// `0xFE`, NOT `0xFF`: the `ordkey` byte-string encoding escapes a content
/// zero as `0x00 0xFF`, so a field terminator (`0x00`) followed by a next
/// field starting with `0xFF` reads back as an escaped zero and the key
/// stops being parseable. A `0xFF` filler therefore poisons every key built
/// from max parts: the first `set_*` on such a bound parses fine, but the
/// key it builds cannot be re-parsed, so the next `set_*` falls back to the
/// max parts and silently discards the previously-set fields. (This exact
/// failure made a `Replace` supersede-scan's upper bound lose its entity,
/// widening the scan to all entities and deleting unrelated facts.)
///
/// The general invariant: no field may begin with the escape byte `0xFF`.
/// Real fields hold UTF-8 text (bytes `<= 0xF4`) or small tag bytes, so
/// `0xFE` both respects the invariant and still dominates every real value.
const MAX_FILLER_BYTE: u8 = 0xFE;

/// One decodable field of a key, addressed by role rather than by byte offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    /// The entity component.
    Entity,
    /// The attribute component.
    Attribute,
}

/// Borrows a variable-length field's *raw* bytes (its encoded segment with the
/// trailing terminator stripped) from a key.
///
/// Entities and attributes are NUL-free UTF-8, so their encoded segment never
/// contains an escaped zero and the raw bytes are exactly the segment minus its
/// terminator, a sub-slice of `bytes`. A key that does not split cleanly (a
/// `min`/`max` sentinel) yields an empty slice.
pub fn field(bytes: &[u8], _tag: u8, which: Field) -> &[u8] {
    let index = match (bytes.first().copied(), which) {
        (Some(ENTITY_KEY_TAG), Field::Entity) => 1,
        (Some(ENTITY_KEY_TAG), Field::Attribute) => 2,
        (Some(ATTRIBUTE_KEY_TAG), Field::Attribute) => 1,
        (Some(ATTRIBUTE_KEY_TAG), Field::Entity) => 2,
        (Some(VALUE_KEY_TAG), Field::Attribute) => 3,
        (Some(VALUE_KEY_TAG), Field::Entity) => 4,
        _ => return &[],
    };
    match split_components(bytes) {
        Some(slices) => slices
            .get(index)
            .map(|s| {
                // The LAST component of a spilled key carries the trailing
                // whole-value hash folded into its slice; take only the
                // terminated segment.
                let len = terminated_len(s, 0).unwrap_or(s.len());
                strip_terminator(&s[..len])
            })
            .unwrap_or(&[]),
        None => &[],
    }
}

/// The value type of a key for the given ordering, or the minimum when the
/// key does not parse.
pub fn value_type(bytes: &[u8], _tag: u8) -> ValueDataType {
    parse_key_ref(bytes)
        .map(|parts| parts.value_type)
        .unwrap_or_else(ValueDataType::min)
}

/// Whether a key's value spilled (the key carries the value's encoded prefix
/// in the slot plus a trailing whole-value hash).
pub fn value_is_spilled(bytes: &[u8], _tag: u8) -> bool {
    parse_key_ref(bytes)
        .map(|parts| parts.value.is_reference())
        .unwrap_or(false)
}

/// Borrows the bytes occupying a key's value SLOT (the full inline encoding,
/// or a spilled value's encoded prefix), or an empty slice when the key does
/// not parse. A spilled key's trailing hash is available via
/// [`value_spill_hash`].
pub fn value_payload(bytes: &[u8], _tag: u8) -> &[u8] {
    match parse_key_ref(bytes) {
        Some(parts) => match parts.value {
            ValueRef::Inline(payload) => payload,
            ValueRef::Spilled { prefix, .. } => prefix,
        },
        None => &[],
    }
}

/// Borrows the 32-byte whole-value hash trailing a spilled key, or `None` for
/// an inline (or unparseable) key.
pub fn value_spill_hash(bytes: &[u8], _tag: u8) -> Option<&[u8]> {
    match parse_key_ref(bytes)?.value {
        ValueRef::Inline(_) => None,
        ValueRef::Spilled { hash, .. } => Some(hash),
    }
}

/// Strips a lone trailing terminator from an encoded, escape-free byte-string
/// segment.
fn strip_terminator(segment: &[u8]) -> &[u8] {
    match segment.split_last() {
        Some((&0x00, head)) => head,
        _ => segment,
    }
}

/// Writes the value SLOT: the type byte followed by the slot payload (the
/// full inline encoding, or a spilled value's encoded prefix — byte-identical
/// in form, so spilled values sort next to inline ones).
///
/// The slot is self-delimiting for a reader that reads the type byte first:
/// the payload is an order-preserving value encoding, fixed-width for
/// numerics and `0x00`-terminated for strings/bytes. So the slot may sit in a
/// non-terminal key position (the VAE ordering). A spilled value's trailing
/// hash is NOT written here; [`build_key`] appends it at the key's end.
fn write_value_slot(parts: &KeyParts, out: &mut Vec<u8>) {
    out.push(parts.value_type.into());
    out.extend_from_slice(parts.value.slot_bytes());
}

/// Builds the key bytes for a given ordering tag from the components, encoding
/// each in the ordering's byte order.
///
/// A spilled value's 32-byte whole-value hash goes at the ABSOLUTE END of the
/// key, after the ordering's last component: the key parses exactly like an
/// inline key and the 32-byte remainder after the final component is the
/// spill signal. Placing it last (rather than beside the value slot) keeps
/// the ordering's interior components (attribute, entity in VAE) sub-sorting
/// normally within a shared-prefix cluster; the hash only tie-breaks.
pub fn build_key(parts: &KeyParts) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(parts.tag);
    match parts.tag {
        ENTITY_KEY_TAG => {
            // EAV: entity, attribute, value slot
            encode_bytes(&parts.entity, &mut out);
            encode_bytes(&parts.attribute, &mut out);
            write_value_slot(parts, &mut out);
        }
        ATTRIBUTE_KEY_TAG => {
            // AEV: attribute, entity, value slot
            encode_bytes(&parts.attribute, &mut out);
            encode_bytes(&parts.entity, &mut out);
            write_value_slot(parts, &mut out);
        }
        VALUE_KEY_TAG => {
            // VAE: value slot, attribute, entity. The value slot leads so the
            // ordering sorts by value; it is self-delimiting, so the attribute
            // and entity that follow are still recoverable.
            write_value_slot(parts, &mut out);
            encode_bytes(&parts.attribute, &mut out);
            encode_bytes(&parts.entity, &mut out);
        }
        _ => {
            // Unknown/other tags: entity then attribute then value slot, a
            // safe default that stays self-delimiting.
            encode_bytes(&parts.entity, &mut out);
            encode_bytes(&parts.attribute, &mut out);
            write_value_slot(parts, &mut out);
        }
    }
    if let ValuePayload::Spilled { hash, .. } = &parts.value {
        out.extend_from_slice(hash);
    }
    out
}

/// The length of a value slot payload starting at `at`, given its `type_byte`.
/// The payload is self-delimited by the order-preserving value encoding:
/// numerics are fixed-width and strings/bytes are `0x00`-terminated, mirroring
/// the [`ordvalue`](crate::artifacts::ordvalue) decoders without allocating.
/// (A spilled value's slot holds its encoded prefix, delimited identically.)
fn value_payload_len(type_byte: u8, bytes: &[u8], at: usize) -> Option<usize> {
    // An unknown discriminant is corruption: reject the key rather than
    // guessing a width (`ValueDataType::from` would silently default to
    // Bytes and misparse the tail).
    if type_byte > u8::from(ValueDataType::Symbol) {
        return None;
    }
    match ValueDataType::from(type_byte) {
        // 128-bit integers: 16 big-endian bytes.
        ValueDataType::UnsignedInt | ValueDataType::SignedInt => Some(16),
        // `f64`: 8 big-endian bytes (the order-preserving `encode_f64`), NOT 16
        // — reading 16 here over-runs the value tail into the following key
        // components, so the key splits into fewer parts than its schema.
        ValueDataType::Float => Some(8),
        // A single byte.
        ValueDataType::Boolean => Some(1),
        // `0x00`-escaped, `0x00`-terminated byte strings.
        ValueDataType::String
        | ValueDataType::Bytes
        | ValueDataType::Record
        | ValueDataType::Entity
        | ValueDataType::Symbol => terminated_len(bytes, at),
    }
}

/// The length of one `0x00`-escaped, `0x00`-terminated byte string starting at
/// `at`, including its escapes and terminator.
fn terminated_len(bytes: &[u8], mut at: usize) -> Option<usize> {
    let start = at;
    while at < bytes.len() {
        if bytes[at] == 0x00 {
            match bytes.get(at + 1) {
                Some(0xFF) => at += 2,
                _ => return Some(at + 1 - start),
            }
        } else {
            at += 1;
        }
    }
    None
}

/// Splits raw key bytes into the *encoded* component slices for its ordering,
/// in the order they contribute to key comparison. Every returned slice
/// borrows from `bytes`, and their concatenation equals `bytes` exactly (the
/// escaped/terminated form is preserved, nothing is decoded). This is what the
/// columnar leaf codec consumes: it matches the ordering's [`Schema`] one slice
/// per component (`tag`, then the ordering's fields).
///
/// Returns `None` on malformed input, in which case the caller falls back to
/// the opaque whole-key component.
pub fn split_components(bytes: &[u8]) -> Option<Vec<&[u8]>> {
    let (tag, _) = bytes.split_first()?;

    let mut out: Vec<&[u8]> = Vec::with_capacity(5);
    out.push(&bytes[0..1]);
    let mut at = 1;

    // Push one variable-length encoded string component starting at `at`.
    macro_rules! push_var {
        () => {{
            let len = terminated_len(bytes, at)?;
            out.push(&bytes[at..at + len]);
            at += len;
        }};
    }

    // Push the value slot as two components (type byte, then payload). The
    // payload is self-delimited by its value encoding; a spilled value's slot
    // holds its encoded prefix, delimited identically.
    macro_rules! push_value_tail {
        () => {{
            let &type_byte = bytes.get(at)?;
            let payload_len = value_payload_len(type_byte, bytes, at + 1)?;
            let end = (at + 1).checked_add(payload_len)?;
            if end > bytes.len() {
                return None;
            }
            out.push(&bytes[at..at + 1]);
            out.push(&bytes[at + 1..end]);
            at = end;
        }};
    }

    match *tag {
        ENTITY_KEY_TAG => {
            push_var!(); // entity
            push_var!(); // attribute
            push_value_tail!();
        }
        ATTRIBUTE_KEY_TAG => {
            push_var!(); // attribute
            push_var!(); // entity
            push_value_tail!();
        }
        VALUE_KEY_TAG => {
            push_value_tail!();
            push_var!(); // attribute
            push_var!(); // entity
        }
        BLOB_KEY_TAG => {
            // The blob index is `tag ++ 32-byte hash`: two components (tag
            // dictionary, hash arena) matching BLOB_SCHEMA, so the columnar
            // codec's component count agrees with the split.
            out.push(&bytes[at..]);
            at = bytes.len();
        }
        _ => return None,
    }

    // A spilled key carries the 32-byte whole-value hash after its final
    // component (the spill signal is exactly this remainder). Fold it into
    // the LAST component's slice: arena columns are length-delimited by the
    // leaf codec (not by terminators), so the fold round-trips byte-exactly,
    // and the component count stays fixed per schema whether or not the
    // value spilled.
    match bytes.len() - at {
        0 => Some(out),
        VALUE_REFERENCE_LENGTH => {
            let last = out.pop()?;
            let start = bytes.len() - VALUE_REFERENCE_LENGTH - last.len();
            out.push(&bytes[start..]);
            Some(out)
        }
        _ => None,
    }
}

/// Parses key bytes back into owned components, dispatching on the tag.
/// Returns `None` on malformed input (missing terminator, short value tail).
///
/// The owned counterpart of [`parse_key_ref`], implemented on top of it so
/// the two can never diverge; use `parse_key_ref` on read/scan paths that can
/// borrow.
pub fn parse_key(bytes: &[u8]) -> Option<KeyParts> {
    let parts = parse_key_ref(bytes)?;
    Some(KeyParts {
        tag: parts.tag,
        entity: parts.entity.into_owned(),
        attribute: parts.attribute.into_owned(),
        value_type: parts.value_type,
        value: match parts.value {
            ValueRef::Inline(payload) => ValuePayload::Inline(payload.to_vec()),
            ValueRef::Spilled { prefix, hash } => ValuePayload::Spilled {
                prefix: prefix.to_vec(),
                hash: hash.to_vec(),
            },
        },
    })
}

/// A key's value payload, borrowed from the key bytes where possible. The
/// borrowed analogue of [`ValuePayload`] for the read/scan path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueRef<'a> {
    /// The value's inline order-preserving bytes, borrowed from the key.
    Inline(&'a [u8]),
    /// A spilled value: the value's encoded key-prefix (occupying the value
    /// slot exactly like an inline payload) and the 32-byte whole-value hash
    /// from the key's end, both borrowed.
    Spilled {
        /// The order-preserving encoding of the value's leading raw bytes.
        prefix: &'a [u8],
        /// The 32-byte whole-value hash (the archive block address).
        hash: &'a [u8],
    },
}

impl ValueRef<'_> {
    /// The bytes occupying the value SLOT of the key: the full inline
    /// encoding, or the spilled value's encoded prefix.
    pub fn slot_bytes(&self) -> &[u8] {
        match self {
            ValueRef::Inline(bytes) => bytes,
            ValueRef::Spilled { prefix, .. } => prefix,
        }
    }

    /// Whether this payload spilled (the key carries a prefix + trailing
    /// whole-value hash instead of the complete encoding).
    pub fn is_reference(&self) -> bool {
        matches!(self, ValueRef::Spilled { .. })
    }

    /// The 32-byte whole-value hash of a spilled payload (the archive block
    /// address), or `None` for an inline payload.
    pub fn spill_hash(&self) -> Option<&[u8]> {
        match self {
            ValueRef::Inline(_) => None,
            ValueRef::Spilled { hash, .. } => Some(hash),
        }
    }
}

/// The components of an artifact key, borrowed from the key bytes where
/// possible. The borrowed, allocation-light analogue of [`KeyParts`] for the
/// read/scan path: entity and attribute are [`Cow`]s that borrow the key bytes
/// when escape-free (the norm for UTF-8 entities/attributes) and own only when
/// a `0x00 0xFF` escape had to be resolved; the value payload always borrows.
///
/// A scan parses each key ONCE into a `KeyRef` and threads it through matching,
/// spill resolution, and reconstruction, so the whole per-entry key handling is
/// a single walk with no intermediate copies (contrast the owned [`KeyParts`],
/// used for key *construction*).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRef<'a> {
    /// The index ordering tag.
    pub tag: u8,
    /// The entity bytes (the full URI, losslessly).
    pub entity: Cow<'a, [u8]>,
    /// The attribute bytes (`namespace/predicate`).
    pub attribute: Cow<'a, [u8]>,
    /// The value type.
    pub value_type: ValueDataType,
    /// The value payload: inline order-preserving bytes or a spilled reference.
    pub value: ValueRef<'a>,
}

/// Parses key bytes into borrowed components, dispatching on the tag. Like
/// [`parse_key`] but borrows the entity/attribute/value bytes from `bytes`
/// (owning only an escaped entity/attribute), so a scan reconstructs entries
/// without per-field allocation. Returns `None` on malformed input.
///
/// The spill signal is the REMAINDER after the ordering's final component:
/// zero bytes left means an inline value, exactly 32 means the value spilled
/// and the remainder is its whole-value hash (the slot then holds the
/// value's encoded prefix). Any other remainder is malformed.
pub fn parse_key_ref(bytes: &[u8]) -> Option<KeyRef<'_>> {
    let (&tag, rest) = bytes.split_first()?;

    // Reads the value slot borrowed from the key: one type byte plus its
    // self-delimiting payload slice (full inline encoding or spilled prefix).
    fn value_slot(bytes: &[u8]) -> Option<(ValueDataType, &[u8], &[u8])> {
        let (&type_byte, rest) = bytes.split_first()?;
        let payload_len = value_payload_len(type_byte, bytes, 1)?;
        let (payload, rest) = rest.split_at_checked(payload_len)?;
        Some((ValueDataType::from(type_byte), payload, rest))
    }

    let (entity, attribute, value_type, payload, rest) = match tag {
        ATTRIBUTE_KEY_TAG => {
            let (attribute, rest) = decode_bytes_cow(rest)?;
            let (entity, rest) = decode_bytes_cow(rest)?;
            let (value_type, payload, rest) = value_slot(rest)?;
            (entity, attribute, value_type, payload, rest)
        }
        VALUE_KEY_TAG => {
            let (value_type, payload, rest) = value_slot(rest)?;
            let (attribute, rest) = decode_bytes_cow(rest)?;
            let (entity, rest) = decode_bytes_cow(rest)?;
            (entity, attribute, value_type, payload, rest)
        }
        // ENTITY_KEY_TAG and unknown tags share the EAV shape, a safe
        // self-delimiting default.
        _ => {
            let (entity, rest) = decode_bytes_cow(rest)?;
            let (attribute, rest) = decode_bytes_cow(rest)?;
            let (value_type, payload, rest) = value_slot(rest)?;
            (entity, attribute, value_type, payload, rest)
        }
    };

    let value = match rest.len() {
        0 => ValueRef::Inline(payload),
        VALUE_REFERENCE_LENGTH => ValueRef::Spilled {
            prefix: payload,
            hash: rest,
        },
        _ => return None,
    };
    Some(KeyRef {
        tag,
        entity,
        attribute,
        value_type,
        value,
    })
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]
    // The dialog_common::test macro requires async test fns; these pure-codec
    // tests await nothing.
    #![allow(clippy::unused_async)]

    use super::*;
    use crate::{ATTRIBUTE_KEY_TAG, ENTITY_KEY_TAG, VALUE_KEY_TAG};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// An inline order-preserving String payload, so a round-trip through
    /// `build_key`/`parse_key` reproduces it exactly.
    fn inline_string(text: &str) -> ValuePayload {
        let mut bytes = Vec::new();
        crate::encode_bytes(text.as_bytes(), &mut bytes);
        ValuePayload::Inline(bytes)
    }

    fn parts(tag: u8, entity: &[u8], attribute: &[u8], value: u8) -> KeyParts {
        KeyParts {
            tag,
            entity: entity.to_vec(),
            attribute: attribute.to_vec(),
            value_type: ValueDataType::String,
            value: inline_string(&format!("v{value}")),
        }
    }

    /// Every ordering round-trips build -> parse unchanged, including
    /// variable-length entities and attributes of different lengths.
    #[dialog_common::test]
    async fn it_round_trips_every_ordering() -> anyhow::Result<()> {
        for tag in [ENTITY_KEY_TAG, ATTRIBUTE_KEY_TAG, VALUE_KEY_TAG] {
            for (entity, attribute) in [
                (b"entity:short".as_slice(), b"person/age".as_slice()),
                (
                    b"entity:a-much-longer-uri-that-exceeds-thirty-two-bytes-easily".as_slice(),
                    b"namespace/some-long-predicate-name".as_slice(),
                ),
                (b"".as_slice(), b"a/b".as_slice()),
            ] {
                let original = parts(tag, entity, attribute, 7);
                let bytes = build_key(&original);
                let parsed = parse_key(&bytes).expect("parses");
                assert_eq!(parsed, original, "tag {tag} round-trip");
            }
        }
        Ok(())
    }

    /// A spilled payload round-trips through every ordering: the value slot
    /// parses exactly like an inline payload, the trailing 32-byte hash is
    /// recovered as the spill signal, and `split_components` covers every
    /// byte with the hash folded into the final component's slice.
    #[dialog_common::test]
    async fn it_round_trips_spilled_keys_in_every_ordering() -> anyhow::Result<()> {
        let mut prefix = Vec::new();
        crate::encode_bytes(b"leading-bytes-of-a-large-value", &mut prefix);
        let spilled = KeyParts {
            tag: ENTITY_KEY_TAG,
            entity: b"entity:doc".to_vec(),
            attribute: b"doc/body".to_vec(),
            value_type: ValueDataType::String,
            value: ValuePayload::Spilled {
                prefix,
                hash: vec![0xAB; VALUE_REFERENCE_LENGTH],
            },
        };
        for tag in [ENTITY_KEY_TAG, ATTRIBUTE_KEY_TAG, VALUE_KEY_TAG] {
            let mut original = spilled.clone();
            original.tag = tag;
            let bytes = build_key(&original);
            assert_eq!(
                &bytes[bytes.len() - VALUE_REFERENCE_LENGTH..],
                &[0xABu8; VALUE_REFERENCE_LENGTH],
                "tag {tag}: the hash trails the key"
            );
            let parsed = parse_key(&bytes).expect("spilled key parses");
            assert_eq!(parsed, original, "tag {tag} spilled round-trip");

            let slices = split_components(&bytes).expect("splits with full coverage");
            let total: usize = slices.iter().map(|slice| slice.len()).sum();
            assert_eq!(total, bytes.len(), "tag {tag}: slices cover the key");
        }
        Ok(())
    }

    /// The min sentinel round-trips build -> parse for every ordering. Its
    /// value tail is an empty *inline* value, which must be self-delimiting (a
    /// lone terminator) so the fields after it in the VAE ordering still parse;
    /// an empty payload would corrupt the parse and drop a `set_*` back to the
    /// max parts, silently widening a value-scan's lower bound.
    #[dialog_common::test]
    async fn it_round_trips_the_min_sentinel() -> anyhow::Result<()> {
        for tag in [ENTITY_KEY_TAG, ATTRIBUTE_KEY_TAG, VALUE_KEY_TAG] {
            let min = KeyParts::min(tag);
            let bytes = build_key(&min);
            let parsed = parse_key(&bytes).expect("min sentinel parses");
            assert_eq!(parsed, min, "tag {tag} min round-trip");
        }
        Ok(())
    }

    /// The full entity is preserved even when longer than 32 bytes (the old
    /// fixed layout truncated-and-hashed past 32; this does not).
    #[dialog_common::test]
    async fn it_preserves_full_long_entities() -> anyhow::Result<()> {
        let long = b"entity:this-uri-is-definitely-more-than-thirty-two-bytes-long-xyz";
        assert!(long.len() > 32);
        let original = parts(ENTITY_KEY_TAG, long, b"a/b", 1);
        let parsed = parse_key(&build_key(&original)).unwrap();
        assert_eq!(parsed.entity, long.to_vec());
        Ok(())
    }

    /// In EAV order, keys sort by entity then attribute; the terminator keeps
    /// the ordering prefix-safe (the "car"/"carpet" entity case).
    #[dialog_common::test]
    async fn it_orders_eav_by_entity_then_attribute() -> anyhow::Result<()> {
        let car = build_key(&parts(ENTITY_KEY_TAG, b"car", b"z/z", 0));
        let carpet = build_key(&parts(ENTITY_KEY_TAG, b"carpet", b"a/a", 0));
        // "car" < "carpet" as entities, and this must hold despite carpet's
        // attribute sorting before car's: the entity terminator decides first.
        assert!(car < carpet, "entity order must dominate the attribute");

        let e1_a = build_key(&parts(ENTITY_KEY_TAG, b"e1", b"a/a", 0));
        let e1_b = build_key(&parts(ENTITY_KEY_TAG, b"e1", b"b/b", 0));
        assert!(e1_a < e1_b, "same entity sorts by attribute");
        Ok(())
    }

    /// Repro: an AEV attribute-only scan range must contain every stored AEV
    /// key for that attribute, regardless of the entity's bytes. The range end
    /// uses the `max` entity filler; a stored key whose entity encoding sorts
    /// above that filler would fall outside the range and be silently dropped.
    #[dialog_common::test]
    async fn it_contains_every_stored_aev_key_in_attribute_scan() -> anyhow::Result<()> {
        use crate::ATTRIBUTE_KEY_TAG;

        let attribute = b"person/name";

        // The attribute-only scan range, as `selector_range` builds it: min/max
        // parts with the attribute set (both sentinels parse — the `0xFE`
        // filler is a valid field — but building the parts directly keeps the
        // fixture independent of the `set_*` chain).
        let mut start_parts = KeyParts::min(ATTRIBUTE_KEY_TAG);
        start_parts.attribute = attribute.to_vec();
        let start = build_key(&start_parts);

        let mut end_parts = KeyParts::max(ATTRIBUTE_KEY_TAG);
        end_parts.attribute = attribute.to_vec();
        let end = build_key(&end_parts);

        // A spread of entity byte patterns: ASCII DIDs plus the highest legal
        // UTF-8 bytes (`0xF4 0x8F 0xBF 0xBF`, U+10FFFF) to probe the filler
        // boundary. Entities are UTF-8 URIs, so `0xFE`/`0xFF` bytes cannot
        // occur in real fields (the composition invariant `MAX_FILLER_BYTE`
        // documents).
        let entities: &[&[u8]] = &[
            b"did:key:z6MkfrQf",
            b"did:key:z6Mkzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            b"\xf4\x8f\xbf\xbf",
            b"a",
        ];

        for entity in entities {
            let mut stored = KeyParts::min(ATTRIBUTE_KEY_TAG);
            stored.attribute = attribute.to_vec();
            stored.entity = entity.to_vec();
            stored.value_type = ValueDataType::String;
            stored.value = inline_string("Alice");
            let key = build_key(&stored);

            assert!(
                start.as_slice() <= key.as_slice(),
                "stored key sorts below range start for entity {entity:?}"
            );
            assert!(
                key.as_slice() <= end.as_slice(),
                "stored key sorts ABOVE range end for entity {entity:?}\n  end={end:02x?}\n  key={key:02x?}"
            );
        }
        Ok(())
    }

    /// A key built from max parts must PARSE, and it must keep parsing as
    /// fields are set one at a time. This is the `set_entity(..).set_attribute(..)`
    /// chain that builds a `Replace` supersede-scan's upper bound: with an
    /// unparseable sentinel, the second `set_*` falls back to max parts and
    /// silently discards the entity, widening the scan to ALL entities — which
    /// deleted unrelated facts (the two-commit data-loss bug).
    #[dialog_common::test]
    async fn it_keeps_set_fields_across_a_max_bound_chain() -> anyhow::Result<()> {
        for tag in [ENTITY_KEY_TAG, ATTRIBUTE_KEY_TAG, VALUE_KEY_TAG] {
            // The raw max sentinel parses.
            let max = build_key(&KeyParts::max(tag));
            let parsed = parse_key(&max).expect("max sentinel parses");
            assert_eq!(parsed, KeyParts::max(tag), "tag {tag} max round-trip");

            // Setting the entity then re-parsing preserves it.
            let mut parts = parse_key(&max).unwrap();
            parts.entity = b"did:key:z6MkExample".to_vec();
            let with_entity = build_key(&parts);
            let reparsed = parse_key(&with_entity).expect("entity-set max parses");
            assert_eq!(reparsed.entity, parts.entity, "tag {tag} entity survives");

            // Setting the attribute afterwards preserves the entity.
            let mut parts = reparsed;
            parts.attribute = b"person/name".to_vec();
            let with_both = build_key(&parts);
            let reparsed = parse_key(&with_both).expect("both-set max parses");
            assert_eq!(
                reparsed.entity, parts.entity,
                "tag {tag} entity survives the attribute set"
            );
            assert_eq!(reparsed.attribute, parts.attribute);
        }
        Ok(())
    }

    /// The max filler dominates every real entity and attribute byte-wise:
    /// scan upper bounds built from it sort at or above every stored key.
    #[dialog_common::test]
    async fn it_max_dominates_real_keys() -> anyhow::Result<()> {
        let real = parts(
            ATTRIBUTE_KEY_TAG,
            b"did:key:z6MkQmQKzPsjyUz49pvaxYdiiZEuQXyNqeBkS88GTrvqnov",
            b"person/name",
            0xFF,
        );
        let mut end = KeyParts::max(ATTRIBUTE_KEY_TAG);
        end.attribute = b"person/name".to_vec();
        assert!(
            build_key(&real) <= build_key(&end),
            "attribute-scan end must dominate every same-attribute key"
        );
        Ok(())
    }

    /// Malformed keys (missing terminator, short value tail) parse to None.
    #[dialog_common::test]
    async fn it_rejects_malformed_keys() -> anyhow::Result<()> {
        assert!(parse_key(&[]).is_none(), "empty");
        // Tag then an unterminated entity string.
        assert!(
            parse_key(&[ENTITY_KEY_TAG, b'a', b'b']).is_none(),
            "unterminated entity"
        );
        // Tag, terminated entity + attribute, but a truncated value tail.
        let mut bytes = vec![ENTITY_KEY_TAG];
        encode_bytes(b"e", &mut bytes);
        encode_bytes(b"a/b", &mut bytes);
        bytes.push(3); // value type but no 32-byte reference
        assert!(parse_key(&bytes).is_none(), "short value tail");
        Ok(())
    }
}
