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
//! - **value reference**: 32 bytes (kept for now; the inline order-preserving
//!   value replaces it in a later step).
//!
//! Because every component is self-delimiting, the components concatenate into
//! a key and a reader splits them back out by scanning, with no length table
//! and no fixed offsets. This module owns only the byte layout per ordering;
//! the `Key` type and `KeyView` traits are refactored onto it separately.

use crate::{
    ATTRIBUTE_KEY_TAG, BLOB_KEY_TAG, ENTITY_KEY_TAG, VALUE_KEY_TAG, ValueDataType, decode_bytes,
    encode_bytes,
};

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
    /// The 32-byte value reference.
    pub value_reference: Vec<u8>,
}

impl KeyParts {
    /// The minimum components for an ordering: empty entity/attribute, the
    /// minimum value type, and an all-zero value reference.
    pub fn min(tag: u8) -> Self {
        Self {
            tag,
            entity: Vec::new(),
            attribute: Vec::new(),
            value_type: ValueDataType::min(),
            value_reference: vec![0u8; 32],
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
            value_reference: vec![0xFFu8; 32],
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
            .map(|s| strip_terminator(s))
            .unwrap_or(&[]),
        None => &[],
    }
}

/// The value type byte of a key for the given ordering, or the minimum when the
/// key does not split cleanly.
pub fn value_type(bytes: &[u8], _tag: u8) -> ValueDataType {
    let index = match bytes.first().copied() {
        Some(ENTITY_KEY_TAG) | Some(ATTRIBUTE_KEY_TAG) => 3,
        Some(VALUE_KEY_TAG) => 1,
        _ => return ValueDataType::min(),
    };
    match split_components(bytes) {
        Some(slices) => slices
            .get(index)
            .and_then(|s| s.first())
            .map(|&b| ValueDataType::from(b))
            .unwrap_or_else(ValueDataType::min),
        None => ValueDataType::min(),
    }
}

/// Borrows the 32-byte value reference of a key, or a zero array when the key
/// does not split cleanly.
pub fn value_reference(bytes: &[u8], _tag: u8) -> &[u8; 32] {
    const ZERO: [u8; 32] = [0u8; 32];
    let index = match bytes.first().copied() {
        Some(ENTITY_KEY_TAG) | Some(ATTRIBUTE_KEY_TAG) => 4,
        Some(VALUE_KEY_TAG) => 2,
        _ => return &ZERO,
    };
    match split_components(bytes) {
        Some(slices) => slices
            .get(index)
            .and_then(|s| <&[u8; 32]>::try_from(*s).ok())
            .unwrap_or(&ZERO),
        None => &ZERO,
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

/// Builds the key bytes for a given ordering tag from the components, encoding
/// each in the ordering's byte order.
pub fn build_key(parts: &KeyParts) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(parts.tag);
    match parts.tag {
        ENTITY_KEY_TAG => {
            // EAV: entity, attribute, value_type, value_reference
            encode_bytes(&parts.entity, &mut out);
            encode_bytes(&parts.attribute, &mut out);
            out.push(parts.value_type.into());
            out.extend_from_slice(&parts.value_reference);
        }
        ATTRIBUTE_KEY_TAG => {
            // AEV: attribute, entity, value_type, value_reference
            encode_bytes(&parts.attribute, &mut out);
            encode_bytes(&parts.entity, &mut out);
            out.push(parts.value_type.into());
            out.extend_from_slice(&parts.value_reference);
        }
        VALUE_KEY_TAG => {
            // VAE: value_type, value_reference, attribute, entity
            out.push(parts.value_type.into());
            out.extend_from_slice(&parts.value_reference);
            encode_bytes(&parts.attribute, &mut out);
            encode_bytes(&parts.entity, &mut out);
        }
        _ => {
            // Unknown/other tags: entity then attribute then value tail, a
            // safe default that stays self-delimiting.
            encode_bytes(&parts.entity, &mut out);
            encode_bytes(&parts.attribute, &mut out);
            out.push(parts.value_type.into());
            out.extend_from_slice(&parts.value_reference);
        }
    }
    out
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

    // The length of one order-preserving byte string starting at `at`,
    // including its escapes and terminator. Mirrors `decode_bytes` scanning
    // without allocating.
    fn encoded_len(bytes: &[u8], mut at: usize) -> Option<usize> {
        let start = at;
        while at < bytes.len() {
            if bytes[at] == 0x00 {
                match bytes.get(at + 1) {
                    // Escaped zero: `0x00 0xFF` is two bytes of content.
                    Some(0xFF) => at += 2,
                    // Lone terminator ends the string.
                    _ => return Some(at + 1 - start),
                }
            } else {
                at += 1;
            }
        }
        None
    }

    // The one-byte value type plus the 32-byte value reference.
    const VALUE_TAIL: usize = 1 + 32;

    let mut out: Vec<&[u8]> = Vec::with_capacity(5);
    out.push(&bytes[0..1]);
    let mut at = 1;

    // Push one variable-length encoded string component starting at `at`.
    macro_rules! push_var {
        () => {{
            let len = encoded_len(bytes, at)?;
            out.push(&bytes[at..at + len]);
            at += len;
        }};
    }

    // Push the fixed value tail (value type + value reference).
    macro_rules! push_value_tail {
        () => {{
            let end = at.checked_add(VALUE_TAIL)?;
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

    if at == bytes.len() { Some(out) } else { None }
}

/// Parses key bytes back into components, dispatching on the tag. Returns
/// `None` on malformed input (missing terminator, short value tail).
pub fn parse_key(bytes: &[u8]) -> Option<KeyParts> {
    let (&tag, rest) = bytes.split_first()?;

    // Reads a fixed value tail: one type byte plus a 32-byte reference.
    fn value_tail(bytes: &[u8]) -> Option<(ValueDataType, Vec<u8>, &[u8])> {
        let (&type_byte, rest) = bytes.split_first()?;
        let (reference, rest) = rest.split_at_checked(32)?;
        Some((ValueDataType::from(type_byte), reference.to_vec(), rest))
    }

    let parts = match tag {
        ENTITY_KEY_TAG => {
            let (entity, rest) = decode_bytes(rest)?;
            let (attribute, rest) = decode_bytes(rest)?;
            let (value_type, value_reference, _rest) = value_tail(rest)?;
            KeyParts {
                tag,
                entity,
                attribute,
                value_type,
                value_reference,
            }
        }
        ATTRIBUTE_KEY_TAG => {
            let (attribute, rest) = decode_bytes(rest)?;
            let (entity, rest) = decode_bytes(rest)?;
            let (value_type, value_reference, _rest) = value_tail(rest)?;
            KeyParts {
                tag,
                entity,
                attribute,
                value_type,
                value_reference,
            }
        }
        VALUE_KEY_TAG => {
            let (&type_byte, rest) = rest.split_first()?;
            let (reference, rest) = rest.split_at_checked(32)?;
            let (attribute, rest) = decode_bytes(rest)?;
            let (entity, _rest) = decode_bytes(rest)?;
            KeyParts {
                tag,
                entity,
                attribute,
                value_type: ValueDataType::from(type_byte),
                value_reference: reference.to_vec(),
            }
        }
        _ => {
            let (entity, rest) = decode_bytes(rest)?;
            let (attribute, rest) = decode_bytes(rest)?;
            let (value_type, value_reference, _rest) = value_tail(rest)?;
            KeyParts {
                tag,
                entity,
                attribute,
                value_type,
                value_reference,
            }
        }
    };
    Some(parts)
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

    fn parts(tag: u8, entity: &[u8], attribute: &[u8], value: u8) -> KeyParts {
        KeyParts {
            tag,
            entity: entity.to_vec(),
            attribute: attribute.to_vec(),
            value_type: ValueDataType::String,
            value_reference: vec![value; 32],
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
        // parts with the attribute set. The max parts do not parse, so the mut
        // path falls back to `max`; we build them directly here.
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
            stored.value_reference = vec![0x42u8; 32];
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
