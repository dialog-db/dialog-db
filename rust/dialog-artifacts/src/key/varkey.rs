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
    ATTRIBUTE_KEY_TAG, ENTITY_KEY_TAG, VALUE_KEY_TAG, ValueDataType, decode_bytes, encode_bytes,
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
