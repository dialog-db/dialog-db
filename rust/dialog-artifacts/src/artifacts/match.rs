//! Pattern matching for artifacts against selectors.
//!
//! This module provides functionality for matching artifacts and index entries
//! against artifact selectors during query operations.

use std::cmp::Ordering;

use crate::{
    ArtifactSelector, ValueDataType, artifacts::selector::Constrained, decode_value,
    key::inline_threshold, key::value_payload, key::varkey::KeyRef,
};

/// Checks whether an already-parsed [`KeyRef`] matches a selector's
/// constraints, used on the scan hot path so an entry's key is parsed once
/// (in the scan) and matched without re-splitting.
///
/// Entity and attribute are stored losslessly, so every comparison (exact and
/// prefix) is exact against the key bytes.
pub fn match_selector_and_key_ref(
    selector: &ArtifactSelector<Constrained>,
    key: &KeyRef<'_>,
) -> bool {
    if let Some(entity) = selector.entity()
        && entity.as_str().as_bytes() != key.entity.as_ref()
    {
        return false;
    }

    if let Some(attribute) = selector.attribute()
        && attribute.as_str().as_bytes() != key.attribute.as_ref()
    {
        return false;
    }

    if let Some(value) = selector.value() {
        if value.data_type() != key.value_type {
            return false;
        }
        // Compare by the same inline-vs-spill encoding the key was built with,
        // so the filter is exact for an inline value (its order-preserving
        // bytes) and a spilled one (its 32-byte reference). The spill flag must
        // agree too: an inline payload and a reference of equal bytes would
        // otherwise falsely match.
        let expected = value_payload(value, inline_threshold());
        if expected.is_reference() != key.value.is_reference()
            || expected.as_bytes() != key.value.as_bytes()
        {
            return false;
        }
    }

    if let Some(prefix) = selector.attribute_prefix() {
        let bytes = prefix.as_bytes();
        let segment = key.attribute.as_ref();
        if bytes.len() > segment.len() || &segment[..bytes.len()] != bytes {
            return false;
        }
    }

    if let Some(prefix) = selector.entity_prefix() {
        let bytes = prefix.as_bytes();
        let segment = key.entity.as_ref();
        if bytes.len() > segment.len() || &segment[..bytes.len()] != bytes {
            return false;
        }
    }

    if let Some(prefix) = selector.value_prefix() {
        // A prefix predicate is a STRING predicate over the inline
        // order-preserving payload (a string's raw UTF-8 bytes). A spilled
        // value carries only its 32-byte reference, so it can never carry a
        // prefix: spill is equality-only, here exactly as on the VAE route
        // (whose scanned band structurally excludes spilled keys). A
        // non-string value never matches, whatever its payload bytes spell.
        if key.value_type != ValueDataType::String || key.value.is_reference() {
            return false;
        }
        let bytes = prefix.as_bytes();
        let payload = key.value.as_bytes();
        if bytes.len() > payload.len() || &payload[..bytes.len()] != bytes {
            return false;
        }
    }

    // Value range bounds compare against the decoded value semantically, so
    // exclusivity (`>`/`<`) and the exact bound value are handled precisely
    // (the key range is a superset that includes the boundary; this drops it
    // when the bound is exclusive).
    if selector.value_lower().is_some() || selector.value_upper().is_some() {
        // A spilled value is equality-only: its reference cannot be
        // range-checked and never satisfies a bound. (Numeric encodings are
        // fixed-width and never spill, so this only excludes non-numeric
        // spills swept into an entity-scoped range.) An undecodable inline
        // payload is corrupt: fail closed rather than admit it.
        if key.value.is_reference() {
            return false;
        }
        let Some((value, rest)) = decode_value(key.value_type, key.value.as_bytes()) else {
            return false;
        };
        if !rest.is_empty() {
            return false;
        }
        // Compare only within the bound's type: `Value`'s derived `PartialOrd`
        // orders across variants by declaration order, not semantically, so a
        // cross-type value must be excluded rather than variant-ordered. The key
        // range already brackets the bound's band, so a differing type here is a
        // spurious neighbor at the band edge.
        if let Some(bound) = selector.value_lower() {
            if value.data_type() != bound.value.data_type() {
                return false;
            }
            match value.partial_cmp(&bound.value) {
                Some(Ordering::Greater) => {}
                Some(Ordering::Equal) if bound.inclusive => {}
                _ => return false,
            }
        }
        if let Some(bound) = selector.value_upper() {
            if value.data_type() != bound.value.data_type() {
                return false;
            }
            match value.partial_cmp(&bound.value) {
                Some(Ordering::Less) => {}
                Some(Ordering::Equal) if bound.inclusive => {}
                _ => return false,
            }
        }
    }

    true
}
