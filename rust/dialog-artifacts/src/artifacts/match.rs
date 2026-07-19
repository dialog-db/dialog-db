//! Pattern matching for artifacts against selectors.
//!
//! This module provides functionality for matching artifacts and index entries
//! against artifact selectors during query operations.

use std::cmp::Ordering;

use dialog_search_tree::Entry;

use crate::{
    ATTRIBUTE_KEY_TAG, ArtifactSelector, AttributeKey, Datum, ENTITY_KEY_TAG, EntityKey, Key,
    KeyView, State, VALUE_KEY_TAG, ValueKey, artifacts::selector::Constrained, decode_value,
    key::inline_threshold, key::value_payload, key::varkey::KeyRef,
};

/// Checks whether an already-parsed [`KeyRef`] matches a selector's
/// constraints. The borrowed-parse equivalent of
/// [`match_selector_and_key_view`], used on the scan hot path so an entry's key
/// is parsed once (in the scan) and matched without re-splitting.
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
        // The inline value payload for a string/bytes value is the raw bytes
        // (the order-preserving encoding of a byte string is the bytes
        // themselves), so the prefix compares directly. A spilled value carries
        // a reference rather than its bytes, so its prefix cannot be checked
        // from the key; it stays in the result (the caller re-checks against the
        // fetched block if it needs an exact filter), matching how a spilled
        // value is handled for exact value matches.
        if !key.value.is_reference() {
            let bytes = prefix.as_bytes();
            let payload = key.value.as_bytes();
            if bytes.len() > payload.len() || &payload[..bytes.len()] != bytes {
                return false;
            }
        }
    }

    // Value range bounds compare against the decoded value semantically, so
    // exclusivity (`>`/`<`) and the exact bound value are handled precisely
    // (the key range is a superset that includes the boundary; this drops it
    // when the bound is exclusive). A spilled value carries only a reference,
    // so it cannot be range-checked from the key; it stays in the result for
    // the caller to re-check, as with exact and prefix value matches.
    if (selector.value_lower().is_some() || selector.value_upper().is_some())
        && !key.value.is_reference()
        && let Some((value, rest)) = decode_value(key.value_type, key.value.as_bytes())
        && rest.is_empty()
    {
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

/// Checks if a key view matches the constraints in an artifact selector.
///
/// This function performs the actual matching logic between selector constraints
/// and key components (entity, attribute, value type, value reference).
///
/// Entity and attribute are now stored losslessly at full length, so every
/// comparison here (exact and prefix) is exact against the key bytes.
fn match_selector_and_key_view<K>(selector: &ArtifactSelector<Constrained>, key: K) -> bool
where
    K: KeyView,
{
    if let Some(entity) = selector.entity()
        && entity.as_str().as_bytes() != key.entity().raw()
    {
        return false;
    }

    if let Some(attribute) = selector.attribute()
        && attribute.as_str().as_bytes() != key.attribute().raw()
    {
        return false;
    }

    if let Some(value) = selector.value() {
        if value.data_type() != key.value_type() {
            return false;
        }
        // Compare by the same inline-vs-spill encoding the key was built with,
        // so the filter is exact for both an inline value (its order-preserving
        // bytes) and a spilled one (its 32-byte reference). The spill flag must
        // also agree: an inline payload and a reference of equal bytes would
        // otherwise falsely match.
        let expected = value_payload(value, inline_threshold());
        if expected.is_reference() != key.value_is_spilled()
            || expected.as_bytes() != key.value_payload()
        {
            return false;
        }
    }

    if let Some(prefix) = selector.attribute_prefix() {
        let bytes = prefix.as_bytes();
        let segment = key.attribute();
        let segment = segment.raw();
        if bytes.len() > segment.len() || &segment[..bytes.len()] != bytes {
            return false;
        }
    }

    if let Some(prefix) = selector.entity_prefix() {
        let bytes = prefix.as_bytes();
        let segment = key.entity();
        let segment = segment.raw();
        if bytes.len() > segment.len() || &segment[..bytes.len()] != bytes {
            return false;
        }
    }

    true
}

/// A trait that may be implemented by anything that is able to be matched
/// against an [`ArtifactSelector`]. In practice, this is implemented for the
/// [`Entry`]s of the various internal indexes of the database.
pub trait MatchCandidate {
    /// Returns true if the implementor matches the given [`ArtifactSelector`]
    fn matches_selector(&self, selector: &ArtifactSelector<Constrained>) -> bool;
}

impl MatchCandidate for Entry<Key, State<Datum>> {
    fn matches_selector(&self, selector: &ArtifactSelector<Constrained>) -> bool {
        // Entity and attribute are stored losslessly, so the key-view match
        // above is exact for every constraint, including prefixes; no datum
        // re-check is needed.
        match self.key.tag() {
            ENTITY_KEY_TAG => match_selector_and_key_view(selector, EntityKey(&self.key)),
            ATTRIBUTE_KEY_TAG => match_selector_and_key_view(selector, AttributeKey(&self.key)),
            VALUE_KEY_TAG => match_selector_and_key_view(selector, ValueKey(&self.key)),
            _ => false,
        }
    }
}
