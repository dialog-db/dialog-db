//! Pattern matching for artifacts against selectors.
//!
//! This module provides functionality for matching artifacts and index entries
//! against artifact selectors during query operations.

use dialog_search_tree::Entry;

use crate::{
    ATTRIBUTE_KEY_TAG, ArtifactSelector, AttributeKey, Datum, ENTITY_KEY_TAG, EntityKey, Key,
    KeyView, State, VALUE_KEY_TAG, ValueKey, artifacts::selector::Constrained,
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
