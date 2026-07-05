//! Pattern matching for artifacts against selectors.
//!
//! This module provides functionality for matching artifacts and index entries
//! against artifact selectors during query operations.

use dialog_search_tree::Entry;

use crate::{
    ATTRIBUTE_KEY_TAG, ArtifactSelector, AttributeKey, Datum, ENTITY_KEY_TAG, ENTITY_RAW_HEAD,
    EntityKey, Key, KeyView, State, VALUE_KEY_TAG, ValueKey, artifacts::selector::Constrained,
};

/// Checks if a key view matches the constraints in an artifact selector.
///
/// This function performs the actual matching logic between selector constraints
/// and key components (entity, attribute, value type, value reference).
fn match_selector_and_key_view<K>(selector: &ArtifactSelector<Constrained>, key: K) -> bool
where
    K: KeyView,
{
    if let Some(entity) = selector.entity()
        && entity.key_bytes() != key.entity().raw()
    {
        return false;
    }

    if let Some(attribute) = selector.attribute()
        && attribute.key_bytes() != key.attribute().raw()
    {
        return false;
    }

    if let Some(value) = selector.value()
        && value.data_type() != key.value_type()
    {
        return false;
    }

    if let Some(value_reference) = selector.value_reference()
        && value_reference != key.value_reference().raw()
    {
        return false;
    }

    // Attribute names are stored raw (zero-padded) in the key, so a
    // byte-prefix check is exact; a prefix longer than the field
    // matches no attribute at all.
    if let Some(prefix) = selector.attribute_prefix() {
        let bytes = prefix.as_bytes();
        let part = key.attribute();
        let segment = part.raw();
        if bytes.len() > segment.len() || &segment[..bytes.len()] != bytes {
            return false;
        }
    }

    // Only the first [`ENTITY_RAW_HEAD`] bytes of the entity URI are
    // stored raw; the tail is hashed. Check what the key can prove —
    // the remainder is re-checked against the datum by
    // [`MatchCandidate::matches_selector`].
    if let Some(prefix) = selector.entity_prefix() {
        let bytes = prefix.as_bytes();
        let head = bytes.len().min(ENTITY_RAW_HEAD);
        let part = key.entity();
        let segment = part.raw();
        if segment[..head] != bytes[..head] {
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
        let key_matches = match self.key.tag() {
            ENTITY_KEY_TAG => match_selector_and_key_view(selector, EntityKey(&self.key)),
            ATTRIBUTE_KEY_TAG => match_selector_and_key_view(selector, AttributeKey(&self.key)),
            VALUE_KEY_TAG => match_selector_and_key_view(selector, ValueKey(&self.key)),
            _ => false,
        };
        if !key_matches {
            return false;
        }

        // An entity prefix longer than the raw head outruns what the
        // key bytes can prove: confirm against the stored URI. A
        // `Removed` entry has no datum to check; it is discarded by
        // the scan regardless of what we answer here.
        if let Some(prefix) = selector.entity_prefix()
            && prefix.len() > ENTITY_RAW_HEAD
            && let State::Added(datum) = &self.value
            && !datum.entity.starts_with(prefix)
        {
            return false;
        }

        true
    }
}
