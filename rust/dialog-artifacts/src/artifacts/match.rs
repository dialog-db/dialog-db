//! Pattern matching for artifacts against selectors.
//!
//! This module provides functionality for matching artifacts and index entries
//! against artifact selectors during query operations.

use std::str;

use dialog_prolly_tree::Entry;

use crate::{
    ATTRIBUTE_KEY_TAG, ArtifactSelector, AttributeKey, AttributePattern, Datum, ENTITY_KEY_TAG,
    EntityKey, Key, KeyView, State, VALUE_KEY_TAG, ValueKey, artifacts::selector::Constrained,
};

/// Splits the attribute slot bytes into `(domain, name)` halves.
///
/// The slot is `domain/name` UTF-8, zero-padded to its full length. Trailing
/// zero bytes are trimmed before splitting on the `/`.
fn split_attribute_slot(bytes: &[u8]) -> (&str, &str) {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let s = str::from_utf8(&bytes[..end]).unwrap_or("");
    s.split_once('/').unwrap_or((s, ""))
}

/// Checks if a key view matches the constraints in an artifact selector.
///
/// This function performs the actual matching logic between selector constraints
/// and key components (entity, attribute halves, value type, value reference).
fn match_selector_and_key_view<K>(selector: &ArtifactSelector<Constrained>, key: K) -> bool
where
    K: KeyView,
{
    if let Some(entity) = selector.entity()
        && entity.key_bytes() != key.entity().raw()
    {
        return false;
    }

    let attr_part = key.attribute();
    let (key_domain, key_name) = split_attribute_slot(attr_part.raw());

    match selector.attribute() {
        Some(AttributePattern::Exact(attribute)) => {
            if attribute.domain() != key_domain || attribute.name() != key_name {
                return false;
            }
        }
        Some(AttributePattern::Domain(domain)) => {
            if domain.as_str() != key_domain {
                return false;
            }
        }
        None => {}
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
        match self.key.tag() {
            ENTITY_KEY_TAG => match_selector_and_key_view(selector, EntityKey(&self.key)),
            ATTRIBUTE_KEY_TAG => match_selector_and_key_view(selector, AttributeKey(&self.key)),
            VALUE_KEY_TAG => match_selector_and_key_view(selector, ValueKey(&self.key)),
            _ => false,
        }
    }
}
