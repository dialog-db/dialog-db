//! Pattern matching for artifacts against selectors.
//!
//! This module provides functionality for matching artifacts and index entries
//! against artifact selectors during query operations.

use dialog_prolly_tree::Entry;

use crate::{
    ATTRIBUTE_KEY_TAG, ArtifactSelector, AttributeKey, Datum, ENTITY_KEY_TAG, EntityKey, Key,
    KeyView, State, VALUE_KEY_TAG, ValueKey, artifacts::selector::Constrained,
};

/// Checks if a key view matches the constraints in an artifact selector.
///
/// This function performs the actual matching logic between selector constraints
/// and key components (entity, attribute, value type, value reference).
fn match_selector_and_key_view<K>(selector: &ArtifactSelector<Constrained>, key: K) -> bool
where
    K: KeyView,
{
    if let Some(entity) = selector.entity() {
        if entity.key_bytes() != key.entity().raw() {
            return false;
        }
    }

    if let Some(attribute) = selector.attribute() {
        if attribute.key_bytes() != key.attribute().raw() {
            return false;
        }
    }

    if let Some(value) = selector.value() {
        if value.data_type() != key.value_type() {
            return false;
        }
    }

    if let Some(value_reference) = selector.value_reference() {
        if value_reference != key.value_reference().raw() {
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
        match self.key.tag() {
            ENTITY_KEY_TAG => match_selector_and_key_view(selector, EntityKey(&self.key)),
            ATTRIBUTE_KEY_TAG => match_selector_and_key_view(selector, AttributeKey(&self.key)),
            VALUE_KEY_TAG => match_selector_and_key_view(selector, ValueKey(&self.key)),
            _ => false,
        }
    }
}
