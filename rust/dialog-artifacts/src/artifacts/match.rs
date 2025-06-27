use dialog_prolly_tree::Entry;

use crate::{
    ArtifactSelector, AttributeKey, Datum, EntityKey, KeyView, State, ValueKey,
    artifacts::selector::Constrained,
};

/// A trait that may be implemented by anything that is able to be matched
/// against an [`ArtifactSelector`]. In practice, this is implemented for the
/// [`Entry`]s of the various internal indexes of the database.
pub trait MatchCandidate {
    /// Returns true if the implementor matches the given [`ArtifactSelector`]
    fn matches_selector(&self, selector: &ArtifactSelector<Constrained>) -> bool;
}

impl MatchCandidate for Entry<EntityKey, State<Datum>> {
    fn matches_selector(&self, selector: &ArtifactSelector<Constrained>) -> bool {
        if let Some(entity) = selector.entity() {
            if entity.key_bytes() != self.key.entity().raw() {
                return false;
            }
        }

        if let Some(attribute) = selector.attribute() {
            if attribute.key_bytes() != self.key.attribute().raw() {
                return false;
            }
        }

        if let Some(value) = selector.value() {
            if value.data_type() != self.key.value_type() {
                return false;
            }
        }

        if let Some(value_reference) = selector.value_reference() {
            // TODO: Should we support comparing `State::Removed`?
            if let State::Added(datum) = &self.value {
                if value_reference != &datum.value_reference() {
                    return false;
                }
            }
        }

        true
    }
}

impl MatchCandidate for Entry<AttributeKey, State<Datum>> {
    fn matches_selector(&self, selector: &ArtifactSelector<Constrained>) -> bool {
        if let Some(entity) = selector.entity() {
            if entity.key_bytes() != self.key.entity().raw() {
                return false;
            }
        }

        if let Some(attribute) = selector.attribute() {
            if attribute.key_bytes() != self.key.attribute().raw() {
                return false;
            }
        }

        if let Some(value) = selector.value() {
            if value.data_type() != self.key.value_type() {
                return false;
            }
        }

        if let Some(value_reference) = selector.value_reference() {
            // TODO: Should we support comparing `State::Removed`?
            if let State::Added(datum) = &self.value {
                if value_reference != &datum.value_reference() {
                    return false;
                }
            }
        }

        true
    }
}

impl MatchCandidate for Entry<ValueKey, State<Datum>> {
    fn matches_selector(&self, selector: &ArtifactSelector<Constrained>) -> bool {
        if let Some(entity) = selector.entity() {
            if entity.key_bytes() != self.key.entity().raw() {
                return false;
            }
        }

        if let Some(attribute) = selector.attribute() {
            if attribute.key_bytes() != self.key.attribute().raw() {
                return false;
            }
        }

        if let Some(value) = selector.value() {
            if value.data_type() != self.key.value_type() {
                return false;
            }
        }

        if let Some(value_reference) = selector.value_reference() {
            // TODO: Should we support comparing `State::Removed`?
            if let State::Added(datum) = &self.value {
                if value_reference != &datum.value_reference() {
                    return false;
                }
            }
        }

        true
    }
}
