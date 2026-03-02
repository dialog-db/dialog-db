use crate::artifact::ArtifactStore;
use crate::concept::application::ConceptRules;
use crate::concept::descriptor::ConceptDescriptor;
use crate::error::EvaluationError;
use dialog_common::{ConditionalSend, ConditionalSync};

/// A read-only data source for query evaluation that provides both fact
/// storage and rule resolution.
///
/// During evaluation, premises call methods on `Source` to look up stored
/// facts and to acquire the deductive rules associated with a concept.
/// [`Session`](crate::Session) and [`QuerySession`](crate::session::QuerySession)
/// both implement `Source`, bridging the artifact store with the
/// [`RuleRegistry`](crate::session::RuleRegistry).
pub trait Source: ArtifactStore + Clone + ConditionalSend + ConditionalSync + 'static {
    /// Acquire rules for the given concept predicate.
    ///
    /// Returns a `ConceptRules` that owns the default rule, any installed rules,
    /// and a per-adornment plan cache. Always returns a value. If no rules were
    /// explicitly registered, an implicit rule (derived from the predicate's
    /// attributes) is used.
    fn acquire(&self, predicate: &ConceptDescriptor) -> Result<ConceptRules, EvaluationError>;
}
