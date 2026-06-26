use crate::artifact::ArtifactStore;
use crate::concept::descriptor::ConceptDescriptor;
use crate::concept::query::ConceptRules;
use crate::error::EvaluationError;
use dialog_common::{ConditionalSend, ConditionalSync};

/// A read-only data source for query evaluation that provides both fact
/// storage and rule resolution.
///
/// During evaluation, premises call methods on `Source` to look up stored
/// facts and to acquire the deductive rules associated with a concept.
///
/// This is the seam for the *layered* rule-resolution model (see
/// `notes/layered-rule-resolution.md`): a query is a stack of layers,
/// each providing facts (via [`ArtifactStore`]) and rules (via
/// [`acquire`](Source::acquire)). Resolution unions each layer's rules
/// the same way facts are unioned. `acquire` is async because a durable
/// layer reads the branch to discover the rules a concept concludes.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Source: ArtifactStore + Clone + ConditionalSend + ConditionalSync + 'static {
    /// Acquire rules for the given concept predicate.
    ///
    /// Returns a `ConceptRules` that owns the implicit rule (derived
    /// from the predicate's attributes) plus any rules the layers
    /// resolve for this concept. Always returns a value; with no
    /// installed rules, only the implicit rule participates.
    async fn acquire(&self, predicate: &ConceptDescriptor)
    -> Result<ConceptRules, EvaluationError>;
}
