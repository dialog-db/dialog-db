//! Pluggable deductive-rule resolution for query sessions.
//!
//! By default a [`QueryLayer`](crate::QueryLayer) surfaces only the
//! *implicit* rule each [`ConceptDescriptor`] carries (the one derived
//! from its attributes). Storing deductive rules as facts in a branch
//! and resolving them at query time is a concern this crate does not
//! want to own: the claim shape, hydration, and caching live in the
//! consumer (e.g. tonk).
//!
//! This module defines the seam. A consumer implements [`RuleSource`]
//! and installs it via
//! [`QueryLayer::with_rules`](crate::QueryLayer::with_rules). At query
//! time the session calls it once per queried concept, handing it a
//! [`RuleClaims`] reader backed by the same branch + overlay union the
//! query reads facts from — so rules asserted into the overlay via
//! `.with(..)` are visible alongside committed ones. When no
//! `RuleSource` is installed, behavior is identical to before: only the
//! implicit rule participates.

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{Artifact, ArtifactSelector, DialogArtifactsError};
use dialog_common::ConditionalSync;
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptRules;
use dialog_query::error::EvaluationError;

/// Read side of the branch + overlay union, handed to a [`RuleSource`]
/// so it can fetch the facts a rule is stored as.
///
/// Object-safe by design: results are returned owned (a `Vec`) rather
/// than as a borrowed stream, keeping the `Select` lifetime out of the
/// trait object so a `RuleSource` can be held as `dyn`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait RuleClaims: ConditionalSync {
    /// Run `selector` against the session's branch + overlay union and
    /// collect every matching artifact.
    async fn select_claims(
        &self,
        selector: ArtifactSelector<Constrained>,
    ) -> Result<Vec<Artifact>, DialogArtifactsError>;
}

/// Resolves the deductive rules that conclude a given concept.
///
/// Implemented by a consumer that knows how deductive rules are stored
/// as facts (the claim shape) and how to hydrate them. The session
/// calls [`resolve`](RuleSource::resolve) once per queried concept,
/// passing the implicit-only [`ConceptRules`] to extend and a
/// [`RuleClaims`] reader to fetch rule facts with.
///
/// The implementation should return the `rules` it was given, having
/// installed any deductive rules concluding `concept` it finds. It may
/// also return `rules` unchanged (no rules for this concept), which is
/// the same outcome as installing no `RuleSource` at all.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait RuleSource: ConditionalSync {
    /// Extend `rules` with every deductive rule concluding `concept`,
    /// reading rule facts through `claims`.
    async fn resolve(
        &self,
        concept: &ConceptDescriptor,
        rules: ConceptRules,
        claims: &dyn RuleClaims,
    ) -> Result<ConceptRules, EvaluationError>;
}
