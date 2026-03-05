use crate::Claim;
use crate::artifact::{ArtifactSelector, ArtifactsAttribute, Constrained};
use crate::attribute::The;
use crate::environment::Environment;
use crate::query::Application;
use crate::query::Output;
use crate::schema::{Cardinality, SEGMENT_READ_COST};
use crate::selection::{Match, Selection};
use crate::types::{Any, Record};
use crate::{Entity, EvaluationError, Parameters, Schema, Source, Term, try_stream};
use dialog_artifacts::{Artifact, Cause};
use futures_util::future::Either;
use std::fmt::Display;
use std::fmt::{Formatter, Result as FmtResult};

use super::all::AttributeQueryAll;

/// Per-match cost of the secondary lookup required when scanning the value
/// index (VAE) with `Cardinality::One`. Each match from the primary scan
/// needs a secondary `(attribute, entity)` lookup to verify it is the
/// winning value — the one with the highest cause.
const SECONDARY_LOOKUP_COST: usize = SEGMENT_READ_COST;

/// Given two artifacts for the same `(attribute, entity)` pair, return the
/// winner. The winner is the artifact with the higher cause; when causes are
/// equal (including both `None`), the fact hash (`Cause::from`) breaks the tie.
fn choose(current: Artifact, challenger: Artifact) -> Artifact {
    match (&current.cause, &challenger.cause) {
        (Some(a), Some(b)) if a > b => current,
        (Some(a), Some(b)) if a < b => challenger,
        (Some(_), None) => current,
        (None, Some(_)) => challenger,
        _ => {
            // Causes are equal — use the fact hash as a deterministic tiebreaker.
            if Cause::from(&current) >= Cause::from(&challenger) {
                current
            } else {
                challenger
            }
        }
    }
}

/// VAE winner verification.
///
/// When only the value is known (VAE scan), groups aren't contiguous, so each
/// candidate from the base scan is verified by a secondary `(attribute, entity)`
/// range scan to find the true winner. Yields the match only if the candidate
/// matches the winner.
fn challenge<S: Source>(
    source: S,
    selector: AttributeQueryAll,
    candidate: Match,
) -> impl Selection {
    try_stream! {
        let relation = selector.attribute();
        let attribute = ArtifactsAttribute::try_from(candidate.lookup(&Term::from(&relation))?)?;
        let entity = Entity::try_from(candidate.lookup(&Term::from(selector.of()))?)?;
        let value = candidate.lookup(selector.is())?;
        let cause = Cause::try_from(candidate.lookup(&Term::from(selector.cause()))?)?;

        let challengers = source.select(ArtifactSelector::new()
            .the(attribute)
            .of(entity));

        let mut winner: Option<Artifact> = None;
        for await each in challengers {
            let challenger = each?;
            winner = Some(match winner {
                None => challenger,
                Some(winner) => choose(winner, challenger),
            });
        }

        if let Some(winner) = winner
            && winner.is == value
            && winner.cause.unwrap_or(Cause([0; 32])) == cause
        {
            yield candidate;
        }
    }
}

/// Winner-selecting attribute query for `Cardinality::One`.
///
/// Wraps an [`AttributeQueryAll`] and applies winner selection logic so that
/// only one value per `(attribute, entity)` pair is yielded — the one with
/// the highest cause.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct AttributeQueryOnly {
    inner: AttributeQueryAll,
}

impl AttributeQueryOnly {
    /// Create a new winner-selecting attribute query.
    pub fn new(the: Term<The>, of: Term<Entity>, is: Term<Any>, cause: Term<Cause>) -> Self {
        Self {
            inner: AttributeQueryAll::new(the, of, is, cause),
        }
    }

    /// Get the 'the' (attribute) term.
    pub fn the(&self) -> &Term<The> {
        self.inner.the()
    }

    /// Get the 'of' (entity) term.
    pub fn of(&self) -> &Term<Entity> {
        self.inner.of()
    }

    /// Get the 'is' (value) parameter.
    pub fn is(&self) -> &Term<Any> {
        self.inner.is()
    }

    /// Get the 'cause' term.
    pub fn cause(&self) -> &Term<Cause> {
        self.inner.cause()
    }

    /// Get the source term (internal claim handle).
    pub fn source(&self) -> &Term<Record> {
        self.inner.source()
    }

    /// Map `Term<The>` to `Term<ArtifactsAttribute>`.
    pub fn attribute(&self) -> Term<ArtifactsAttribute> {
        self.inner.attribute()
    }

    /// Returns the schema describing this application's parameters.
    pub fn schema(&self) -> Schema {
        self.inner.schema()
    }

    /// Estimate cost for Cardinality::One semantics.
    ///
    /// When only the value is known (neither entity nor attribute), each match
    /// from the VAE scan requires a secondary lookup, adding
    /// `SECONDARY_LOOKUP_COST` per match.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        let the = self.inner.the().is_bound(env);
        let of = self.inner.of().is_bound(env);
        let is = self.inner.is().is_bound(env);

        let base = Cardinality::One.estimate(the, of, is)?;

        if is && !the && !of {
            Some(base + SECONDARY_LOOKUP_COST)
        } else {
            Some(base)
        }
    }

    /// Returns the parameters for this query.
    pub fn parameters(&self) -> Parameters {
        self.inner.parameters()
    }

    /// EAV/AEV scan: results are grouped by `(attribute, entity)`.
    /// Buffer the winning candidate and yield when the group changes.
    fn select_winners<S: Source, M: Selection>(self, source: S, selection: M) -> impl Selection {
        let selector = self.inner;
        try_stream! {
            for await each in selection {
                let base = each?;
                let resolved = selector.resolve(&base);
                let mut candidate: Option<Artifact> = None;

                for await artifact in source.select((&resolved).try_into()?) {
                    let artifact = artifact?;

                    candidate = Some(match candidate.take() {
                        Some(current) if current.the == artifact.the && current.of == artifact.of => {
                            choose(current, artifact)
                        }
                        Some(winner) => {
                            let mut extension = base.clone();
                            selector.merge(&mut extension, &winner)?;
                            yield extension;
                            artifact
                        }
                        None => artifact,
                    });
                }

                if let Some(winner) = candidate.take() {
                    let mut extension = base.clone();
                    selector.merge(&mut extension, &winner)?;
                    yield extension;
                }
            }
        }
    }

    /// Evaluate with winner selection based on scan strategy.
    ///
    /// - **EAV/AEV** (entity or attribute known): results are grouped by
    ///   `(attribute, entity)`. A sliding window yields the winner per group.
    /// - **VAE** (only value known): delegates to the inner base scan then
    ///   verifies each candidate with a secondary lookup.
    pub fn evaluate<S: Source, M: Selection>(self, source: S, selection: M) -> impl Selection {
        let entity_known = matches!(self.inner.of(), Term::Constant(_));
        let attribute_known = matches!(self.inner.the(), Term::Constant(_));

        if entity_known || attribute_known {
            Either::Left(self.select_winners(source, selection))
        } else {
            let inner_clone = self.inner.clone();
            let candidates = self.inner.evaluate(source.clone(), selection);
            Either::Right(
                candidates.try_flat_map(move |input| {
                    challenge(source.clone(), inner_clone.clone(), input)
                }),
            )
        }
    }

    /// Execute this query, returning a stream of claims.
    pub fn perform<S: Source>(self, source: &S) -> impl Output<Claim>
    where
        Self: Sized,
    {
        Application::perform(self, source)
    }
}

impl Application for AttributeQueryOnly {
    type Conclusion = Claim;

    fn evaluate<S: Source, M: Selection>(self, selection: M, source: &S) -> impl Selection {
        self.evaluate(source.clone(), selection)
    }

    fn realize(&self, input: Match) -> Result<Claim, EvaluationError> {
        input.prove(self.inner.source())
    }
}

impl TryFrom<&AttributeQueryOnly> for ArtifactSelector<Constrained> {
    type Error = EvaluationError;

    fn try_from(from: &AttributeQueryOnly) -> Result<Self, Self::Error> {
        ArtifactSelector::try_from(&from.inner)
    }
}

impl Display for AttributeQueryOnly {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self.inner, f)
    }
}
