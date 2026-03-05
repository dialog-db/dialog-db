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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Output;
    use crate::{Session, the};
    use dialog_storage::MemoryStorageBackend;

    macro_rules! assert_relation {
        ($artifacts:expr, $the:expr, $of:expr, $is:expr) => {{
            let mut session = Session::open($artifacts.clone());
            let mut tx = session.edit();
            tx.assert($the.clone().of($of.clone()).is($is));
            session.commit(tx).await.unwrap();
        }};
    }

    #[dialog_common::test]
    async fn it_selects_winner_with_constant_entity() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        assert_relation!(artifacts, name_attr, alice, "Alice".to_string());
        assert_relation!(artifacts, name_attr, alice, "Alicia".to_string());

        let query = AttributeQueryOnly::new(
            Term::var("the"),
            Term::from(alice.clone()),
            Term::var("value"),
            Term::var("cause"),
        );

        let session = Session::open(artifacts);
        let results = query.perform(&session).try_vec().await?;

        assert_eq!(
            results.len(),
            1,
            "EAV path should yield one winner per (attribute, entity)"
        );
        assert_eq!(results[0].of(), &alice);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_selects_winner_with_constant_attribute() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let name_attr = the!("person/name");

        assert_relation!(artifacts, name_attr, alice, "Alice".to_string());
        assert_relation!(artifacts, name_attr, alice, "Alicia".to_string());
        assert_relation!(artifacts, name_attr, bob, "Bob".to_string());
        assert_relation!(artifacts, name_attr, bob, "Robert".to_string());

        let query = AttributeQueryOnly::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
        );

        let session = Session::open(artifacts);
        let results = query.perform(&session).try_vec().await?;

        assert_eq!(
            results.len(),
            2,
            "AEV path should yield one winner per entity"
        );

        let alice_results: Vec<_> = results.iter().filter(|f| f.of() == &alice).collect();
        let bob_results: Vec<_> = results.iter().filter(|f| f.of() == &bob).collect();

        assert_eq!(alice_results.len(), 1);
        assert_eq!(bob_results.len(), 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_selects_winner_via_vae_path() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        assert_relation!(artifacts, name_attr, alice, "Alice".to_string());
        assert_relation!(artifacts, name_attr, alice, "Alicia".to_string());

        // First find the winner via AEV to know which value wins.
        let aev_query = AttributeQueryOnly::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
        );

        let session = Session::open(artifacts.clone());
        let aev_results = aev_query.perform(&session).try_vec().await?;
        assert_eq!(aev_results.len(), 1);
        let winner_value = aev_results[0].is().clone();

        // VAE path: only value known, both the and of are variables.
        let vae_query = AttributeQueryOnly::new(
            Term::var("the"),
            Term::var("person"),
            Term::Constant(winner_value.clone()),
            Term::var("cause"),
        );

        let session = Session::open(artifacts.clone());
        let vae_results = vae_query.perform(&session).try_vec().await?;

        assert_eq!(
            vae_results.len(),
            1,
            "VAE path should verify and return the winner"
        );
        assert_eq!(vae_results[0].is(), &winner_value);

        Ok(())
    }

    #[dialog_common::test]
    async fn choose_prefers_higher_cause() {
        use dialog_artifacts::{Artifact, Cause};
        use std::str::FromStr;

        let attr = dialog_artifacts::Attribute::from_str("person/name").unwrap();
        let entity = Entity::new().unwrap();

        let older = Artifact {
            the: attr.clone(),
            of: entity.clone(),
            is: crate::Value::String("Alice".into()),
            cause: Some(Cause([1u8; 32])),
        };

        let newer = Artifact {
            the: attr,
            of: entity,
            is: crate::Value::String("Alicia".into()),
            cause: Some(Cause([2u8; 32])),
        };

        let winner = choose(older.clone(), newer.clone());
        assert_eq!(winner.cause, newer.cause, "Higher cause should win");

        // Reversed argument order should produce the same winner.
        let winner2 = choose(newer.clone(), older.clone());
        assert_eq!(winner2.cause, newer.cause);
    }

    #[dialog_common::test]
    async fn choose_uses_fact_hash_for_equal_causes() {
        use dialog_artifacts::{Artifact, Cause};
        use std::str::FromStr;

        let attr = dialog_artifacts::Attribute::from_str("person/name").unwrap();
        let entity = Entity::new().unwrap();

        let a = Artifact {
            the: attr.clone(),
            of: entity.clone(),
            is: crate::Value::String("Alice".into()),
            cause: Some(Cause([1u8; 32])),
        };

        let b = Artifact {
            the: attr,
            of: entity,
            is: crate::Value::String("Alicia".into()),
            cause: Some(Cause([1u8; 32])),
        };

        let winner_ab = choose(a.clone(), b.clone());
        let winner_ba = choose(b.clone(), a.clone());

        // The winner should be deterministic regardless of argument order.
        assert_eq!(
            Cause::from(&winner_ab),
            Cause::from(&winner_ba),
            "Tiebreaker should be deterministic"
        );
    }
}
