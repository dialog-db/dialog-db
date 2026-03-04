use crate::Cardinality;
use crate::Claim;
use crate::artifact::{ArtifactSelector, ArtifactsAttribute, Constrained};
use crate::attribute::The;
use crate::environment::Environment;
use crate::negation::Negation;
use crate::proposition::Proposition;
use crate::query::Application;
use crate::query::Output;
use crate::schema::SEGMENT_READ_COST;
use crate::selection::{Match, Selection};
use crate::types::{Any, Record};
use crate::{
    Entity, EvaluationError, Field, Parameters, Premise, Requirement, Schema, Source, Term, Type,
    Value, try_stream,
};
use dialog_artifacts::{Artifact, Cause};
use futures_util::future::Either;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::fmt::{Formatter, Result as FmtResult};
use std::ops::Not;

/// Per-match cost of the secondary lookup required when scanning the value
/// index (VAE) with `Cardinality::One`. Each match from the primary scan
/// needs a secondary `(attribute, entity)` lookup to verify it is the
/// winning value — the one with the highest cause.
const SECONDARY_LOOKUP_COST: usize = SEGMENT_READ_COST;

/// Given two artifacts for the same `(attribute, entity)` pair, return the
/// winner. The winner is the artifact with the higher cause; when causes are
/// equal (including both `None`), the fact hash (`Cause::from`) breaks the tie.
fn pick_winner(current: Artifact, challenger: Artifact) -> Artifact {
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
/// candidate from `evaluate_cardinality_many` is verified by a secondary
/// `(attribute, entity)` range scan to find the true winner. Yields the
/// match only if the candidate matches the winner.
fn verify_winner<S: Source>(source: S, selector: RelationQuery, input: Match) -> impl Selection {
    try_stream! {
        let attribute_term = selector.attribute();
        let attribute = ArtifactsAttribute::try_from(input.lookup(&Term::from(&attribute_term))?)?;
        let entity: Entity = Entity::try_from(input.lookup(&Term::from(selector.of()))?)?;
        let candidate_value: Value = input.lookup(selector.is())?;
        let candidate_cause: Cause = Cause::try_from(input.lookup(&Term::from(selector.cause()))?)?;

        let verification_selector = ArtifactSelector::new()
            .the(attribute)
            .of(entity);

        let mut winner: Option<Artifact> = None;
        for await result in source.select(verification_selector) {
            let artifact = result?;
            winner = Some(match winner {
                None => artifact,
                Some(current) => pick_winner(current, artifact),
            });
        }

        if let Some(w) = winner {
            let winner_cause = w.cause.unwrap_or(Cause([0; 32]));
            if w.is == candidate_value && winner_cause == candidate_cause {
                yield input;
            }
        }
    }
}

/// A relation premise bound to specific term arguments.
///
/// Represents a query against the fact store in the form
/// `(the, of, is, cause)` where each position is a [`Term`] — either a
/// constant that constrains the lookup or a variable that will be bound
/// by the results.
///
/// The `the` field is a [`Term<The>`] representing the relation identifier
/// (e.g., `"person/name"`). The optional cardinality metadata is used by
/// the cost estimator during planning.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RelationQuery {
    /// The relation identifier (e.g., "person/name")
    the: Term<The>,
    /// The entity
    of: Term<Entity>,
    /// The value
    is: Term<Any>,
    /// The cause/provenance
    cause: Term<Cause>,
    /// Internal handle for claim storage. Automatically assigned a unique
    /// variable name so it can key into the match's claim map. Not exposed
    /// in schema, parameters, or cost estimation.
    // TODO: Once Value::Record supports the RecordFormat trait proposed in
    // https://github.com/dialog-db/dialog-db/pull/221 this can bind a
    // Value::Record directly, eliminating the separate claims map on Match.
    source: Term<Record>,
    /// Cardinality metadata, when the attribute is known.
    /// None when the attribute is a variable; defaults to `Many`.
    cardinality: Option<Cardinality>,
}

impl RelationQuery {
    /// Create a new relation application.
    pub fn new(
        the: Term<The>,
        of: Term<Entity>,
        is: Term<Any>,
        cause: Term<Cause>,
        cardinality: Option<Cardinality>,
    ) -> Self {
        Self {
            the,
            of,
            is,
            cause,
            source: Term::<Record>::unique(),
            cardinality,
        }
    }

    /// Get the 'the' (attribute) term.
    pub fn the(&self) -> &Term<The> {
        &self.the
    }

    /// Get the 'of' (entity) term.
    pub fn of(&self) -> &Term<Entity> {
        &self.of
    }

    /// Get the 'is' (value) parameter.
    pub fn is(&self) -> &Term<Any> {
        &self.is
    }

    /// Get the 'cause' term.
    pub fn cause(&self) -> &Term<Cause> {
        &self.cause
    }

    /// Merge a matched artifact into a match: store the claim and bind
    /// the/of/is/cause values to the corresponding terms.
    fn merge(&self, candidate: &mut Match, artifact: &Artifact) -> Result<(), EvaluationError> {
        let claim = Claim::from(artifact);
        candidate.cite(&self.source, &claim)?;
        candidate.bind(&Term::<Any>::from(&self.the), Value::from(claim.the()))?;
        candidate.bind(
            &Term::<Any>::from(&self.of),
            Value::Entity(claim.of().clone()),
        )?;
        candidate.bind(&self.is, claim.is().clone())?;
        candidate.bind(
            &Term::<Any>::from(&self.cause),
            Value::Bytes(claim.cause().clone().0.into()),
        )?;
        Ok(())
    }

    /// Get the cardinality, defaulting to `Cardinality::Many` if not set
    /// (unknown relations are assumed to have many values).
    pub fn cardinality(&self) -> Cardinality {
        self.cardinality.unwrap_or(Cardinality::Many)
    }

    /// Map `Term<The>` to `Term<ArtifactsAttribute>`.
    pub fn attribute(&self) -> Term<ArtifactsAttribute> {
        match &self.the {
            Term::Constant(value) => Term::Constant(value.clone()),
            Term::Variable {
                name: Some(name), ..
            } => Term::var(name.clone()),
            Term::Variable { name: None, .. } => Term::blank(),
        }
    }

    /// Returns the schema describing this application's parameters.
    pub fn schema(&self) -> Schema {
        let requirement = Requirement::new_group();
        let mut schema = Schema::new();

        schema.insert(
            "the".to_string(),
            Field {
                description: "The relation identifier".to_string(),
                content_type: Some(Type::Symbol),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );

        schema.insert(
            "of".to_string(),
            Field {
                description: "Entity of the relation".to_string(),
                content_type: Some(Type::Entity),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );

        schema.insert(
            "is".to_string(),
            Field {
                description: "Value of the relation".to_string(),
                content_type: None,
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );

        schema
    }

    /// Estimate cost based on how many parameters are constrained and cardinality.
    ///
    /// When cardinality is `One` and only the value is known (neither entity nor
    /// attribute), each match from the VAE scan requires a secondary lookup on
    /// the `(attribute, entity)` pair to verify that the matched value is the
    /// winner. This adds `SECONDARY_LOOKUP_COST` per match to the estimate.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        let the = self.the.is_bound(env);
        let of = self.of.is_bound(env);
        let is = self.is.is_bound(env);

        let base = self.cardinality().estimate(the, of, is)?;

        if self.cardinality() == Cardinality::One && is && !the && !of {
            Some(base + SECONDARY_LOOKUP_COST)
        } else {
            Some(base)
        }
    }

    /// Returns the parameters for this relation application.
    pub fn parameters(&self) -> Parameters {
        let mut params = Parameters::new();

        params.insert("the".to_string(), Term::<Any>::from(&self.the));
        params.insert("of".to_string(), Term::<Any>::from(&self.of));
        params.insert("is".to_string(), self.is.clone());
        params
    }
}

impl RelationQuery {
    /// Resolves variables from the given match.
    pub fn resolve_from_match(&self, source: &Match) -> Self {
        let the = self.the.resolve(source);
        let of = self.of.resolve(source);
        let is = match source.lookup(&self.is) {
            Ok(value) => Term::Constant(value),
            Err(_) => self.is.clone(),
        };
        let cause = self.cause.resolve(source);

        Self {
            the,
            of,
            is,
            cause,
            source: self.source.clone(),
            cardinality: self.cardinality,
        }
    }

    /// Evaluate with fact provenance tracking.
    ///
    /// For `Cardinality::Many`, all matching artifacts are yielded.
    ///
    /// For `Cardinality::One`, only the winning artifact per `(attribute, entity)`
    /// pair is yielded. The strategy depends on which index the storage layer
    /// uses — see [`Self::evaluate_cardinality_one`].
    pub fn evaluate_with_provenance<S: Source, M: Selection>(
        self,
        source: S,
        selection: M,
    ) -> impl Selection {
        if self.cardinality() == Cardinality::One {
            Either::Left(self.evaluate_cardinality_one(source, selection))
        } else {
            Either::Right(self.evaluate_cardinality_many(source, selection))
        }
    }

    /// Evaluate yielding all matching artifacts.
    fn evaluate_cardinality_many<S: Source, M: Selection>(
        self,
        source: S,
        selection: M,
    ) -> impl Selection {
        let selector = self;
        try_stream! {
            for await candidate in selection {
                let base = candidate?;
                let selection = selector.resolve_from_match(&base);

                for await artifact in source.select((&selection).try_into()?) {
                    let artifact = artifact?;
                    let mut extension = base.clone();
                    selector.merge(&mut extension, &artifact)?;
                    yield extension;
                }
            }
        }
    }

    /// Evaluate yielding only the winning artifact per `(attribute, entity)`.
    ///
    /// - **EAV/AEV** (entity or attribute known): results are grouped by
    ///   `(attribute, entity)`. A sliding window buffers the candidate and
    ///   yields the winner when the group changes.
    ///
    /// - **VAE** (only value known): groups are scattered. Uses
    ///   `evaluate_cardinality_many` to produce candidates, then flat-maps
    ///   each through a winner verification that does a secondary
    ///   `(attribute, entity)` range scan.
    fn evaluate_cardinality_one<S: Source, M: Selection>(
        self,
        source: S,
        selection: M,
    ) -> impl Selection {
        let entity_known = matches!(&self.of, Term::Constant(_));
        let attribute_known = matches!(&self.the, Term::Constant(_));

        if entity_known || attribute_known {
            Either::Left(self.select_winners(source, selection))
        } else {
            let selector = self.clone();
            let candidates = self.evaluate_cardinality_many(source.clone(), selection);
            Either::Right(
                candidates.try_flat_map(move |input| {
                    verify_winner(source.clone(), selector.clone(), input)
                }),
            )
        }
    }

    /// EAV/AEV scan: results are grouped by `(attribute, entity)`.
    /// Buffer the winning candidate and yield when the group changes.
    fn select_winners<S: Source, M: Selection>(self, source: S, selection: M) -> impl Selection {
        let selector = self;
        try_stream! {
            for await each in selection {
                let base = each?;
                let selection = selector.resolve_from_match(&base);
                let mut candidate: Option<Artifact> = None;

                for await artifact in source.select((&selection).try_into()?) {
                    let artifact = artifact?;

                    let same_group = candidate
                        .as_ref()
                        .is_some_and(|c| c.the == artifact.the && c.of == artifact.of);

                    if same_group {
                        candidate = Some(pick_winner(candidate.unwrap(), artifact));
                    } else {
                        if let Some(winner) = candidate.take() {
                            let mut extension = base.clone();
                            selector.merge(&mut extension, &winner)?;
                            yield extension;
                        }
                        candidate = Some(artifact);
                    }
                }

                if let Some(winner) = candidate.take() {
                    let mut extension = base.clone();
                    selector.merge(&mut extension, &winner)?;
                    yield extension;
                }
            }
        }
    }

    /// Execute this relation application, returning a stream of relations.
    pub fn perform<S: Source>(self, source: &S) -> impl Output<Claim>
    where
        Self: Sized,
    {
        Application::perform(self, source)
    }
}

impl Application for RelationQuery {
    type Conclusion = Claim;

    fn evaluate<S: Source, M: Selection>(self, selection: M, source: &S) -> impl Selection {
        self.evaluate_with_provenance(source.clone(), selection)
    }

    fn realize(&self, input: Match) -> Result<Claim, EvaluationError> {
        input.prove(&self.source)
    }
}

impl TryFrom<&RelationQuery> for ArtifactSelector<Constrained> {
    type Error = EvaluationError;

    fn try_from(from: &RelationQuery) -> Result<Self, Self::Error> {
        let mut selector: Option<ArtifactSelector<Constrained>> = None;

        if let Term::Constant(the) = &from.the {
            let relation = ArtifactsAttribute::try_from(the.clone()).map_err(|_| {
                EvaluationError::Store("Could not convert value to Attribute".to_string())
            })?;
            selector = Some(match selector {
                None => ArtifactSelector::new().the(relation),
                Some(s) => s.the(relation),
            });
        }

        match &from.of {
            Term::Constant(of) => {
                let entity = Entity::try_from(of.clone()).map_err(|_| {
                    EvaluationError::Store("Could not convert value to Entity".to_string())
                })?;
                selector = Some(match selector {
                    None => ArtifactSelector::new().of(entity.clone()),
                    Some(s) => s.of(entity),
                });
            }
            Term::Variable { .. } => {}
        }

        match &from.is {
            Term::Constant(value) => {
                selector = Some(match selector {
                    None => ArtifactSelector::new().is(value.clone()),
                    Some(s) => s.is(value.clone()),
                });
            }
            Term::Variable { .. } => {}
        }

        selector.ok_or_else(|| EvaluationError::EmptySelector {
            message: "At least one field must be constrained".to_string(),
        })
    }
}

impl Display for RelationQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "Claim {{")?;
        write!(f, "the: {},", self.the)?;
        write!(f, "of: {},", self.of)?;
        write!(f, "is: {},", self.is)?;
        write!(f, "cause: {},", self.cause)?;
        write!(f, "}}")
    }
}

impl Not for RelationQuery {
    type Output = Premise;

    fn not(self) -> Self::Output {
        Premise::Unless(Negation::not(Proposition::Relation(Box::new(self))))
    }
}

impl From<RelationQuery> for Proposition {
    fn from(application: RelationQuery) -> Self {
        Proposition::Relation(Box::new(application))
    }
}

impl From<RelationQuery> for Premise {
    fn from(application: RelationQuery) -> Self {
        Premise::Assert(Proposition::Relation(Box::new(application)))
    }
}

impl From<&RelationQuery> for Premise {
    fn from(application: &RelationQuery) -> Self {
        Premise::Assert(Proposition::Relation(Box::new(application.clone())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Output;
    use crate::selection::{Match, Selection};
    use crate::{Session, the};
    use dialog_storage::MemoryStorageBackend;

    #[dialog_common::test]
    async fn it_evaluates_relation_with_provenance() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::the;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        let claims = vec![name_attr.clone().of(alice.clone()).is("Alice".to_string())];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let rel_app = RelationQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::Many),
        );

        let session = Session::open(artifacts);
        let selection = rel_app.evaluate_with_provenance(session, Match::new().seed());

        let results = Selection::try_vec(selection).await?;

        assert_eq!(results.len(), 1);

        let candidate = &results[0];

        assert!(candidate.contains(&Term::var("person")));
        assert!(candidate.contains(&Term::var("name")));

        let person_id: Entity = Entity::try_from(candidate.lookup(&Term::var("person"))?)?;
        let name_value: Value = candidate.lookup(&Term::var("name"))?;

        assert_eq!(person_id, alice);
        assert_eq!(name_value, Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_single_value_for_cardinality_one() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::the;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        // Assert two different values for the same entity-attribute pair
        // in separate transactions so both persist in the store.
        let mut session = Session::open(artifacts.clone());
        session
            .transact(vec![
                name_attr.clone().of(alice.clone()).is("Alice".to_string()),
            ])
            .await?;

        let mut session = Session::open(artifacts.clone());
        session
            .transact(vec![
                name_attr.clone().of(alice.clone()).is("Alicia".to_string()),
            ])
            .await?;

        // Query with Cardinality::One — should return only one value
        let rel_app = RelationQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts.clone());
        let results = rel_app.perform(&session).try_vec().await?;

        assert_eq!(
            results.len(),
            1,
            "Cardinality::One should return only one value per entity-attribute pair, got {}",
            results.len()
        );

        // Query with Cardinality::Many — should return both values
        let rel_app_many = RelationQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::Many),
        );

        let session = Session::open(artifacts);
        let results_many = rel_app_many.perform(&session).try_vec().await?;

        assert_eq!(
            results_many.len(),
            2,
            "Cardinality::Many should return all values, got {}",
            results_many.len()
        );

        Ok(())
    }

    /// Helper macro: insert a relation in its own transaction so it persists
    /// independently (the transaction layer collapses duplicates within
    /// a single transaction).
    macro_rules! assert_relation {
        ($artifacts:expr, $the:expr, $of:expr, $is:expr) => {{
            let mut session = Session::open($artifacts.clone());
            let mut tx = session.edit();
            tx.assert($the.clone().of($of.clone()).is($is));
            session.commit(tx).await.unwrap();
        }};
    }

    // Cardinality::One with entity known (EAV scan).
    #[dialog_common::test]
    async fn it_selects_winner_via_eav_scan() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");
        let age_attr = the!("person/age");

        // Two conflicting values for person/name of alice
        assert_relation!(artifacts, name_attr, alice, "Alice".to_string());
        assert_relation!(artifacts, name_attr, alice, "Alicia".to_string());
        // One value for person/age of alice
        assert_relation!(artifacts, age_attr, alice, 30i64);

        // Entity is known → EAV scan, attribute is variable
        let rel_app = RelationQuery::new(
            Term::var("the"),
            Term::from(alice.clone()),
            Term::var("value"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts);
        let results = rel_app.perform(&session).try_vec().await?;

        // Should get exactly 2 facts: one winner for person/name, one for person/age
        assert_eq!(
            results.len(),
            2,
            "EAV scan with Cardinality::One should return one value per attribute, got {}",
            results.len()
        );

        let name_results: Vec<_> = results.iter().filter(|f| *f.the() == name_attr).collect();
        let age_results: Vec<_> = results.iter().filter(|f| *f.the() == age_attr).collect();

        assert_eq!(name_results.len(), 1, "Should have exactly one name result");
        assert_eq!(age_results.len(), 1, "Should have exactly one age result");
        assert_eq!(age_results[0].is(), &Value::SignedInt(30));

        Ok(())
    }

    // Cardinality::One with attribute known, entity unknown (AEV scan).
    #[dialog_common::test]
    async fn it_selects_winner_via_aev_scan() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let name_attr = the!("person/name");

        // Two conflicting values for alice's name
        assert_relation!(artifacts, name_attr, alice, "Alice".to_string());
        assert_relation!(artifacts, name_attr, alice, "Alicia".to_string());

        // Two conflicting values for bob's name
        assert_relation!(artifacts, name_attr, bob, "Bob".to_string());
        assert_relation!(artifacts, name_attr, bob, "Robert".to_string());

        // Attribute is known, entity is variable → AEV scan
        let rel_app = RelationQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts);
        let results = rel_app.perform(&session).try_vec().await?;

        // Should get exactly 2 facts: one winner per entity
        assert_eq!(
            results.len(),
            2,
            "AEV scan with Cardinality::One should return one value per entity, got {}",
            results.len()
        );

        let alice_results: Vec<_> = results.iter().filter(|f| f.of() == &alice).collect();
        let bob_results: Vec<_> = results.iter().filter(|f| f.of() == &bob).collect();

        assert_eq!(
            alice_results.len(),
            1,
            "Should have exactly one alice result"
        );
        assert_eq!(bob_results.len(), 1, "Should have exactly one bob result");

        Ok(())
    }

    // Cardinality::One with only value known (VAE scan).
    #[dialog_common::test]
    async fn it_selects_winner_via_vae_scan() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        // Two conflicting values for alice's name
        assert_relation!(artifacts, name_attr, alice, "Alice".to_string());
        assert_relation!(artifacts, name_attr, alice, "Alicia".to_string());

        // Determine the expected winner: query with attribute known to get the
        // winner from AEV, then verify the VAE lookup matches.
        let aev_app = RelationQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts.clone());
        let aev_results = aev_app.perform(&session).try_vec().await?;
        assert_eq!(aev_results.len(), 1);
        let expected_winner_value = aev_results[0].is().clone();

        // Now query by the winning value with only value known → VAE scan
        let vae_app = RelationQuery::new(
            Term::var("the"),
            Term::var("person"),
            Term::Constant(expected_winner_value.clone()),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts.clone());
        let vae_results = vae_app.perform(&session).try_vec().await?;

        // The winning value should appear (the secondary lookup confirms it wins)
        assert_eq!(
            vae_results.len(),
            1,
            "VAE scan should return the winner after secondary lookup, got {}",
            vae_results.len()
        );
        assert_eq!(vae_results[0].is(), &expected_winner_value);

        // Query by the losing value — the secondary lookup should filter it out
        let losing_value = if expected_winner_value == Value::String("Alice".into()) {
            Value::String("Alicia".into())
        } else {
            Value::String("Alice".into())
        };

        let vae_loser_app = RelationQuery::new(
            Term::var("the"),
            Term::var("person"),
            Term::Constant(losing_value),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts);
        let vae_loser_results = vae_loser_app.perform(&session).try_vec().await?;

        assert_eq!(
            vae_loser_results.len(),
            0,
            "VAE scan for the losing value should return nothing, got {}",
            vae_loser_results.len()
        );

        Ok(())
    }

    // Verify that the winner is deterministic.
    #[dialog_common::test]
    async fn it_picks_deterministic_winner() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        assert_relation!(artifacts, name_attr, alice, "Alice".to_string());
        assert_relation!(artifacts, name_attr, alice, "Alicia".to_string());

        // EAV path (entity known)
        let eav_app = RelationQuery::new(
            Term::var("the"),
            Term::from(alice.clone()),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts.clone());
        let eav_results = eav_app.perform(&session).try_vec().await?;
        let eav_name_results: Vec<_> = eav_results
            .iter()
            .filter(|f| *f.the() == name_attr)
            .collect();
        assert_eq!(eav_name_results.len(), 1);
        let eav_winner = eav_name_results[0].is().clone();

        // AEV path (attribute known)
        let aev_app = RelationQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts.clone());
        let aev_results = aev_app.perform(&session).try_vec().await?;
        let aev_alice: Vec<_> = aev_results.iter().filter(|f| f.of() == &alice).collect();
        assert_eq!(aev_alice.len(), 1);
        let aev_winner = aev_alice[0].is().clone();

        assert_eq!(
            eav_winner, aev_winner,
            "EAV and AEV paths should pick the same winner"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_relation_from_the() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        let claims = vec![name_attr.of(alice.clone()).is("Alice".to_string())];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let rel_app = RelationQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            None,
        );

        // Verify the term
        assert_eq!(rel_app.the(), &Term::from(the!("person/name")));

        let session = Session::open(artifacts);
        let results = rel_app.perform(&session).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of(), &alice);
        assert_eq!(results[0].is(), &Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_executes_relation_query() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::the;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        let claims = vec![name_attr.clone().of(alice.clone()).is("Alice".to_string())];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let rel_app = RelationQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::Many),
        );

        let session = Session::open(artifacts);
        let results = rel_app.perform(&session).try_vec().await?;

        assert_eq!(results.len(), 1);
        let fact = &results[0];
        assert_eq!(fact.the(), &name_attr);
        assert_eq!(fact.of(), &alice);
        assert_eq!(fact.is(), &Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_facts() -> anyhow::Result<()> {
        use crate::Statement;
        use crate::artifact::Artifacts;
        use crate::attribute::AttributeStatement;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;

        let alice_name: AttributeStatement = the!("user/name")
            .of(alice.clone())
            .is("Alice".to_string())
            .into();

        let mut session = Session::open(artifacts.clone());
        session.transact(vec![alice_name.clone()]).await?;

        let query_constant = RelationQuery::new(
            Term::from(the!("user/name")),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let results = query_constant
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of(), &alice);
        assert_eq!(results[0].is(), &Value::String("Alice".to_string()));

        let mut session = Session::open(artifacts.clone());
        session.transact([alice_name.revert()]).await?;

        let query2 = RelationQuery::new(
            Term::from(the!("user/name")),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let results2 = query2
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results2.len(), 0, "Fact should be retracted");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_mixes_constants_and_variables() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let alice = Entity::new()?;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let claims = vec![the!("user/name").of(alice.clone()).is("Alice".to_string())];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let mixed_query = RelationQuery::new(
            Term::from(the!("user/name")),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let results = mixed_query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results.len(), 1, "Should find Alice's name fact");
        assert_eq!(results[0].domain(), "user");
        assert_eq!(results[0].name(), "name");
        assert_eq!(results[0].of(), &alice);
        assert_eq!(results[0].is(), &Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_without_descriptor() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            the!("user/name").of(alice.clone()).is("Alice".to_string()),
            the!("user/name").of(bob.clone()).is("Bob".to_string()),
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let query = RelationQuery::new(
            Term::from(the!("user/name")),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let results = query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results.len(), 2, "Should find both Alice and Bob");

        let has_alice = results
            .iter()
            .any(|f| f.of == alice && f.is == Value::String("Alice".to_string()));
        let has_bob = results
            .iter()
            .any(|f| f.of == bob && f.is == Value::String("Bob".to_string()));
        assert!(has_alice && has_bob);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_accepts_string_literal_as_value_term() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;

        let claims = vec![the!("user/name").of(alice.clone()).is("Alice".to_string())];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let query = RelationQuery::new(
            Term::from(the!("user/name")),
            alice.clone().into(),
            Term::constant("Alice".to_string()),
            Term::blank(),
            None,
        );

        let results = query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(results.len(), 1);

        Ok(())
    }
}
