use crate::Cardinality;
use crate::Claim;
pub use crate::artifact::Attribute;
pub use crate::artifact::{ArtifactSelector, Constrained};
use crate::attribute::The;
use crate::environment::Environment;
pub use crate::error::{AnalyzerError, QueryResult};
use crate::negation::Negation;
pub use crate::proposition::Proposition;
use crate::query::Application;
pub use crate::query::Output;
use crate::relation::descriptor::RelationDescriptor;
use crate::schema::SEGMENT_READ_COST;
use crate::selection::{Answer, Answers, Evidence};
use crate::{
    Entity, Field, Parameter, Parameters, Premise, QueryError, Requirement, Schema, Source, Term,
    Type, Value, try_stream,
};
use dialog_artifacts::{Artifact, Cause};
use futures_util::future::Either;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

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
/// answer only if the candidate matches the winner.
fn verify_winner<S: Source>(source: S, selector: RelationQuery, input: Answer) -> impl Answers {
    try_stream! {
        let attribute_term = selector.attribute();
        let attribute: Attribute = input.get(&attribute_term)?;
        let entity: Entity = input.get(selector.of())?;
        let candidate_value: Value = input.resolve(selector.is())?;
        let candidate_cause: Cause = input.get(selector.cause())?;

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
/// (e.g., `"person/name"`). The optional [`RelationDescriptor`] provides
/// type and cardinality metadata used by the cost estimator during planning.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RelationQuery {
    /// The relation identifier (e.g., "person/name")
    the: Term<The>,
    /// The entity
    of: Term<Entity>,
    /// The value
    is: Parameter,
    /// The cause/provenance
    cause: Term<Cause>,
    /// Type and cardinality metadata, when the attribute is known.
    /// None when the is a variable.
    relation: Option<RelationDescriptor>,
}

impl RelationQuery {
    /// Create a new relation application.
    pub fn new(
        the: Term<The>,
        of: Term<Entity>,
        is: impl Into<Parameter>,
        cause: Term<Cause>,
        relation: Option<RelationDescriptor>,
    ) -> Self {
        let is = is.into();
        Self {
            the,
            of,
            is,
            cause,
            relation,
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
    pub fn is(&self) -> &Parameter {
        &self.is
    }

    /// Get the 'cause' term.
    pub fn cause(&self) -> &Term<Cause> {
        &self.cause
    }

    /// Get the relation descriptor, if known.
    pub fn relation(&self) -> Option<&RelationDescriptor> {
        self.relation.as_ref()
    }

    /// Resolve an artifact into a `Claim`.
    pub fn resolve(&self, artifact: &Artifact) -> Claim {
        Claim {
            the: The::from(artifact.the.clone()),
            of: artifact.of.clone(),
            is: artifact.is.clone(),
            cause: artifact.cause.clone().unwrap_or(Cause([0; 32])),
        }
    }

    /// Get the cardinality, defaulting to `Cardinality::Many` if the relation
    /// descriptor is not set (unknown relations are assumed to have many values).
    pub fn cardinality(&self) -> Cardinality {
        self.relation
            .as_ref()
            .map(|r| r.cardinality)
            .unwrap_or(Cardinality::Many)
    }

    /// Map `Term<The>` to `Term<Attribute>`.
    pub fn attribute(&self) -> Term<Attribute> {
        match &self.the {
            Term::Constant(the) => Term::Constant(Attribute::from(the)),
            Term::Variable { name, .. } => Term::Variable { name: name.clone() },
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

        params.insert("the".to_string(), Parameter::from(&self.the));
        params.insert("of".to_string(), Parameter::from(&self.of));
        params.insert("is".to_string(), self.is.clone());
        params
    }
}

impl RelationQuery {
    /// Resolves variables from the given answer.
    pub fn resolve_from_answer(&self, source: &Answer) -> Self {
        let the = source.resolve_term(&self.the);
        let of = source.resolve_term(&self.of);
        let is = source.resolve_parameter(&self.is);
        let cause = source.resolve_term(&self.cause);

        Self {
            the,
            of,
            is,
            cause,
            relation: self.relation.clone(),
        }
    }

    /// Evaluate with fact provenance tracking.
    ///
    /// For `Cardinality::Many`, all matching artifacts are yielded.
    ///
    /// For `Cardinality::One`, only the winning artifact per `(attribute, entity)`
    /// pair is yielded. The strategy depends on which index the storage layer
    /// uses — see [`Self::evaluate_cardinality_one`].
    pub fn evaluate_with_provenance<S: Source, M: Answers>(
        self,
        source: S,
        answers: M,
    ) -> impl Answers {
        if self.cardinality() == Cardinality::One {
            Either::Left(self.evaluate_cardinality_one(source, answers))
        } else {
            Either::Right(self.evaluate_cardinality_many(source, answers))
        }
    }

    /// Evaluate yielding all matching artifacts.
    fn evaluate_cardinality_many<S: Source, M: Answers>(
        self,
        source: S,
        answers: M,
    ) -> impl Answers {
        let selector = self;
        try_stream! {
            for await each in answers {
                let input = each?;
                let selection = selector.resolve_from_answer(&input);

                for await artifact in source.select((&selection).try_into()?) {
                    let artifact = artifact?;
                    let relation = selector.resolve(&artifact);

                    let mut answer = input.clone();
                    answer.merge(Evidence::Relation {
                        application: &selector,
                        fact: &relation,
                    })?;
                    yield answer;
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
    fn evaluate_cardinality_one<S: Source, M: Answers>(
        self,
        source: S,
        answers: M,
    ) -> impl Answers {
        let entity_known = matches!(&self.of, Term::Constant(_));
        let attribute_known = matches!(&self.the, Term::Constant(_));

        if entity_known || attribute_known {
            Either::Left(self.select_winners(source, answers))
        } else {
            let selector = self.clone();
            let candidates = self.evaluate_cardinality_many(source.clone(), answers);
            Either::Right(
                candidates.try_flat_map(move |input| {
                    verify_winner(source.clone(), selector.clone(), input)
                }),
            )
        }
    }

    /// EAV/AEV scan: results are grouped by `(attribute, entity)`.
    /// Buffer the winning candidate and yield when the group changes.
    fn select_winners<S: Source, M: Answers>(self, source: S, answers: M) -> impl Answers {
        let selector = self;
        try_stream! {
            for await each in answers {
                let input = each?;
                let selection = selector.resolve_from_answer(&input);
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
                            let fact = selector.resolve(&winner);
                            let mut answer = input.clone();
                            answer.merge(Evidence::Relation {
                                application: &selector,
                                fact: &fact,
                            })?;
                            yield answer;
                        }
                        candidate = Some(artifact);
                    }
                }

                if let Some(winner) = candidate.take() {
                    let fact = selector.resolve(&winner);
                    let mut answer = input.clone();
                    answer.merge(Evidence::Relation {
                        application: &selector,
                        fact: &fact,
                    })?;
                    yield answer;
                }
            }
        }
    }

    /// Construct a Claim from the given answer by resolving all terms.
    pub fn realize(&self, source: Answer) -> Result<Claim, QueryError> {
        let the_term = match &self.the {
            Term::Variable { name: None, .. } => Term::Variable {
                name: Some("__the".to_string()),
            },
            term => term.clone(),
        };
        let of_term = match &self.of {
            Term::Variable { name: None, .. } => Term::Variable {
                name: Some("__of".to_string()),
            },
            term => term.clone(),
        };
        let is_param = match &self.is {
            Parameter::Variable { name: None, .. } => Parameter::Variable {
                name: Some("__is".to_string()),
                typ: None,
            },
            param => param.clone(),
        };

        let the: The = match &the_term {
            Term::Constant(t) => t.clone(),
            _ => source.get(&the_term)?,
        };

        Ok(Claim {
            the,
            of: source.get(&of_term)?,
            is: source.resolve(&is_param)?,
            cause: Cause([0; 32]),
        })
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

    fn evaluate<S: Source, M: Answers>(self, answers: M, source: &S) -> impl Answers {
        self.evaluate_with_provenance(source.clone(), answers)
    }

    fn realize(&self, input: Answer) -> Result<Claim, QueryError> {
        input.realize(self)
    }
}

impl TryFrom<&RelationQuery> for ArtifactSelector<Constrained> {
    type Error = QueryError;

    fn try_from(from: &RelationQuery) -> Result<Self, Self::Error> {
        let mut selector: Option<ArtifactSelector<Constrained>> = None;

        if let Term::Constant(the) = &from.the {
            let attr = Attribute::from(the);
            selector = Some(match selector {
                None => ArtifactSelector::new().the(attr),
                Some(s) => s.the(attr),
            });
        }

        match &from.of {
            Term::Constant(of) => {
                selector = Some(match selector {
                    None => ArtifactSelector::new().of(of.to_owned()),
                    Some(s) => s.of(of.to_owned()),
                });
            }
            Term::Variable { .. } => {}
        }

        match &from.is {
            Parameter::Constant(value) => {
                selector = Some(match selector {
                    None => ArtifactSelector::new().is(value.clone()),
                    Some(s) => s.is(value.clone()),
                });
            }
            Parameter::Variable { .. } => {}
        }

        selector.ok_or_else(|| QueryError::EmptySelector {
            message: "At least one field must be constrained".to_string(),
        })
    }
}

impl Display for RelationQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Claim {{")?;
        write!(f, "the: {},", self.the)?;
        write!(f, "of: {},", self.of)?;
        write!(f, "is: {},", self.is)?;
        write!(f, "cause: {},", self.cause)?;
        write!(f, "}}")
    }
}

impl std::ops::Not for RelationQuery {
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
    use crate::selection::{Answer, Answers};
    use crate::{Association, Session, the};
    use dialog_storage::MemoryStorageBackend;
    use futures_util::stream::once;

    #[dialog_common::test]
    async fn it_evaluates_relation_with_provenance() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::the;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        let claims = vec![Association {
            the: name_attr.clone(),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        }];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let rel_app = RelationQuery::new(
            Term::Constant(the!("person/name")),
            Term::var("person"),
            Parameter::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::Many,
            )),
        );

        let session = Session::open(artifacts);
        let initial_answer = once(async move { Ok(Answer::new()) });
        let answers = rel_app.evaluate_with_provenance(session, initial_answer);

        let results = Answers::try_vec(answers).await?;

        assert_eq!(results.len(), 1);

        let answer = &results[0];

        assert!(answer.contains(&Parameter::var("person")));
        assert!(answer.contains(&Parameter::var("name")));

        let person_id: Entity = answer.get(&Term::var("person"))?;
        let name_value: Value = answer.resolve(&Parameter::var("name"))?;

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
            .transact(vec![Association {
                the: name_attr.clone(),
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            }])
            .await?;

        let mut session = Session::open(artifacts.clone());
        session
            .transact(vec![Association {
                the: name_attr.clone(),
                of: alice.clone(),
                is: Value::String("Alicia".to_string()),
            }])
            .await?;

        // Query with Cardinality::One — should return only one value
        let rel_app = RelationQuery::new(
            Term::Constant(the!("person/name")),
            Term::var("person"),
            Parameter::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::One,
            )),
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
            Term::Constant(the!("person/name")),
            Term::var("person"),
            Parameter::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::Many,
            )),
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
            session
                .transact(vec![Association {
                    the: $the.clone(),
                    of: $of.clone(),
                    is: $is,
                }])
                .await
                .unwrap();
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
        assert_relation!(artifacts, name_attr, alice, Value::String("Alice".into()));
        assert_relation!(artifacts, name_attr, alice, Value::String("Alicia".into()));
        // One value for person/age of alice
        assert_relation!(artifacts, age_attr, alice, Value::SignedInt(30));

        // Entity is known → EAV scan, attribute is variable
        let rel_app = RelationQuery::new(
            Term::var("the"),
            Term::Constant(alice.clone()),
            Parameter::var("value"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
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

        let name_results: Vec<_> = results
            .iter()
            .filter(|f| f.the() == Attribute::from(&name_attr))
            .collect();
        let age_results: Vec<_> = results
            .iter()
            .filter(|f| f.the() == Attribute::from(&age_attr))
            .collect();

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
        assert_relation!(artifacts, name_attr, alice, Value::String("Alice".into()));
        assert_relation!(artifacts, name_attr, alice, Value::String("Alicia".into()));

        // Two conflicting values for bob's name
        assert_relation!(artifacts, name_attr, bob, Value::String("Bob".into()));
        assert_relation!(artifacts, name_attr, bob, Value::String("Robert".into()));

        // Attribute is known, entity is variable → AEV scan
        let rel_app = RelationQuery::new(
            Term::Constant(the!("person/name")),
            Term::var("person"),
            Parameter::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::One,
            )),
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
        assert_relation!(artifacts, name_attr, alice, Value::String("Alice".into()));
        assert_relation!(artifacts, name_attr, alice, Value::String("Alicia".into()));

        // Determine the expected winner: query with attribute known to get the
        // winner from AEV, then verify the VAE lookup matches.
        let aev_app = RelationQuery::new(
            Term::Constant(the!("person/name")),
            Term::var("person"),
            Parameter::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::One,
            )),
        );

        let session = Session::open(artifacts.clone());
        let aev_results = aev_app.perform(&session).try_vec().await?;
        assert_eq!(aev_results.len(), 1);
        let expected_winner_value = aev_results[0].is().clone();

        // Now query by the winning value with only value known → VAE scan
        let vae_app = RelationQuery::new(
            Term::var("the"),
            Term::var("person"),
            Parameter::Constant(expected_winner_value.clone()),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::One,
            )),
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
            Parameter::Constant(losing_value),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::One,
            )),
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

        assert_relation!(artifacts, name_attr, alice, Value::String("Alice".into()));
        assert_relation!(artifacts, name_attr, alice, Value::String("Alicia".into()));

        // EAV path (entity known)
        let eav_app = RelationQuery::new(
            Term::var("the"),
            Term::Constant(alice.clone()),
            Parameter::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let session = Session::open(artifacts.clone());
        let eav_results = eav_app.perform(&session).try_vec().await?;
        let eav_name_results: Vec<_> = eav_results
            .iter()
            .filter(|f| f.the() == Attribute::from(&name_attr))
            .collect();
        assert_eq!(eav_name_results.len(), 1);
        let eav_winner = eav_name_results[0].is().clone();

        // AEV path (attribute known)
        let aev_app = RelationQuery::new(
            Term::Constant(the!("person/name")),
            Term::var("person"),
            Parameter::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::One,
            )),
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

        let claims = vec![Association {
            the: name_attr.into(),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        }];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let rel_app = RelationQuery::new(
            Term::Constant(the!("person/name")),
            Term::var("person"),
            Parameter::var("name"),
            Term::var("cause"),
            None,
        );

        // Verify the term
        assert_eq!(rel_app.the(), &Term::Constant(the!("person/name")));

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

        let claims = vec![Association {
            the: name_attr.clone(),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        }];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let rel_app = RelationQuery::new(
            Term::Constant(the!("person/name")),
            Term::var("person"),
            Parameter::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::Many,
            )),
        );

        let session = Session::open(artifacts);
        let results = rel_app.perform(&session).try_vec().await?;

        assert_eq!(results.len(), 1);
        let fact = &results[0];
        assert_eq!(fact.the(), Attribute::from(name_attr));
        assert_eq!(fact.of(), &alice);
        assert_eq!(fact.is(), &Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_facts() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;

        let alice_name = Association {
            the: the!("user/name"),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        };

        let mut session = Session::open(artifacts.clone());
        session.transact(vec![alice_name.clone()]).await?;

        let query_constant = RelationQuery::new(
            Term::Constant(the!("user/name")),
            alice.clone().into(),
            Parameter::blank(),
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
        session.transact([!alice_name]).await?;

        let query2 = RelationQuery::new(
            Term::Constant(the!("user/name")),
            alice.clone().into(),
            Parameter::blank(),
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

        let claims = vec![Association {
            the: the!("user/name"),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        }];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let mixed_query = RelationQuery::new(
            Term::Constant(the!("user/name")),
            alice.clone().into(),
            Parameter::blank(),
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
            Association {
                the: the!("user/name"),
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Association {
                the: the!("user/name"),
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let query = RelationQuery::new(
            Term::Constant(the!("user/name")),
            Term::blank(),
            Parameter::blank(),
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

        let claims = vec![Association {
            the: the!("user/name"),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        }];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let query = RelationQuery::new(
            Term::Constant(the!("user/name")),
            alice.clone().into(),
            Parameter::from("Alice".to_string()),
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
