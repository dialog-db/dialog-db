pub use super::Application;
use crate::Cardinality;
use crate::Fact;
pub use crate::artifact::Attribute;
pub use crate::artifact::{ArtifactSelector, Constrained};
pub use crate::context::new_context;
pub use crate::error::{AnalyzerError, QueryResult};
pub use crate::query::Output;
use crate::query::Query;
use crate::selection::{Answer, Answers, Evidence};
use crate::{Entity, Field, Parameters, QueryError, Requirement, Schema, Term, Type, Value};
use crate::{EvaluationContext, Source, try_stream};
use dialog_artifacts::{Artifact, ArtifactStore, Cause};
use serde::{Deserialize, Serialize};
use std::fmt::Display;


use crate::predicate::RelationDescriptor;

/// Per-match cost of the secondary lookup required when scanning the value
/// index (VAE) with `Cardinality::One`. Each match from the primary scan
/// needs a secondary `(attribute, entity)` lookup to verify it is the
/// winning value — the one with the highest cause.
const SECONDARY_LOOKUP_COST: usize = crate::application::fact::SEGMENT_READ_COST;

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

/// Check whether `candidate` is the cardinality-one winner for its
/// `(attribute, entity)` pair by performing a secondary lookup.
///
/// Scans all values for the candidate's `(attribute, entity)` pair, finds the
/// true winner by cause comparison (then fact hash tiebreaker), and returns
/// `true` only if the candidate matches.
///
/// This is needed when the primary scan uses the VAE index (only value known),
/// because different values for the same `(attribute, entity)` pair are
/// scattered across the scan and the sliding-window approach cannot be used.
async fn is_cardinality_one_winner<S: ArtifactStore>(
    source: &S,
    candidate: &Artifact,
) -> Result<bool, QueryError> {
    use futures_util::StreamExt;

    let verification_selector = ArtifactSelector::new()
        .the(candidate.the.clone())
        .of(candidate.of.clone());

    let mut winner: Option<Artifact> = None;
    let mut stream = std::pin::pin!(source.select(verification_selector));

    while let Some(result) = stream.next().await {
        let artifact = result.map_err(|e| QueryError::FactStore(e.to_string()))?;
        winner = Some(match winner {
            None => artifact,
            Some(current) => pick_winner(current, artifact),
        });
    }

    match winner {
        Some(w) => Ok(w.is == candidate.is && w.cause == candidate.cause),
        None => Ok(false),
    }
}

/// Compiled relation query with separate namespace and name terms.
///
/// Unlike `FactApplication` which stores a combined `Term<Attribute>`,
/// `RelationApplication` separates namespace and name so the type system
/// can express queries like "all relations in namespace X" (not yet executable
/// at the storage layer, but the types are ready).
///
/// At the storage boundary, namespace + name constants are combined into a
/// `dialog_artifacts::Attribute` via `"namespace/name".parse()`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RelationApplication {
    /// Namespace of the attribute (e.g., "person")
    namespace: Term<String>,
    /// Name of the attribute within namespace (e.g., "name")
    name: Term<String>,
    /// The entity
    of: Term<Entity>,
    /// The value
    is: Term<Value>,
    /// The cause/provenance
    cause: Term<Cause>,
    /// Type and cardinality metadata, when the attribute is known.
    /// None when namespace/name are variables.
    relation: Option<RelationDescriptor>,
}

impl RelationApplication {
    /// Create a new relation application with separate namespace and name.
    pub fn new(
        namespace: Term<String>,
        name: Term<String>,
        of: Term<Entity>,
        is: Term<Value>,
        cause: Term<Cause>,
        relation: Option<RelationDescriptor>,
    ) -> Self {
        Self {
            namespace,
            name,
            of,
            is,
            cause,
            relation,
        }
    }

    /// Create a relation application from a combined `Term<Attribute>`,
    /// splitting it into namespace and name terms.
    ///
    /// Used during migration from `FactApplication` where callers already
    /// have a combined attribute term.
    pub fn from_attribute(
        the: Term<Attribute>,
        of: Term<Entity>,
        is: Term<Value>,
        cause: Term<Cause>,
        relation: Option<RelationDescriptor>,
    ) -> Self {
        let (namespace, name) = match the {
            Term::Constant(attr) => {
                let s = attr.to_string();
                if let Some((ns, n)) = s.split_once('/') {
                    (
                        Term::Constant(ns.to_string()),
                        Term::Constant(n.to_string()),
                    )
                } else {
                    // Fallback: use whole string as name, empty namespace
                    (Term::Constant(String::new()), Term::Constant(s))
                }
            }
            Term::Variable {
                name: var_name,
                content_type: _,
            } => {
                // When the combined attribute is a variable, both namespace
                // and name become variables. We suffix the variable name to
                // distinguish them internally.
                let ns_var = var_name.as_ref().map(|n| format!("{}_ns", n));
                let name_var = var_name.as_ref().map(|n| format!("{}_name", n));
                (
                    Term::Variable {
                        name: ns_var,
                        content_type: Default::default(),
                    },
                    Term::Variable {
                        name: name_var,
                        content_type: Default::default(),
                    },
                )
            }
        };

        Self {
            namespace,
            name,
            of,
            is,
            cause,
            relation,
        }
    }

    /// Get the namespace term.
    pub fn namespace(&self) -> &Term<String> {
        &self.namespace
    }

    /// Get the name term.
    pub fn name(&self) -> &Term<String> {
        &self.name
    }

    /// Get the 'of' (entity) term.
    pub fn of(&self) -> &Term<Entity> {
        &self.of
    }

    /// Get the 'is' (value) term.
    pub fn is(&self) -> &Term<Value> {
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

    /// Get the cardinality, defaulting to `Cardinality::Many` if the relation
    /// descriptor is not set (unknown relations are assumed to have many values).
    pub fn cardinality(&self) -> Cardinality {
        self.relation
            .as_ref()
            .map(|r| r.cardinality)
            .unwrap_or(Cardinality::Many)
    }

    /// Combine namespace + name back into a `Term<Attribute>`.
    ///
    /// - If both are constants, produces `Term::Constant("namespace/name".parse())`
    /// - If either is a variable, produces a variable term (using the name variable's name
    ///   or falling back to "the")
    pub fn attribute(&self) -> Term<Attribute> {
        match (&self.namespace, &self.name) {
            (Term::Constant(ns), Term::Constant(n)) => {
                let combined = format!("{}/{}", ns, n);
                Term::Constant(
                    combined
                        .parse::<Attribute>()
                        .expect("Failed to parse combined attribute"),
                )
            }
            // If either is a variable, we can't form a constant attribute
            (_, Term::Variable { name, .. }) | (Term::Variable { name, .. }, _) => {
                let var_name = name.clone().or_else(|| Some("the".to_string()));
                Term::Variable {
                    name: var_name,
                    content_type: Default::default(),
                }
            }
        }
    }

    /// Returns the schema describing this application's parameters.
    pub fn schema(&self) -> Schema {
        let requirement = Requirement::new_group();
        let mut schema = Schema::new();

        schema.insert(
            "namespace".to_string(),
            Field {
                description: "Namespace of the attribute".to_string(),
                content_type: Some(Type::String),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );

        schema.insert(
            "name".to_string(),
            Field {
                description: "Name of the attribute".to_string(),
                content_type: Some(Type::String),
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
    pub fn estimate(&self, env: &crate::Environment) -> Option<usize> {
        // The attribute is "known" if both namespace and name are bound
        let the = env.contains(&self.namespace) && env.contains(&self.name);
        let of = env.contains(&self.of);
        let is = env.contains(&self.is);

        let base = self.cardinality().estimate(the, of, is)?;

        // When cardinality is One and only value is known, the storage layer
        // uses the VAE index which does not group by (attribute, entity).
        // Each match needs a secondary (attribute, entity) lookup to confirm
        // the value is the winner, adding cost per match.
        if self.cardinality() == Cardinality::One && is && !the && !of {
            Some(base + SECONDARY_LOOKUP_COST)
        } else {
            Some(base)
        }
    }

    /// Returns the parameters for this relation application.
    pub fn parameters(&self) -> Parameters {
        let mut params = Parameters::new();

        params.insert("namespace".to_string(), self.namespace.as_unknown());
        params.insert("name".to_string(), self.name.as_unknown());
        params.insert("of".to_string(), self.of.as_unknown());
        params.insert("is".to_string(), self.is.clone());
        params
    }
}

impl RelationApplication {
    /// Resolves variables from the given answer.
    pub fn resolve_from_answer(&self, source: &Answer) -> Self {
        let namespace = source.resolve_term(&self.namespace);
        let name = source.resolve_term(&self.name);
        let of = source.resolve_term(&self.of);
        let is = source.resolve_term(&self.is);
        let cause = source.resolve_term(&self.cause);

        Self {
            namespace,
            name,
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
    /// pair is yielded. The winner is chosen by comparing causes (higher wins);
    /// when causes are equal, the fact hash (`Cause::from(&artifact)`) breaks the
    /// tie.
    ///
    /// The strategy depends on which index the storage layer uses:
    ///
    /// - **EAV** (entity known): results are sorted by entity, then attribute,
    ///   then value. We buffer a candidate per `(attribute, entity)` group and
    ///   yield the winner when the group changes.
    ///
    /// - **AEV** (attribute known, entity unknown): results are sorted by
    ///   attribute, then entity, then value. Same sliding-window approach —
    ///   yield when entity changes.
    ///
    /// - **VAE** (only value known): results are sorted by value, then attribute,
    ///   then entity. Different values for the same `(attribute, entity)` pair
    ///   are scattered across the scan, so each match requires a secondary
    ///   lookup on `(attribute, entity)` to confirm it is the winner.
    pub fn evaluate_with_provenance<S: Source, M: Answers>(
        &self,
        source: S,
        answers: M,
    ) -> impl Answers {
        let selector = self.clone();
        try_stream! {
            for await each in answers {
                let input = each?;
                let selection = selector.resolve_from_answer(&input);

                if selector.cardinality() != Cardinality::One {
                    // Cardinality::Many — yield everything.
                    for await artifact in source.select((&selection).try_into()?) {
                        let artifact = artifact?;
                        let fact = Fact::from(&artifact);

                        let mut answer = input.clone();
                        answer.merge(Evidence::Relation {
                            application: &selector,
                            fact: &fact,
                        })?;
                        yield answer;
                    }
                } else {
                    // Cardinality::One — determine strategy from which fields
                    // are constants in the resolved selection (mirroring the
                    // index priority in the storage layer).
                    //
                    // When entity or attribute is known the storage uses EAV or
                    // AEV, both of which group results by (attribute, entity).
                    // A sliding window suffices. When neither is known we fall
                    // back to the VAE index where groups are scattered, so each
                    // match needs a secondary lookup to validate.
                    let require_validation =
                        !matches!(&selection.of, Term::Constant(_))
                        && !matches!(
                            (&selection.namespace, &selection.name),
                            (Term::Constant(_), Term::Constant(_))
                        );

                    let artifacts = source.select((&selection).try_into()?);

                    if require_validation {
                        // VAE scan: results are sorted by value, then
                        // attribute, then entity — different values for the
                        // same (attribute, entity) pair are scattered. Each
                        // match needs a secondary lookup to verify it wins.
                        for await artifact in artifacts {
                            let artifact = artifact?;

                            if is_cardinality_one_winner(&source, &artifact).await? {
                                let fact = Fact::from(&artifact);
                                let mut answer = input.clone();
                                answer.merge(Evidence::Relation {
                                    application: &selector,
                                    fact: &fact,
                                })?;
                                yield answer;
                            }
                        }
                    } else {
                        // EAV or AEV scan: results are grouped by
                        // (attribute, entity). Buffer the winning candidate
                        // and yield when the group changes.
                        let mut candidate: Option<Artifact> = None;

                        for await artifact in artifacts {
                            let artifact = artifact?;

                            let same_group = candidate
                                .as_ref()
                                .is_some_and(|c| c.the == artifact.the && c.of == artifact.of);

                            if same_group {
                                candidate = Some(pick_winner(candidate.unwrap(), artifact));
                            } else {
                                if let Some(winner) = candidate.take() {
                                    let fact = Fact::from(&winner);
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
                            let fact = Fact::from(&winner);
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
        }
    }

    /// Construct a Fact from the given answer by resolving all terms.
    pub fn realize(&self, source: Answer) -> Result<Fact<Value>, QueryError> {
        // Convert blank variables to internal names for retrieval
        let namespace_term = match &self.namespace {
            Term::Variable { name: None, .. } => Term::Variable {
                name: Some("__namespace".to_string()),
                content_type: Default::default(),
            },
            term => term.clone(),
        };
        let name_term = match &self.name {
            Term::Variable { name: None, .. } => Term::Variable {
                name: Some("__name".to_string()),
                content_type: Default::default(),
            },
            term => term.clone(),
        };
        let of_term = match &self.of {
            Term::Variable { name: None, .. } => Term::Variable {
                name: Some("__of".to_string()),
                content_type: Default::default(),
            },
            term => term.clone(),
        };
        let is_term = match &self.is {
            Term::Variable { name: None, .. } => Term::Variable {
                name: Some("__is".to_string()),
                content_type: Default::default(),
            },
            term => term.clone(),
        };

        // Combine namespace + name to get the attribute
        let the: Attribute = match (&namespace_term, &name_term) {
            (Term::Constant(ns), Term::Constant(n)) => format!("{}/{}", ns, n)
                .parse()
                .map_err(|_| QueryError::FactStore("Invalid attribute".to_string()))?,
            _ => {
                // Try to resolve from the answer
                let ns: String = source.get(&namespace_term)?;
                let n: String = source.get(&name_term)?;
                format!("{}/{}", ns, n)
                    .parse()
                    .map_err(|_| QueryError::FactStore("Invalid attribute".to_string()))?
            }
        };

        Ok(Fact::Assertion {
            the,
            of: source.get(&of_term)?,
            is: source.get(&is_term)?,
            cause: Cause([0; 32]),
        })
    }

    /// Execute this relation application as a query, returning a stream of facts.
    pub fn query<S: Source>(&self, source: &S) -> impl Output<Fact>
    where
        Self: Sized,
    {
        use futures_util::stream::once;

        let initial_answer = once(async move { Ok(Answer::new()) });
        let answers = self.evaluate_with_provenance(source.clone(), initial_answer);
        let query = self.clone();

        try_stream! {
            for await answer in answers {
                yield answer?.realize_relation(&query)?;
            }
        }
    }
}

impl Query<Fact> for RelationApplication {
    fn evaluate<S: Source, M: Answers>(&self, context: EvaluationContext<S, M>) -> impl Answers {
        self.evaluate_with_provenance(context.source, context.selection)
    }

    fn realize(&self, input: Answer) -> Result<Fact, QueryError> {
        input.realize_relation(self)
    }
}

impl TryFrom<&RelationApplication> for ArtifactSelector<Constrained> {
    type Error = QueryError;

    fn try_from(from: &RelationApplication) -> Result<Self, Self::Error> {
        let mut selector: Option<ArtifactSelector<Constrained>> = None;

        // Combine namespace + name constants into a single attribute
        if let (Term::Constant(ns), Term::Constant(n)) = (&from.namespace, &from.name) {
            let combined = format!("{}/{}", ns, n);
            if let Ok(attr) = combined.parse::<Attribute>() {
                selector = Some(match selector {
                    None => ArtifactSelector::new().the(attr),
                    Some(s) => s.the(attr),
                });
            }
        }

        // Convert entity (of)
        match &from.of {
            Term::Constant(of) => {
                selector = Some(match selector {
                    None => ArtifactSelector::new().of(of.to_owned()),
                    Some(s) => s.of(of.to_owned()),
                });
            }
            Term::Variable { .. } => {}
        }

        // Convert value (is)
        match &from.is {
            Term::Constant(value) => {
                selector = Some(match selector {
                    None => ArtifactSelector::new().is(value.clone()),
                    Some(s) => s.is(value.clone()),
                });
            }
            Term::Variable { .. } => {}
        }

        selector.ok_or_else(|| QueryError::EmptySelector {
            message: "At least one field must be constrained".to_string(),
        })
    }
}

impl Display for RelationApplication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Relation {{")?;
        write!(f, "namespace: {},", self.namespace)?;
        write!(f, "name: {},", self.name)?;
        write!(f, "of: {},", self.of)?;
        write!(f, "is: {},", self.is)?;
        write!(f, "cause: {},", self.cause)?;
        write!(f, "}}")
    }
}

impl std::ops::Not for RelationApplication {
    type Output = crate::Premise;

    fn not(self) -> Self::Output {
        crate::Premise::Exclude(crate::Negation::not(Application::Relation(self)))
    }
}

impl From<RelationApplication> for Application {
    fn from(application: RelationApplication) -> Self {
        Application::Relation(application)
    }
}

impl From<RelationApplication> for crate::Premise {
    fn from(application: RelationApplication) -> Self {
        crate::Premise::Apply(Application::Relation(application))
    }
}

impl From<&RelationApplication> for crate::Premise {
    fn from(application: &RelationApplication) -> Self {
        crate::Premise::Apply(Application::Relation(application.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Output;
    use crate::selection::{Answer, Answers};
    use crate::{Relation, Session};
    use dialog_storage::MemoryStorageBackend;
    use futures_util::stream::once;

    #[dialog_common::test]
    async fn test_relation_application_with_provenance() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = "person/name".parse::<Attribute>()?;

        let claims = vec![Relation {
            the: name_attr.clone(),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        }];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let rel_app = RelationApplication::new(
            Term::Constant("person".to_string()),
            Term::Constant("name".to_string()),
            Term::var("person"),
            Term::var("name"),
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

        assert!(answer.contains(&Term::<Entity>::var("person")));
        assert!(answer.contains(&Term::<Value>::var("name")));

        let person_id: Entity = answer.get(&Term::var("person"))?;
        let name_value: Value = answer.resolve(&Term::<Value>::var("name"))?;

        assert_eq!(person_id, alice);
        assert_eq!(name_value, Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn test_cardinality_one_returns_single_value() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = "person/name".parse::<Attribute>()?;

        // Assert two different values for the same entity-attribute pair
        // in separate transactions so both persist in the store.
        let mut session = Session::open(artifacts.clone());
        session
            .transact(vec![Relation {
                the: name_attr.clone(),
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            }])
            .await?;

        let mut session = Session::open(artifacts.clone());
        session
            .transact(vec![Relation {
                the: name_attr.clone(),
                of: alice.clone(),
                is: Value::String("Alicia".to_string()),
            }])
            .await?;

        // Query with Cardinality::One — should return only one value
        let rel_app = RelationApplication::new(
            Term::Constant("person".to_string()),
            Term::Constant("name".to_string()),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::One,
            )),
        );

        let session = Session::open(artifacts.clone());
        let results = rel_app.query(&session).try_vec().await?;

        assert_eq!(
            results.len(),
            1,
            "Cardinality::One should return only one value per entity-attribute pair, got {}",
            results.len()
        );

        // Query with Cardinality::Many — should return both values
        let rel_app_many = RelationApplication::new(
            Term::Constant("person".to_string()),
            Term::Constant("name".to_string()),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::Many,
            )),
        );

        let session = Session::open(artifacts);
        let results_many = rel_app_many.query(&session).try_vec().await?;

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
                .transact(vec![Relation {
                    the: $the.clone(),
                    of: $of.clone(),
                    is: $is,
                }])
                .await
                .unwrap();
        }};
    }

    // Cardinality::One with entity known (EAV scan).
    // The index sorts by E, A, V so all values for a given (entity, attribute)
    // pair are contiguous. The sliding window picks the winner by cause, then
    // by fact hash as a tiebreaker.
    #[dialog_common::test]
    async fn test_cardinality_one_entity_known_eav_scan() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = "person/name".parse::<Attribute>()?;
        let age_attr = "person/age".parse::<Attribute>()?;

        // Two conflicting values for person/name of alice
        assert_relation!(artifacts, name_attr, alice, Value::String("Alice".into()));
        assert_relation!(artifacts, name_attr, alice, Value::String("Alicia".into()));
        // One value for person/age of alice
        assert_relation!(artifacts, age_attr, alice, Value::SignedInt(30));

        // Entity is known → EAV scan, attribute is variable
        let rel_app = RelationApplication::new(
            Term::var("ns"),
            Term::var("attr"),
            Term::Constant(alice.clone()),
            Term::var("value"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let session = Session::open(artifacts);
        let results = rel_app.query(&session).try_vec().await?;

        // Should get exactly 2 facts: one winner for person/name, one for person/age
        assert_eq!(
            results.len(),
            2,
            "EAV scan with Cardinality::One should return one value per attribute, got {}",
            results.len()
        );

        let name_results: Vec<_> = results.iter().filter(|f| f.the() == &name_attr).collect();
        let age_results: Vec<_> = results.iter().filter(|f| f.the() == &age_attr).collect();

        assert_eq!(name_results.len(), 1, "Should have exactly one name result");
        assert_eq!(age_results.len(), 1, "Should have exactly one age result");
        assert_eq!(age_results[0].is(), &Value::SignedInt(30));

        Ok(())
    }

    // Cardinality::One with attribute known, entity unknown (AEV scan).
    // The index sorts by A, E, V so all values for a given (attribute, entity)
    // pair are contiguous. The sliding window picks the winner when the entity
    // changes in the scan.
    #[dialog_common::test]
    async fn test_cardinality_one_attribute_known_aev_scan() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let name_attr = "person/name".parse::<Attribute>()?;

        // Two conflicting values for alice's name
        assert_relation!(artifacts, name_attr, alice, Value::String("Alice".into()));
        assert_relation!(artifacts, name_attr, alice, Value::String("Alicia".into()));

        // Two conflicting values for bob's name
        assert_relation!(artifacts, name_attr, bob, Value::String("Bob".into()));
        assert_relation!(artifacts, name_attr, bob, Value::String("Robert".into()));

        // Attribute is known, entity is variable → AEV scan
        let rel_app = RelationApplication::new(
            Term::Constant("person".to_string()),
            Term::Constant("name".to_string()),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::One,
            )),
        );

        let session = Session::open(artifacts);
        let results = rel_app.query(&session).try_vec().await?;

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
    // The index sorts by V, A, E — different values for the same (attribute,
    // entity) pair are in separate regions of the scan. Each match needs a
    // secondary lookup to verify it is the cardinality-one winner.
    #[dialog_common::test]
    async fn test_cardinality_one_value_known_vae_scan() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = "person/name".parse::<Attribute>()?;

        // Two conflicting values for alice's name
        assert_relation!(artifacts, name_attr, alice, Value::String("Alice".into()));
        assert_relation!(artifacts, name_attr, alice, Value::String("Alicia".into()));

        // Determine the expected winner: query with attribute known to get the
        // winner from AEV, then verify the VAE lookup matches.
        let aev_app = RelationApplication::new(
            Term::Constant("person".to_string()),
            Term::Constant("name".to_string()),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::One,
            )),
        );

        let session = Session::open(artifacts.clone());
        let aev_results = aev_app.query(&session).try_vec().await?;
        assert_eq!(aev_results.len(), 1);
        let expected_winner_value = aev_results[0].is().clone();

        // Now query by the winning value with only value known → VAE scan
        let vae_app = RelationApplication::new(
            Term::var("ns"),
            Term::var("attr"),
            Term::var("person"),
            Term::Constant(expected_winner_value.clone()),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::One,
            )),
        );

        let session = Session::open(artifacts.clone());
        let vae_results = vae_app.query(&session).try_vec().await?;

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

        let vae_loser_app = RelationApplication::new(
            Term::var("ns"),
            Term::var("attr"),
            Term::var("person"),
            Term::Constant(losing_value),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::One,
            )),
        );

        let session = Session::open(artifacts);
        let vae_loser_results = vae_loser_app.query(&session).try_vec().await?;

        assert_eq!(
            vae_loser_results.len(),
            0,
            "VAE scan for the losing value should return nothing, got {}",
            vae_loser_results.len()
        );

        Ok(())
    }

    // Verify that the winner is deterministic: regardless of query path
    // (EAV, AEV, VAE), the same value wins for a given (attribute, entity).
    #[dialog_common::test]
    async fn test_cardinality_one_winner_is_deterministic() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = "person/name".parse::<Attribute>()?;

        assert_relation!(artifacts, name_attr, alice, Value::String("Alice".into()));
        assert_relation!(artifacts, name_attr, alice, Value::String("Alicia".into()));

        // EAV path (entity known)
        let eav_app = RelationApplication::new(
            Term::var("ns"),
            Term::var("attr"),
            Term::Constant(alice.clone()),
            Term::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let session = Session::open(artifacts.clone());
        let eav_results = eav_app.query(&session).try_vec().await?;
        let eav_name_results: Vec<_> = eav_results
            .iter()
            .filter(|f| f.the() == &name_attr)
            .collect();
        assert_eq!(eav_name_results.len(), 1);
        let eav_winner = eav_name_results[0].is().clone();

        // AEV path (attribute known)
        let aev_app = RelationApplication::new(
            Term::Constant("person".to_string()),
            Term::Constant("name".to_string()),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::One,
            )),
        );

        let session = Session::open(artifacts.clone());
        let aev_results = aev_app.query(&session).try_vec().await?;
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
    async fn test_relation_application_from_attribute() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = "person/name".parse::<Attribute>()?;

        let claims = vec![Relation {
            the: name_attr.clone(),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        }];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Use from_attribute to create a RelationApplication
        let rel_app = RelationApplication::from_attribute(
            Term::Constant(name_attr),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            None,
        );

        // Verify namespace/name were split correctly
        assert_eq!(rel_app.namespace(), &Term::Constant("person".to_string()));
        assert_eq!(rel_app.name(), &Term::Constant("name".to_string()));

        let session = Session::open(artifacts);
        let results = rel_app.query(&session).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of(), &alice);
        assert_eq!(results[0].is(), &Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn test_relation_application_query() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = "person/name".parse::<Attribute>()?;

        let claims = vec![Relation {
            the: name_attr.clone(),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        }];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let rel_app = RelationApplication::new(
            Term::Constant("person".to_string()),
            Term::Constant("name".to_string()),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(
                Some(Type::String),
                Cardinality::Many,
            )),
        );

        let session = Session::open(artifacts);
        let results = rel_app.query(&session).try_vec().await?;

        assert_eq!(results.len(), 1);
        let fact = &results[0];
        assert_eq!(fact.the(), &name_attr);
        assert_eq!(fact.of(), &alice);
        assert_eq!(fact.is(), &Value::String("Alice".to_string()));

        Ok(())
    }
}
