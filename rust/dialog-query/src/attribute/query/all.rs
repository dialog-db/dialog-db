use crate::Cardinality;
use crate::Claim;
use crate::artifact::{ArtifactSelector, ArtifactsAttribute, Constrained};
use crate::attribute::The;
use crate::environment::Environment;
use crate::query::Application;
use crate::query::Output;
use crate::selection::{Match, Selection};
use crate::source::SelectRules;
use crate::types::{Any, Record};
use crate::{
    Entity, EvaluationError, Field, Parameters, Requirement, Schema, Term, Type, Value, try_stream,
};
use dialog_artifacts::{Artifact, Cause, Select};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::fmt::{Formatter, Result as FmtResult};

/// Base EAV scan query that yields all matching artifacts.
///
/// Represents a query against the fact store in the form
/// `(the, of, is, cause)` where each position is a [`Term`] — either a
/// constant that constrains the lookup or a variable that will be bound
/// by the results. All matches are yielded without cardinality filtering.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AttributeQueryAll {
    /// The relation identifier (e.g., "person/name")
    the: Term<The>,
    /// The entity
    of: Term<Entity>,
    /// The value
    is: Term<Any>,
    /// The cause/provenance
    cause: Term<Cause>,
    /// Internal handle for claim storage.
    source: Term<Record>,
}

impl AttributeQueryAll {
    /// Create a new attribute query that yields all matches.
    pub fn new(the: Term<The>, of: Term<Entity>, is: Term<Any>, cause: Term<Cause>) -> Self {
        Self {
            the,
            of,
            is,
            cause,
            source: Term::<Record>::unique(),
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

    /// Get the source term (internal claim handle).
    pub fn source(&self) -> &Term<Record> {
        &self.source
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

    /// Merge a matched artifact into a match: store the claim and bind
    /// the/of/is/cause values to the corresponding terms.
    pub(crate) fn merge(
        &self,
        candidate: &mut Match,
        artifact: &Artifact,
    ) -> Result<(), EvaluationError> {
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

    /// Resolves variables from the given match.
    pub fn resolve(&self, source: &Match) -> Self {
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

    /// Estimate cost for Cardinality::Many semantics.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        let the = self.the.is_bound(env);
        let of = self.of.is_bound(env);
        let is = self.is.is_bound(env);

        Cardinality::Many.estimate(the, of, is)
    }

    /// Returns the parameters for this query.
    pub fn parameters(&self) -> Parameters {
        let mut params = Parameters::new();

        params.insert("the".to_string(), Term::<Any>::from(&self.the));
        params.insert("of".to_string(), Term::<Any>::from(&self.of));
        params.insert("is".to_string(), self.is.clone());
        params
    }

    /// Evaluate yielding all matching artifacts.
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        env: &'a Env,
        selection: M,
    ) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        let selector = self;
        try_stream! {
            for await candidate in selection {
                let base = candidate?;
                let selection = selector.resolve(&base);

                let stream = Provider::<Select<'_>>::execute(env, (&selection).try_into()?).await?;
                for await artifact in stream {
                    let artifact = artifact?;
                    let mut extension = base.clone();
                    selector.merge(&mut extension, &artifact)?;
                    yield extension;
                }
            }
        }
    }

    /// Execute this query, returning a stream of claims.
    pub fn perform<'a, Env>(self, env: &'a Env) -> impl Output<Claim> + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
        Self: Sized,
    {
        Application::perform(self, env)
    }
}

impl Application for AttributeQueryAll {
    type Conclusion = Claim;

    fn evaluate<'a, Env, M: Selection + 'a>(self, selection: M, env: &'a Env) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        self.evaluate(env, selection)
    }

    fn realize(&self, input: Match) -> Result<Claim, EvaluationError> {
        input.prove(&self.source)
    }
}

impl TryFrom<&AttributeQueryAll> for ArtifactSelector<Constrained> {
    type Error = EvaluationError;

    fn try_from(from: &AttributeQueryAll) -> Result<Self, Self::Error> {
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

impl Display for AttributeQueryAll {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "Claim {{")?;
        write!(f, "the: {},", self.the)?;
        write!(f, "of: {},", self.of)?;
        write!(f, "is: {},", self.is)?;
        write!(f, "cause: {},", self.cause)?;
        write!(f, "}}")
    }
}

#[cfg(all(test, feature = "repository-tests"))]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::query::Output;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::the;
    use dialog_repository::helpers::{test_operator, test_repo};

    #[dialog_common::test]
    async fn it_scans_with_all_variables() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of(), &alice);
        assert_eq!(results[0].is(), &Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_scans_with_constant_entity() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .commit()
            .perform(&operator)
            .await?;

        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::from(alice.clone()),
            Term::var("name"),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of(), &alice);
        assert_eq!(results[0].is(), &Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_multiple_values() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alicia".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::from(alice.clone()),
            Term::var("name"),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(
            results.len(),
            2,
            "AttributeQueryAll should return all values, not just the winner"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_scans_with_constant_value() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alicia".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::constant("Alice".to_string()),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is(), &Value::String("Alice".to_string()));

        Ok(())
    }
}
