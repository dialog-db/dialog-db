use crate::Cardinality;
use crate::Claim;
use crate::artifact::{ArtifactSelector, ArtifactsAttribute, Constrained};
use crate::attribute::The;
use crate::attribute::query::Resolution;
use crate::environment::Environment;
use crate::query::Application;
use crate::query::Output;
use crate::selection::{Match, Selection};
use crate::source::SelectRules;
use crate::type_system::Type as Kind;
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
    /// Create a new attribute query. The resolution policy is
    /// derived from `is`'s kind: a set-widened (`Nothing`-bit-set)
    /// kind yields [`Resolution::Optional`] — one Absent fallback
    /// row on miss; otherwise [`Resolution::Required`] — zero rows
    /// on miss.
    pub fn new(the: Term<The>, of: Term<Entity>, is: Term<Any>, cause: Term<Cause>) -> Self {
        Self {
            the,
            of,
            is,
            cause,
            source: Term::<Record>::unique(),
        }
    }

    /// Resolution policy derived from `is`'s kind. A set-widened
    /// `is` (admits `Nothing`) is [`Resolution::Optional`];
    /// otherwise [`Resolution::Required`].
    pub fn resolution(&self) -> Resolution {
        if self.is.is_optional() {
            Resolution::Optional
        } else {
            Resolution::Required
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

    /// Resolves variables from the given match. `Absent` bindings
    /// leave the term unchanged (same as unbound) — only Present
    /// bindings substitute.
    pub fn resolve(&self, source: &Match) -> Self {
        let the = self.the.resolve(source);
        let of = self.of.resolve(source);
        let is = match source.lookup(&self.is).and_then(|b| b.content()) {
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
                content_type: Some(Kind::primitive(Type::Symbol)),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );

        schema.insert(
            "of".to_string(),
            Field {
                description: "Entity of the relation".to_string(),
                content_type: Some(Kind::primitive(Type::Entity)),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );

        // The `is` term's kind already encodes optionality via the
        // `Nothing` atom. `None` means "no static info" — the
        // unifier resolves at rule-compile time.
        schema.insert(
            "is".to_string(),
            Field {
                description: "Value of the relation".to_string(),
                content_type: self.is.kind(),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );

        // The `cause` slot is bound by the merge step on every
        // Present row; when the query is optional the fallback
        // row binds it to `Absent`, so the slot is set-widened.
        let cause_content = if self.is.is_optional() {
            Kind::primitive(Type::Bytes).optional()
        } else {
            Kind::primitive(Type::Bytes)
        };
        schema.insert(
            "cause".to_string(),
            Field {
                description: "Causal stamp of the relation".to_string(),
                content_type: Some(cause_content),
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
        params.insert("cause".to_string(), Term::<Any>::from(&self.cause));
        params
    }

    /// Evaluate yielding all matching artifacts.
    ///
    /// With [`Resolution::Required`] (the default), zero rows are
    /// yielded for an input when no fact matches the lookup —
    /// standard EAV semantics.
    ///
    /// With [`Resolution::Optional`], if no fact matches for an
    /// input row, a single fallback row is yielded with the `is`
    /// slot (and the `cause` slot, if a named variable) bound to
    /// [`Binding::Absent`](crate::Binding::Absent). This is the
    /// row-layer signal for "we looked, no fact found."
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

                let mut produced = false;
                let stream = Provider::<Select<'_>>::execute(env, (&selection).try_into()?).await?;
                for await artifact in stream {
                    let artifact = artifact?;
                    let mut extension = base.clone();
                    selector.merge(&mut extension, &artifact)?;
                    produced = true;
                    yield extension;
                }

                // Optional fallback: if no rows were produced for
                // this input and the resolution is Optional, yield
                // one row with `is` (and named `cause`) bound to
                // Absent.
                if !produced && selector.is.is_optional() {
                    let mut fallback = base;
                    fallback.bind_absent(&selector.is)?;
                    let cause_term: Term<Any> = Term::<Any>::from(&selector.cause);
                    fallback.bind_absent(&cause_term)?;
                    yield fallback;
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
            let (d, n) = relation.split();
            selector = Some(match selector {
                None => ArtifactSelector::new().within(d).named(n),
                Some(s) => s.within(d).named(n),
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

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::query::Output;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::the;
    use dialog_repository::helpers::{test_operator_with_profile, test_repo};

    #[dialog_common::test]
    async fn it_scans_with_all_variables() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
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

    /// `AttributeQueryAll::schema()` declares the `the` slot as
    /// a singleton primitive over `Symbol`.
    #[dialog_common::test]
    fn schema_the_slot_is_primitive_symbol() {
        let query = AttributeQueryAll::new(
            Term::var("the"),
            Term::var("of"),
            Term::var("is"),
            Term::var("cause"),
        );
        let schema = query.schema();
        let the = schema.get("the").expect("the field present");
        let content = the.content_type().expect("symbol kind present");
        assert!(!content.is_optional());
        assert_eq!(content.as_value_type(), Some(Type::Symbol));
        assert!(matches!(content, Kind::Primitive(_)));
    }

    /// `AttributeQueryAll::schema()` declares the `of` slot as
    /// a singleton primitive over `Entity`.
    #[dialog_common::test]
    fn schema_of_slot_is_primitive_entity() {
        let query = AttributeQueryAll::new(
            Term::var("the"),
            Term::var("of"),
            Term::var("is"),
            Term::var("cause"),
        );
        let schema = query.schema();
        let of = schema.get("of").expect("of field present");
        let content = of.content_type().expect("entity kind present");
        assert_eq!(content.as_value_type(), Some(Type::Entity));
    }

    /// `AttributeQueryAll::schema()` declares the `is` slot as
    /// `None` (unknown) when the term carries no static info —
    /// the unifier narrows at rule-compile time.
    #[dialog_common::test]
    fn schema_is_slot_is_unknown_when_untyped() {
        let query = AttributeQueryAll::new(
            Term::var("the"),
            Term::var("of"),
            Term::var("is"),
            Term::var("cause"),
        );
        let schema = query.schema();
        let is = schema.get("is").expect("is field present");
        assert!(
            is.content_type().is_none(),
            "untyped `is` term should yield unknown content_type"
        );
    }
}
