//! FactSelector for querying facts by pattern matching
//!
//! This module implements the `FactSelector<T>` type which represents a pattern
//! for matching facts in the knowledge base. Facts are represented as (entity, attribute, value)
//! triples, and FactSelector allows you to specify patterns for each component using:
//!
//! - **Constants**: Exact values to match (e.g., specific entity IDs, attribute names)
//! - **Variables**: Named placeholders that can be bound to values during matching
//! - **Wildcards**: Anonymous matchers that accept any value
//!
//! The selector supports both direct querying (when all terms are constants) and
//! pattern matching evaluation (when variables are involved).

use crate::artifact::{ArtifactSelector, Attribute, Constrained, Entity, Value};
use crate::error::{PlanError, QueryError, QueryResult};
use crate::plan::{EvaluationContext, EvaluationPlan};
use crate::query::{Query, Source};
use crate::selection::{Match, Selection};
use crate::syntax::VariableScope;
use crate::term::Term;
use crate::types::Scalar;
use async_stream::try_stream;
use std::fmt::Display;
// Remove unused import - dialog_storage::Storage doesn't exist
use serde::{Deserialize, Serialize};

/// FactSelector for pattern matching facts during queries
///
/// Represents a pattern for matching facts in the knowledge base. Each fact is a triple
/// of (entity, attribute, value), and FactSelector allows specifying constraints on each:
///
/// # JSON Serialization
/// FactSelector serializes to JSON with optional fields:
/// ```json
/// {
///   "the": "attribute_name",           // Attribute constraint
///   "of": { "?": { "name": "user" } },  // Entity variable
///   "is": "constant_value"             // Value constraint
/// }
/// ```
///
/// # Generic Parameter T
/// The type parameter T represents the expected value type:
/// - `FactSelector<Value>`: Can match any value type (most common)
/// - `FactSelector<String>`: Only matches string values
/// - `FactSelector<Entity>`: Only matches entity values
///
/// # Pattern Matching
/// Each field can be:
/// - `None`: No constraint on this component
/// - `Some(Term::Constant(...))`: Must match exact value
/// - `Some(Term::TypedVariable(...))`: Binds to any matching value
/// - `Some(Term::Any)`: Matches any value without binding
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(
    bound = "T: crate::types::IntoValueDataType + Clone + std::fmt::Debug + Serialize + for<'a> Deserialize<'a> + 'static"
)]
pub struct FactSelector<T: Scalar = Value> {
    /// The attribute term (predicate) - what property this fact describes
    ///
    /// Examples: "user/name", "user/email", Term::var("attr")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub the: Option<Term<Attribute>>,

    /// The entity term (subject) - what entity this fact is about
    ///
    /// Examples: specific Entity ID, Term::var("user"), Term::new()
    #[serde(skip_serializing_if = "Option::is_none")]
    pub of: Option<Term<Entity>>,

    /// The value term (object) - what value the attribute has for the entity
    ///
    /// Examples: "Alice", 42, Term::var("value"), Term::new()
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is: Option<Term<T>>,

    /// Optional fact configuration (reserved for future use)
    ///
    /// May be used for metadata, constraints, or other fact-level configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fact: Option<serde_json::Value>,
}

impl Display for FactSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Fact {{")?;

        if let Some(the) = &self.the {
            write!(f, "the: {},", the)?;
        }

        if let Some(of) = &self.of {
            write!(f, "the: {},", of)?;
        }

        if let Some(is) = &self.is {
            write!(f, "the: {},", is)?;
        }

        write!(f, "}}")
    }
}

pub const BASE_COST: usize = 100;
pub const ENTITY_COST: usize = 500;
pub const ATTRIBUTE_COST: usize = 200;
pub const VALUE_COST: usize = 300;
pub const UNBOUND_COST: usize = BASE_COST + ENTITY_COST + ATTRIBUTE_COST + VALUE_COST;

/// Core FactSelector functionality
///
/// Provides constructor and builder methods for creating fact patterns.
impl<T: Scalar> FactSelector<T> {
    /// Create a new empty fact selector with all fields as None
    ///
    /// This creates a completely unconstrained selector that would match all facts.
    /// Use the builder methods (the(), of(), is()) to add constraints.
    pub fn new() -> Self {
        Self {
            the: None,
            of: None,
            is: None,
            fact: None,
        }
    }

    /// Set the attribute (predicate) constraint
    ///
    /// Accepts types that implement IntoAttributeTerm:
    /// - String literals: `"user/name"`
    /// - Attribute constants: `Attribute::parse("user/name")`
    /// - Variables: `Term::<Attribute>::var("attr")`
    /// - Wildcards: `Term::<Attribute>::new()`
    pub fn the<The: crate::term::IntoAttributeTerm>(mut self, the: The) -> Self {
        self.the = Some(the.into_attribute_term());
        self
    }

    /// Set the entity (subject) constraint
    ///
    /// Accepts anything convertible to Term<Entity>:
    /// - Entity constants: `Entity::new()`
    /// - Variables: `Term::<Entity>::var("user")`
    /// - Wildcards: `Term::<Entity>::new()`
    pub fn of<Of: Into<Term<Entity>>>(mut self, entity: Of) -> Self {
        self.of = Some(entity.into());
        self
    }

    /// Set the value (object) constraint
    ///
    /// Accepts anything convertible to Term<T>:
    /// - Value constants: `"Alice"`, `42`, `true`
    /// - Variables: `Term::<T>::var("value")`
    /// - Wildcards: `Term::<T>::new()`
    pub fn is<V: Into<Term<T>>>(mut self, value: V) -> Self {
        self.is = Some(value.into());
        self
    }

    /// Create an execution plan for this fact selector
    ///
    /// Analyzes the selector and variable scope to create an optimized execution plan.
    /// The plan includes cost estimates and dependency information for query optimization.
    pub fn plan(&self, scope: &VariableScope) -> Result<FactSelectorPlan<T>, PlanError> {
        // We start with a cost estimate that assumes nothing is known.
        let mut cost = UNBOUND_COST;
        let mut provides = VariableScope::new();

        // If self.of is in scope we subtract ENTITY_COST from estimate
        if let Some(of) = &self.of {
            if scope.contains(&of) {
                cost -= ENTITY_COST;
                provides.add(of);
            }
        }

        // If self.the is in scope we subtract ATTRIBUTE_COST from the cost
        if let Some(the) = &self.the {
            if scope.contains(&the) {
                cost -= ATTRIBUTE_COST;
                provides.add(the);
            }
        }

        if let Some(is) = &self.is {
            if scope.contains(&is) {
                cost -= VALUE_COST;
                provides.add(is);
            }
        }

        // if cost is below UNBOUND_COST we have some term in the selector &
        // during evaluation we will be able to produce constrained selector
        // in this case we can return a plan, otherwise we return an error
        if cost < UNBOUND_COST {
            Ok(FactSelectorPlan {
                selector: self.clone(),
                provides,
                cost,
            })
        } else {
            let selector = FactSelector::from(self);
            Err(PlanError::UnconstrainedSelector { selector })
        }
    }

    pub fn resolve(&self, frame: &Match) -> Self {
        FactSelector {
            the: self.the.clone().map(|term| frame.resolve(&term)),
            of: self.of.clone().map(|term| frame.resolve(&term)),
            is: self.is.clone().map(|term| frame.resolve(&term)),
            fact: None,
        }
    }

    /// Convert to ArtifactSelector if all terms are constants (no variables)
    pub fn to_artifact_selector(&self) -> QueryResult<ArtifactSelector<Constrained>> {
        self.try_into()
    }

    pub fn resolve_artifact_selector(
        &self,
        frame: &Match,
    ) -> Result<ArtifactSelector<Constrained>, QueryError> {
        (&self.resolve(frame)).try_into()
    }
}

impl<T: Scalar> TryFrom<&FactSelector<T>> for ArtifactSelector<Constrained> {
    type Error = QueryError;

    fn try_from(fact_selector: &FactSelector<T>) -> Result<Self, Self::Error> {
        let mut selector: Option<ArtifactSelector<Constrained>> = None;

        // Convert attribute (the)
        if let Some(term) = &fact_selector.the {
            match term {
                Term::Constant(the) => {
                    selector = Some(match selector {
                        None => ArtifactSelector::new().the(the.to_owned()),
                        Some(s) => s.the(the.to_owned()),
                    });
                }
                Term::Variable { .. } => {}
            }
        }

        // Convert entity (of)
        if let Some(term) = &fact_selector.of {
            match term {
                Term::Constant(of) => {
                    selector = Some(match selector {
                        None => ArtifactSelector::new().of(of.to_owned()),
                        Some(s) => s.of(of.to_owned()),
                    });
                }
                Term::Variable { .. } => {}
            }
        }

        // Convert value (is)
        if let Some(term) = &fact_selector.is {
            match term {
                Term::Constant(value) => {
                    let converted_value = value.as_value();
                    selector = Some(match selector {
                        None => ArtifactSelector::new().is(converted_value),
                        Some(s) => s.is(converted_value),
                    });
                }
                Term::Variable { .. } => {}
            }
        }

        selector.ok_or_else(|| QueryError::EmptySelector {
            message: "At least one field must be constrained".to_string(),
        })
    }
}

impl<T: Scalar> From<&FactSelector<T>> for FactSelector<Value> {
    fn from(selector: &FactSelector<T>) -> Self {
        FactSelector {
            the: selector.the.clone(),
            of: selector.of.clone(),
            is: selector.is.clone().map(|is| is.as_unknown()),
            fact: selector.fact.clone(),
        }
    }
}

impl<T: Scalar> Query for FactSelector<T> {
    fn query<S: Source>(&self, store: &S) -> QueryResult<impl Selection> {
        use crate::try_stream;

        let scope = &VariableScope::new();
        let plan = self.plan(&scope).map_err(|e| QueryError::from(e))?;
        let context = crate::plan::fresh(store.clone());

        // Use try_stream to create a stream that owns the plan
        Ok(try_stream! {
            for await result in plan.evaluate(context) {
                yield result?;
            }
        })
    }
}

/// Execution plan for a fact selector operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactSelectorPlan<T: Scalar = Value> {
    /// The fact selector operation to execute
    pub selector: FactSelector<T>,
    /// Cost estimate for this operation
    pub cost: usize,

    pub provides: VariableScope,
}

impl<T: Scalar> FactSelectorPlan<T> {
    pub fn cost(&self) -> usize {
        self.cost
    }

    pub fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let selector = self.selector.clone();
        try_stream! {
            for await frame in context.selection {
                let frame = frame?;
                let selection = selector.resolve(&frame);
                for await artifact in context.source.select((&selection).try_into()?) {
                    let artifact = artifact?;

                    // Create a new frame by unifying the artifact with our pattern
                    let mut new_frame = frame.clone();

                    // Unify entity if we have an entity variable using type-safe unify
                    if let Some(entity_term) = &selection.of {
                        new_frame = new_frame.unify(entity_term.clone(), Value::Entity(artifact.of)).map_err(|e| QueryError::FactStore(e.to_string()))?;
                    }

                    // Unify attribute if we have an attribute variable using type-safe unify
                    if let Some(attr_term) = &selection.the {
                        new_frame = new_frame.unify(attr_term.clone(), Value::Symbol(artifact.the)).map_err(|e| QueryError::FactStore(e.to_string()))?;
                    }

                    // Unify value if we have a value variable using type-safe unify
                    if let Some(value_term) = &selection.is {
                        new_frame = new_frame.unify_value(value_term.clone(), artifact.is).map_err(|e| QueryError::FactStore(e.to_string()))?;
                    }

                    yield new_frame;
                }
            }
        }
    }
}

impl<T: Scalar> EvaluationPlan for FactSelectorPlan<T> {
    fn provides(&self) -> &VariableScope {
        &self.provides
    }

    fn cost(&self) -> usize {
        self.cost
    }

    fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        FactSelectorPlan::evaluate(self, context)
    }
}

impl<T: Scalar> TryFrom<&FactSelectorPlan<T>> for ArtifactSelector<Constrained> {
    type Error = QueryError;

    fn try_from(plan: &FactSelectorPlan<T>) -> Result<Self, Self::Error> {
        (&plan.selector).try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::Type;
    use crate::artifact::Value;

    #[test]
    fn test_fact_selector_by_attribute() {
        let fact_selector: FactSelector<Value> = FactSelector::new().the("person/name");

        if let Some(Term::Constant(attr)) = &fact_selector.the {
            assert_eq!(attr.to_string(), "person/name");
        } else {
            panic!("Expected constant attribute term");
        }
        assert!(fact_selector.of.is_none());
        assert!(fact_selector.is.is_none());
    }

    #[test]
    fn test_fact_selector_json_serialization() {
        let fact_selector: FactSelector<Value> = FactSelector::new().the("person/name");

        // Should be able to serialize and deserialize
        let serialized = serde_json::to_string(&fact_selector).unwrap();
        let deserialized: FactSelector<Value> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(fact_selector, deserialized);
    }

    #[test]
    fn test_fact_selector_new() {
        // Test the new method
        let fact_selector: FactSelector<Value> = FactSelector::new().the("user/name");

        assert!(fact_selector.the.is_some());
        assert!(fact_selector.of.is_none());
        assert!(fact_selector.is.is_none());

        if let Some(Term::Constant(attr)) = &fact_selector.the {
            assert_eq!(attr.to_string(), "user/name");
        } else {
            panic!("Expected constant attribute");
        }
    }

    #[test]
    fn test_fact_selector_with_variables() {
        let fact_selector: FactSelector<Value> = FactSelector::new()
            .the("user/name")
            .of(Term::var("user"))
            .is(Term::<Value>::var("name"));

        assert!(fact_selector.the.is_some());
        assert!(fact_selector.of.is_some());
        assert!(fact_selector.is.is_some());

        // Check attribute is constant
        if let Some(Term::Constant(attr)) = &fact_selector.the {
            assert_eq!(attr.to_string(), "user/name");
        } else {
            panic!("Expected constant attribute");
        }

        // Check entity variable (should be typed as Entity)
        if let Some(term) = &fact_selector.of {
            assert_eq!(term.name().unwrap(), "user");
            assert_eq!(term.content_type(), Some(Type::Entity));
        } else {
            panic!("Expected variable for entity");
        }

        // Check value variable (should be typed as Value, which returns None)
        if let Some(term) = &fact_selector.is {
            assert_eq!(term.name().unwrap(), "name");
            // Value type returns None since it can hold any type
            assert!(term.content_type().is_none());
        } else {
            panic!("Expected variable for value");
        }
    }

    #[test]
    fn test_fact_selector_with_constant_value() {
        let fact_selector = FactSelector::new().the("user/email").is("user@example.com");

        if let Some(Term::Constant(value)) = &fact_selector.is {
            assert_eq!(*value, "user@example.com");
        } else {
            panic!("Expected constant value");
        }
    }

    #[test]
    fn test_fact_selector_builder_api() {
        // Test basic builder syntax with Term::var()
        let fact_selector1: FactSelector<Value> = FactSelector::new()
            .the("gozala.io/name")
            .of(Term::var("user"));

        assert!(fact_selector1.the.is_some());
        assert!(fact_selector1.of.is_some());
        assert!(fact_selector1.is.is_none());

        // Test starting with different methods
        let fact_selector2: FactSelector<Value> = FactSelector::new()
            .the("user/name")
            .of(Term::var("user"))
            .is(Value::String("John".to_string()));

        let fact_selector3: FactSelector<Value> = FactSelector::new()
            .the("user/name")
            .of(Term::var("user"))
            .is(Term::<Value>::var("name"));

        let fact_selector4 = FactSelector::new()
            .is(Value::String("active".to_string()))
            .the("user/status");

        // All should create valid Assertion patterns
        assert!(fact_selector2.the.is_some());
        assert!(fact_selector2.of.is_some());
        assert!(fact_selector2.is.is_some());

        assert!(fact_selector3.the.is_some());
        assert!(fact_selector3.of.is_some());
        assert!(fact_selector3.is.is_some());

        assert!(fact_selector4.the.is_some());
        assert!(fact_selector4.of.is_none());
        assert!(fact_selector4.is.is_some());
    }

    #[test]
    fn test_fact_selector_builder_flexible_order() {
        // Test that order doesn't matter
        let fact_selector1: FactSelector<Value> = FactSelector::new()
            .the("user/email")
            .of(Term::var("user"))
            .is(Term::<Value>::var("email"));

        let fact_selector2: FactSelector<Value> = FactSelector::new()
            .of(Term::var("user"))
            .is(Term::<Value>::var("email"))
            .the("user/email");

        let fact_selector3: FactSelector<Value> = FactSelector::new()
            .is(Term::<Value>::var("email"))
            .the("user/email")
            .of(Term::<Entity>::var("user"));

        // All should have the same pattern content
        assert_eq!(fact_selector1.the, fact_selector2.the);
        assert_eq!(fact_selector1.the, fact_selector3.the);
        assert_eq!(fact_selector1.of, fact_selector2.of);
        assert_eq!(fact_selector1.of, fact_selector3.of);
        assert_eq!(fact_selector1.is, fact_selector2.is);
        assert_eq!(fact_selector1.is, fact_selector3.is);
    }

    #[test]
    fn test_fact_selector_builder_with_variable_constructors() {
        // Test builder API with Term::var() constructors
        let fact_selector: FactSelector<Value> = FactSelector::new()
            .the("user/name")
            .of(Term::var("user"))
            .is(Term::<Value>::var("name"));

        assert!(fact_selector.the.is_some());
        assert!(fact_selector.of.is_some());
        assert!(fact_selector.is.is_some());
    }

    // Tests from fact_selector_test.rs
    use crate::artifact::{Artifacts, Attribute, Entity};
    use crate::syntax::VariableScope;
    use crate::{plan::EvaluationContext, Fact, Session};
    use crate::{selection::Match, QueryError};
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;
    use futures_util::{stream, StreamExt};

    #[tokio::test]
    async fn test_query_pattern() -> Result<()> {
        // This test verifies that FactSelectorPlan::evaluate follows the familiar-query pattern:
        // 1. Convert fact selector to ArtifactSelector using constants
        // 2. Call store.select() to get matching artifacts
        // 3. Unify each artifact with the pattern and existing bindings
        // 4. Return successful frames

        // Setup: Create in-memory storage and artifacts store
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        // Step 1: Create test data
        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let facts = vec![
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                alice.clone(),
                Value::String("Alice".to_string()),
            ),
            Fact::assert(
                "user/email".parse::<Attribute>()?,
                alice.clone(),
                Value::String("alice@example.com".to_string()),
            ),
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
            Fact::assert(
                "user/email".parse::<Attribute>()?,
                bob.clone(),
                Value::String("bob@example.com".to_string()),
            ),
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        // Step 2: Create fact selector with constants (following familiar-query pattern)
        let fact_selector: FactSelector<Value> = FactSelector::new()
            .the("user/name") // Constant attribute - this will be used for ArtifactSelector
            .of(Term::var("user")) // Variable entity - this will be unified
            .is(Term::<Value>::var("name")); // Variable value - this will be unified

        // Step 3: Create plan and test the familiar-query pattern
        let scope = VariableScope::new();
        let plan = fact_selector
            .plan(&scope)
            .expect("Plan should succeed since we have a constant attribute");

        // Step 4: Create evaluation context with initial empty selection
        let initial_match = Match::new();
        let initial_selection = stream::iter(vec![Ok(initial_match)]);
        let session = Session::open(artifacts.clone());
        let context = EvaluationContext::single(session, initial_selection, VariableScope::new());

        // Step 5: Execute the plan using familiar-query pattern
        let result_stream = plan.evaluate(context);
        let match_frames: Vec<Match> = result_stream
            .collect::<Vec<Result<Match, QueryError>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<Match>, QueryError>>()?;

        // Step 6: Verify results
        assert_eq!(match_frames.len(), 2, "Should match both Alice and Bob");

        // Verify that the frames contain variable bindings
        for frame in match_frames {
            // Use untyped variables for frame operations since Terms convert to untyped
            let user_term = Term::<Entity>::var("user");
            let name_term = Term::<String>::var("name");

            // Check that the frame contains bindings for our variables
            assert!(
                frame.has(&user_term),
                "Frame should contain binding for 'user' variable"
            );
            assert!(
                frame.has(&name_term),
                "Frame should contain binding for 'name' variable"
            );

            // Check that the bindings are correct
            if let Ok(entity) = frame.get(&user_term) {
                if let Ok(name) = frame.get(&name_term) {
                    // Should be either Alice or Bob
                    assert!(
                        name == "Alice" || name == "Bob",
                        "Name should be Alice or Bob"
                    );
                    if name == "Alice" {
                        assert_eq!(entity, alice, "Alice entity should match");
                    } else {
                        assert_eq!(entity, bob, "Bob entity should match");
                    }
                } else {
                    panic!("Name binding should be a string");
                }
            } else {
                panic!("User binding should be an entity");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_no_constants_pending() -> Result<()> {
        // Test that queries with no constants return pending with impediments

        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let _artifacts = Artifacts::anonymous(storage_backend).await?;

        // Create fact selector with all variables (no constants)
        let fact_selector: FactSelector<Value> = FactSelector::new()
            .the(Term::var("attr")) // Variable
            .of(Term::var("entity")) // Variable
            .is(Term::<Value>::var("value")); // Variable

        // Create plan - this should return error because all fields are unbound variables
        let scope = VariableScope::new();
        let plan_result = fact_selector.plan(&scope);

        // Planning should return error with solutions
        match plan_result {
            Err(plan_error) => {
                assert!(
                    matches!(plan_error, PlanError::UnconstrainedSelector { .. }),
                    "Should produce UnconstrainedSelector error"
                );
            }
            Ok(_) => {
                panic!("Expected planning error for unexecutable query");
            }
        }

        Ok(())
    }
}
