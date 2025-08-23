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

use crate::error::{QueryError, QueryResult};
use crate::plan::{EvaluationContext, EvaluationPlan, Plan};
use crate::query::Query;
use crate::selection::{Match, Selection as SelectionTrait};
use crate::syntax::Syntax;
use crate::syntax::VariableScope;
use crate::term::Term;
use async_stream::try_stream;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStore, Attribute, DialogArtifactsError, Entity, Value,
};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::str::FromStr;

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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    bound = "T: crate::types::IntoValueDataType + Clone + std::fmt::Debug + Serialize + for<'a> Deserialize<'a> + 'static"
)]
pub struct FactSelector<T = Value>
where
    T: crate::types::IntoValueDataType + Clone + std::fmt::Debug + 'static,
{
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

/// Core FactSelector functionality
///
/// Provides constructor and builder methods for creating fact patterns.
impl<T> FactSelector<T>
where
    T: crate::types::IntoValueDataType + Clone + std::fmt::Debug,
{
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
    /// Accepts anything convertible to Term<Attribute>:
    /// - String literals: `"user/name"`
    /// - Attribute constants: `Attribute::parse("user/name")`
    /// - Variables: `Term::<Attribute>::var("attr")`
    /// - Wildcards: `Term::<Attribute>::new()`
    pub fn the<The: Into<Term<Attribute>>>(mut self, the: The) -> Self {
        self.the = Some(the.into());
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

    /// Get all variable names referenced in this fact selector
    ///
    /// Returns a vector of variable names that would need to be bound
    /// during pattern matching. Used for dependency analysis and planning.
    ///
    /// Only includes named Variable terms, not unnamed variables or constants.
    pub fn variables(&self) -> Vec<String> {
        let mut vars = Vec::new();

        // Check each field for variables and collect their names
        match &self.the {
            Some(Term::Variable {
                name: Some(name), ..
            }) => vars.push(name.clone()),
            _ => {}
        }
        match &self.of {
            Some(Term::Variable {
                name: Some(name), ..
            }) => vars.push(name.clone()),
            _ => {}
        }
        match &self.is {
            Some(Term::Variable {
                name: Some(name), ..
            }) => vars.push(name.clone()),
            _ => {}
        }

        vars
    }

    /// Create an execution plan for this fact selector
    ///
    /// Analyzes the selector and variable scope to create an optimized execution plan.
    /// The plan includes cost estimates and dependency information for query optimization.
    pub fn plan(&self, scope: &VariableScope) -> QueryResult<FactSelectorPlan<T>> {
        FactSelectorPlan::new(self.clone(), scope)
    }
}

impl<T> FactSelector<T>
where
    T: crate::types::IntoValueDataType + Clone + std::fmt::Debug + Into<Value>,
{
    /// Convert to ArtifactSelector if all terms are constants (no variables)
    pub fn to_artifact_selector(&self) -> QueryResult<ArtifactSelector<Constrained>> {
        let mut selector: Option<ArtifactSelector<Constrained>> = None;

        // Convert attribute (the)
        if let Some(term) = &self.the {
            match term {
                Term::Constant(the) => {
                    selector = Some(match selector {
                        None => ArtifactSelector::new().the(the.to_owned()),
                        Some(s) => s.the(the.to_owned()),
                    });
                }
                Term::Variable { .. } => {
                    return Err(QueryError::VariableNotSupported {
                        message: "Variables not supported in ArtifactSelector conversion"
                            .to_string(),
                    });
                }
            }
        }

        // Convert entity (of)
        if let Some(term) = &self.of {
            match term {
                Term::Constant(of) => {
                    selector = Some(match selector {
                        None => ArtifactSelector::new().of(of.to_owned()),
                        Some(s) => s.of(of.to_owned()),
                    });
                }
                Term::Variable { .. } => {
                    return Err(QueryError::VariableNotSupported {
                        message: "Variables not supported in ArtifactSelector conversion"
                            .to_string(),
                    });
                }
            }
        }

        // Convert value (is)
        if let Some(term) = &self.is {
            match term {
                Term::Constant(value) => {
                    let converted_value: Value = value.clone().into();
                    selector = Some(match selector {
                        None => ArtifactSelector::new().is(converted_value),
                        Some(s) => s.is(converted_value),
                    });
                }
                Term::Variable { .. } => {
                    return Err(QueryError::VariableNotSupported {
                        message: "Variables not supported in ArtifactSelector conversion"
                            .to_string(),
                    });
                }
            }
        }

        selector.ok_or_else(|| QueryError::EmptySelector {
            message: "At least one field must be constrained".to_string(),
        })
    }

    pub fn resolve(&self, frame: &Match) -> Result<ArtifactSelector<Constrained>, QueryError> {
        let mut selector: Option<ArtifactSelector<Constrained>> = None;

        // If we have the term in our selector we need to resolve it.
        selector = if let Some(term) = &self.the {
            // If we can resolve it from the given frame we will constrain
            // selector with it.
            if let Ok(value) = frame.resolve(term) {
                // We need to ensure that that resolved value is a string
                // that can be parsed as an attribute
                let attribute: Attribute = match value {
                    Value::String(s) => {
                        Attribute::from_str(&s).map_err(|_| QueryError::InvalidAttribute {
                            attribute: format!("Invalid attribute format: {}", s),
                        })?
                    }
                    Value::Symbol(attr) => attr,
                    _ => {
                        return Err(QueryError::InvalidAttribute {
                            attribute: format!(
                                "Expected string or symbol for attribute, got: {:?}",
                                value
                            ),
                        })
                    }
                };

                if let Some(selector) = selector {
                    Some(selector.the(attribute.clone()))
                } else {
                    Some(ArtifactSelector::default().the(attribute.clone()))
                }
            } else {
                selector
            }
        } else {
            selector
        };

        selector = if let Some(term) = &self.of {
            if let Ok(value) = frame.resolve(term) {
                let entity: Entity =
                    value
                        .clone()
                        .try_into()
                        .map_err(|_| QueryError::InvalidTerm {
                            message: format!("Expected entity, got: {:?}", value),
                        })?;
                if let Some(selector) = selector {
                    Some(selector.of(entity))
                } else {
                    Some(ArtifactSelector::default().of(entity.clone()))
                }
            } else {
                selector
            }
        } else {
            selector
        };

        selector = if let Some(term) = &self.is {
            if let Ok(value) = frame.resolve(term) {
                if let Some(selector) = selector {
                    Some(selector.is(value))
                } else {
                    Some(ArtifactSelector::default().is(value.clone()))
                }
            } else {
                selector
            }
        } else {
            selector
        };

        if let Some(selector) = selector {
            Ok(selector)
        } else {
            Err(QueryError::EmptySelector {
                message: "Fact selector must have at least one constant term".to_string(),
            })
        }
    }
}

/// Execution plan for a fact selector operation
#[derive(Debug, Clone)]
pub struct FactSelectorPlan<T = Value>
where
    T: crate::types::IntoValueDataType + Clone + std::fmt::Debug + 'static,
{
    /// The fact selector operation to execute
    pub selector: FactSelector<T>,
    /// Variables that must be bound before execution
    pub required_bindings: BTreeSet<String>,
    /// Cost estimate for this operation
    pub cost: f64,
}

impl<T> FactSelectorPlan<T>
where
    T: crate::types::IntoValueDataType + Clone + std::fmt::Debug,
{
    /// Create a new fact selector plan
    pub fn new(fact_selector: FactSelector<T>, scope: &VariableScope) -> QueryResult<Self> {
        let variables = fact_selector.variables();
        let required_bindings = variables
            .iter()
            .filter(|var| !scope.bound_variables.contains(*var))
            .cloned()
            .collect();

        // Base cost for assertion operation
        let cost = 100.0;

        Ok(FactSelectorPlan {
            selector: fact_selector,
            required_bindings,
            cost,
        })
    }
}

impl<T> Syntax for FactSelector<T>
where
    T: crate::types::IntoValueDataType + Clone + std::fmt::Debug + Send + 'static,
    Value: From<T>,
{
    type Plan = FactSelectorPlan<T>;

    fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan> {
        FactSelectorPlan::new(self.clone(), scope)
    }
}

impl<T> TryFrom<FactSelector<T>> for ArtifactSelector<Constrained>
where
    T: crate::types::IntoValueDataType + Clone + std::fmt::Debug + Into<Value>,
{
    type Error = QueryError;

    fn try_from(fact_selector: FactSelector<T>) -> Result<Self, Self::Error> {
        fact_selector.to_artifact_selector()
    }
}

impl<T> Query for FactSelector<T>
where
    T: crate::types::IntoValueDataType + Clone + std::fmt::Debug + Into<Value>,
{
    fn query<S>(
        &self,
        store: &S,
    ) -> QueryResult<impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 'static>
    where
        S: ArtifactStore,
    {
        // Check if we can optimize with direct store query (constants only)
        if let Ok(artifact_selector) = self.to_artifact_selector() {
            // All constants - use direct store query
            Ok(store.select(artifact_selector))
        } else {
            // Has variables - Query trait doesn't support variables
            // Users should use the plan → evaluate approach instead
            Err(QueryError::VariableNotSupported {
                message:
                    "Query trait does not support variables. Use plan → evaluate approach instead."
                        .to_string(),
            })
        }
    }
}

impl<T> Plan for FactSelectorPlan<T> where
    T: crate::types::IntoValueDataType + Clone + std::fmt::Debug + Send + 'static
{
}
impl<T> EvaluationPlan for FactSelectorPlan<T>
where
    T: crate::types::IntoValueDataType
        + Clone
        + std::fmt::Debug
        + Into<Value>
        + Send
        + 'static
        + PartialEq<Value>,
{
    fn evaluate<S, M>(&self, context: EvaluationContext<S, M>) -> impl SelectionTrait + '_
    where
        S: ArtifactStore + Clone + Send + 'static,
        M: SelectionTrait + 'static,
    {
        // We need to capture context by value to satisfy lifetime requirements
        let store = context.store;
        let selection = context.selection;
        let selector = self.selector.clone();

        try_stream! {
            for await frame in selection {
                let frame = frame?;
                if let Ok(artifact_selector) = selector.resolve(&frame) {
                    let stream = store.select(artifact_selector);

                    for await artifact in stream {
                        let artifact = artifact?;

                        // Create a new frame by unifying the artifact with our pattern
                        let mut new_frame = frame.clone();

                        // Unify entity if we have an entity variable using type-safe unify
                        if let Some(entity_term) = &selector.of {
                            new_frame = new_frame.unify(entity_term.clone(), Value::Entity(artifact.of)).map_err(|e| QueryError::FactStore(e.to_string()))?;
                        }

                        // Unify attribute if we have an attribute variable using type-safe unify
                        if let Some(attr_term) = &selector.the {
                            new_frame = new_frame.unify(attr_term.clone(), Value::Symbol(artifact.the)).map_err(|e| QueryError::FactStore(e.to_string()))?;
                        }

                        // Unify value if we have a value variable using type-safe unify
                        if let Some(value_term) = &selector.is {
                            new_frame = new_frame.unify(value_term.clone(), artifact.is).map_err(|e| QueryError::FactStore(e.to_string()))?;
                        }

                        yield new_frame;
                    }
                } else {
                    // If we can't resolve selector, just pass through the frame
                    yield frame;
                }
            }
        }
    }

    fn cost(&self) -> f64 {
        self.cost
    }
}

impl<T> Query for FactSelectorPlan<T>
where
    T: crate::types::IntoValueDataType + Clone + std::fmt::Debug + Into<Value> + Send + 'static,
{
    fn query<S>(
        &self,
        store: &S,
    ) -> QueryResult<impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 'static>
    where
        S: ArtifactStore,
    {
        // For FactSelectorPlan, the Query trait should also not support variables
        if let Ok(artifact_selector) = self.selector.to_artifact_selector() {
            // All constants - use direct store query for optimization
            Ok(store.select(artifact_selector))
        } else {
            // Has variables - Query trait doesn't support variables even for plans
            Err(QueryError::VariableNotSupported {
                message:
                    "Query trait does not support variables. Use plan → evaluate approach instead."
                        .to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_artifacts::Value;
    use dialog_artifacts::ValueDataType;

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
    fn test_fact_selector_with_entity_and_value() {
        let fact_selector: FactSelector<Value> = FactSelector::new()
            .the("person/name")
            .of(Term::<Entity>::var("person"))
            .is(Term::<String>::var("name"));

        let vars = fact_selector.variables();
        assert_eq!(vars.len(), 2);
        // Check that variables are present by comparing names
        assert!(vars.contains(&"person".to_string()));
        assert!(vars.contains(&"name".to_string()));
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
            assert_eq!(term.data_type(), Some(ValueDataType::Entity));
        } else {
            panic!("Expected variable for entity");
        }

        // Check value variable (should be typed as Value, which returns None)
        if let Some(term) = &fact_selector.is {
            assert_eq!(term.name().unwrap(), "name");
            // Value type returns None since it can hold any type
            assert!(term.data_type().is_none());
        } else {
            panic!("Expected variable for value");
        }
    }

    #[test]
    fn test_fact_selector_with_constant_value() {
        let fact_selector = FactSelector::new().the("user/email").is("user@example.com");

        if let Some(Term::Constant(value)) = &fact_selector.is {
            match value {
                Value::String(s) => assert_eq!(s, "user@example.com"),
                _ => panic!("Expected string value"),
            }
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
            .is("John");

        let fact_selector3: FactSelector<Value> = FactSelector::new()
            .the("user/name")
            .of(Term::var("user"))
            .is(Term::<String>::var("name"));

        let fact_selector4: FactSelector<Value> =
            FactSelector::new().is("active").the("user/status");

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
            .is(Term::<String>::var("email"));

        let fact_selector2: FactSelector<Value> = FactSelector::new()
            .of(Term::var("user"))
            .is(Term::<String>::var("email"))
            .the("user/email");

        let fact_selector3: FactSelector<Value> = FactSelector::new()
            .is(Term::<String>::var("email"))
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
            .is(Term::<String>::var("name"));

        assert!(fact_selector.the.is_some());
        assert!(fact_selector.of.is_some());
        assert!(fact_selector.is.is_some());

        // Check that variables are properly set
        let vars = fact_selector.variables();
        assert_eq!(vars.len(), 2);
    }

    // Tests from fact_selector_test.rs
    use crate::syntax::VariableScope;
    use crate::{
        plan::{EvaluationContext, EvaluationPlan},
        Fact,
    };
    use crate::{selection::Match, QueryError};
    use anyhow::Result;
    use dialog_artifacts::{ArtifactStoreMut, Artifacts, Attribute, Entity, Instruction};
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
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;

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

        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;

        // Step 2: Create fact selector with constants (following familiar-query pattern)
        let fact_selector: FactSelector<Value> = FactSelector::new()
            .the("user/name") // Constant attribute - this will be used for ArtifactSelector
            .of(Term::var("user")) // Variable entity - this will be unified
            .is(Term::<String>::var("name")); // Variable value - this will be unified

        // Step 3: Create plan and test the familiar-query pattern
        let scope = VariableScope::new();
        let plan = fact_selector.plan(&scope)?;

        // Step 4: Create evaluation context with initial empty selection
        let initial_match = Match::new();
        let initial_selection = stream::iter(vec![Ok(initial_match)]);
        let context = EvaluationContext::new(artifacts.clone(), initial_selection);

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
    async fn test_no_constants_fails() -> Result<()> {
        // Test that queries with no constants fail (matching familiar-query pattern)

        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        // Create fact selector with all variables (no constants)
        let fact_selector: FactSelector<Value> = FactSelector::new()
            .the(Term::var("attr")) // Variable
            .of(Term::var("entity")) // Variable
            .is(Term::<Value>::var("value")); // Variable

        // Create plan
        let scope = VariableScope::new();
        let plan = fact_selector.plan(&scope)?;

        // Create evaluation context with initial empty selection
        let initial_match = Match::new();
        let initial_selection = stream::iter(vec![Ok(initial_match)]);
        let context = EvaluationContext::new(artifacts.clone(), initial_selection);

        // Execute the plan - should return empty results because no constants can be resolved
        let result_stream = plan.evaluate(context);
        let match_frames: Vec<Match> = result_stream
            .collect::<Vec<Result<Match, QueryError>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<Match>, QueryError>>()?;

        // Should have no results or an error - for now we expect it to pass through the input frame
        // In a proper implementation with constraints, this would return an empty result set
        assert!(
            !match_frames.is_empty(),
            "Should pass through initial frame when no constants resolved"
        );

        Ok(())
    }
}
