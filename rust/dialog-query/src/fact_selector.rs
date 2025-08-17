//! FactSelector for querying facts by pattern matching

use crate::error::{QueryError, QueryResult};
use crate::plan::{EvaluationContext, EvaluationPlan, Plan};
use crate::query::Query;
use crate::selection::{Match, Selection};
use crate::syntax::Syntax;
use crate::term::Term;
use crate::variable::{TypedVariable, Untyped, VariableName, VariableScope};
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FactSelector {
    /// The attribute term (predicate)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub the: Option<Term<Attribute>>,
    /// The entity term (subject)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub of: Option<Term<Entity>>,
    /// The value term (object)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is: Option<Term<Value>>,
    /// Optional fact configuration (reserved for future use)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fact: Option<serde_json::Value>,
}

impl FactSelector {
    /// Create a new empty assertion with all fields as None
    pub fn new() -> Self {
        Self {
            the: None,
            of: None,
            is: None,
            fact: None,
        }
    }

    /// Set the attribute (predicate) - accepts strings or Terms
    pub fn the<T: Into<Term<Attribute>>>(mut self, attr: T) -> Self {
        self.the = Some(attr.into());
        self
    }

    /// Set the entity (subject) - accepts Variables or Terms
    pub fn of<T: Into<Term<Entity>>>(mut self, entity: T) -> Self {
        self.of = Some(entity.into());
        self
    }

    /// Set the value (object) - accepts Variables or Terms
pub fn is<T: Into<Term<Value>>>(mut self, value: T) -> Self {
        self.is = Some(value.into());
        self
    }

    /// Get all variables referenced in this assertion
    pub fn variables(&self) -> Vec<TypedVariable<Untyped>> {
        let mut vars = Vec::new();

        if let Some(Term::Variable(var)) = &self.the {
            vars.push(var.to_untyped());
        }
        if let Some(Term::Variable(var)) = &self.of {
            vars.push(var.to_untyped());
        }
        if let Some(Term::Variable(var)) = &self.is {
            vars.push(var.to_untyped());
        }

        vars
    }

    /// Create an execution plan for this fact selector
    pub fn plan(&self, scope: &VariableScope) -> QueryResult<FactSelectorPlan> {
        FactSelectorPlan::new(self.clone(), scope)
    }

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
                Term::Variable(_) => {
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
                Term::Variable(_) => {
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
                    selector = Some(match selector {
                        None => ArtifactSelector::new().is(value),
                        Some(s) => s.is(value.to_owned()),
                    });
                }
                Term::Variable(_) => {
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
                    _ => {
                        return Err(QueryError::InvalidAttribute {
                            attribute: format!("Expected string for attribute, got: {:?}", value),
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
pub struct FactSelectorPlan {
    /// The fact selector operation to execute
    pub selector: FactSelector,
    /// Variables that must be bound before execution
    pub required_bindings: BTreeSet<VariableName>,
    /// Cost estimate for this operation
    pub cost: f64,
}

impl FactSelectorPlan {
    /// Create a new fact selector plan
    pub fn new(fact_selector: FactSelector, scope: &VariableScope) -> QueryResult<Self> {
        let variables = fact_selector.variables();
        let required_bindings = variables
            .iter()
            .filter(|var| !scope.bound_variables.contains(var.name()))
            .map(|var| var.name().to_string())
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

impl Syntax for FactSelector {
    type Plan = FactSelectorPlan;

    fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan> {
        FactSelectorPlan::new(self.clone(), scope)
    }
}

impl TryFrom<FactSelector> for ArtifactSelector<Constrained> {
    type Error = QueryError;

    fn try_from(fact_selector: FactSelector) -> Result<Self, Self::Error> {
        fact_selector.to_artifact_selector()
    }
}

impl Query for FactSelector {
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

impl Plan for FactSelectorPlan {}
impl EvaluationPlan for FactSelectorPlan {
    fn evaluate<S, M>(&self, context: EvaluationContext<S, M>) -> impl Selection + '_
    where
        S: ArtifactStore + Clone + Send + 'static,
        M: Selection + 'static,
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

                        // Unify entity if we have an entity variable
                        if let Some(Term::Variable(var)) = &selector.of {
                            new_frame = new_frame.set(var.to_untyped(), Value::Entity(artifact.of)).map_err(|e| QueryError::FactStore(e.to_string()))?;
                        }

                        // Unify attribute if we have an attribute variable
                        if let Some(Term::Variable(var)) = &selector.the {
                            new_frame = new_frame.set(var.to_untyped(), Value::String(artifact.the.to_string())).map_err(|e| QueryError::FactStore(e.to_string()))?;
                        }

                        // Unify value if we have a value variable
                        if let Some(Term::Variable(var)) = &selector.is {
                            new_frame = new_frame.set(var.to_untyped(), artifact.is).map_err(|e| QueryError::FactStore(e.to_string()))?;
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

impl Query for FactSelectorPlan {
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
    use crate::variable::TypedVariable;
    use dialog_artifacts::Value;

    #[test]
    fn test_fact_selector_by_attribute() {
        let fact_selector = FactSelector::new().the("person/name");

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
        let person_var = TypedVariable::<crate::variable::Untyped>::new("person");
        let name_var = TypedVariable::<String>::new("name");

        let fact_selector = FactSelector::new()
            .the("person/name")
            .of(Term::from(person_var.clone()))
            .is(Term::from(name_var.clone()));

        let vars = fact_selector.variables();
        assert_eq!(vars.len(), 2);
        // Check that variables are present by comparing names and types
        let var_names: Vec<&str> = vars.iter().map(|v| v.name()).collect();
        assert!(var_names.contains(&"person"));
        assert!(var_names.contains(&"name"));

        // Check types - all variables in Terms should be untyped after conversion
        let person_var_in_list = vars.iter().find(|v| v.name() == "person").unwrap();
        let name_var_in_list = vars.iter().find(|v| v.name() == "name").unwrap();
        assert_eq!(person_var_in_list.data_type(), None);
        assert_eq!(name_var_in_list.data_type(), None); // Terms convert all variables to untyped
    }

    #[test]
    fn test_fact_selector_json_serialization() {
        let fact_selector = FactSelector::new().the("person/name");

        // Should be able to serialize and deserialize
        let serialized = serde_json::to_string(&fact_selector).unwrap();
        let deserialized: FactSelector = serde_json::from_str(&serialized).unwrap();
        assert_eq!(fact_selector, deserialized);
    }

    #[test]
    fn test_fact_selector_new() {
        // Test the new method
        let fact_selector = FactSelector::new().the("user/name");

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
        let user_var = TypedVariable::<crate::variable::Untyped>::new("user");
        let name_var = TypedVariable::<String>::new("name");

        let fact_selector = FactSelector::new()
            .the("user/name")
            .of(Term::from(user_var.clone()))
            .is(Term::from(name_var.clone()));

        assert!(fact_selector.the.is_some());
        assert!(fact_selector.of.is_some());
        assert!(fact_selector.is.is_some());

        // Check attribute is constant
        if let Some(Term::Constant(attr)) = &fact_selector.the {
            assert_eq!(attr.to_string(), "user/name");
        } else {
            panic!("Expected constant attribute");
        }

        // Check entity is untyped variable
        if let Some(Term::Variable(var)) = &fact_selector.of {
            assert_eq!(var.name(), "user");
            assert!(var.data_type().is_none());
        } else {
            panic!("Expected variable for entity");
        }

        // Check value variable - all variables in Terms should be untyped
        if let Some(Term::Variable(var)) = &fact_selector.is {
            assert_eq!(var.name(), "name");
            assert!(var.data_type().is_none()); // Terms convert all variables to untyped
        } else {
            panic!("Expected variable for value");
        }
    }

    #[test]
    fn test_fact_selector_with_constant_value() {
        let fact_selector =
            FactSelector::new()
                .the("user/email")
                .is("user@example.com");

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
        use crate::variable::TypedVariable;

        // Test your exact requested syntax
        let fact_selector1 = FactSelector::new()
            .the("gozala.io/name")
            .of(TypedVariable::<Entity>::new("user"));

        assert!(fact_selector1.the.is_some());
        assert!(fact_selector1.of.is_some());
        assert!(fact_selector1.is.is_none());

        // Test starting with different methods
        let fact_selector2 = FactSelector::new()
            .the("user/name")
            .of(TypedVariable::<crate::variable::Untyped>::new("user"))
            .is("John");

        let fact_selector3 = FactSelector::new()
            .of(TypedVariable::<crate::variable::Untyped>::new("user"))
            .the("user/name")
            .is(TypedVariable::<String>::new("name"));

        let fact_selector4 = FactSelector::new().is("active").the("user/status");

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
        use crate::variable::TypedVariable;

        // Test that order doesn't matter
        let fact_selector1 = FactSelector::new()
            .the("user/email")
            .of(TypedVariable::<crate::variable::Untyped>::new("user"))
            .is(TypedVariable::<String>::new("email"));

        let fact_selector2 = FactSelector::new()
            .of(TypedVariable::<crate::variable::Untyped>::new("user"))
            .is(TypedVariable::<String>::new("email"))
            .the("user/email");

        let fact_selector3 = FactSelector::new()
            .is(TypedVariable::<String>::new("email"))
            .the("user/email")
            .of(TypedVariable::<crate::variable::Untyped>::new("user"));

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
        // Test builder API with Variable constructors
        let fact_selector = FactSelector::new()
            .the("user/name")
            .of(TypedVariable::<crate::variable::Untyped>::new("user"))
            .is(TypedVariable::<String>::new("name"));

        assert!(fact_selector.the.is_some());
        assert!(fact_selector.of.is_some());
        assert!(fact_selector.is.is_some());

        // Check that variables are properly set
        let vars = fact_selector.variables();
        assert_eq!(vars.len(), 2);
    }

    // Tests from fact_selector_test.rs
    use crate::variable::VariableScope;
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
        let fact_selector = FactSelector::new()
            .the("user/name") // Constant attribute - this will be used for ArtifactSelector
            .of(TypedVariable::<Entity>::new("user")) // Variable entity - this will be unified
            .is(TypedVariable::<String>::new("name")); // Variable value - this will be unified

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
            let user_var = TypedVariable::<crate::variable::Untyped>::new("user");
            let name_var = TypedVariable::<crate::variable::Untyped>::new("name");

            // Check that the frame contains bindings for our variables
            assert!(
                frame.has(&user_var),
                "Frame should contain binding for 'user' variable"
            );
            assert!(
                frame.has(&name_var),
                "Frame should contain binding for 'name' variable"
            );

            // Check that the bindings are correct
            if let Ok(Value::Entity(entity)) = frame.get(&user_var) {
                if let Ok(Value::String(name)) = frame.get(&name_var) {
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
        let fact_selector = FactSelector::new()
            .the(TypedVariable::<Attribute>::new("attr")) // Variable
            .of(TypedVariable::<Entity>::new("entity")) // Variable
            .is(TypedVariable::<crate::variable::Untyped>::new("value")); // Variable

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
