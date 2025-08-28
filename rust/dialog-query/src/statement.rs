//! Statement types for rule conditions
//!
//! This module defines statements that can appear in rule conditions (premises).
//! Statements represent concrete patterns that can be matched against facts
//! in the knowledge base during rule evaluation.

use crate::error::QueryResult;
use crate::fact_selector::FactSelector;
use crate::plan::EvaluationPlan;
use crate::premise::Premise;
use crate::syntax::{Syntax, VariableScope};
use serde::{Deserialize, Serialize};

/// Statements that can appear in rule conditions (premises)
///
/// This enum represents the different types of statements that can appear in the "when"
/// part of rules. Statements are the building blocks of rule conditions and define
/// what must be true for a rule to fire.
///
/// # Design Philosophy
///
/// Statements follow the datalog tradition where rule conditions are built from
/// atomic statements that can be:
/// - Fact selectors (match facts in the knowledge base)
/// - Future: Negations, aggregations, built-in predicates, etc.
///
/// # Usage in Rules
///
/// Statements are primarily created within rule `when()` methods using
/// array literal syntax for clean, readable rule definitions.
///
/// # Current Implementation
///
/// Currently focused on fact selectors for the initial implementation.
/// The design allows for easy extension with additional statement types
/// as needed (negation, aggregation, built-ins, etc.).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Statement {
    /// Fact selector statement - matches facts by pattern
    ///
    /// This is the fundamental building block for rule conditions. A fact selector
    /// defines a pattern that must match against facts stored in the knowledge base.
    ///
    /// # Pattern Matching
    ///
    /// Fact selectors can match on any combination of:
    /// - **Attribute** (the): What property/relationship (e.g., "person/name")
    /// - **Entity** (of): Which entity the fact is about
    /// - **Value** (is): What value the property has
    ///
    /// # Variable Binding
    ///
    /// Use variables to create joins between statements. When multiple statements
    /// use the same variable name (like "person"), they create a join condition
    /// that ensures the statements match the same entity.
    ///
    /// # Usage Pattern
    ///
    /// Most commonly created using the convenience constructors:
    /// - `Statement::fact_selector(selector)`
    /// - `Statement::fact(the, of, is)`
    #[serde(rename = "select")]
    Select(FactSelector<crate::artifact::Value>),
}

impl Premise for Statement {
    type Plan = StatementPlan;

    fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan> {
        match self {
            Statement::Select(selector) => {
                let selector_plan = selector.plan(scope)?;
                Ok(StatementPlan::Select(selector_plan))
            }
        }
    }
}

/// Execution plans for statements
///
/// This mirrors the Statement enum but contains the execution plans
/// rather than the syntax forms.
#[derive(Debug, Clone)]
pub enum StatementPlan {
    /// Plan for executing a fact selector statement
    Select(<FactSelector<crate::artifact::Value> as Syntax>::Plan),
}

impl crate::plan::Plan for StatementPlan {}

impl EvaluationPlan for StatementPlan {
    fn cost(&self) -> f64 {
        match self {
            StatementPlan::Select(plan) => plan.cost(),
        }
    }

    fn evaluate<S, M>(
        &self,
        context: crate::plan::EvaluationContext<S, M>,
    ) -> impl crate::Selection + '_
    where
        S: crate::artifact::ArtifactStore + Clone + Send + 'static,
        M: crate::Selection + 'static,
    {
        match self {
            StatementPlan::Select(plan) => plan.evaluate(context),
        }
    }
}

/// Convenience constructors for creating statements in rules
///
/// These constructors provide ergonomic ways to create statements within
/// rule `when()` methods. They handle the common patterns and reduce boilerplate.
impl Statement {
    /// Create a fact selector statement from a FactSelector
    ///
    /// Use this when you need full control over the selector construction,
    /// or when using the builder pattern from FactSelector.
    ///
    /// # Example
    ///
    /// Create a statement using a FactSelector builder:
    /// `Statement::fact_selector(FactSelector::new().the(attr).of(entity).is(value))`
    pub fn select(selector: FactSelector<crate::artifact::Value>) -> Self {
        Statement::Select(selector)
    }

    /// Create a fact selector from individual terms (the most common pattern)
    ///
    /// This is the most commonly used constructor for creating fact matching
    /// statements. Pass `None` for any component you don't want to constrain.
    ///
    /// # Parameters
    ///
    /// - `the`: The attribute/property to match (e.g., "person/name")
    /// - `of`: The entity the fact is about (often a variable for joins)
    /// - `is`: The value the property should have (constant or variable)
    ///
    /// # Example - Exact Match
    ///
    /// Find facts where person/name = "Alice" on any entity:
    /// `Statement::fact(Some(attr_term), None, Some(value_term))`
    ///
    /// # Example - Variable Join
    ///
    /// Find person/name for entity "person", capture the name as "name":
    /// `Statement::fact(Some(attr_term), Some(Term::var("person")), Some(Term::var("name")))`
    pub fn fact(
        the: Option<crate::term::Term<crate::artifact::Attribute>>,
        of: Option<crate::term::Term<crate::artifact::Entity>>,
        is: Option<crate::term::Term<crate::artifact::Value>>,
    ) -> Self {
        Statement::Select(FactSelector {
            the,
            of,
            is,
            fact: None,
        })
    }
}
