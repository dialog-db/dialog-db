//! Statement types for rule conditions
//!
//! This module defines statements that can appear in rule conditions (premises).
//! Statements represent concrete patterns that can be matched against facts
//! in the knowledge base during rule evaluation.

use crate::artifact::Value;
use crate::fact_selector::{FactSelector, FactSelectorPlan};
use crate::plan::{EvaluationContext, EvaluationPlan, PlanError, PlanResult};
use crate::premise::Premise;
use crate::query::Store;
use crate::selection::Selection;
use crate::syntax::VariableScope;
use crate::{Attribute, QueryError, Term};
use async_stream::try_stream;
use dialog_common::ConditionalSend;
use futures_core::Stream;
use serde::{Deserialize, Serialize};

/// Statements that can appear in rule conditions (premises)
///
/// This enum represents the different types of statements that can appear in the "when"
/// part of rules. Statements are the building blocks of rule conditions and define
/// what must be true for a rule to fire.
///
/// # Design Philosophy
///
/// The Statement enum follows the familiar-query pattern where different types of
/// logical operations are represented as discrete types that implement common traits.
/// This allows for compositional rule building while maintaining type safety.
///
/// # Variants
///
/// - `Select`: Matches facts against a pattern using FactSelector
/// - `Realize`: (Commented out) Would execute concept realizations through Join operations
///
/// # Usage in Rules
///
/// Statements are typically created using the convenience constructors:
/// - `Statement::select()` - Create from a FactSelector
/// - `Statement::fact()` - Create a fact selector from individual terms
///
/// # JSON Serialization
///
/// Statements serialize to JSON with a `type` field indicating the variant:
/// ```json
/// {
///   "type": "select",
///   "the": "person/name",
///   "of": {"?": {"name": "user"}},
///   "is": "Alice"
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Statement {
    /// Fact selection statement
    ///
    /// Matches facts in the knowledge base against a pattern. This is the most
    /// common type of statement, used for querying and joining facts.
    ///
    /// # JSON Structure
    /// ```json
    /// {
    ///   "type": "select",
    ///   "the": "attribute_name",
    ///   "of": {"?": {"name": "entity_var"}},
    ///   "is": "constant_or_variable"
    /// }
    /// ```
    ///
    /// # Usage Pattern
    ///
    /// Most commonly created using the convenience constructors:
    /// - `Statement::fact_selector(selector)`
    /// - `Statement::fact(the, of, is)`
    #[serde(rename = "select")]
    Select(FactSelector<Value>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Select {
        facts: Vec<FactSelector<Value>>,
    },
    ApplyRule {
        /// Rule identifier (concept name)
        rule: String,
        /// Set of terms applied to the rule
        terms: Vec<(String, Term<Value>)>,
    },
    ApplyFormula {
        /// Fact identifier (concept name)
        formula: String,
        /// Set of terms applied to the formula
        terms: Vec<(String, Term<Value>)>,
    },
}

pub trait Out<T>: Stream<Item = Result<T, QueryError>> + 'static + ConditionalSend {}

impl<T, S> Out<T> for S where S: Stream<Item = Result<T, QueryError>> + 'static + ConditionalSend {}

pub trait ContextExt {
    fn rules(&self, name: &String) -> impl Out<Rule>;
    fn formulas(&self, name: &String) -> impl Out<Formula>;
}

impl<S: Store, M: Selection> ContextExt for EvaluationContext<S, M> {
    fn rules(&self, _name: &String) -> impl Out<Rule> {
        try_stream! {
            yield unimplemented!()
        }
    }

    fn formulas(&self, _name: &String) -> impl Out<Formula> {
        try_stream! {
            yield Formula { name: "example".to_string() }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Formula {
    pub name: String,
}

impl Formula {
    pub fn run(&self) -> impl Selection {
        try_stream! {
            yield unimplemented!()
        }
    }
}

#[derive(Debug, Clone)]
pub struct Rule {
    pub name: String,
    pub head: Vec<(String, Attribute<Value>)>,
    pub body: Vec<Expression>,
}

impl Rule {
    pub fn execute<S: Store, M: Selection>(
        &self,
        _context: EvaluationContext<S, M>,
    ) -> impl Selection {
        try_stream! {
            yield unimplemented!()
        }
    }
}

impl Expression {
    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        let expression = self.clone(); // Move the expression out of self
        let ctx = &context;
        try_stream! {
            match expression {
                Expression::Select { facts } => {
                    // For Select expressions with multiple facts, we should join them
                    // This is simplified - in practice you'd want proper join planning
                    let store = context.store;
                    // let mut current_selection: std::pin::Pin<Box<dyn Selection>> = Box::pin(context.selection);
                    let mut selection: std::pin::Pin<Box<dyn Selection>> = Box::pin(context.selection);

                    for selector in facts {
                        let scope = VariableScope::new();
                        let plan = selector.plan(&scope)?;
                        // current_selection = Box::pin(plan.evaluate(*ctx));

                        let context = EvaluationContext { selection, store: store.clone() };
                        let new_selection = Box::pin(plan.evaluate(context));
                        selection = new_selection;
                    }

                    for await frame in selection {
                        yield frame?;
                    }
                },
                Expression::ApplyRule { rule, terms: _ } => {
                    let rules = context.rules(&rule);
                    let store = context.store.clone();
                    let mut selection: std::pin::Pin<Box<dyn Selection>> = Box::pin(context.selection);

                    for await each in rules {
                        let rule = each?;
                        let rule_context = EvaluationContext {
                            store: store.clone(),
                            selection,
                        };
                        let rule_selection = Box::pin(rule.execute(rule_context));

                        selection = rule_selection;
                    }

                    for await frame in selection {
                        yield frame?;
                    }
                },
                Expression::ApplyFormula { formula, terms: _ } => {
                    let formulas = context.formulas(&formula);
                    for await result in formulas {
                        let formula = result?;
                        let selection = Box::pin(formula.run());
                        for await frame in selection {
                            yield frame?;
                        }
                    }
                },

            }
        }
    }
}

impl Premise for Statement {
    type Plan = StatementPlan;

    fn plan(&self, scope: &VariableScope) -> PlanResult<Self::Plan> {
        match self {
            Statement::Select(selector) => match selector.plan(scope) {
                Ok(selector_plan) => Ok(StatementPlan::Select(selector_plan)),
                Err(plan_error) => Err(PlanError {
                    description: format!("{}", plan_error),
                }),
            },
        }
    }

    fn cells(&self) -> VariableScope {
        match self {
            Statement::Select(selector) => selector.cells(),
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
    Select(FactSelectorPlan<Value>),
}

impl EvaluationPlan for StatementPlan {
    fn cost(&self) -> usize {
        match self {
            StatementPlan::Select(plan) => plan.cost(),
        }
    }

    fn provides(&self) -> VariableScope {
        match self {
            StatementPlan::Select(plan) => plan.provides(),
        }
    }

    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
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
    pub fn select(selector: FactSelector<Value>) -> Self {
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
