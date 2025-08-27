
//! Predicate types for rule-based query patterns
//!
//! This module defines the predicate system used in rules. Predicates represent
//! patterns that can be matched against facts in the knowledge base. This supports
//! the rule-based deduction system described in the design document.

use crate::error::QueryResult;
use crate::fact_selector::FactSelector;
use crate::plan::EvaluationPlan;
use crate::syntax::{Syntax, VariableScope};
use serde::{Deserialize, Serialize};

/// A predicate that can be used in rule conditions and conclusions
///
/// This trait represents patterns that can be planned for execution.
/// Predicates are the building blocks of rules - they describe what must be true
/// for a rule to apply or what will be concluded when a rule fires.
pub trait Predicate: Clone + std::fmt::Debug {
    /// The type of plan this predicate produces when planned
    type Plan: EvaluationPlan;

    /// Create an execution plan for this predicate
    ///
    /// The plan describes how to evaluate this predicate against a knowledge base.
    /// Variable scope tracks which variables are already bound in the current context.
    fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan>;
}

/// Represents different types of predicates that can appear in rules
///
/// This enum provides a unified representation for the various predicate types
/// supported by the rule system, focusing on selector conjuncts for the initial
/// implementation. For simplicity, we focus on Value-typed predicates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PredicateForm {
    /// A fact selector predicate - matches facts by pattern
    ///
    /// This is the most basic predicate type, representing a pattern match
    /// against facts in the knowledge base. For example:
    /// - Match all facts with attribute "person/name"
    /// - Find entities with specific property values
    /// - Bind variables to fact components
    #[serde(rename = "fact_selector")]
    FactSelector(FactSelector<dialog_artifacts::Value>),
}

impl Predicate for PredicateForm {
    type Plan = PredicateFormPlan;

    fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan> {
        match self {
            PredicateForm::FactSelector(selector) => {
                let selector_plan = selector.plan(scope)?;
                Ok(PredicateFormPlan::FactSelector(selector_plan))
            }
        }
    }
}

/// Execution plans for predicate forms
///
/// This mirrors the PredicateForm enum but contains the execution plans
/// rather than the syntax forms.
#[derive(Debug, Clone)]
pub enum PredicateFormPlan {
    /// Plan for executing a fact selector predicate
    FactSelector(<FactSelector<dialog_artifacts::Value> as Syntax>::Plan),
}

impl crate::plan::Plan for PredicateFormPlan {}

impl EvaluationPlan for PredicateFormPlan {
    fn cost(&self) -> f64 {
        match self {
            PredicateFormPlan::FactSelector(plan) => plan.cost(),
        }
    }

    fn evaluate<S, M>(&self, context: crate::plan::EvaluationContext<S, M>) -> impl crate::Selection + '_
    where
        S: dialog_artifacts::ArtifactStore + Clone + Send + 'static,
        M: crate::Selection + 'static,
    {
        match self {
            PredicateFormPlan::FactSelector(plan) => plan.evaluate(context),
        }
    }
}

/// Convenience constructors for common predicate patterns
impl PredicateForm {
    /// Create a fact selector predicate
    pub fn fact_selector(selector: FactSelector<dialog_artifacts::Value>) -> Self {
        PredicateForm::FactSelector(selector)
    }

    /// Create a fact selector from individual terms
    pub fn fact(
        the: Option<crate::term::Term<dialog_artifacts::Attribute>>,
        of: Option<crate::term::Term<dialog_artifacts::Entity>>,
        is: Option<crate::term::Term<dialog_artifacts::Value>>,
    ) -> Self {
        PredicateForm::FactSelector(FactSelector {
            the,
            of,
            is,
            fact: None,
        })
    }
}
