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

/// Represents different types of statements that can appear in rule conditions
///
/// This enum provides a unified representation for the various statement types
/// supported by the rule system, focusing on selector conjuncts for the initial
/// implementation. For simplicity, we focus on Value-typed statements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Statement {
    /// A fact selector statement - matches facts by pattern
    ///
    /// This is the most basic statement type, representing a pattern match
    /// against facts in the knowledge base. For example:
    /// - Match all facts with attribute "person/name"
    /// - Find entities with specific property values
    /// - Bind variables to fact components
    #[serde(rename = "select")]
    Select(FactSelector<crate::artifact::Value>),
}

impl Premise for Statement {
    type Plan = StatementPlan;

    fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan> {
        match self {
            Statement::Select(selector) => {
                let selector_plan = selector.plan(scope)?;
                Ok(StatementPlan::FactSelector(selector_plan))
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
    FactSelector(<FactSelector<dialog_artifacts::Value> as Syntax>::Plan),
}

impl crate::plan::Plan for StatementPlan {}

impl EvaluationPlan for StatementPlan {
    fn cost(&self) -> f64 {
        match self {
            StatementPlan::FactSelector(plan) => plan.cost(),
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
            StatementPlan::FactSelector(plan) => plan.evaluate(context),
        }
    }
}

/// Convenience constructors for common statement patterns
impl Statement {
    /// Create a fact selector statement
    pub fn fact_selector(selector: FactSelector<crate::artifact::Value>) -> Self {
        Statement::Select(selector)
    }

    /// Create a fact selector from individual terms
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
