//! Premise trait for rule conditions
//!
//! This module defines the premise system used in rule conditions. Premises represent
//! patterns that can be matched against facts in the knowledge base during rule evaluation.
//!
//! Note: Premises are only used in rule conditions (the "when" part), not in conclusions.

use crate::error::QueryResult;
use crate::plan::EvaluationPlan;
use crate::syntax::VariableScope;

/// A premise that can be used in rule conditions
///
/// This trait represents premises that can be planned for evaluation in rule
/// conditions. Premises describe what must be true for a rule to apply. They
/// are NOT used for rule conclusions - only for the conditions that must be
/// satisfied.
pub trait Premise: Clone + std::fmt::Debug + Sized {
    /// The type of plan this premise produces when planned
    type Plan: EvaluationPlan;

    /// Create an evaluation plan for this premise
    ///
    /// The plan describes how to evaluate this premise against a knowledge base.
    /// Variable scope tracks which variables are already bound in the current
    /// context.
    fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan>;
}
