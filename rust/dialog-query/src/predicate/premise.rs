use crate::error::PlanError;
use crate::plan::EvaluationPlan;
use crate::syntax::VariableScope;

/// A premise that can be used in rule conditions
///
/// This trait represents premises that can be planned for evaluation in rule
/// conditions. Premises describe what must be true for a rule to apply. They
/// are NOT used for rule conclusions - only for the conditions that must be
/// satisfied.
pub trait Premise: Clone + std::fmt::Debug {
    /// The type of plan this premise produces when planned
    type Plan: EvaluationPlan;

    /// Create an evaluation plan for this premise
    ///
    /// The plan describes how to evaluate this premise against a knowledge base.
    /// Variable scope tracks which variables are already bound in the current
    /// context.
    fn plan(&self, scope: &VariableScope) -> Result<Self::Plan, PlanError>;
}
