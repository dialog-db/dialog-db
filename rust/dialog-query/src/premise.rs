//! Premise trait for rule conditions
//!
//! This module defines the premise system used in rule conditions. Premises represent
//! patterns that can be matched against facts in the knowledge base during rule evaluation.
//!
//! Note: Premises are only used in rule conditions (the "when" part), not in conclusions.

pub use super::constraint::Constraint;
pub use super::context::new_context;
pub use super::negation::Negation;
use super::proposition::FormulaApplication;
pub use super::proposition::Proposition;
pub use crate::environment::Environment;
pub use crate::error::{AnalyzerError, PlanError, QueryResult};
pub use crate::{EvaluationContext, Source, selection::Answers};
use futures_util::future::Either;
use std::fmt::Display;

/// Represents a premise in a rule - a condition that must be satisfied.
///
/// Premises can be:
/// - **Applications**: Query the knowledge base (facts, concepts, formulas)
/// - **Constraints**: Express relationships between variables (equality, etc.)
/// - **Exclusions**: Negated premises that filter out matches
///
/// TODO: Large enum variant - Constrain (320 bytes) is much larger than other variants.
/// Consider boxing Constraint to reduce memory usage when storing Apply/Exclude variants.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Premise {
    /// A positive premise that queries the knowledge base.
    When(Proposition),
    /// A constraint that relates variables (equality, comparison, etc.).
    Where(Constraint),
    /// A negated premise that excludes matches from the selection.
    Unless(Negation),
}

impl Premise {
    /// Estimate the cost of this premise given the current environment.
    /// Returns None if the premise cannot be executed without more constraints.
    pub fn estimate(&self, env: &crate::Environment) -> Option<usize> {
        match self {
            Premise::When(application) => application.estimate(env),
            Premise::Where(constraint) => constraint.estimate(env),
            Premise::Unless(negation) => negation.estimate(env),
        }
    }

    /// Returns the parameter bindings for this premise
    pub fn parameters(&self) -> crate::Parameters {
        match self {
            Premise::When(application) => application.parameters(),
            Premise::Where(constraint) => constraint.parameters(),
            Premise::Unless(negation) => negation.parameters(),
        }
    }

    /// Returns the schema describing this premise's parameters
    pub fn schema(&self) -> crate::Schema {
        match self {
            Premise::When(application) => application.schema(),
            Premise::Where(constraint) => constraint.schema(),
            Premise::Unless(negation) => negation.schema(),
        }
    }

    /// Analyze this premise in the given environment.
    /// Returns either a viable plan (ready to execute) or a blocked plan (missing requirements).
    pub fn analyze(&self, env: &crate::Environment) -> crate::analyzer::Analysis {
        let mut analysis = crate::analyzer::Analysis::from(self.clone());
        analysis.update(env);
        analysis
    }

    /// Evaluate this premise with the given context
    pub fn evaluate<S: Source, M: Answers>(self, context: EvaluationContext<S, M>) -> impl Answers {
        match self {
            Premise::When(application) => Either::Left(Either::Left(application.evaluate(context))),
            Premise::Where(constraint) => Either::Left(Either::Right(constraint.evaluate(context))),
            Premise::Unless(negation) => Either::Right(negation.evaluate(context)),
        }
    }

    /// Execute this premise against the given store
    pub fn perform<S: Source>(self, store: &S) -> impl Answers {
        let context = new_context(store.clone());
        self.evaluate(context)
    }
}

impl Display for Premise {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Premise::When(application) => Display::fmt(&application, f),
            Premise::Where(constraint) => Display::fmt(&constraint, f),
            Premise::Unless(negation) => Display::fmt(&negation, f),
        }
    }
}

impl From<Constraint> for Premise {
    fn from(constraint: Constraint) -> Self {
        Premise::Where(constraint)
    }
}

impl From<FormulaApplication> for Premise {
    fn from(application: FormulaApplication) -> Self {
        Premise::When(Proposition::Formula(application))
    }
}
