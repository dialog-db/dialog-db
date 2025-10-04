//! Premise trait for rule conditions
//!
//! This module defines the premise system used in rule conditions. Premises represent
//! patterns that can be matched against facts in the knowledge base during rule evaluation.
//!
//! Note: Premises are only used in rule conditions (the "when" part), not in conclusions.

use async_stream::try_stream;

pub use super::application::Application;
use super::application::{FactApplication, FormulaApplication};
pub use super::context::{new_context, EvaluationPlan};
pub use super::negation::Negation;
pub use crate::error::{AnalyzerError, PlanError, QueryResult};
pub use crate::syntax::VariableScope;
pub use crate::Dependencies;
pub use crate::{EvaluationContext, Selection, Source};
use std::fmt::Display;

/// Represents a premise in a rule - a condition that must be satisfied.
/// Can be either a positive application or a negated exclusion.
#[derive(Debug, Clone, PartialEq)]
pub enum Premise {
    /// A positive premise that produces matches.
    Apply(Application),
    /// A negated premise that excludes matches from the selection.
    Exclude(Negation),
}

impl Premise {
    /// Estimate the cost of this premise given the current environment.
    /// Returns None if the premise cannot be executed without more constraints.
    pub fn estimate(&self, env: &crate::VariableScope) -> Option<usize> {
        match self {
            Premise::Apply(application) => application.estimate(env),
            Premise::Exclude(negation) => negation.estimate(env),
        }
    }

    pub fn parameters(&self) -> crate::Parameters {
        match self {
            Premise::Apply(application) => application.parameters(),
            Premise::Exclude(negation) => negation.parameters(),
        }
    }

    pub fn schema(&self) -> crate::Schema {
        match self {
            Premise::Apply(application) => application.schema(),
            Premise::Exclude(negation) => negation.schema(),
        }
    }

    /// Analyze this premise in the given environment.
    /// Returns either a viable plan (ready to execute) or a blocked plan (missing requirements).
    pub fn analyze(&self, env: &crate::VariableScope) -> crate::analyzer::Analysis {
        let mut analysis = crate::analyzer::Analysis::from(self.clone());
        analysis.update(env);
        analysis
    }

    /// Evaluate this premise with the given context
    pub fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl crate::Selection {
        let source = self.clone();
        try_stream! {
            match source {
                Premise::Apply(application) => {
                    for await each in application.evaluate(context) {
                        yield each?;
                    }
                },
                Premise::Exclude(negation) => {
                    for await each in negation.evaluate(context) {
                        yield each?;
                    }
                },
            }
        }
    }

    pub fn query<S: Source>(&self, store: &S) -> QueryResult<impl Selection> {
        let store = store.clone();
        let context = new_context(store);
        let selection = self.evaluate(context);
        Ok(selection)
    }
}

impl Display for Premise {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Premise::Apply(application) => Display::fmt(&application, f),
            Premise::Exclude(negation) => Display::fmt(&negation, f),
        }
    }
}

impl From<FormulaApplication> for Premise {
    fn from(application: FormulaApplication) -> Self {
        Premise::Apply(Application::Formula(application))
    }
}

impl From<FactApplication> for Premise {
    fn from(selector: FactApplication) -> Self {
        Premise::Apply(Application::Fact(selector))
    }
}

impl From<&FactApplication> for Premise {
    fn from(selector: &FactApplication) -> Self {
        Premise::Apply(Application::Fact(selector.clone()))
    }
}
