pub use crate::analyzer::{Analysis, AnalyzerError};
pub use crate::application::RuleApplication;
pub use crate::predicate::Concept;
pub use crate::premise::Premise;
pub use crate::{Attribute, Dependencies, Parameters, Requirement, Value};
use std::collections::HashSet;
use std::fmt::Display;

/// Represents a deductive rule that can be applied creating a premise.
#[derive(Debug, Clone, PartialEq)]
pub struct DeductiveRule {
    /// Conclusion that this rule reaches if all premises hold. This is
    /// typically what datalog calls rule head.
    pub conclusion: Concept,
    /// Premises that must hold for rule to reach it's conclusion. Typically
    /// datalog calls these rule body.
    pub premises: Vec<Premise>,
}
impl DeductiveRule {
    /// Returns the names of the parameters for this rule.
    pub fn parameters(&self) -> HashSet<String> {
        let mut params = HashSet::new();
        for (name, _) in self.conclusion.attributes.iter() {
            params.insert(name.clone());
        }
        params.insert("this".to_string());
        params
    }

    /// Analyzes this rule identifying its dependencies and estimated execution
    /// budget. It also verifies that all rule parameters are utilized by the
    /// rule premises and returns an error if any are not.
    pub fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        let conclusion = &self.conclusion;
        // We will collect rule dependencies and compute their cost based on
        // the use in the rule premises.
        let mut dependencies = Dependencies::new();
        // We will collect all internal dependencies which correspond to
        // variable terms that are not shared with outside scope. We do so
        // in order to identify if there are any unresolvable dependencies
        // and in the local rule budget.
        let mut variables = Dependencies::new();

        let mut cost: usize = 0;
        // Analyze each premise and account their dependencies into the rule's
        // dependencies and budget.
        for premise in self.premises.iter() {
            let analysis = premise.analyze()?;
            cost += analysis.cost;

            // Go over every dependency of every premise and estimate their
            // cost for the rule. If dependency is a parameter of the rule
            // updates dependency cost, otherwise it capture in the local
            // variables to reflect in the total cost
            for (name, dependency) in analysis.dependencies.iter() {
                if conclusion.contains(name) {
                    dependencies.merge(name.into(), dependency);
                } else {
                    variables.merge(name.into(), dependency);
                }
            }
        }

        // Now that we have processed all premises we expect all the
        // parameters to be in the dependencies and all should be derived.
        // If some parameter is not in the dependencies that implies that
        // parameter was not used which is an error because it could not be
        // derived by the rule. If some parameter is required dependency that
        // implies that formula makes use of it, but there is no premise that
        // binds it.
        let mut dependencies = Dependencies::new();
        for name in conclusion.parameters() {
            if let Some(dependency) = variables.lookup(name) {
                match dependency {
                    // If rule attribute is a required dependency it implies that
                    // no premise derives it while some formula utilizes it which
                    // is not allowed.
                    Requirement::Required => Err(AnalyzerError::UnboundVariable {
                        rule: self.clone(),
                        variable: name.to_string(),
                    }),
                    // Otherwise add a variable to the dependencies
                    Requirement::Derived(cost) => {
                        dependencies.desire(name.into(), cost.clone());
                        Ok(())
                    }
                }
            }
            // If there is no dependency on the rule parameter it can not be
            // derived which indicates that rule definition is invalid.
            else {
                Err(AnalyzerError::UnusedParameter {
                    rule: self.clone(),
                    parameter: name.to_string(),
                })
            }?;
        }

        // Next we check if there is any required variable if so we
        // raise an error because it can not be derived by this rule.
        variables
            .iter()
            .find(|(_, level)| matches!(level, Requirement::Required))
            .map_or(Ok(()), |(variable, _)| {
                Err(AnalyzerError::RequiredLocalVariable {
                    rule: self.clone(),
                    variable: variable.to_string(),
                })
            })?;

        // If we got this far we know all the dependencies and have an estimate
        // cost of executions.
        Ok(Analysis {
            cost: cost + variables.cost(),
            dependencies,
        })
    }

    /// Creates a rule application by binding the provided terms to this rule's parameters.
    /// Validates that all required parameters are provided and returns an error if the
    /// application would be invalid.
    pub fn apply(&self, terms: Parameters) -> Result<RuleApplication, AnalyzerError> {
        let application = RuleApplication::new(self.clone(), terms);
        application.analyze().and(Ok(application))
    }
}
impl Display for DeductiveRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.conclusion.operator)?;
        for (name, attribute) in self.conclusion.attributes.iter() {
            write!(f, "{}: {},", name, attribute.data_type)?;
        }
        write!(f, "}}")
    }
}
